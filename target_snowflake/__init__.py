#!/usr/bin/env python3

import argparse
import io
import json
import logging
import os
import sys
import copy

from typing import Dict, List
from joblib import Parallel, delayed, parallel_backend
from jsonschema import Draft7Validator, FormatChecker
from singer import get_logger

import target_snowflake.file_formats.csv as csv
import target_snowflake.file_formats.parquet as parquet
import target_snowflake.stream_utils as stream_utils

from target_snowflake.db_sync import DbSync
from target_snowflake.file_format import FileFormatTypes
from target_snowflake.exceptions import (
    RecordValidationException,
    UnexpectedValueTypeException,
    InvalidValidationOperationException
)

LOGGER = get_logger('target_snowflake')

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

DEFAULT_BATCH_SIZE_ROWS = 100000
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {'type': ['null', 'string'],
                                                                            'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {'type': ['null', 'string']}

    return extended_schema_message


def emit_state(state):
    """Print state to stdout"""
    if state is not None:
        line = json.dumps(state)
        LOGGER.info('Emitting state %s', line)
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def load_table_cache(config):
    """Load table cache from snowflake metadata"""
    table_cache = []
    if not ('disable_table_cache' in config and config['disable_table_cache']):
        LOGGER.info('Getting catalog objects from table cache...')

        db = DbSync(config) # pylint: disable=invalid-name
        table_cache = db.get_table_columns(
            table_schemas=stream_utils.get_schema_names_from_config(config))

    return table_cache


# pylint: disable=too-many-locals,too-many-branches,too-many-statements,invalid-name
def persist_lines(config, lines, table_cache=None) -> None:
    """Main loop to read and consume singer messages from stdin"""
    state = None
    flushed_state = None
    schemas = {}
    key_properties = {}
    validators = {}
    records_to_load = {}
    row_count = {}
    stream_to_sync = {}
    total_row_count = {}
    batch_size_rows = config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS)

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error('Unable to parse:\n%s', line)
            raise

        if 'type' not in o:
            raise Exception(f"Line is missing required key 'type': {line}")

        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")
            if o['stream'] not in schemas:
                raise Exception(
                    f"A record for stream {o['stream']} was encountered before a corresponding schema")

            # Get schema for this record's stream
            stream = o['stream']

            stream_utils.adjust_timestamps_in_record(o['record'], schemas[stream])

            # Validate record
            if config.get('validate_records'):
                try:
                    validators[stream].validate(stream_utils.float_to_decimal(o['record']))
                except Exception as ex:
                    if type(ex).__name__ == "InvalidOperation":
                        raise InvalidValidationOperationException(
                            f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                            "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                            "or more) Try removing 'multipleOf' methods from JSON schema.") from ex
                    raise RecordValidationException(f"Record does not pass schema validation. RECORD: {o['record']}") \
                        from ex

            primary_key_string = stream_to_sync[stream].record_primary_key_string(o['record'])
            if not primary_key_string:
                primary_key_string = 'RID-{}'.format(total_row_count[stream])

            if stream not in records_to_load:
                records_to_load[stream] = {}

            # increment row count only when a new PK is encountered in the current batch
            if primary_key_string not in records_to_load[stream]:
                row_count[stream] += 1
                total_row_count[stream] += 1

            # append record
            if config.get('add_metadata_columns') or config.get('hard_delete'):
                records_to_load[stream][primary_key_string] = stream_utils.add_metadata_values_to_record(o)
            else:
                records_to_load[stream][primary_key_string] = o['record']

            if row_count[stream] >= batch_size_rows:
                # flush all streams, delete records if needed, reset counts and then emit current state
                if config.get('flush_all_streams'):
                    filter_streams = None
                else:
                    filter_streams = [stream]

                # Flush and return a new state dict with new positions only for the flushed streams
                flushed_state = flush_streams(
                    records_to_load,
                    row_count,
                    stream_to_sync,
                    config,
                    state,
                    flushed_state,
                    filter_streams=filter_streams)

                # emit last encountered state
                emit_state(copy.deepcopy(flushed_state))

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")

            stream = o['stream']
            new_schema = stream_utils.float_to_decimal(o['schema'])

            # Update and flush only if the the schema is new or different than
            # the previously used version of the schema
            if stream not in schemas or schemas[stream] != new_schema:

                schemas[stream] = new_schema
                validators[stream] = Draft7Validator(schemas[stream], format_checker=FormatChecker())

                # flush records from previous stream SCHEMA
                # if same stream has been encountered again, it means the schema might have been altered
                # so previous records need to be flushed
                if row_count.get(stream, 0) > 0:
                    flushed_state = flush_streams(records_to_load,
                                                  row_count,
                                                  stream_to_sync,
                                                  config,
                                                  state,
                                                  flushed_state)

                    # emit latest encountered state
                    emit_state(flushed_state)

                # key_properties key must be available in the SCHEMA message.
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")

                # Log based and Incremental replications on tables with no Primary Key
                # cause duplicates when merging UPDATE events.
                # Stop loading data by default if no Primary Key.
                #
                # If you want to load tables with no Primary Key:
                #  1) Set ` 'primary_key_required': false ` in the target-snowflake config.json
                #  or
                #  2) Use fastsync [postgres-to-snowflake, mysql-to-snowflake, etc.]
                if config.get('primary_key_required', True) and len(o['key_properties']) == 0:
                    LOGGER.critical('Primary key is set to mandatory but not defined in the [%s] stream', stream)
                    raise Exception("key_properties field is required")

                key_properties[stream] = o['key_properties']

                if config.get('add_metadata_columns') or config.get('hard_delete'):
                    stream_to_sync[stream] = DbSync(config, add_metadata_columns_to_schema(o), table_cache)
                else:
                    stream_to_sync[stream] = DbSync(config, o, table_cache)

                stream_to_sync[stream].create_schema_if_not_exists()
                stream_to_sync[stream].sync_table()

                row_count[stream] = 0
                total_row_count[stream] = 0

        elif t == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION message')

        elif t == 'STATE':
            LOGGER.debug('Setting state to %s', o['value'])
            state = o['value']

            # Initially set flushed state
            if not flushed_state:
                flushed_state = copy.deepcopy(state)

        else:
            raise Exception(f"Unknown message type {o['type']} in message {o}")

    # if some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        flushed_state = flush_streams(records_to_load, row_count, stream_to_sync, config, state, flushed_state)

    # emit latest state
    emit_state(copy.deepcopy(flushed_state))


# pylint: disable=too-many-arguments
def flush_streams(
        streams,
        row_count,
        stream_to_sync,
        config,
        state,
        flushed_state,
        filter_streams=None):
    """
    Flushes all buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: Snowflake db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :return: State dict with flushed positions
    """
    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)

    # Parallelism 0 means auto parallelism:
    #
    # Auto parallelism trying to flush streams efficiently with auto defined number
    # of threads where the number of threads is the number of streams that need to
    # be loaded but it's not greater than the value of max_parallelism
    if parallelism == 0:
        n_streams_to_flush = len(streams.keys())
        if n_streams_to_flush > max_parallelism:
            parallelism = max_parallelism
        else:
            parallelism = n_streams_to_flush

    # Select the required streams to flush
    if filter_streams:
        streams_to_flush = filter_streams
    else:
        streams_to_flush = streams.keys()

    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=parallelism):
        Parallel()(delayed(load_stream_batch)(
            stream=stream,
            records=streams[stream],
            row_count=row_count,
            db_sync=stream_to_sync[stream],
            no_compression=config.get('no_compression'),
            delete_rows=config.get('hard_delete'),
            temp_dir=config.get('temp_dir')
        ) for stream in streams_to_flush)

    # reset flushed stream records to empty to avoid flushing same records
    for stream in streams_to_flush:
        streams[stream] = {}

        # Update flushed streams
        if filter_streams:
            # update flushed_state position if we have state information for the stream
            if state is not None and stream in state.get('bookmarks', {}):
                # Create bookmark key if not exists
                if 'bookmarks' not in flushed_state:
                    flushed_state['bookmarks'] = {}
                # Copy the stream bookmark from the latest state
                flushed_state['bookmarks'][stream] = copy.deepcopy(state['bookmarks'][stream])

        # If we flush every bucket use the latest state
        else:
            flushed_state = copy.deepcopy(state)

    # Return with state message with flushed positions
    return flushed_state


def load_stream_batch(stream, records, row_count, db_sync, no_compression=False, delete_rows=False,
                      temp_dir=None):
    """Load one batch of the stream into target table"""
    # Load into snowflake
    if row_count[stream] > 0:
        flush_records(stream, records, db_sync, temp_dir, no_compression)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)

        # reset row count for the current stream
        row_count[stream] = 0


def flush_records(stream: str,
                  records: List[Dict],
                  db_sync: DbSync,
                  temp_dir: str = None,
                  no_compression: bool = False) -> None:
    """
    Takes a list of record messages and loads it into the snowflake target table

    Args:
        stream: Name of the stream
        records: List of dictionary, that represents multiple csv lines. Dict key is the column name, value is the
                 column value
        row_count:
        db_sync: A DbSync object
        temp_dir: Directory where intermediate temporary files will be created. (Default: OS specificy temp directory)
        no_compression: Disable to use compressed files. (Default: False)

    Returns:
        None
    """
    # Generate file on disk in the required format
    filepath = db_sync.file_format.records_to_file(records,
                                                   db_sync.flatten_schema,
                                                   compression=not no_compression,
                                                   dest_dir=temp_dir,
                                                   data_flattening_max_level=db_sync.data_flattening_max_level)

    # Get file stats
    row_count = len(records)
    size_bytes = os.path.getsize(filepath)

    # Upload to s3 and load into Snowflake
    s3_key = db_sync.put_to_stage(filepath, stream, row_count, temp_dir=temp_dir)
    db_sync.load_file(s3_key, row_count, size_bytes)

    # Delete file from local disk and from s3
    os.remove(filepath)
    db_sync.delete_from_stage(stream, s3_key)


def main():
    """Main function"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='Config file')
    args = arg_parser.parse_args()

    if args.config:
        with open(args.config) as config_input:
            config = json.load(config_input)
    else:
        config = {}

    # Init columns cache
    table_cache = load_table_cache(config)

    # Consume singer messages
    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_lines(config, singer_messages, table_cache)

    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
