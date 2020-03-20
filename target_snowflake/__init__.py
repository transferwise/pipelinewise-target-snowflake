#!/usr/bin/env python3

import argparse
import io
import json
import logging
import os
import sys
import copy

from datetime import datetime
from decimal import Decimal
from tempfile import mkstemp
from typing import Dict
from dateutil import parser
from dateutil.parser import ParserError
from joblib import Parallel, delayed, parallel_backend
from jsonschema import Draft4Validator, FormatChecker
from singer import get_logger

from target_snowflake.db_sync import DbSync

LOGGER = get_logger('target_snowflake')

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

DEFAULT_BATCH_SIZE_ROWS = 100000
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel

# max timestamp/datetime supported in SF, used to reset all invalid dates that are beyond this value
MAX_TIMESTAMP = '9999-12-31 23:59:59.999999'

# max time supported in SF, used to reset all invalid times that are beyond this value
MAX_TIME = '23:59:59.999999'


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""
    pass


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""
    pass


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


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


def add_metadata_values_to_record(record_message, stream_to_sync):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message['record']
    extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    extended_record['_sdc_deleted_at'] = record_message.get('record', {}).get('_sdc_deleted_at')

    return extended_record


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.info('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def get_schema_names_from_config(config):
    default_target_schema = config.get('default_target_schema')
    schema_mapping = config.get('schema_mapping', {})
    schema_names = []

    if default_target_schema:
        schema_names.append(default_target_schema)

    if schema_mapping:
        for source_schema, target in schema_mapping.items():
            schema_names.append(target.get('target_schema'))

    return schema_names


def load_table_cache(config):
    table_cache = []
    if not ('disable_table_cache' in config and config['disable_table_cache']):
        LOGGER.info("Getting catalog objects from table cache...")

        db = DbSync(config)
        table_cache = db.get_table_columns(
            table_schemas=get_schema_names_from_config(config))

    return table_cache


def adjust_timestamps_in_record(record: Dict, schema: Dict) -> None:
    """
    Goes through every field that is of type date/datetime/time and if its value is out of range,
    resets it to MAX value accordingly
    Args:
        record: record containing properties and values
        schema: json schema that has types of each property
    """
    # creating this internal function to avoid duplicating code and too many nested blocks.
    def reset_new_value(record: Dict, key: str, format: str):
        try:
            parser.parse(record[key])
        except ParserError:
            record[key] = MAX_TIMESTAMP if format != 'time' else MAX_TIME

    for key, value in record.items():
        if value is not None and key in schema['properties']:
            if 'anyOf' in schema['properties'][key]:
                for type_dict in schema['properties'][key]['anyOf']:
                    if 'string' in type_dict['type'] and type_dict.get('format', None) in {'date-time', 'time', 'date'}:
                        reset_new_value(record, key, type_dict['format'])
                        break
            else:
                if 'string' in schema['properties'][key]['type'] and \
                        schema['properties'][key].get('format', None) in {'date-time', 'time', 'date'}:
                    reset_new_value(record, key, schema['properties'][key]['format'])


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def persist_lines(config, lines, table_cache=None) -> None:
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
            LOGGER.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))

        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            adjust_timestamps_in_record(o['record'], schemas[stream])

            # Validate record
            if config.get('validate_records'):
                try:
                    validators[stream].validate(float_to_decimal(o['record']))
                except Exception as ex:
                    if type(ex).__name__ == "InvalidOperation":
                        raise InvalidValidationOperationException(
                            f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                            "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                            "or more) Try removing 'multipleOf' methods from JSON schema.")
                    raise RecordValidationException(f"Record does not pass schema validation. RECORD: {o['record']}")

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
                records_to_load[stream][primary_key_string] = add_metadata_values_to_record(o, stream_to_sync[stream])
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
                raise Exception("Line is missing required key 'stream': {}".format(line))

            stream = o['stream']

            schemas[stream] = float_to_decimal(o['schema'])
            validators[stream] = Draft4Validator(schemas[stream], format_checker=FormatChecker())

            # flush records from previous stream SCHEMA
            # if same stream has been encountered again, it means the schema might have been altered
            # so previous records need to be flushed
            if row_count.get(stream, 0) > 0:
                flushed_state = flush_streams(records_to_load, row_count, stream_to_sync, config, state, flushed_state)

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
                LOGGER.critical("Primary key is set to mandatory but not defined in the [{}] stream".format(stream))
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
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']

            # Initially set flushed state
            if not flushed_state:
                flushed_state = copy.deepcopy(state)

        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

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
            records_to_load=streams[stream],
            row_count=row_count,
            db_sync=stream_to_sync[stream],
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


def load_stream_batch(stream, records_to_load, row_count, db_sync, delete_rows=False, temp_dir=None):
    # Load into snowflake
    if row_count[stream] > 0:
        flush_records(stream, records_to_load, row_count[stream], db_sync, temp_dir)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)

        # reset row count for the current stream
        row_count[stream] = 0


def flush_records(stream, records_to_load, row_count, db_sync, temp_dir=None):
    if temp_dir:
        os.makedirs(temp_dir, exist_ok=True)
    csv_fd, csv_file = mkstemp(suffix='.csv', prefix='records_', dir=temp_dir)

    with open(csv_fd, 'w+b') as f:
        for record in records_to_load.values():
            csv_line = db_sync.record_to_csv_line(record)
            f.write(bytes(csv_line + '\n', 'UTF-8'))

    size_bytes = os.path.getsize(csv_file)
    s3_key = db_sync.put_to_stage(csv_file, stream, row_count, temp_dir=temp_dir)
    db_sync.load_csv(s3_key, row_count, size_bytes)

    os.remove(csv_file)
    db_sync.delete_from_stage(s3_key)


def main():
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
