#!/usr/bin/env python3

import argparse
import io
import json

import logging

import sys
import copy

from datetime import datetime
from decimal import Decimal

from typing import Dict
from dateutil import parser
from dateutil.parser import ParserError

from jsonschema import Draft7Validator, FormatChecker
from singer import get_logger

from target_snowflake.db_sync import DbSync
import target_snowflake.utils as utils

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

from target_snowflake.handlers import record as record_handler


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


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def persist_lines(config, lines, table_cache=None) -> None:
    # global variables
    state = None
    flushed_state = None
    schemas = {}
    validators = {}

    batch_size_rows = config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS)
    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)
    # parallelism of 0 means auto parallelism
    # - use as many threads as possible, up to max_parallelism
    if parallelism == 0:
        parallelism = max_parallelism

    # RECORD variables
    records_to_load = {}
    row_count = {}
    total_row_count = {}
    stream_to_sync = {}
    buckets_to_flush = []

    # BATCH variables
    batches_to_flush = []

    # SCHEMA variables
    key_properties = {}

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

        if t == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']

            # Initially set flushed state
            if not flushed_state:
                flushed_state = copy.deepcopy(state)

        elif t == 'SCHEMA':

            utils.check_message_has_stream(line, o)

            stream = o['stream']
            new_schema = utils.float_to_decimal(o['schema'])

            # Update and flush only if the the schema is new or different than
            # the previously used version of the schema
            if stream not in schemas or schemas[stream] != new_schema:

                # Save old schema and validator in case we need them to flush records
                old_schema = schemas.get(stream)
                old_validator = validators.get(stream)
                # Update schema and validator
                schemas[stream] = new_schema
                validators[stream] = Draft7Validator(schemas[stream], format_checker=FormatChecker())

                # flush records from previous stream SCHEMA
                # if same stream has been encountered again, it means the schema might have been altered
                # so previous records need to be flushed
                if row_count.get(stream, 0) > 0:
                    flushed_state = record_handler.flush_stream_buckets(
                        config=config, schema=old_schema, validator=old_validator,
                        buckets=records_to_load, streams_to_flush=[stream],
                        parallelism=parallelism, row_count=row_count, state=state,
                        flushed_state=flushed_state
                    )

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
                    stream_to_sync[stream] = DbSync(
                        config, utils.add_metadata_columns_to_schema(o),
                        table_cache
                    )
                else:
                    stream_to_sync[stream] = DbSync(config, o, table_cache)

                stream_to_sync[stream].create_schema_if_not_exists()
                stream_to_sync[stream].sync_table()

                row_count[stream] = 0
                total_row_count[stream] = 0

        elif t == 'RECORD':

            utils.check_message_has_stream(line, o)
            utils.check_message_has_schema(line, o, schemas)

            record_message = o
            record = record_message['record']
            stream = record_message['stream']

            # Get primary key for this record
            primary_key_string = stream_to_sync[stream].record_primary_key_string(record)
            if not primary_key_string:
                primary_key_string = 'RID-{}'.format(total_row_count[stream])

            # If no bucket for records in this stream exists, create one.
            if stream not in records_to_load:
                records_to_load[stream] = {}

            # increment row count only when a new PK is encountered in the current batch
            if primary_key_string not in records_to_load[stream]:
                row_count[stream] += 1
                total_row_count[stream] += 1

            # add record_message to relevant bucket (by PK)
            records_to_load[stream][primary_key_string] = record_message

            # track full buckets
            if row_count[stream] >= batch_size_rows:
                buckets_to_flush.append(stream)

            # Do we have enough full buckets to begin flushing them?
            if (
                len(buckets_to_flush) == parallelism
                or (len(buckets_to_flush) > 0 and config.get('flush_all_streams'))
            ):
                if config.get('flush_all_streams'):
                    streams = records_to_load.keys()
                else:  # only flush full buckets
                    streams = buckets_to_flush

                # Flush and return a new state dict with new positions only for the flushed streams
                flushed_state = record_handler.flush_stream_buckets(
                    config=config, schemas=schemas, validators=validators,
                    buckets=records_to_load, streams_to_flush=streams,
                    parallelism=parallelism, row_count=row_count,
                    stream_to_sync=stream_to_sync, state=state,
                    flushed_state=flushed_state
                )
                # emit last encountered state
                emit_state(copy.deepcopy(flushed_state))

        elif t == 'BATCH':

            utils.check_message_has_stream(line, o)
            utils.check_message_has_schema(line, o, schemas)

            batch_message = o
            # batches are already bucketed by stream, so we can
            # just track batches
            batches_to_flush.append(batch_message)

            # Do we have enough batches to begin flushing them?
            if (
                len(batches_to_flush) == parallelism
                or (len(batches_to_flush) > 0 and config.get('flush_all_streams'))
            ):
                # flush batches
                batches_to_flush, flushed_state = record_handler.flush_batches(
                    config=config, schemas=schemas, validators=validators,
                    batches=batches_to_flush, parallelism=parallelism,
                    stream_to_sync=stream_to_sync, state=state,
                    flushed_state=flushed_state
                )
                # emit last encountered state
                emit_state(copy.deepcopy(flushed_state))

        elif t == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION message')

        else:
            raise Exception(
                f"Unknown message type {o['type']} in message {o}"
            )

    # if some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        streams = list(records_to_load.keys())
        flushed_state = record_handler.flush_stream_buckets(
            config=config, schemas=schemas, validators=validators,
            buckets=records_to_load, streams_to_flush=streams,
            parallelism=parallelism, row_count=row_count,
            stream_to_sync=stream_to_sync, state=state,
            flushed_state=flushed_state
        )
        # emit latest state
        emit_state(copy.deepcopy(flushed_state))

    # if there are any remaining batches to flush, flush them
    if len(batches_to_flush) > 0:
        batches_to_flush, flushed_state = record_handler.flush_batches(
            config=config, schemas=schemas, validators=validators,
            batches=batches_to_flush, parallelism=parallelism,
            stream_to_sync=stream_to_sync, state=state,
            flushed_state=flushed_state
        )
        # emit latest state
        emit_state(copy.deepcopy(flushed_state))


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
