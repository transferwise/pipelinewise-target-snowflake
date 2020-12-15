import os
import json
import copy
import gzip
from tempfile import mkstemp
from target_snowflake import LOGGER
from joblib import Parallel, delayed, parallel_backend
from target_snowflake.handlers import (
    adjust_timestamps_in_record, validate_record,
    add_metadata_values_to_record,
    add_metadata_columns_to_schema
)


# pylint: disable=too-many-arguments
def flush_stream_buckets(
    config, schemas, validators, buckets, streams_to_flush,
    parallelism, row_count, stream_to_sync, state, flushed_state
):
    """
    Flushes buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: Snowflake db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :return: State dict with flushed positions
    """
    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=parallelism):
        Parallel()(
            delayed(load_records)(
                config=config,
                schema=schemas[stream],
                validator=validators[stream],
                stream=stream,
                records=buckets[stream].values(),
                row_count=row_count[stream],
                db_sync=stream_to_sync[stream]
            ) for stream in streams_to_flush
        )

    # reset flushed stream records to empty to avoid flushing same records
    for stream in streams_to_flush:
        # clear processed records
        buckets[stream] = {}
        row_count[stream] = 0
        # update flushed_state position if we have state information for the stream
        if state is not None and stream in state.get('bookmarks', {}):
            # Create bookmark key if not exists
            if 'bookmarks' not in flushed_state:
                flushed_state['bookmarks'] = {}
            # Copy the stream bookmark from the latest state
            flushed_state['bookmarks'][stream] = copy.deepcopy(state['bookmarks'][stream])

    # Return with state message with flushed positions
    return flushed_state


# pylint: disable=too-many-arguments
def flush_batches(
    config, schemas, validators, batches,
    parallelism, stream_to_sync, state, flushed_state
):
    """
    Flushes a list of batches.
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: Snowflake db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :return: State dict with flushed positions
    """
    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=parallelism):
        Parallel()(
            delayed(load_batch)(
                config=config,
                schema=schemas[batch.get('stream')],
                validator=validators[batch.get('stream')],
                stream=batch.get('stream'),
                batch=batch,
                row_count=batch.get('batch_size'),
                db_sync=stream_to_sync[batch.get('stream')]
            ) for batch in batches
        )

    # reset flushed stream records to empty to avoid flushing same records
    for batch in batches:
        stream = batch.get('stream')
        # update flushed_state position if we have state information for the stream
        if (state is not None) and (stream in state.get('bookmarks', {})):
            # Create bookmark key if not exists
            if 'bookmarks' not in flushed_state:
                flushed_state['bookmarks'] = {}
            # Copy the stream bookmark from the latest state
            flushed_state['bookmarks'][stream] = copy.deepcopy(state['bookmarks'][stream])

    # clear processed batches
    batches = []
    # Return with state message with flushed positions
    return batches, flushed_state


def transform_records(config, schema, record_messages, validator):
    """ Generator to transform a collection of record messages from the same stream.
    """
    for record_message in record_messages:
        # Validate record
        # TODO: should we validate before or after transforming?
        if config.get('validate_records'):
            validate_record(record_message, validator)

        # Add metadata values to record
        if config.get('add_metadata_columns') or config.get('hard_delete'):
            record_message = add_metadata_values_to_record(record_message)

        # Truncate timestamps that are unsupported by Snowflake
        record_message = adjust_timestamps_in_record(
            record_message=record_message, schema=schema
        )
        yield record_message


def load_line(line):
    try:
        return json.loads(line)
    except json.decoder.JSONDecodeError:
        LOGGER.error("Unable to parse:\n{}".format(line))
        raise


def read_lines(file_handler):
    """ Generator for reading large files containing singer records.
    Returns blocks of records at a time.
    """
    for line in file_handler:
        yield load_line(line)


def load_batch(
    config, schema, validator, stream,
    batch, row_count, db_sync
):
    filepath = batch.get('filepath')
    assert (batch.get('format') == 'jsonl') and (batch.get('compression') is None), \
        "This tap only supports uncompressed jsonl files (for now)."
    with open(filepath, 'r') as batch_file:
        records = read_lines(batch_file)
        load_records(
            config, schema, validator, stream,
            records, row_count, db_sync
        )


def load_records(
    config, schema, validator, stream,
    records, row_count, db_sync
):
    # Get config values
    no_compression = config.get('no_compression', False)
    delete_rows = config.get('hard_delete', False)
    temp_dir = config.get('temp_dir')

    # Load into snowflake
    if records:

        # make tmp directory and file
        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)
        csv_fd, csv_file = mkstemp(suffix='.csv', prefix='records_', dir=temp_dir)

        # get record csv transformer
        record_to_csv_line_transformer = db_sync.record_to_csv_line

        # get record transformation generator
        transformed_records = transform_records(config, schema, records, validator)

        # Using gzip or plain file object
        if no_compression:
            with open(csv_fd, 'wb') as outfile:
                write_records_to_file(
                    records=transformed_records,
                    outfile=outfile,
                    record_to_csv_line_transformer=record_to_csv_line_transformer
                )
        else:
            with open(csv_fd, 'wb') as outfile:
                with gzip.GzipFile(filename=csv_file, mode='wb', fileobj=outfile) as gzipfile:
                    transformed_records = transform_records(config, schema, records, validator)
                    write_records_to_file(
                        records=transformed_records,
                        outfile=gzipfile,
                        record_to_csv_line_transformer=record_to_csv_line_transformer
                    )

        # upload csv to s3
        size_bytes = os.path.getsize(csv_file)
        s3_key = db_sync.put_to_stage(csv_file, stream, row_count, temp_dir=temp_dir)

        # load csv into Snowflake
        db_sync.load_csv(s3_key, row_count, size_bytes)

        # clean up local and s3 staging files
        os.remove(csv_file)
        db_sync.delete_from_stage(stream, s3_key)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)

        # reset row count for the current stream
        row_count = 0


def write_records_to_file(records, outfile, record_to_csv_line_transformer):
    for record in records:
        csv_line = record_to_csv_line_transformer(record)
        outfile.write(bytes(csv_line + '\n', 'UTF-8'))
