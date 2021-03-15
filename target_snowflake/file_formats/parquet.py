"""Parquet file format functions"""
import os
import pandas

from typing import Dict, List
from tempfile import mkstemp

import target_snowflake.flattening as flattening


def create_copy_sql(table_name: str,
                    stage_name: str,
                    s3_key: str,
                    file_format_name: str,
                    columns: List):
    """Generate a Parquet compatible snowflake COPY INTO command"""
    return "COPY INTO {} ({}) " \
           "FROM (SELECT {} FROM '@{}/{}') " \
           "FILE_FORMAT = (format_name='{}')".format(
        table_name,
        ', '.join([c['name'] for c in columns]),
        ', '.join(["{}($1:{}) {}".format(c['trans'],
                                         c['json_element_name'],
                                         c['name'])
                   for i, c in enumerate(columns)]),
        stage_name,
        s3_key,
        file_format_name)


def create_merge_sql(table_name: str,
                     stage_name: str,
                     s3_key: str,
                     file_format_name: str,
                     columns: List,
                     pk_merge_condition: str) -> str:
    """Generate a Parquet compatible snowflake MERGE INTO command"""
    return "MERGE INTO {} t USING (" \
           "SELECT {} " \
           "FROM '@{}/{}' " \
           "(FILE_FORMAT => '{}')) s " \
           "ON {} " \
           "WHEN MATCHED THEN UPDATE SET {} " \
           "WHEN NOT MATCHED THEN " \
           "INSERT ({}) " \
           "VALUES ({})".format(
        table_name,
        ', '.join(["{}($1:{}) {}".format(c['trans'],
                                         c['json_element_name'],
                                         c['name'])
                   for i, c in enumerate(columns)]),
        stage_name,
        s3_key,
        file_format_name,
        pk_merge_condition,
        ', '.join(['{0}=s.{0}'.format(c['name']) for c in columns]),
        ', '.join([c['name'] for c in columns]),
        ', '.join(['s.{}'.format(c['name']) for c in columns]))


def records_to_dataframe(records: Dict,
                         schema: Dict,
                         data_flattening_max_level: int = 0) -> pandas.DataFrame:
    """
    Transforms a list of record messages into pandas dataframe with flattened records

    Args:
        records: List of dictionaries that represents a batch of singer record messages
        data_flattening_max_level: Max level of auto flattening if a record message has nested objects. (Default: 0)

    Returns:
        Pandas dataframe
    """
    flattened_records = []

    for record in records.values():
        flatten_record = flattening.flatten_record(record, schema, max_level=data_flattening_max_level)
        flattened_records.append(flatten_record)

    return pandas.DataFrame(data=flattened_records)


def records_to_file(records: Dict,
                    schema: Dict,
                    suffix: str = 'parquet',
                    prefix: str = 'batch_',
                    compression: bool = False,
                    dest_dir: str = None,
                    data_flattening_max_level: int = 0):
    """
    Transforms a list of dictionaries with records messages to a parquet file

    Args:
        records: List of dictionaries that represents a batch of singer record messages
        schema: JSONSchema of the records
        suffix: Generated filename suffix
        prefix: Generated filename prefix
        compression: Gzip compression enabled or not (Default: False)
        dest_dir: Directory where the parquet file will be generated. (Default: OS specificy temp directory)
        data_flattening_max_level: Max level of auto flattening if a record message has nested objects. (Default: 0)

    Returns:
        Absolute path of the generated parquet file
    """
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)

    if compression:
        file_suffix = f'.{suffix}.gz'
        parquet_compression='gzip'
    else:
        file_suffix = f'.{suffix}'
        parquet_compression = None

    filename = mkstemp(suffix=file_suffix, prefix=prefix, dir=dest_dir)[1]

    dataframe = records_to_dataframe(records, schema, data_flattening_max_level)
    dataframe.to_parquet(filename, compression=parquet_compression)

    return filename
