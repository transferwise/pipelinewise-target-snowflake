import unittest
import re

from pandas._testing import assert_frame_equal
from pandas import DataFrame

import target_snowflake.file_formats.parquet as parquet


class TestParquet(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.config = {}

    def test_records_to_dataframe(self):
        records = {
            '1': {
                'key1': 1,
                'key2': '2031-01-22',
                'key3': '10000-01-22 12:04:22',
                'key4': '12:01:01',
                'key5': 'I\'m good',
                'key6': None,
            },
            '2': {
                'key1': 2,
                'key2': '2032-01-22',
                'key3': '10000-01-22 12:04:22',
                'key4': '13:01:01',
                'key5': 'I\'m good too',
                'key6': None,
            },
            '3': {
                'key1': 3,
                'key2': '2033-01-22',
                'key3': '10000-01-22 12:04:22',
                'key4': '14:01:01',
                'key5': 'I want to be good',
                'key6': None,
            }
        }

        schema = {}
        assert_frame_equal(parquet.records_to_dataframe(records=records, schema=schema),
                           DataFrame({
                               'key1': [1, 2, 3],
                               'key2': ['2031-01-22', '2032-01-22', '2033-01-22'],
                               'key3': ['10000-01-22 12:04:22', '10000-01-22 12:04:22', '10000-01-22 12:04:22'],
                               'key4': ['12:01:01', '13:01:01', '14:01:01'],
                               'key5': ['I\'m good', 'I\'m good too', 'I want to be good'],
                               'key6': [None, None, None]}))

    def test_create_copy_sql(self):
        self.assertEqual(parquet.create_copy_sql(table_name='foo_table',
                                                 stage_name='foo_stage',
                                                 s3_key='foo_s3_key.parquet',
                                                 file_format_name='foo_file_format',
                                                 columns=[{'name': 'COL_1', 'json_element_name': 'col_1', 'trans': ''},
                                                          {'name': 'COL_2', 'json_element_name': 'colTwo', 'trans': ''},
                                                          {'name': 'COL_3', 'json_element_name': 'col_3',
                                                           'trans': 'parse_json'}]),

                         "COPY INTO foo_table (COL_1, COL_2, COL_3) FROM ("
                         "SELECT ($1:col_1) COL_1, ($1:colTwo) COL_2, parse_json($1:col_3) COL_3 "
                         "FROM '@foo_stage/foo_s3_key.parquet'"
                         ") "
                         "FILE_FORMAT = (format_name='foo_file_format')")

    def test_create_merge_sql(self):

        subject = parquet.create_merge_sql(table_name='foo_table',
                                              stage_name='foo_stage',
                                              s3_key='foo_s3_key.parquet',
                                              file_format_name='foo_file_format',
                                              columns=[
                                                  {'name': 'COL_1', 'json_element_name': 'col_1', 'trans': ''},
                                                  {'name': 'COL_2', 'json_element_name': 'colTwo', 'trans': ''},
                                                  {'name': 'COL_3', 'json_element_name': 'col_3',
                                                   'trans': 'parse_json'}
                                              ],
                                              pk_merge_condition='s.COL_1 = t.COL_1').replace("\n", "")
        sanitized_subject = re.sub(' +', ' ', subject)

        self.assertEqual(
            sanitized_subject,
            "MERGE INTO foo_table t USING ("
            "SELECT ($1:col_1) COL_1, ($1:colTwo) COL_2, ($1:col_3) COL_3 "
            "FROM '@foo_stage/foo_s3_key.parquet' "
            "(FILE_FORMAT => 'foo_file_format')) s "
            "ON s.COL_1 = t.COL_1 "
            "WHEN MATCHED THEN UPDATE SET COL_1 = CASE WHEN s.COL_1 = 'ppw_ignore-4BVdmNiaHxpsFC3wDkwb' THEN t.COL_1 "
            "ELSE (s.COL_1) END, COL_2 = CASE WHEN s.COL_2 = 'ppw_ignore-4BVdmNiaHxpsFC3wDkwb' THEN t.COL_2 "
            "ELSE (s.COL_2) END, COL_3 = CASE WHEN s.COL_3 = 'ppw_ignore-4BVdmNiaHxpsFC3wDkwb' THEN t.COL_3 "
            "ELSE parse_json(s.COL_3) END "
            "WHEN NOT MATCHED THEN "
            "INSERT (COL_1, COL_2, COL_3) "
            "VALUES (CASE WHEN s.COL_1 = 'ppw_ignore-4BVdmNiaHxpsFC3wDkwb' THEN NULL ELSE (s.COL_1) END, "
            "CASE WHEN s.COL_2 = 'ppw_ignore-4BVdmNiaHxpsFC3wDkwb' THEN NULL ELSE (s.COL_2) END, "
            "CASE WHEN s.COL_3 = 'ppw_ignore-4BVdmNiaHxpsFC3wDkwb' THEN NULL ELSE parse_json(s.COL_3) END)"
        )
