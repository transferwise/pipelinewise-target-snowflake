import unittest
import os
import itertools

from datetime import datetime, timedelta
from unittest.mock import patch

import target_snowflake


def _mock_record_to_csv_line(record):
    return record


class TestTargetSnowflake(unittest.TestCase):

    def setUp(self):
        self.config = {}

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_same_schema_expect_flushing_once(self, dbSync_mock,
                                                                 flush_streams_mock):
        self.config['batch_size_rows'] = 20

        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('target_snowflake.datetime')
    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_40_records_with_batch_wait_limit(self, dbSync_mock, flush_streams_mock, dateTime_mock):

        start_time = datetime(2021, 4, 6, 0, 0, 0)
        increment = 11
        counter = itertools.count()

        # Move time forward by {{increment}} seconds every time utcnow() is called
        dateTime_mock.utcnow.side_effect = lambda: start_time + timedelta(seconds=increment * next(counter))

        self.config['batch_size_rows'] = 100
        self.config['batch_wait_limit_seconds'] = 10
        self.config['flush_all_streams'] = True

        # Expecting 40 records
        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        # Expecting flush after every records + 1 at the end
        assert flush_streams_mock.call_count == 41

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.flush_streams')
    def test_archive_load_files(self, flush_streams_mock, dbSync_mock):
        self.config['id'] = 'test-tap-id'
        self.config['archive_load_files.enabled'] = True
        self.config['db_conn.s3_bucket'] = 'dummy_bucket'

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        archive_load_tables_data = flush_streams_mock.call_args[0][6]
        assert archive_load_tables_data == {
            'logical1-logical1_table2': {
                'tap': 'test-tap-id',
                'column': 'cid',
                'min': 1,
                'max': 20
            }
        }

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.load_stream_batch')
    def test_archive_load_files_2(self, load_stream_batch_mock, dbSync_mock):
        self.config['id'] = 'test-tap-id'
        self.config['archive_load_files.enabled'] = True
        self.config['db_conn.s3_bucket'] = 'dummy_bucket'

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        target_snowflake.persist_lines(self.config, lines)

        archive_load_tables_data = load_stream_batch_mock.call_args[1]['archive_load_files']
        assert archive_load_tables_data == {
            'tap': 'test-tap-id',
            'column': 'cid',
            'min': 1,
            'max': 20
        }

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_3(self, os_remove_mock, dbSync_mock):
        self.config['id'] = 'test-tap-id'
        self.config['archive_load_files.enabled'] = True
        self.config['db_conn.s3_bucket'] = 'dummy_bucket'

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'mock-s3-key'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        assert copy_to_archive_args[0] == 'logical1-logical1_table2'
        assert copy_to_archive_args[1] == 'mock-s3-key'
        # assert copy_to_archive_args[2] == 'archive/test-tap-id/logical1_table2/logical1_table2_\d{8}-\d{6}-\d{6}.csv.gz'
        assert copy_to_archive_args[3] == {
            'tap': 'test-tap-id',
            'schema': 'logical1',
            'table': 'logical1_table2',
            'archive-load-files-primary-column': 'cid',
            'archive-load-files-primary-column-min': 1,
            'archive-load-files-primary-column-max': 20
        }
