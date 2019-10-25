import unittest
import os
import logging

from unittest.mock import patch

import target_snowflake


class TestTargetSnowflake(unittest.TestCase):

    def setUp(self):
        self.config = {}

    @patch('target_snowflake.NamedTemporaryFile')
    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock, flush_streams_mock, temp_file_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()
