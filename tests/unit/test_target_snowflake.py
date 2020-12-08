import unittest
import os
import gzip
import tempfile
import mock

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

        flush_streams_mock.assert_called_once()

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

        flush_streams_mock.assert_called_once()

    def test_adjust_timestamps_in_record(self):
        record = {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '10000-01-22 12:04:22',
            'key4': '25:01:01',
            'key5': 'I\'m good',
            'key6': None
        }

        schema = {
            'properties': {
                'key1': {
                    'type': ['null', 'string', 'integer'],
                },
                'key2': {
                    'anyOf': [
                        {'type': ['null', 'string'], 'format': 'date'},
                        {'type': ['null', 'string']}
                    ]
                },
                'key3': {
                    'type': ['null', 'string'], 'format': 'date-time',
                },
                'key4': {
                    'anyOf': [
                        {'type': ['null', 'string'], 'format': 'time'},
                        {'type': ['null', 'string']}
                    ]
                },
                'key5': {
                    'type': ['null', 'string'],
                },
                'key6': {
                    'type': ['null', 'string'], 'format': 'time',
                },
            }
        }

        target_snowflake.adjust_timestamps_in_record(record, schema)

        self.assertDictEqual({
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '9999-12-31 23:59:59.999999',
            'key4': '23:59:59.999999',
            'key5': 'I\'m good',
            'key6': None
        }, record)

    def test_write_record_to_uncompressed_file(self):
        records = {'pk_1': 'data1,data2,data3,data4'}

        # Write uncompressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with open(csv_file.name, 'wb') as f:
            target_snowflake.write_record_to_file(f, records, _mock_record_to_csv_line)

        # Read and validate uncompressed CSV file
        with open(csv_file.name, 'rt') as f:
            self.assertEquals(f.readlines(), ['data1,data2,data3,data4\n'])

        os.remove(csv_file.name)

    def test_write_record_to_compressed_file(self):
        records = {'pk_1': 'data1,data2,data3,data4'}

        # Write gzip compressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with gzip.open(csv_file.name, 'wb') as f:
            target_snowflake.write_record_to_file(f, records, _mock_record_to_csv_line)

        # Read and validate gzip compressed CSV file
        with gzip.open(csv_file.name, 'rt') as f:
            self.assertEquals(f.readlines(), ['data1,data2,data3,data4\n'])

        os.remove(csv_file.name)


    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_verify_snowpipe_usage(self, dbSync_mock,
                                   flush_streams_mock, monkeypatch):
        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()
        monkeypatch.setattr('builtins.input', lambda _: "I Agree")

        assert target_snowflake._verify_snowpipe_usage() == 'dict with all key values=1'
