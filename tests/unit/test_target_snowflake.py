from importlib.resources import path
import io
import json
import unittest
import os
import gzip
import tempfile
from unittest import mock
import itertools

from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import target_snowflake
from target_snowflake import db_sync


def _mock_record_to_csv_line(record):
    return record


class TestTargetSnowflake(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.maxDiff = None

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
        self.assertEqual(flush_streams_mock.call_count, 41)

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_incremental_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True
        self.config['s3_bucket'] = 'dummy_bucket'

        with open(f'{os.path.dirname(__file__)}/resources/messages-simple-table.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        self.assertEqual(copy_to_archive_args[1], 'test_tap_id/test_simple_table/some-name_date_batch_hash.csg.gz')
        self.assertDictEqual(copy_to_archive_args[2], {
            'tap': 'test_tap_id',
            'schema': 'tap_mysql_test',
            'table': 'test_simple_table',
            'archived-by': 'pipelinewise_target_snowflake',
            'incremental-key': 'id',
            'incremental-key-min': '1',
            'incremental-key-max': '5'
        })

    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.os.remove')
    def test_archive_load_files_log_based_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        target_snowflake.persist_lines(self.config, lines)

        copy_to_archive_args = instance.copy_to_archive.call_args[0]
        self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        self.assertEqual(copy_to_archive_args[1], 'test_tap_id/logical1_table2/some-name_date_batch_hash.csg.gz')
        self.assertDictEqual(copy_to_archive_args[2], {
            'tap': 'test_tap_id',
            'schema': 'logical1',
            'table': 'logical1_table2',
            'archived-by': 'pipelinewise_target_snowflake'
        })

    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_persist_lines_with_only_state_messages(self, dbSync_mock, flush_streams_mock):
        """
        Given only state messages, target should emit the last one
        """

        self.config['batch_size_rows'] = 5

        with open(f'{os.path.dirname(__file__)}/resources/streams_only_state.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # catch stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            target_snowflake.persist_lines(self.config, lines)

        flush_streams_mock.assert_not_called()

        self.assertEqual(
            buf.getvalue().strip(),
            '{"bookmarks": {"tap_mysql_test-test_simple_table": {"replication_key": "id", '
            '"replication_key_value": 100, "version": 1}}}')


    @patch('target_snowflake.flush_streams')
    @patch('target_snowflake.DbSync')
    def test_verify_snowpipe_usage(self, dbSync_mock,
                                   flush_streams_mock):
        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_snowflake.persist_lines(self.config, lines)
        flush_streams_mock.assert_called_once()


    @patch('target_snowflake.DbSync')
    @patch('target_snowflake.DbSync')
    def test_verify_snowpipe_usage(self, dbsync_mock1, dbsync_mock2):
        """ Test setting of snowpipe usage """
        min_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-target-schema",
            'file_format': "dummy-value",
            's3_bucket': 'dummy-bucket',
            'stage': 'dummy_schema.dummy_stage',
            's3_key_prefix': 'dummy_key_prefix/',
            'load_via_snowpipe': True
        }
        # Set the key properties for stream2 to check ignoring of load_via_snowpipe case
        dbsync_mock2.stream_schema_message = {"key_properties": ["col1"]}
        input_stream = {"stream1": dbsync_mock1, "stream2": dbsync_mock2}
        expected_output = {"stream1": True, "stream2": False}
        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        output = target_snowflake._set_stream_snowpipe_usage(input_stream, min_config)
        self.assertEqual(output, expected_output)


    @patch('target_snowflake.db_sync.DbSync.query')
    def test_verify_snowpipe_copy(self, query_patch):
        """ Test setting of snowpipe copy command usage """
        query_patch.return_value = [{'type': 'CSV'}]
        target_snowflake.db_sync.SimpleIngestManager = MagicMock()
        target_snowflake.db_sync.SimpleIngestManager().get_history = MagicMock(return_value={"files":[ {"name": "dummy_file01.csv", "rowsInserted": 0, "rowsParsed": 0 } ]
                                                                                , "pipe": "dummy_pipe"
                                                                                , "completeResult": "dummy_result"
                                                                                })
        
        dummy_target_schema = "dummy-target-schema"
        dummy_db_name = "dummy-database"
        dummy_file_format = "dummy-file-format"
        dummy_stage = "dummy_schema.dummy_stage"
        dummy_stream_name = "STREAM1"

        minimal_config = {
            'account': "dummy-value",
            'dbname': dummy_db_name,
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': dummy_target_schema,
            'file_format': dummy_file_format,
            's3_bucket': 'dummy-bucket',
            'stage': dummy_stage,
            's3_key_prefix': 'dummy_key_prefix/',
            'load_via_snowpipe': True
        }

        s3_config = {}
        record_stream_name = "dummy_tap_schema-dummy_tap_table"
        schema_record = json.loads('{"type": "SCHEMA", "stream": "public-table1", "schema": {"definitions": {"sdc_recursive_boolean_array": {"items": {"$ref": "#/definitions/sdc_recursive_boolean_array"}, "type": ["null", "boolean", "array"]}, "sdc_recursive_integer_array": {"items": {"$ref": "#/definitions/sdc_recursive_integer_array"}, "type": ["null", "integer", "array"]}, "sdc_recursive_number_array": {"items": {"$ref": "#/definitions/sdc_recursive_number_array"}, "type": ["null", "number", "array"]}, "sdc_recursive_object_array": {"items": {"$ref": "#/definitions/sdc_recursive_object_array"}, "type": ["null", "object", "array"]}, "sdc_recursive_string_array": {"items": {"$ref": "#/definitions/sdc_recursive_string_array"}, "type": ["null", "string", "array"]}, "sdc_recursive_timestamp_array": {"format": "date-time", "items": {"$ref": "#/definitions/sdc_recursive_timestamp_array"}, "type": ["null", "string", "array"]}}, "properties": {"cid": {"maximum": 2147483647, "minimum": -2147483648, "type": ["integer"]}, "cvarchar": {"type": ["null", "string"]}, "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"}}, "type": "object"}, "key_properties": ["cid"], "bookmark_properties": ["lsn"]}')
        DbSync_obj = db_sync.DbSync({**minimal_config, **s3_config}, schema_record)


        input_stream = {"stream1": DbSync_obj}        

        expected_value = f"""create pipe {dummy_db_name}.{dummy_target_schema}.{dummy_stream_name}_s3_pipe as
                            copy into {dummy_db_name}.{dummy_target_schema}."{dummy_stream_name}" ("_SDC_DELETED_AT", "CID", "CVARCHAR")
                            from @{dummy_db_name}.{dummy_stage}
                            file_format = (format_name = {dummy_db_name}.{dummy_file_format} )
                            ;"""                                                               

        with mock.patch.object(DbSync_obj, 'query') as mock_query:
            DbSync_obj.load_via_snowpipe("s3://dummy_s3_key","stream1")
            mock_query.assert_any_call(expected_value)


    @patch('target_snowflake.db_sync.DbSync.query')
    def test_verify_snowpipe_copy_on_error(self, query_patch):
        """ Test setting of snowpipe copy command usage """
        query_patch.return_value = [{'type': 'CSV'}]
        target_snowflake.db_sync.SimpleIngestManager = MagicMock()
        target_snowflake.db_sync.SimpleIngestManager().get_history = MagicMock(return_value={"files":[ {"name": "dummy_file01.csv", "rowsInserted": 0, "rowsParsed": 0 } ]
                                                                                , "pipe": "dummy_pipe"
                                                                                , "completeResult": "dummy_result"
                                                                                })
        
        dummy_target_schema = "dummy-target-schema"
        dummy_db_name = "dummy-database"
        dummy_file_format = "dummy-file-format"
        dummy_stage = "dummy_schema.dummy_stage"
        dummy_stream_name = "STREAM1"

        minimal_config = {
            'account': "dummy-value",
            'dbname': dummy_db_name,
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': dummy_target_schema,
            'file_format': dummy_file_format,
            's3_bucket': 'dummy-bucket',
            'stage': dummy_stage,
            's3_key_prefix': 'dummy_key_prefix/',
            'load_via_snowpipe': True,
            'on_error': "CONTINUE"
        }

        s3_config = {}
        record_stream_name = "dummy_tap_schema-dummy_tap_table"
        schema_record = json.loads('{"type": "SCHEMA", "stream": "public-table1", "schema": {"definitions": {"sdc_recursive_boolean_array": {"items": {"$ref": "#/definitions/sdc_recursive_boolean_array"}, "type": ["null", "boolean", "array"]}, "sdc_recursive_integer_array": {"items": {"$ref": "#/definitions/sdc_recursive_integer_array"}, "type": ["null", "integer", "array"]}, "sdc_recursive_number_array": {"items": {"$ref": "#/definitions/sdc_recursive_number_array"}, "type": ["null", "number", "array"]}, "sdc_recursive_object_array": {"items": {"$ref": "#/definitions/sdc_recursive_object_array"}, "type": ["null", "object", "array"]}, "sdc_recursive_string_array": {"items": {"$ref": "#/definitions/sdc_recursive_string_array"}, "type": ["null", "string", "array"]}, "sdc_recursive_timestamp_array": {"format": "date-time", "items": {"$ref": "#/definitions/sdc_recursive_timestamp_array"}, "type": ["null", "string", "array"]}}, "properties": {"cid": {"maximum": 2147483647, "minimum": -2147483648, "type": ["integer"]}, "cvarchar": {"type": ["null", "string"]}, "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"}}, "type": "object"}, "key_properties": ["cid"], "bookmark_properties": ["lsn"]}')
        DbSync_obj = db_sync.DbSync({**minimal_config, **s3_config}, schema_record)


        input_stream = {"stream1": DbSync_obj}        

        expected_value = f"""create pipe {dummy_db_name}.{dummy_target_schema}.{dummy_stream_name}_s3_pipe as
                            copy into {dummy_db_name}.{dummy_target_schema}."{dummy_stream_name}" ("_SDC_DELETED_AT", "CID", "CVARCHAR")
                            from @{dummy_db_name}.{dummy_stage}
                            file_format = (format_name = {dummy_db_name}.{dummy_file_format} )
                            ON_ERROR = CONTINUE;"""                                                               

        with mock.patch.object(DbSync_obj, 'query') as mock_query:
            DbSync_obj.load_via_snowpipe("s3://dummy_s3_key","stream1")
            mock_query.assert_any_call(expected_value)
