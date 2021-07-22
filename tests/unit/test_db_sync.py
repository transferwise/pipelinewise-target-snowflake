import unittest
import json

from unittest.mock import patch

from target_snowflake import db_sync
from target_snowflake.file_format import FileFormatTypes
from target_snowflake.exceptions import InvalidFileFormatException, FileFormatNotFoundException, PrimaryKeyNotFoundException


class TestDBSync(unittest.TestCase):
    """
    Unit Tests
    """

    def setUp(self):
        self.config = {}

        self.json_types = {
            'str': {"type": ["string"]},
            'str_or_null': {"type": ["string", "null"]},
            'dt': {"type": ["string"], "format": "date-time"},
            'dt_or_null': {"type": ["string", "null"], "format": "date-time"},
            'time': {"type": ["string"], "format": "time"},
            'time_or_null': {"type": ["string", "null"], "format": "time"},
            'binary': {"type": ["string", "null"], "format": "binary"},
            'num': {"type": ["number"]},
            'int': {"type": ["integer"]},
            'int_or_str': {"type": ["integer", "string"]},
            'bool': {"type": ["boolean"]},
            'obj': {"type": ["object"]},
            'arr': {"type": ["array"]},
        }

    def test_config_validation(self):
        """Test configuration validator"""
        validator = db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)), 0)

        # Minimal configuration should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration with schema mapping - (nr_of_errors >= 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        self.assertEqual(len(validator(config_with_schema_mapping)), 0)

        # Configuration with external stage
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['s3_bucket'] = 'dummy-value'
        config_with_external_stage['stage'] = 'dummy-value'
        self.assertEqual(len(validator(config_with_external_stage)), 0)

        # Configuration with invalid stage: Only s3_bucket defined - (nr_of_errors >= 0)
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['s3_bucket'] = 'dummy-value'
        self.assertGreater(len(validator(config_with_external_stage)), 0)

        # Configuration with invalid stage: Only stage defined - (nr_of_errors >= 0)
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['stage'] = 'dummy-value'
        self.assertGreater(len(validator(config_with_external_stage)), 0)

        # Configuration with archive_load_files but no s3_bucket
        config_with_archive_load_files = minimal_config.copy()
        config_with_archive_load_files['archive_load_files'] = True
        self.assertGreater(len(validator(config_with_external_stage)), 0)

    def test_column_type_mapping(self):
        """Test JSON type to Snowflake column type mappings"""
        mapper = db_sync.column_type

        # Snowflake column types
        sf_types = {
            'str': 'text',
            'str_or_null': 'text',
            'dt': 'timestamp_ntz',
            'dt_or_null': 'timestamp_ntz',
            'time': 'time',
            'time_or_null': 'time',
            'binary': 'binary',
            'num': 'float',
            'int': 'number',
            'int_or_str': 'text',
            'bool': 'boolean',
            'obj': 'variant',
            'arr': 'variant',
        }

        # Mapping from JSON schema types to Snowflake column types
        for key, val in self.json_types.items():
            self.assertEqual(mapper(val), sf_types[key])

    def test_column_trans(self):
        """Test column transformation"""
        trans = db_sync.column_trans

        # Snowflake column transformations
        sf_trans = {
            'str': '',
            'str_or_null': '',
            'dt': '',
            'dt_or_null': '',
            'time': '',
            'time_or_null': '',
            'binary': 'to_binary',
            'num': '',
            'int': '',
            'int_or_str': '',
            'bool': '',
            'obj': 'parse_json',
            'arr': 'parse_json',
        }

        # Getting transformations for every JSON type
        for key, val in self.json_types.items():
            self.assertEqual(trans(val), sf_trans[key])

    def test_create_query_tag(self):
        self.assertIsNone(db_sync.create_query_tag(None))
        self.assertEqual(db_sync.create_query_tag('This is a test query tag'), 'This is a test query tag')
        self.assertEqual(db_sync.create_query_tag('Loading into {{database}}.{{schema}}.{{table}}',
                                        database='test_database',
                                        schema='test_schema',
                                        table='test_table'), 'Loading into test_database.test_schema.test_table')
        self.assertEqual(db_sync.create_query_tag('Loading into {{database}}.{{schema}}.{{table}}',
                                        database=None,
                                        schema=None,
                                        table=None), 'Loading into ..')

        # JSON formatted query tags with variables
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='test_database',
            schema='test_schema',
            table='test_table')
        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        self.assertEqual(json.loads(json_query_tag), {
            'database': 'test_database',
            'schema': 'test_schema',
            'table': 'test_table'
        })

        # JSON formatted query tags with variables quotes in the middle
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='test"database',
            schema='test"schema',
            table='test"table')

        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        self.assertEqual(json.loads(json_query_tag), {
            'database': 'test"database',
            'schema': 'test"schema',
            'table': 'test"table'
        })

        # JSON formatted query tags with quoted variables
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='"test_database"',
            schema='"test_schema"',
            table='"test_table"')
        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        self.assertEqual(json.loads(json_query_tag), {
            'database': 'test_database',
            'schema': 'test_schema',
            'table': 'test_table'
        })

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_parallelism(self, query_patch):
        query_patch.return_value = [{ 'type': 'CSV' }]

        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value"
        }

        # Using external stages should allow parallelism
        external_stage_with_parallel = {
            's3_bucket': 'dummy-bucket',
            'stage': 'dummy_schema.dummy_stage',
            'parallelism': 5
        }

        self.assertEqual(db_sync.DbSync({**minimal_config,
                                         **external_stage_with_parallel}).connection_config['parallelism'], 5)

        # Using snowflake table stages should enforce single thread parallelism
        table_stage_with_parallel = {
            'parallelism': 5
        }
        self.assertEqual(db_sync.DbSync({**minimal_config,
                                         **table_stage_with_parallel}).connection_config['parallelism'], 1)

    @patch('target_snowflake.upload_clients.s3_upload_client.S3UploadClient.copy_object')
    @patch('target_snowflake.db_sync.DbSync.query')
    def test_copy_to_archive(self, query_patch, copy_object_patch):
        query_patch.return_value = [{'type': 'CSV'}]
        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value",
            's3_bucket': 'dummy-bucket',
            'stage': 'dummy_schema.dummy_stage'
        }

        # Assert default values (same bucket, 'archive' as the archive prefix)
        s3_config = {}
        dbsync = db_sync.DbSync({**minimal_config, **s3_config})
        dbsync.copy_to_archive('source/file', 'tap/schema/file', {'meta': "data"})

        self.assertEqual(copy_object_patch.call_args[0][0], 'dummy-bucket/source/file')
        self.assertEqual(copy_object_patch.call_args[0][1], 'dummy-bucket')
        self.assertEqual(copy_object_patch.call_args[0][2], 'archive/tap/schema/file')

        # Assert custom archive bucket and prefix
        s3_config = {
            'archive_load_files_s3_bucket': "custom-bucket",
            'archive_load_files_s3_prefix': "custom-prefix"
        }
        dbsync = db_sync.DbSync({**minimal_config, **s3_config})
        dbsync.copy_to_archive('source/file', 'tap/schema/file', {'meta': "data"})

        self.assertEqual(copy_object_patch.call_args[0][0], 'dummy-bucket/source/file')
        self.assertEqual(copy_object_patch.call_args[0][1], 'custom-bucket')
        self.assertEqual(copy_object_patch.call_args[0][2], 'custom-prefix/tap/schema/file')

    def test_safe_column_name(self):
        self.assertEqual(db_sync.safe_column_name("columnname"), '"COLUMNNAME"')
        self.assertEqual(db_sync.safe_column_name("columnName"), '"COLUMNNAME"')
        self.assertEqual(db_sync.safe_column_name("column-name"), '"COLUMN-NAME"')
        self.assertEqual(db_sync.safe_column_name("column name"), '"COLUMN NAME"')

    def json_element_name(self):
        self.assertEqual(db_sync.safe_column_name("columnname"), 'columnname"')
        self.assertEqual(db_sync.safe_column_name("columnName"), 'columnName"')
        self.assertEqual(db_sync.safe_column_name("column-name"), 'column-name')
        self.assertEqual(db_sync.safe_column_name('"column name"'), '"column name"')

    @patch('target_snowflake.db_sync.DbSync.query')
    def test_record_primary_key_string(self, query_patch):
        query_patch.return_value = [{'type': 'CSV'}]
        minimal_config = {
            'account': "dummy-value",
            'dbname': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'warehouse': "dummy-value",
            'default_target_schema': "dummy-value",
            'file_format': "dummy-value"
        }

        stream_schema_message = {"stream": "public-table1",
                                 "schema": {
                                     "properties": {
                                         "id": { "type": ["integer"]},
                                         "c_str": {"type": ["null", "string"]}}},
                                 "key_properties": ["id"]}

        # Single primary key string
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 123}), '123')

        # Composite primary key string
        stream_schema_message['key_properties'] = ['id', 'c_str']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 123, 'c_str': 'xyz'}), '123,xyz')

        # Missing field as PK
        stream_schema_message['key_properties'] = ['invalid_col']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        with self.assertRaisesRegex(PrimaryKeyNotFoundException,
                                    "Cannot find \['invalid_col'\] primary key\(s\) in record\. Available fields: \['id', 'c_str'\]"):
            dbsync.record_primary_key_string({'id': 123, 'c_str': 'xyz'})
