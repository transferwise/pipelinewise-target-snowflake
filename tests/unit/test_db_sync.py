import unittest
import json

from target_snowflake import db_sync


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

    def test_stream_name_to_dict(self):
        """Test identifying catalog, schema and table names from fully qualified stream and table names"""
        # Singer stream name format (Default '-' separator)
        self.assertEqual(
            db_sync.stream_name_to_dict('my_table'),
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"})

        # Singer stream name format (Default '-' separator)
        self.assertEqual(
            db_sync.stream_name_to_dict('my_schema-my_table'),
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"})

        # Singer stream name format (Default '-' separator)
        self.assertEqual(
            db_sync.stream_name_to_dict('my_catalog-my_schema-my_table'),
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"})

        # Snowflake table format (Custom '.' separator)
        self.assertEqual(
            db_sync.stream_name_to_dict('my_table', separator='.'),
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"})

        # Snowflake table format (Custom '.' separator)
        self.assertEqual(
            db_sync.stream_name_to_dict('my_schema.my_table', separator='.'),
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"})

        # Snowflake table format (Custom '.' separator)
        self.assertEqual(
            db_sync.stream_name_to_dict('my_catalog.my_schema.my_table', separator='.'),
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"})

    def test_flatten_schema(self):
        """Test flattening of SCHEMA messages"""
        flatten_schema = db_sync.flatten_schema

        # Schema with no object properties should be empty dict
        schema_with_no_properties = {"type": "object"}
        self.assertEqual(flatten_schema(schema_with_no_properties), {})

        not_nested_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]}}}

        # NO FLATTENING - Schema with simple properties should be a plain dictionary
        self.assertEqual(flatten_schema(not_nested_schema), not_nested_schema['properties'])

        nested_schema_with_no_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {"type": ["null", "object"]}}}

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        self.assertEqual(flatten_schema(nested_schema_with_no_properties),
                          nested_schema_with_no_properties['properties'])

        nested_schema_with_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                        "nested_prop2": {"type": ["null", "string"]},
                        "nested_prop3": {
                            "type": ["null", "object"],
                            "properties": {
                                "multi_nested_prop1": {"type": ["null", "string"]},
                                "multi_nested_prop2": {"type": ["null", "string"]}
                            }
                        }
                    }
                }
            }
        }

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        # No flattening (default)
        self.assertEqual(flatten_schema(nested_schema_with_properties), nested_schema_with_properties['properties'])

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=0),
                          nested_schema_with_properties['properties'])

        # FLATTENING - Schema with object type property but without further properties should be a dict with
        # flattened properties
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=1),
                          {
                              'c_pk': {'type': ['null', 'integer']},
                              'c_varchar': {'type': ['null', 'string']},
                              'c_int': {'type': ['null', 'integer']},
                              'c_obj__nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop2': {'type': ['null', 'string']},
                              'c_obj__nested_prop3': {
                                  'type': ['null', 'object'],
                                  "properties": {
                                      "multi_nested_prop1": {"type": ["null", "string"]},
                                      "multi_nested_prop2": {"type": ["null", "string"]}
                                  }
                              }
                          })

        # FLATTENING - Schema with object type property but without further properties should be a dict with
        # flattened properties
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=10),
                          {
                              'c_pk': {'type': ['null', 'integer']},
                              'c_varchar': {'type': ['null', 'string']},
                              'c_int': {'type': ['null', 'integer']},
                              'c_obj__nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop2': {'type': ['null', 'string']},
                              'c_obj__nested_prop3__multi_nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop3__multi_nested_prop2': {'type': ['null', 'string']}
                          })

    def test_flatten_record(self):
        """Test flattening of RECORD messages"""
        flatten_record = db_sync.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        self.assertEqual(flatten_record(empty_record), {})

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENING - Record with simple properties should be a plain dictionary
        self.assertEqual(flatten_record(not_nested_record), not_nested_record)

        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "c_int": 1,
            "c_obj": {
                "nested_prop1": "value_1",
                "nested_prop2": "value_2",
                "nested_prop3": {
                    "multi_nested_prop1": "multi_value_1",
                    "multi_nested_prop2": "multi_value_2",
                }}}

        # NO FLATTENING - No flattening (default)
        self.assertEqual(flatten_record(nested_record),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {'
                                       '"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
                          })

        # NO FLATTENING
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=0),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {'
                                       '"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
                          })

        # SEMI FLATTENING
        #   max_level: 1 : Semi-flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=1),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": '
                                                     '"multi_value_2"}'
                          })

        # FLATTENING
        self.assertEqual(flatten_record(nested_record, max_level=10),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                              "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
                          })

    def test_flatten_record_with_flatten_schema(self):
        flatten_record = db_sync.flatten_record

        flatten_schema = {
            "id": {
                "type": [
                    "object",
                    "array",
                    "null"
                ]
            }
        }

        test_cases = [
            (
                True,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": "1",
                    "data": "xyz"
                }
            ),
            (
                False,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": 1,
                    "data": "xyz"
                }
            )
        ]

        for idx, (should_use_flatten_schema, record, expected_output) in enumerate(test_cases):
            output = flatten_record(record, flatten_schema if should_use_flatten_schema else None)
            self.assertEqual(output, expected_output, f"Test {idx} failed. Testcase: {test_cases[idx]}")

    def test_create_query_tag(self):
        assert db_sync.create_query_tag(None) is None
        assert db_sync.create_query_tag('This is a test query tag') == 'This is a test query tag'
        assert db_sync.create_query_tag('Loading into {{database}}.{{schema}}.{{table}}',
                                        database='test_database',
                                        schema='test_schema',
                                        table='test_table') == 'Loading into test_database.test_schema.test_table'
        assert db_sync.create_query_tag('Loading into {{database}}.{{schema}}.{{table}}',
                                        database=None,
                                        schema=None,
                                        table=None) == 'Loading into ..'

        # JSON formatted query tags with variables
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='test_database',
            schema='test_schema',
            table='test_table')
        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        assert json.loads(json_query_tag) == {
            'database': 'test_database',
            'schema': 'test_schema',
            'table': 'test_table'
        }

        # JSON formatted query tags with variables quotes in the middle
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='test"database',
            schema='test"schema',
            table='test"table')

        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        assert json.loads(json_query_tag) == {
            'database': 'test"database',
            'schema': 'test"schema',
            'table': 'test"table'
        }

        # JSON formatted query tags with quoted variables
        json_query_tag = db_sync.create_query_tag(
            '{"database": "{{database}}", "schema": "{{schema}}", "table": "{{table}}"}',
            database='"test_database"',
            schema='"test_schema"',
            table='"test_table"')
        # Load the generated JSON formatted query tag to make sure it's a valid JSON
        assert json.loads(json_query_tag) == {
            'database': 'test_database',
            'schema': 'test_schema',
            'table': 'test_table'
        }
