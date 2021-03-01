import unittest
import pytest

from decimal import Decimal

import target_snowflake.stream_utils as stream_utils
from target_snowflake.exceptions import UnexpectedValueTypeException


class TestSchemaUtils(unittest.TestCase):

    def setUp(self):
        self.config = {}

    def test_get_schema_names_from_config(self):
        """Test schema name extractor"""
        # Empty config
        assert stream_utils.get_schema_names_from_config({}) == []

        # Default target schema
        assert stream_utils.get_schema_names_from_config({
            'default_target_schema': 'test_schema_for_default'
        }) == ['test_schema_for_default']

        # Schema mapping should support multiple schemas
        assert stream_utils.get_schema_names_from_config({
            'schema_mapping': {
                'stream_1': {
                    'target_schema': 'test_schema_for_stream_1'
                },
                'stream_2': {
                    'target_schema': 'test_schema_for_stream_2'
                }
            }
        }) == ['test_schema_for_stream_1',
               'test_schema_for_stream_2']

        # Default and schema mapping should be combined
        assert stream_utils.get_schema_names_from_config({
            'default_target_schema': 'test_schema_for_default',
            'schema_mapping': {
                'stream_1': {
                    'target_schema': 'test_schema_for_stream_1'
                },
                'stream_2': {
                    'target_schema': 'test_schema_for_stream_2'
                }
            }
        }) == ['test_schema_for_default',
               'test_schema_for_stream_1',
               'test_schema_for_stream_2']

    def test_adjust_timestamps_in_record(self):
        """Test if timestamps converted to the acceptable valid ranges"""
        record = {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '10000-01-22 12:04:22',
            'key4': '25:01:01',
            'key5': 'I\'m good',
            'key6': None,
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

        stream_utils.adjust_timestamps_in_record(record, schema)

        assert record == {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '9999-12-31 23:59:59.999999',
            'key4': '23:59:59.999999',
            'key5': 'I\'m good',
            'key6': None
        }

    def test_adjust_timestamps_in_record_unexpected_int_will_raise_exception(self):
        """Test if timestamps converted to the acceptable valid ranges"""
        record = {
            'key': 100,
        }

        schema = {
            'properties': {
                'key': {'type': ['null', 'string'], 'format': 'date'},
            }
        }

        with pytest.raises(UnexpectedValueTypeException):
            stream_utils.adjust_timestamps_in_record(record, schema)

    def test_float_to_decimal(self):
        """Test if float values are converted to singer compatible Decimal types"""
        # Simple numeric value
        assert stream_utils.float_to_decimal(1.123) == Decimal("1.123")

        # List of numeric values
        assert stream_utils.float_to_decimal([1.123, 2.234, 3.345, 'this is not float']) == [
            Decimal("1.123"),
            Decimal("2.234"),
            Decimal("3.345"),
            'this is not float'
        ]

        # Nested dictionary
        assert stream_utils.float_to_decimal({
            'k1': 1.123,
            'k2': 2.234,
            'k3': [1.123, 2.234, 3.345, 'this is not float'],
            'k4': {
                'j1': 'foo',
                'j2': [1.123, 2.234, 3.345, 'this is not float again'],
                'j3': {
                    'k1': 1.123
                }
            }
        }) == {
            'k1': Decimal("1.123"),
            'k2': Decimal("2.234"),
            'k3': [
                Decimal("1.123"),
                Decimal("2.234"),
                Decimal("3.345"),
                'this is not float',
            ],
            'k4': {
                'j1': 'foo',
                'j2': [
                    Decimal("1.123"),
                    Decimal("2.234"),
                    Decimal("3.345"),
                    'this is not float again',
                ],
                'j3': {
                    'k1': Decimal("1.123")
                }
            }
        }

    def test_add_metadata_values_to_record(self):
        """Test if _sdc metadata columns can be added to the record message"""
        record_message = {
            'type': 'RECORD',
            'record': {
                'field_1': 123,
                'field_2': 123,
            }
        }
        record_message_with_metadata = stream_utils.add_metadata_values_to_record(record_message)

        assert record_message_with_metadata == {
            'field_1': 123,
            'field_2': 123,
            '_sdc_batched_at': record_message_with_metadata['_sdc_batched_at'],
            '_sdc_extracted_at': None,
            '_sdc_deleted_at': None
        }

    def test_stream_name_to_dict(self):
        """Test identifying catalog, schema and table names from fully qualified stream and table names"""
        # Singer stream name format (Default '-' separator)
        assert stream_utils.stream_name_to_dict('my_table') == \
               {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert stream_utils.stream_name_to_dict('my_schema-my_table') == \
               {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert stream_utils.stream_name_to_dict('my_catalog-my_schema-my_table') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}

        # Snowflake table format (Custom '.' separator)
        assert stream_utils.stream_name_to_dict('my_table', separator='.') == \
               {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Snowflake table format (Custom '.' separator)
        assert stream_utils.stream_name_to_dict('my_schema.my_table', separator='.') == \
                {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Snowflake table format (Custom '.' separator)
        assert stream_utils.stream_name_to_dict('my_catalog.my_schema.my_table', separator='.') == \
                {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}
