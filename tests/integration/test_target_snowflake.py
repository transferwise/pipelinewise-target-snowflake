import unittest
import os
import json
import datetime
import target_snowflake
import snowflake

from nose.tools import assert_raises 
from target_snowflake.db_sync import DbSync
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import SnowflakeFileEncryptionMaterial

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils


METADATA_COLUMNS = [
    '_SDC_EXTRACTED_AT',
    '_SDC_BATCHED_AT',
    '_SDC_DELETED_AT'
]


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    @classmethod
    def setUp(self):
        self.config = test_utils.get_test_config()
        snowflake = DbSync(self.config)
        if self.config['default_target_schema']:
            snowflake.query("DROP SCHEMA IF EXISTS {}".format(self.config['default_target_schema']))


    def remove_metadata_columns_from_rows(self, rows):
        """Removes metadata columns from a list of rows"""
        d_rows = []
        for r in rows:
            # Copy the original row to a new dict to keep the original dict
            # and remove metadata columns
            d_row = r.copy()
            for md_c in METADATA_COLUMNS:
                d_row.pop(md_c, None)

            # Add new row without metadata columns to the new list
            d_rows.append(d_row)

        return d_rows


    def assert_metadata_columns_exist(self, rows):
        """This is a helper assertion that checks if every row in a list has metadata columns"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertTrue(md_c in r)


    def assert_metadata_columns_not_exist(self, rows):
        """This is a helper assertion that checks metadata columns don't exist in any row"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertFalse(md_c in r)


    def assert_three_streams_are_into_snowflake(self, should_metadata_columns_exist=False, should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in Snowflake tables correctly.
        Useful to check different loading methods (unencrypted, Client-Side encryption, gzip, etc.)
        without duplicating assertions
        """
        snowflake = DbSync(self.config)
        default_target_schema = self.config.get('default_target_schema', '')
        schema_mapping = self.config.get('schema_mapping', {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = "tap_mysql_test"

        # Get loaded rows from tables
        table_one = snowflake.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))


        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}
        ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_one), expected_table_one)

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_DATE': datetime.datetime(2019, 2, 1, 15, 12, 45)},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_DATE': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ]
        else:
            expected_table_two = [
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_DATE': datetime.datetime(2019, 2, 10, 2, 0, 0)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_two), expected_table_two)

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if not should_hard_deleted_rows:
            expected_table_three = [
                    {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_TIME': datetime.time(4, 0, 0)},
                    {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_TIME': datetime.time(7, 15, 0)},
                    {'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '3', 'C_TIME': datetime.time(23, 0, 3)}
            ]
        else:
            expected_table_three = [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_TIME': datetime.time(4, 0, 0)},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_TIME': datetime.time(7, 15, 0)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_three), expected_table_three)

        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)


    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with assert_raises(json.decoder.JSONDecodeError):
            target_snowflake.persist_lines(self.config, tap_lines)


    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with assert_raises(Exception):
            target_snowflake.persist_lines(self.config, tap_lines)


    def test_loading_tables_with_no_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['client_side_encryption_master_key'] = ''
        target_snowflake.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_into_snowflake()


    def test_loading_tables_with_client_side_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['client_side_encryption_master_key'] = os.environ.get('CLIENT_SIDE_ENCRYPTION_MASTER_KEY')
        target_snowflake.persist_lines(self.config, tap_lines)

        self.assert_three_streams_are_into_snowflake()


    def test_loading_tables_with_client_side_encryption_and_wrong_master_key(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load but using a well formatted but wrong master key
        self.config['client_side_encryption_master_key'] = "Wr0n6m45t3rKeY0123456789a0123456789a0123456="
        with assert_raises(snowflake.connector.errors.ProgrammingError):
            target_snowflake.persist_lines(self.config, tap_lines)


    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        target_snowflake.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake(should_metadata_columns_exist=True)


    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        target_snowflake.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )


    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        target_snowflake.persist_lines(self.config, tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_into_snowflake(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )


    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        target_snowflake.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = snowflake.query("SELECT * FROM {}.test_table_unicode".format(target_schema))

        self.assertEqual(
            table_unicode,
            [
                    {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                    {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                    {'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                    {'C_INT': 4, 'C_PK': 4, 'C_VARCHAR': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                    {'C_INT': 5, 'C_PK': 5, 'C_VARCHAR': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                    {'C_INT': 6, 'C_PK': 6, 'C_VARCHAR': 'Special Characters: [",\'!@£$%^&*()]'}
            ])


    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        target_snowflake.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = snowflake.query("SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            table_non_db_friendly_columns,
            [
                    {'C_PK': 1, 'CAMELCASECOLUMN': 'Dummy row 1', 'MINUS-COLUMN': 'Dummy row 1'},
                    {'C_PK': 2, 'CAMELCASECOLUMN': 'Dummy row 2', 'MINUS-COLUMN': 'Dummy row 2'},
                    {'C_PK': 3, 'CAMELCASECOLUMN': 'Dummy row 3', 'MINUS-COLUMN': 'Dummy row 3'},
                    {'C_PK': 4, 'CAMELCASECOLUMN': 'Dummy row 4', 'MINUS-COLUMN': 'Dummy row 4'},
                    {'C_PK': 5, 'CAMELCASECOLUMN': 'Dummy row 5', 'MINUS-COLUMN': 'Dummy row 5'},
            ])


    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects into VARIANT columns without flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Load with default settings - Flattening disabled
        target_snowflake.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        unflattened_table = snowflake.query("""
            SELECT c_pk
                  ,TO_CHAR(c_array) c_array
                  ,TO_CHAR(c_object) c_object
                  ,TO_CHAR(c_object) c_object_with_props
                  ,TO_CHAR(c_nested_object) c_nested_object
              FROM {}.test_table_nested_schema
             ORDER BY c_pk""".format(target_schema))

        # Should be valid nested JSON strings
        self.assertEqual(
            unflattened_table,
            [{
                'C_PK': 1,
                'C_ARRAY': '[1,2,3]',
                'C_OBJECT': '{"key_1":"value_1"}',
                'C_OBJECT_WITH_PROPS': '{"key_1":"value_1"}',
                'C_NESTED_OBJECT': '{"nested_prop_1":"nested_value_1","nested_prop_2":"nested_value_2","nested_prop_3":{"multi_nested_prop_1":"multi_value_1","multi_nested_prop_2":"multi_value_2"}}'
            }])


    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Turning on data flattening
        self.config['data_flattening_max_level'] = 10

        # Load with default settings - Flattening disabled
        target_snowflake.persist_lines(self.config, tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = snowflake.query("SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(target_schema))

        # Should be flattened columns
        self.assertEqual(
            flattened_table,
            [{
                'C_PK': 1,
                'C_ARRAY': '[\n  1,\n  2,\n  3\n]',
                'C_OBJECT': None,   # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                'C_OBJECT_WITH_PROPS__KEY_1': 'value_1',
                'C_NESTED_OBJECT__NESTED_PROP_1': 'nested_value_1',
                'C_NESTED_OBJECT__NESTED_PROP_2': 'nested_value_2',
                'C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_1': 'multi_value_1',
                'C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_2': 'multi_value_2',
            }])


    def test_column_name_change(self):
        """Tests correct renaming of snowflake columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams-modified-column.json')

        # Load with default settings
        target_snowflake.persist_lines(self.config, tap_lines_before_column_name_change)
        target_snowflake.persist_lines(self.config, tap_lines_after_column_name_change)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # Get the previous column name from information schema in test_table_two
        previous_column_name = snowflake.query("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_catalog = '{}'
               AND table_schema = '{}'
               AND table_name = 'TEST_TABLE_TWO'
               AND ordinal_position = 1
            """.format(
                self.config.get('dbname', '').upper(),
                target_schema.upper()))[0]["COLUMN_NAME"]

        # Table one should have no changes
        self.assertEqual(
            table_one,
            [{'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}])

        # Table two should have versioned column
        self.assertEquals(
            table_two,
            [
                {previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45), 'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1', 'C_DATE': None},
                {previous_column_name: datetime.datetime(2019, 2, 10, 2), 'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2', 'C_DATE': '2019-02-12 02:00:00'},
                {previous_column_name: None, 'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '2', 'C_DATE': '2019-02-15 02:00:00'}
            ]
        )

        # Table three should have renamed columns
        self.assertEqual(
            table_three,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_TIME': datetime.time(4, 0), 'C_VARCHAR': '1', 'C_TIME_RENAMED': None},
                {'C_INT': 2, 'C_PK': 2, 'C_TIME': datetime.time(7, 15), 'C_VARCHAR': '2', 'C_TIME_RENAMED': None},
                {'C_INT': 3, 'C_PK': 3, 'C_TIME': datetime.time(23, 0, 3), 'C_VARCHAR': '3', 'C_TIME_RENAMED': datetime.time(8, 15)},
                {'C_INT': 4, 'C_PK': 4, 'C_TIME': None, 'C_VARCHAR': '4', 'C_TIME_RENAMED': datetime.time(23, 0, 3)}
            ])
