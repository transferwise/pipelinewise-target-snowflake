import datetime
import json
import unittest
import mock
import os

from nose.tools import assert_raises

import target_snowflake
from target_snowflake.db_sync import DbSync

from snowflake.connector.errors import ProgrammingError

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
    maxDiff = None

    def setUp(self):
        self.config = test_utils.get_test_config()
        snowflake = DbSync(self.config)

        # Drop target schema
        if self.config['default_target_schema']:
            snowflake.query("DROP SCHEMA IF EXISTS {}".format(self.config['default_target_schema']))

        # Delete target schema entries from PIPELINEWISE.COLUMNS
        if self.config['stage']:
            snowflake.query("CREATE TABLE IF NOT EXISTS {}.columns (table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, data_type VARCHAR)".format(snowflake.pipelinewise_schema))
            snowflake.query("DELETE FROM {}.columns WHERE TABLE_SCHEMA ilike '{}'".format(snowflake.pipelinewise_schema, self.config['default_target_schema']))

    def persist_lines_with_cache(self, lines):
        """Enables table caching option and loads singer messages into snowflake.

        Table caching mechanism is creating and maintaining an extra table in snowflake about
        the table structures. It's very similar to the INFORMATION_SCHEMA.COLUMNS system views
        but querying INFORMATION_SCHEMA is slow especially when lot of taps running
        in parallel.

        Selecting from a real table instead of INFORMATION_SCHEMA and keeping it
        in memory while the target-snowflake is running results better load performance.
        """
        information_schema_cache = target_snowflake.load_information_schema_cache(self.config)
        target_snowflake.persist_lines(self.config, lines, information_schema_cache)

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

    def assert_three_streams_are_into_snowflake(self, should_metadata_columns_exist=False,
                                                should_hard_deleted_rows=False):
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

    def assert_logical_streams_are_in_snowflake(self, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = snowflake.query("SELECT CID, CTIMENTZ, CTIMETZ FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY CID".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'CID': 1, 'CVARCHAR': "inserted row", 'CVARCHAR2': None},
            {'CID': 2, 'CVARCHAR': 'inserted row', "CVARCHAR2": "inserted row"},
            {'CID': 3, 'CVARCHAR': "inserted row", 'CVARCHAR2': "inserted row"},
            {'CID': 4, 'CVARCHAR': "inserted row", 'CVARCHAR2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = [
            {'CID': 1, 'CVARCHAR': "updated row"},
            {'CID': 2, 'CVARCHAR': 'updated row'},
            {'CID': 3, 'CVARCHAR': "updated row"},
            {'CID': 5, 'CVARCHAR': "updated row"},
            {'CID': 7, 'CVARCHAR': "updated row"},
            {'CID': 8, 'CVARCHAR': 'updated row'},
            {'CID': 9, 'CVARCHAR': "updated row"},
            {'CID': 10, 'CVARCHAR': 'updated row'}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'CID': 1, 'CVARCHAR': "updated row"},
            {'CID': 2, 'CVARCHAR': 'updated row'},
            {'CID': 3, 'CVARCHAR': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'CID': 1, 'CTIMENTZ': None, 'CTIMETZ': None},
            {'CID': 2, 'CTIMENTZ': datetime.time(23, 0, 15), 'CTIMETZ': datetime.time(23, 0, 15)},
            {'CID': 3, 'CTIMENTZ': datetime.time(12, 0, 15), 'CTIMETZ': datetime.time(12, 0, 15)},
            {'CID': 4, 'CTIMENTZ': datetime.time(12, 0, 15), 'CTIMETZ': datetime.time(9, 0, 15)},
            {'CID': 5, 'CTIMENTZ': datetime.time(12, 0, 15), 'CTIMETZ': datetime.time(15, 0, 15)},
            {'CID': 6, 'CTIMENTZ': datetime.time(0, 0), 'CTIMETZ': datetime.time(0, 0)},
            {'CID': 8, 'CTIMENTZ': datetime.time(0, 0), 'CTIMETZ': datetime.time(1, 0)},
            {'CID': 9, 'CTIMENTZ': datetime.time(0, 0), 'CTIMETZ': datetime.time(0, 0)}
        ]

        if should_metadata_columns_exist:
            self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_two), expected_table_two)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_three), expected_table_three)
            self.assertEqual(table_four, expected_table_four)
        else:
            self.assertEqual(table_one, expected_table_one)
            self.assertEqual(table_two, expected_table_two)
            self.assertEqual(table_three, expected_table_three)
            self.assertEqual(table_four, expected_table_four)

    def assert_logical_streams_are_in_snowflake_and_are_empty(self):
        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = snowflake.query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = snowflake.query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = snowflake.query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = snowflake.query("SELECT CID, CTIMENTZ, CTIMETZ FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY CID".format(target_schema))

        self.assertEqual(table_one, [])
        self.assertEqual(table_two, [])
        self.assertEqual(table_three, [])
        self.assertEqual(table_four, [])

    #################################
    #           TESTS               #
    #################################

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with assert_raises(json.decoder.JSONDecodeError):
            self.persist_lines_with_cache(tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with assert_raises(Exception):
            self.persist_lines_with_cache(tap_lines)

    def test_loading_tables_with_no_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['client_side_encryption_master_key'] = ''
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

    def test_loading_tables_with_client_side_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['client_side_encryption_master_key'] = os.environ.get('CLIENT_SIDE_ENCRYPTION_MASTER_KEY')
        self.persist_lines_with_cache(tap_lines)

        self.assert_three_streams_are_into_snowflake()

    def test_loading_tables_with_client_side_encryption_and_wrong_master_key(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load but using a well formatted but wrong master key
        self.config['client_side_encryption_master_key'] = "Wr0n6m45t3rKeY0123456789a0123456789a0123456="
        with assert_raises(ProgrammingError):
            self.persist_lines_with_cache(tap_lines)

    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake(should_metadata_columns_exist=True)

    def test_loading_tables_with_defined_parallelism(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Using fixed 1 thread parallelism
        self.config['parallelism'] = 1
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake()

    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_snowflake(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )

    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_into_snowflake(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )

    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = snowflake.query("SELECT * FROM {}.test_table_unicode ORDER BY C_INT".format(target_schema))

        self.assertEqual(
            table_unicode,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                {'C_INT': 3, 'C_PK': 3,
                 'C_VARCHAR': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                {'C_INT': 4, 'C_PK': 4, 'C_VARCHAR': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                {'C_INT': 5, 'C_PK': 5, 'C_VARCHAR': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                {'C_INT': 6, 'C_PK': 6, 'C_VARCHAR': 'Special Characters: [",\'!@£$%^&*()]'}
            ])

    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = snowflake.query(
            "SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

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
        self.persist_lines_with_cache(tap_lines)

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
        self.persist_lines_with_cache(tap_lines)

        # Get loaded rows from tables
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = snowflake.query(
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(target_schema))

        # Should be flattened columns
        self.assertEqual(
            flattened_table,
            [{
                'C_PK': 1,
                'C_ARRAY': '[\n  1,\n  2,\n  3\n]',
                'C_OBJECT': None,
                # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                'C_OBJECT_WITH_PROPS__KEY_1': 'value_1',
                'C_NESTED_OBJECT__NESTED_PROP_1': 'nested_value_1',
                'C_NESTED_OBJECT__NESTED_PROP_2': 'nested_value_2',
                'C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_1': 'multi_value_1',
                'C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_2': 'multi_value_2',
            }])

    def test_column_name_change(self):
        """Tests correct renaming of snowflake columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            'messages-with-three-streams-modified-column.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines_before_column_name_change)
        self.persist_lines_with_cache(tap_lines_after_column_name_change)

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
                {previous_column_name: datetime.datetime(2019, 2, 1, 15, 12, 45), 'C_INT': 1, 'C_PK': 1,
                 'C_VARCHAR': '1', 'C_DATE': None},
                {previous_column_name: datetime.datetime(2019, 2, 10, 2), 'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': '2',
                 'C_DATE': '2019-02-12 02:00:00'},
                {previous_column_name: None, 'C_INT': 3, 'C_PK': 3, 'C_VARCHAR': '2', 'C_DATE': '2019-02-15 02:00:00'}
            ]
        )

        # Table three should have renamed columns
        self.assertEqual(
            table_three,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_TIME': datetime.time(4, 0), 'C_VARCHAR': '1', 'C_TIME_RENAMED': None},
                {'C_INT': 2, 'C_PK': 2, 'C_TIME': datetime.time(7, 15), 'C_VARCHAR': '2', 'C_TIME_RENAMED': None},
                {'C_INT': 3, 'C_PK': 3, 'C_TIME': datetime.time(23, 0, 3), 'C_VARCHAR': '3',
                 'C_TIME_RENAMED': datetime.time(8, 15)},
                {'C_INT': 4, 'C_PK': 4, 'C_TIME': None, 'C_VARCHAR': '4', 'C_TIME_RENAMED': datetime.time(23, 0, 3)}
            ])

    def test_logical_streams_from_pg_with_hard_delete_and_default_batch_size_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_and_no_records_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams-no-records.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines_with_cache(tap_lines)

        self.assert_logical_streams_are_in_snowflake_and_are_empty()

    def test_information_schema_cache_create_and_update(self):
        """Newly created and altered tables must be cached automatically for later use.

        Information_schema_columns cache is a copy of snowflake INFORMATION_SCHAME.COLUMNS table to avoid the error of
        'Information schema query returned too much data. Please repeat query with more selective predicates.'.
        """
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            'messages-with-three-streams-modified-column.json')

        # Load with default settings
        self.persist_lines_with_cache(tap_lines_before_column_name_change)
        self.persist_lines_with_cache(tap_lines_after_column_name_change)

        # Get data form information_schema cache table
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        information_schema_cache = snowflake.query("""
            SELECT table_schema
                  ,table_name
                  ,column_name
                  ,data_type
              FROM {}.columns
             WHERE table_schema = '{}'
             ORDER BY table_schema, table_name, column_name
            """.format(
                snowflake.pipelinewise_schema,
                target_schema.upper()))

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

        # Every column has to be in the cached information_schema with the latest versions
        self.assertEqual(
            information_schema_cache,
            [
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_ONE', 'COLUMN_NAME': 'C_INT',
                 'DATA_TYPE': 'NUMBER'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_ONE', 'COLUMN_NAME': 'C_PK',
                 'DATA_TYPE': 'NUMBER'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_ONE', 'COLUMN_NAME': 'C_VARCHAR',
                 'DATA_TYPE': 'TEXT'},

                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_THREE', 'COLUMN_NAME': 'C_INT',
                 'DATA_TYPE': 'NUMBER'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_THREE', 'COLUMN_NAME': 'C_PK',
                 'DATA_TYPE': 'NUMBER'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_THREE', 'COLUMN_NAME': 'C_TIME',
                 'DATA_TYPE': 'TIME'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_THREE', 'COLUMN_NAME': 'C_TIME_RENAMED',
                 'DATA_TYPE': 'TIME'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_THREE', 'COLUMN_NAME': 'C_VARCHAR',
                 'DATA_TYPE': 'TEXT'},

                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_TWO', 'COLUMN_NAME': 'C_DATE',
                 'DATA_TYPE': 'TEXT'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_TWO', 'COLUMN_NAME': previous_column_name,
                 'DATA_TYPE': 'TIMESTAMP_NTZ'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_TWO', 'COLUMN_NAME': 'C_INT',
                 'DATA_TYPE': 'NUMBER'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_TWO', 'COLUMN_NAME': 'C_PK',
                 'DATA_TYPE': 'NUMBER'},
                {'TABLE_SCHEMA': target_schema.upper(), 'TABLE_NAME': 'TEST_TABLE_TWO', 'COLUMN_NAME': 'C_VARCHAR',
                 'DATA_TYPE': 'TEXT'}
            ])

    def test_information_schema_cache_outdated(self):
        """If informations schema cache is not up to date then it should fail"""
        tap_lines_with_multi_streams = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # 1) Simulate an out of data cache:
        # Table is in cache but not exists in database
        snowflake = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '').upper()
        snowflake.query("""
            CREATE TABLE IF NOT EXISTS {}.columns (table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, data_type VARCHAR)
        """.format(snowflake.pipelinewise_schema))
        snowflake.query("""
            INSERT INTO {0}.columns (table_schema, table_name, column_name, data_type)
            SELECT '{1}', 'TEST_TABLE_ONE', 'DUMMY_COLUMN_1', 'TEXT' UNION
            SELECT '{1}', 'TEST_TABLE_ONE', 'DUMMY_COLUMN_2', 'TEXT' UNION
            SELECT '{1}', 'TEST_TABLE_TWO', 'DUMMY_COLUMN_3', 'TEXT'
        """.format(snowflake.pipelinewise_schema, target_schema))

        # Loading into an outdated information_schema cache should fail with table not exists
        with self.assertRaises(Exception):
            self.persist_lines_with_cache(tap_lines_with_multi_streams)

        # 2) Simulate an out of data cache:
        # Table is in cache structure is not in sync with the actual table in the database
        snowflake.query("CREATE SCHEMA IF NOT EXISTS {}".format(target_schema))
        snowflake.query("CREATE OR REPLACE TABLE {}.test_table_one (C_PK NUMBER, C_INT NUMBER, C_VARCHAR TEXT)".format(target_schema))

        # Loading into an outdated information_schema cache should fail with columns exists
        # It should try adding the new column based on the values in cache but the column already exists
        with self.assertRaises(Exception):
            self.persist_lines_with_cache(tap_lines_with_multi_streams)

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_with_no_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when no intermediate flush required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size big enough to never has to flush in the middle
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.persist_lines_with_cache(tap_lines)

        # State should be emitted only once with the latest received STATE message
        self.assertEquals(
            mock_emit_state.mock_calls,
            [
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}})
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_snowflake(True)

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_with_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when intermediate flushes required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.persist_lines_with_cache(tap_lines)

        # State should be emitted multiple times, updating the positions only in the stream which got flushed
        self.assertEquals(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flushed edgydata until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #2 - Flushed logical1-logical1_table2 until lsn: 108201336
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #3 - Flushed logical1-logical1_table2 until lsn: 108237600
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #4 - Flushed logical1-logical1_table2 until lsn: 108238768
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #5 - Flushed logical1-logical1_table2 until lsn: 108239704,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream lsn: 108240872,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_snowflake(True)

    @mock.patch('target_snowflake.emit_state')
    def test_flush_streams_with_intermediate_flushes_on_all_streams(self, mock_emit_state):
        """Test emitting states when intermediate flushes required and flush_all_streams is enabled"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.config['flush_all_streams'] = True
        self.persist_lines_with_cache(tap_lines)

        # State should be emitted 6 times, flushing every stream and updating every stream position
        self.assertEquals(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flush every stream until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #2 - Flush every stream until lsn 108201336
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #3 - Flush every stream until lsn: 108237600
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #4 - Flush every stream until lsn: 108238768
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #5 - Flush every stream until lsn: 108239704,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream until lsn: 108240872,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_snowflake(True)
