import json
import sys
from typing import List, Dict, Union

import snowflake.connector
import re
import time

from singer import get_logger
import target_snowflake.flattening as flattening
import target_snowflake.stream_utils as stream_utils
from target_snowflake.file_format import FileFormat, FileFormatTypes

from target_snowflake.exceptions import TooManyRecordsException
from target_snowflake.upload_clients.s3_upload_client import S3UploadClient
from target_snowflake.upload_clients.snowflake_upload_client import SnowflakeUploadClient


def validate_config(config):
    """Validate configuration"""
    errors = []
    s3_required_config_keys = [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse',
        's3_bucket',
        'stage',
        'file_format'
    ]

    snowflake_required_config_keys = [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse',
        'file_format'
    ]

    required_config_keys = []

    # Use external stages if both s3_bucket and stage defined
    if config.get('s3_bucket', None) and config.get('stage', None):
        required_config_keys = s3_required_config_keys
    # Use table stage if none s3_bucket and stage defined
    elif not config.get('s3_bucket', None) and not config.get('stage', None):
        required_config_keys = snowflake_required_config_keys
    else:
        errors.append("Only one of 's3_bucket' or 'stage' keys defined in config. "
                      "Use both of them if you want to use an external stage when loading data into snowflake "
                      "or don't use any of them if you want ot use table stages.")

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    # Check if archive load files option is using external stages
    archive_load_files = config.get('archive_load_files', False)
    if archive_load_files and not config.get('s3_bucket', None):
        errors.append('Archive load files option can be used only with external s3 stages. Please define s3_bucket.')

    return errors


def column_type(schema_property):
    """Take a specific schema property and return the snowflake equivalent column type"""
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'text'
    if 'object' in property_type or 'array' in property_type:
        col_type = 'variant'

    # Every date-time JSON value is currently mapped to TIMESTAMP_NTZ
    elif property_format == 'date-time':
        col_type = 'timestamp_ntz'
    elif property_format == 'time':
        col_type = 'time'
    elif property_format == 'binary':
        col_type = 'binary'
    elif 'number' in property_type:
        col_type = 'float'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'text'
    elif 'integer' in property_type:
        col_type = 'number'
    elif 'boolean' in property_type:
        col_type = 'boolean'

    return col_type


def column_trans(schema_property):
    """Generate SQL transformed columns syntax"""
    property_type = schema_property['type']
    col_trans = ''
    if 'object' in property_type or 'array' in property_type:
        col_trans = 'parse_json'
    elif schema_property.get('format') == 'binary':
        col_trans = 'to_binary'

    return col_trans


def safe_column_name(name):
    """Generate SQL friendly column name"""
    return '"{}"'.format(name).upper()

def json_element_name(name):
    """Generate SQL friendly semi structured element reference name"""
    return '"{}"'.format(name)

def column_clause(name, schema_property):
    """Generate DDL column name with column type string"""
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def primary_column_names(stream_schema_message):
    """Generate list of SQL friendly PK column names"""
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


# pylint: disable=invalid-name
def create_query_tag(query_tag_pattern: str, database: str = None, schema: str = None, table: str = None) -> str:
    """
    Generate a string to tag executed queries in Snowflake.
    Replaces tokens `schema` and `table` with the appropriate values.

    Example with tokens:
        'Loading data into {schema}.{table}'

    Args:
        query_tag_pattern:
        database: optional value to replace {{database}} token in query_tag_pattern
        schema: optional value to replace {{schema}} token in query_tag_pattern
        table: optional value to replace {{table}} token in query_tag_pattern

    Returns:
        String if query_tag_patter defined otherwise None
    """
    if not query_tag_pattern:
        return None

    query_tag = query_tag_pattern

    # replace tokens, taking care of json formatted value compatibility
    for k, v in {
        '{{database}}': json.dumps(database.strip('"')).strip('"') if database else None,
        '{{schema}}': json.dumps(schema.strip('"')).strip('"') if schema else None,
        '{{table}}': json.dumps(table.strip('"')).strip('"') if table else None
    }.items():
        if k in query_tag:
            query_tag = query_tag.replace(k, v or '')

    return query_tag


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class DbSync:
    """DbSync class"""

    def __init__(self, connection_config, stream_schema_message=None, table_cache=None, file_format_type=None):
        """
            connection_config:      Snowflake connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating CSV and before
                                    uploading data into Snowflake.

                                    If stream_schema_message is not defined that we can use
                                    the DbSync instance as a generic purpose connection to
                                    Snowflake and can run individual queries. For example
                                    collecting catalog informations from Snowflake for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message
        self.table_cache = table_cache

        # logger to be used across the class's methods
        self.logger = get_logger('target_snowflake')

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            self.logger.error('Invalid configuration:\n   * %s', '\n   * '.join(config_errors))
            sys.exit(1)

        if self.connection_config.get('stage', None):
            stage = stream_utils.stream_name_to_dict(self.connection_config['stage'], separator='.')
            if not stage['schema_name']:
                self.logger.error(
                    "The named external stage object in config has to use the <schema>.<stage_name> format.")
                sys.exit(1)

        self.schema_name = None
        self.grantees = None
        self.file_format = FileFormat(self.connection_config['file_format'], self.query, file_format_type)

        if not self.connection_config.get('stage') and self.file_format.file_format_type == FileFormatTypes.PARQUET:
            self.logger.error("Table stages with Parquet file format is not suppported. "
                              "Use named stages with Parquet file format or table stages with CSV files format")
            sys.exit(1)

        # Init stream schema pylint: disable=line-too-long
        if self.stream_schema_message is not None:
            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #                                     not specified explicitly for a given stream in
            #                                     the `schema_mapping` object
            #   2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
            #                                     Example config.json:
            #                                           "schema_mapping": {
            #                                               "my_tap_stream_id": {
            #                                                   "target_schema": "my_snowflake_schema",
            #                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_utils.stream_name_to_dict(stream_name)['schema_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')
            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception(
                    "Target schema name not defined in config. "
                    "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines "
                    "target schema for {} stream.".format(stream_name))

            #  Define grantees
            #  ---------------
            #  Grantees can be defined in multiple ways:
            #
            #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to a given role
            #                                                       for every incoming stream if not specified explicitly
            #                                                       in the `schema_mapping` object
            #   2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
            #                                                       for a given stream.
            #                                                       Example config.json:
            #                                                           "schema_mapping": {
            #                                                               "my_tap_stream_id": {
            #                                                                   "target_schema": "my_snowflake_schema",
            #                                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                                               }
            #                                                           }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions',
                                                                              self.grantees)

            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flattening.flatten_schema(stream_schema_message['schema'],
                                                            max_level=self.data_flattening_max_level)

        # Use external stage
        if connection_config.get('s3_bucket', None):
            self.upload_client = S3UploadClient(connection_config)
        # Use table stage
        else:
            # Enforce no parallelism with table stages.
            # The PUT command in the snowflake-python-connector is using boto3.create_client() function which is not
            # thread safe. More info at https://github.com/boto/boto3/issues/801
            if connection_config.get('parallelism') != 1:
                self.logger.warning('Enforcing to use single thread parallelism with table stages. '
                                    'The PUT command in the snowflake-python-connector is using boto3 create_client() '
                                    'function which is not thread safe. '
                                    'If you need parallel file upload please use external stage by adding s3_bucket '
                                    'key to the configuration')
                connection_config['parallelism'] = 1

            self.upload_client = SnowflakeUploadClient(connection_config, self)

    def open_connection(self):
        """Open snowflake connection"""
        stream = None
        if self.stream_schema_message:
            stream = self.stream_schema_message['stream']

        return snowflake.connector.connect(
            user=self.connection_config['user'],
            password=self.connection_config['password'],
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse'],
            role=self.connection_config.get('role', None),
            autocommit=True,
            session_parameters={
                # Quoted identifiers should be case sensitive
                'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
                'QUERY_TAG': create_query_tag(self.connection_config.get('query_tag'),
                                              database=self.connection_config['dbname'],
                                              schema=self.schema_name,
                                              table=self.table_name(stream, False, True))
            }
        )

    def query(self, query: Union[str, List[str]], params: Dict = None, max_records=0) -> List[Dict]:
        """Run an SQL query in snowflake"""
        result = []

        if params is None:
            params = {}
        else:
            if 'LAST_QID' in params:
                self.logger.warning('LAST_QID is a reserved prepared statement parameter name, '
                                    'it will be overridden with each executed query!')

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    self.logger.info('Starting Transaction')
                    cur.execute("START TRANSACTION")
                    queries = query
                else:
                    queries = [query]

                qid = None

                # pylint: disable=invalid-name
                for q in queries:

                    # update the LAST_QID
                    params['LAST_QID'] = qid

                    self.logger.info("Running query: '%s' with Params %s", q, params)

                    cur.execute(q, params)
                    qid = cur.sfqid

                    # Raise exception if returned rows greater than max allowed records
                    if 0 < max_records < cur.rowcount:
                        raise TooManyRecordsException(
                            f"Query returned too many records. This query can return max {max_records} records")

                    result = cur.fetchall()

        return result

    def table_name(self, stream_name, is_temporary, without_schema=False):
        """Generate target table name"""
        if not stream_name:
            return None

        stream_dict = stream_utils.stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        sf_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            sf_table_name = '{}_temp'.format(sf_table_name)

        if without_schema:
            return f'"{sf_table_name.upper()}"'

        return f'{self.schema_name}."{sf_table_name.upper()}"'

    def record_primary_key_string(self, record):
        """Generate a unique PK string in the record"""
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flattening.flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)
        try:
            key_props = [str(flatten[p]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            self.logger.error(
                'Cannot find %s primary key(s) in record: %s', self.stream_schema_message['key_properties'],
                flatten)
            raise exc
        return ','.join(key_props)

    def put_to_stage(self, file, stream, count, temp_dir=None):
        """Upload file to snowflake stage"""
        self.logger.info('Uploading %d rows to stage', count)
        return self.upload_client.upload_file(file, stream, temp_dir)

    def delete_from_stage(self, stream, s3_key):
        """Delete file from snowflake stage"""
        self.logger.info('Deleting %s from stage', format(s3_key))
        self.upload_client.delete_object(stream, s3_key)

    def copy_to_archive(self, s3_source_key, s3_archive_key, s3_archive_metadata):
        """
        Copy file from snowflake stage to archive.

        s3_source_key: The s3 key to copy, assumed to exist in the bucket configured as 's3_bucket'

        s3_archive_key: The key to use in archive destination. This will be prefixed with the config value
                        'archive_load_files_s3_prefix'. If none is specified, 'archive' will be used as the prefix.

                        As destination bucket, the config value 'archive_load_files_s3_bucket' will be used. If none is
                        specified, the bucket configured as 's3_bucket' will be used.

        s3_archive_metadata: This dict will be merged with any metadata in the source file.

        """
        source_bucket = self.connection_config.get('s3_bucket')

        # Get archive s3_bucket from config, or use same bucket if not specified
        archive_bucket = self.connection_config.get('archive_load_files_s3_bucket', source_bucket)

        # Determine prefix to use in archive s3 bucket
        default_archive_prefix = 'archive'
        archive_prefix = self.connection_config.get('archive_load_files_s3_prefix', default_archive_prefix)
        prefixed_archive_key = '{}/{}'.format(archive_prefix, s3_archive_key)

        copy_source = '{}/{}'.format(source_bucket, s3_source_key)

        self.logger.info('Copying %s to archive location %s', copy_source, prefixed_archive_key)
        self.upload_client.copy_object(copy_source, archive_bucket, prefixed_archive_key, s3_archive_metadata)

    def get_stage_name(self, stream):
        """Generate snowflake stage name"""
        stage = self.connection_config.get('stage', None)
        if stage:
            return stage

        table_name = self.table_name(stream, False, without_schema=True)
        return f"{self.schema_name}.%{table_name}"

    def load_file(self, s3_key, count, size_bytes):
        """Load a supported file type from snowflake stage into target table"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        self.logger.info("Loading %d rows into '%s'", count, self.table_name(stream, False))

        # Get list if columns with types
        columns_with_trans = [
            {
                "name": safe_column_name(name),
                "json_element_name": json_element_name(name),
                "trans": column_trans(schema)
            }
            for (name, schema) in self.flatten_schema.items()
        ]

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                inserts = 0
                updates = 0

                # Insert or Update with MERGE command if primary key defined
                if len(self.stream_schema_message['key_properties']) > 0:
                    merge_sql = self.file_format.formatter.create_merge_sql(table_name=self.table_name(stream, False),
                                                                            stage_name=self.get_stage_name(stream),
                                                                            s3_key=s3_key,
                                                                            file_format_name=
                                                                                self.connection_config['file_format'],
                                                                            columns=columns_with_trans,
                                                                            pk_merge_condition=
                                                                                self.primary_key_merge_condition())
                    self.logger.debug('Running query: %s', merge_sql)
                    cur.execute(merge_sql)

                    # Get number of inserted and updated records - MERGE does insert and update
                    results = cur.fetchall()
                    if len(results) > 0:
                        inserts = results[0].get('number of rows inserted', 0)
                        updates = results[0].get('number of rows updated', 0)

                # Insert only with COPY command if no primary key
                else:
                    copy_sql = self.file_format.formatter.create_copy_sql(table_name=self.table_name(stream, False),
                                                                          stage_name=self.get_stage_name(stream),
                                                                          s3_key=s3_key,
                                                                          file_format_name=
                                                                            self.connection_config['file_format'],
                                                                          columns=columns_with_trans)
                    self.logger.debug('Running query: %s', copy_sql)
                    cur.execute(copy_sql)

                    # Get number of inserted records - COPY does insert only
                    results = cur.fetchall()
                    if len(results) > 0:
                        inserts = results[0].get('rows_loaded', 0)

                self.logger.info('Loading into %s: %s',
                    self.table_name(stream, False),
                    json.dumps({'inserts': inserts, 'updates': updates, 'size_bytes': size_bytes}))

    def primary_key_merge_condition(self):
        """Generate SQL join condition on primary keys for merge SQL statements"""
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{0} = t.{0}'.format(c) for c in names])

    def column_names(self):
        """Get list of columns in the schema"""
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self, is_temporary=False):
        """Generate CREATE TABLE SQL"""
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message.get('key_properties', [])) > 0 else []

        return 'CREATE {}TABLE IF NOT EXISTS {} ({}) {}'.format(
            'TEMP ' if is_temporary else '',
            self.table_name(stream_schema_message['stream'], is_temporary),
            ', '.join(columns + primary_key),
            'data_retention_time_in_days = 0 ' if is_temporary else 'data_retention_time_in_days = 1 '
        )

    def grant_usage_on_schema(self, schema_name, grantee):
        """Grant usage on schema"""
        query = "GRANT USAGE ON SCHEMA {} TO ROLE {}".format(schema_name, grantee)
        self.logger.info("Granting USAGE privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(query)

    # pylint: disable=invalid-name
    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        """Grant select on all tables in schema"""
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}".format(schema_name, grantee)
        self.logger.info(
            "Granting SELECT ON ALL TABLES privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(query)

    @classmethod
    def grant_privilege(cls, schema, grantees, grant_method):
        """Grant privileges on target schema"""
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def delete_rows(self, stream):
        """Hard delete rows from target table"""
        table = self.table_name(stream, False)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(table)
        self.logger.info("Deleting rows from '%s' table... %s", table, query)
        self.logger.info('DELETE %d', len(self.query(query)))

    def create_schema_if_not_exists(self):
        """Create target schema if not exists"""
        schema_name = self.schema_name
        schema_rows = 0

        # table_cache is an optional pre-collected list of available objects in snowflake
        if self.table_cache:
            schema_rows = list(filter(lambda x: x['SCHEMA_NAME'] == schema_name.upper(), self.table_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(f"SHOW SCHEMAS LIKE '{schema_name.upper()}'")

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
            self.logger.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
            self.query(query)

            self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

            # Refresh columns cache if required
            if self.table_cache:
                self.table_cache = self.get_table_columns(table_schemas=[self.schema_name])

    def get_tables(self, table_schemas=None):
        """Get list of tables of certain schema(s) from snowflake metadata"""
        tables = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get tables in schema
                show_tables = f"SHOW TERSE TABLES IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW TABLES to table
                select = """
                    SELECT "schema_name" AS schema_name
                          ,"name"        AS table_name
                      FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """
                queries.extend([show_tables, select])

                # Run everything in one transaction
                try:
                    tables = self.query(queries, max_records=9999)

                # Catch exception when schema not exists and SHOW TABLES throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(r'002043 \(02000\):.*\n.*does not exist.*', str(sys.exc_info()[1])):
                        raise exc
        else:
            raise Exception("Cannot get table columns. List of table schemas empty")

        return tables

    def get_table_columns(self, table_schemas=None):
        """Get list of columns and tables of certain schema(s) from snowflake metadata"""
        table_columns = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get column data types by SHOW COLUMNS
                show_columns = f"SHOW COLUMNS IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW COLUMNS to table and insert results into the cache COLUMNS table
                #
                # ----------------------------------------------------------------------------------------
                # Character and numeric columns display their generic data type rather than their defined
                # data type (i.e. TEXT for all character types, FIXED for all fixed-point numeric types,
                # and REAL for all floating-point numeric types).
                # Further info at https://docs.snowflake.net/manuals/sql-reference/sql/show-columns.html
                # ----------------------------------------------------------------------------------------
                select = """
                    SELECT "schema_name" AS schema_name
                          ,"table_name"  AS table_name
                          ,"column_name" AS column_name
                          ,CASE PARSE_JSON("data_type"):type::varchar
                             WHEN 'FIXED' THEN 'NUMBER'
                             WHEN 'REAL'  THEN 'FLOAT'
                             ELSE PARSE_JSON("data_type"):type::varchar
                           END data_type
                      FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """

                queries.extend([show_columns, select])

                # Run everything in one transaction
                try:
                    columns = self.query(queries, max_records=9999)

                    if not columns:
                        self.logger.warning('No columns discovered in the schema "%s"',
                                            f"{self.connection_config['dbname']}.{schema}")
                    else:
                        table_columns.extend(columns)

                # Catch exception when schema not exists and SHOW COLUMNS throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(r'002003 \(02000\):.*\n.*does not exist or not authorized.*',
                                    str(sys.exc_info()[1])):
                        raise exc

        else:
            raise Exception("Cannot get table columns. List of table schemas empty")

        return table_columns

    def refresh_table_cache(self):
        """Refreshes the internal table cache"""
        self.table_cache = self.get_table_columns([self.schema_name])

    def update_columns(self):
        """Adds required but not existing columns the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        all_table_columns = []

        if self.table_cache:
            all_table_columns = self.table_cache
        else:
            all_table_columns = self.get_table_columns(table_schemas=[self.schema_name])

        # Find the specific table
        columns = list(filter(lambda x: x['SCHEMA_NAME'] == self.schema_name.upper() and
                                        f'"{x["TABLE_NAME"].upper()}"' == table_name,
                              all_table_columns))

        columns_dict = {column['COLUMN_NAME'].upper(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.flatten_schema.items()
            if name.upper() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.upper() in columns_dict and
               columns_dict[name.upper()]['DATA_TYPE'].upper() != column_type(properties_schema).upper() and

               # Don't alter table if TIMESTAMP_NTZ detected as the new required column type
               #
               # Target-snowflake maps every data-time JSON types to TIMESTAMP_NTZ but sometimes
               # a TIMESTAMP_TZ column is already available in the target table (i.e. created by fastsync initial load)
               # We need to exclude this conversion otherwise we loose the data that is already populated
               # in the column
               column_type(properties_schema).upper() != 'TIMESTAMP_NTZ'
        ]

        for (column_name, column) in columns_to_replace:
            # self.drop_column(column_name, stream)
            self.version_column(column_name, stream)
            self.add_column(column, stream)

        # Refresh table cache if required
        if self.table_cache and (columns_to_add or columns_to_replace):
            self.table_cache = self.get_table_columns(table_schemas=[self.schema_name])

    def drop_column(self, column_name, stream):
        """Drops column from an existing table"""
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream, False), column_name)
        self.logger.info('Dropping column: %s', drop_column)
        self.query(drop_column)

    def version_column(self, column_name, stream):
        """Versions a column in an existing table"""
        version_column = "ALTER TABLE {} RENAME COLUMN {} TO \"{}_{}\"".format(self.table_name(stream, False),
                                                                               column_name,
                                                                               column_name.replace("\"", ""),
                                                                               time.strftime("%Y%m%d_%H%M"))
        self.logger.info('Versioning column: %s', version_column)
        self.query(version_column)

    def add_column(self, column, stream):
        """Adds a new column to an existing table"""
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream, False), column)
        self.logger.info('Adding column: %s', add_column)
        self.query(add_column)

    def sync_table(self):
        """Creates or alters the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        table_name_with_schema = self.table_name(stream, False)

        if self.table_cache:
            found_tables = list(filter(lambda x: x['SCHEMA_NAME'] == self.schema_name.upper() and
                                                 f'"{x["TABLE_NAME"].upper()}"' == table_name,
                                       self.table_cache))
        else:
            found_tables = [table for table in (self.get_tables([self.schema_name.upper()]))
                            if f'"{table["TABLE_NAME"].upper()}"' == table_name]

        if len(found_tables) == 0:
            query = self.create_table_query()
            self.logger.info('Table %s does not exist. Creating...', table_name_with_schema)
            self.query(query)

            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)

            # Refresh columns cache if required
            if self.table_cache:
                self.table_cache = self.get_table_columns(table_schemas=[self.schema_name])
        else:
            self.logger.info('Table %s exists', table_name_with_schema)
            self.update_columns()
