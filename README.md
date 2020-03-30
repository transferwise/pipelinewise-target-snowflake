# pipelinewise-target-snowflake

[![PyPI version](https://badge.fury.io/py/pipelinewise-target-snowflake.svg)](https://badge.fury.io/py/pipelinewise-target-snowflake)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-target-snowflake.svg)](https://pypi.org/project/pipelinewise-target-snowflake/)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that loads data into Snowflake following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

## How to use it

The recommended method of running this target is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Target Snowflake](https://transferwise.github.io/pipelinewise/connectors/targets/snowflake.html)

If you want to run this [Singer Target](https://singer.io) independently please read further.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-target-snowflake
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

## Flow diagram

![Flow Diagram](flow-diagram.jpg)

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-snowflake --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Snowflake.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

### Pre-requirements

You need to create two objects in snowflake in one schema before start using this target.

1. A named external stage object on S3. This will be used to upload the CSV files to S3 and to MERGE data into snowflake tables.

```
CREATE STAGE {schema}.{stage_name}
url='s3://{s3_bucket}'
credentials=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
encryption=(MASTER_KEY='{client_side_encryption_master_key}');
```

The `encryption` option is optional and used for client side encryption. If you want client side encryption enabled you'll need
to define the same master key in the target `config.json`. Furhter details below in the Configuration settings section.
Instead of `credentials` you can also use `storage_integration`.

2. A named file format. This will be used by the MERGE/COPY commands to parse the CSV files correctly from S3:

`CREATE file format IF NOT EXISTS {schema}.{file_format_name} type = 'CSV' escape='\\' field_optionally_enclosed_by='"';`


### Configuration settings

Running the the target connector requires a `config.json` file. Example with the minimal settings:

   ```json
   {

     "account": "rtxxxxx.eu-central-1",
     "dbname": "database_name",
     "user": "my_user",
     "password": "password",
     "warehouse": "my_virtual_warehouse",
     "s3_bucket": "bucket_name",
     "stage": "snowflake_external_stage_object_name",
     "file_format": "snowflake_file_format_object_name",
     "default_target_schema": "my_target_schema"
   }
   ```

Full list of options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| account                             | String  | Yes        | Snowflake account name (i.e. rtXXXXX.eu-central-1)            |
| dbname                              | String  | Yes        | Snowflake Database name                                       |
| user                                | String  | Yes        | Snowflake User                                                |
| password                            | String  | Yes        | Snowflake Password                                            |
| warehouse                           | String  | Yes        | Snowflake virtual warehouse name                              |
| aws_access_key_id                   | String  | No         | S3 Access Key Id. If not provided, AWS_ACCESS_KEY_ID environment variable or IAM role will be used |
| aws_secret_access_key               | String  | No         | S3 Secret Access Key. If not provided, AWS_SECRET_ACCESS_KEY environment variable or IAM role will be used |
| aws_session_token                   | String  | No         | AWS Session token. If not provided, AWS_SESSION_TOKEN environment variable will be used |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can upload files into specific directories in the S3 bucket. |
| stage                               | String  | Yes        | Named external stage name created at pre-requirements section. Has to be a fully qualified name including the schema name |
| file_format                         | String  | Yes        | Named file format name created at pre-requirements section. Has to be a fully qualified name including the schema name. |
| batch_size_rows                     | Integer |            | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into Snowflake. |
| flush_all_streams                   | Boolean |            | (Default: False) Flush and load every stream into Snowflake when one batch is full. Warning: This may trigger the COPY command to use files with low number of records, and may cause performance problems. |
| parallelism                         | Integer |            | (Default: 0) The number of threads used to flush tables. 0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for each CPU core. Any other positive number will create that number of threads, up to parallelism_max. |
| parallelism_max                     | Integer |            | (Default: 16) Max number of parallel threads to use when flushing tables. |
| default_target_schema               | String  |            | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.    |
| default_target_schema_select_permission | String  |            | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created tables to a specific role or a list of roles. If `schema_mapping` is not defined then every stream sent by the tap is granted accordingly.   |
| schema_mapping                      | Object  |            | Useful if you want to load multiple streams from one tap to multiple Snowflake schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. Note, that using `schema_mapping` you can overwrite the `default_target_schema_select_permission` value to grant SELECT permissions to different groups per schemas or optionally you can create indices automatically for the replicated tables.<br><br> **Note**: This is an experimental feature and recommended to use via PipelineWise YAML files that will generate the object mapping in the right JSON format. For further info check a [PipelineWise YAML Example]
| disable_table_cache                 | Boolean |            | (Default: False) By default the connector caches the available table structures in Snowflake at startup. In this way it doesn't need to run additional queries when ingesting data to check if altering the target tables is required. With `disable_table_cache` option you can turn off this caching. You will always see the most recent table structures but will cause an extra query runtime. |
| client_side_encryption_master_key   | String  |            | (Default: None) When this is defined, Client-Side Encryption is enabled. The data in S3 will be encrypted, No third parties, including Amazon AWS and any ISPs, can see data in the clear. Snowflake COPY command will decrypt the data once it's in Snowflake. The master key must be 256-bit length and must be encoded as base64 string. |
| client_side_encryption_stage_object | String  |            | (Default: None) Required when `client_side_encryption_master_key` is defined. The name of the encrypted stage object in Snowflake that created separately and using the same encryption master key. |
| add_metadata_columns                | Boolean |            | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in snowflake etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_SDC_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_SDC_DELETED_AT` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recongisable in Snowflake. |
| hard_delete                         | Boolean |            | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in Snowflake to delete rows in tables. It's achieved by continuously checking the  `_SDC_DELETED_AT` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well. |
| data_flattening_max_level           | Integer |            | (Default: 0) Object type RECORD items from taps can be loaded into VARIANT columns as JSON (default) or we can flatten the schema by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off. |
| primary_key_required                | Boolean |            | (Default: True) Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined. |
| validate_records                    | Boolean |            | (Default: False) Validate every single record message to the corresponding JSON schema. This option is disabled by default and invalid RECORD messages will fail only at load time by Snowflake. Enabling this option will detect invalid records earlier but could cause performance degradation. |
| temp_dir                            | String  |            | (Default: platform-dependent) Directory of temporary CSV files with RECORD messages. |
| no_compression                      | Boolean |            | (Default: False) Generate uncompressed CSV files when loading to Snowflake. Normally, by default GZIP compressed files are generated. |

### To run tests:

1. Define environment variables that requires running the tests
```
  export TARGET_SNOWFLAKE_ACCOUNT=<snowflake-account-name>
  export TARGET_SNOWFLAKE_DBNAME=<snowflake-database-name>
  export TARGET_SNOWFLAKE_USER=<snowflake-user>
  export TARGET_SNOWFLAKE_PASSWORD=<snowfale-password>
  export TARGET_SNOWFLAKE_WAREHOUSE=<snowflake-warehouse>
  export TARGET_SNOWFLAKE_SCHEMA=<snowflake-schema>
  export TARGET_SNOWFLAKE_AWS_ACCESS_KEY=<aws-access-key-id>
  export TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY=<aws-access-secret-access-key>
  export TARGET_SNOWFLAKE_S3_BUCKET=<s3-external-bucket>
  export TARGET_SNOWFLAKE_S3_KEY_PREFIX=<bucket-directory>
  export TARGET_SNOWFLAKE_STAGE=<stage-object-with-schema-name>
  export TARGET_SNOWFLAKE_FILE_FORMAT=<file-format-object-with-schema-name>
  export CLIENT_SIDE_ENCRYPTION_MASTER_KEY=<client_side_encryption_master_key>
  export CLIENT_SIDE_ENCRYPTION_STAGE_OBJECT=<client_side_encryption_stage_object>
```

2. Install python test dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .[test]
```

3. To run unit tests:
```
  nosetests --where=tests/unit
```

4. To run integration tests:
```
  nosetests --where=tests/integration
```

### To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install pylint
  pylint target_snowflake -d C,W,unexpected-keyword-arg,duplicate-code
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
