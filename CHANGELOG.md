1.9.1 (2020-12-02)
-------------------

- Fixed a dependency issue
- Add everything from the unreleased `1.9.0`

1.9.0 (2020-11-18) - NOT RELEASED TO PyPI
-----------------------------------------

- Use snowflake table stages by default to load data into tables
- Add optional `query_tag` parameter
- Add optional `role` parameter to use custom roles
- Fixed an issue when generated file names were not compatible with windows
- Bump `joblib` to `0.16.0` to be python 3.8 compatible
- Bump `snowflake-connectory-python` to `2.3.6`
- Bump `boto3` to `1.16.20`

1.8.0 (2020-08-03)
-------------------

- Fixed an issue when `pipelinewise-target-snowflake` failed when `QUOTED_IDENTIFIERS_IGNORE_CASE` snowflake parameter set to true
- Add `aws_profile` option to support Profile based authentication to S3
- Add option to authenticate to S3 using `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` environment variables
- Add `s3_endpoint_url` and `s3_region_name` options to support non-native S3 accounts
- Flush stream only if the new schema is not the same as the previous one

1.7.0 (2020-07-23)
-------------------

- Add `s3_acl` option to support ACL for S3 upload
- Fixed an issue when no primary key error logged as `INFO` and not as `ERROR`

1.6.6 (2020-06-26)
-------------------

- Fixed an issue when new columns sometimes not added to target table
- Fixed an issue when the query runner returned incorrect value when multiple queries running in one transaction

1.6.5 (2020-06-17)
-------------------

- Switch jsonschema to use Draft7Validator

1.6.4 (2020-04-20)
-------------------

- Fix loading tables with space in the name

1.6.3 (2020-04-03)
-------------------

- Generate compressed CSV files by default. Optionally can be disabled by the `no_compression` config option

1.6.2 (2020-03-23)
-------------------

- Log inserts, updates and csv size_bytes in a more consumable format

1.6.1 (2020-03-12)
-------------------

- Use SHOW SCHEMAS|TABLES|COLUMNS instead of INFORMATION_SCHEMA

1.6.0 (2020-03-06)
-------------------

- Support usage of reserved words as table names.

1.5.0 (2020-02-18)
-------------------

- Support custom logging configuration by setting `LOGGING_CONF_FILE` env variable to the absolute path of a .conf file

1.4.1 (2020-01-31)
-------------------

- Change default /tmp folder for encrypting files

1.4.0 (2020-01-28)
-------------------

- Make AWS key optional and obtain it secondarily from env vars

1.3.0 (2020-01-15)
-------------------

- Add temp_dir optional parameter to config

1.2.1 (2020-01-13)
-------------------

- Fixed issue when JSON value not sent correctly

1.2.0 (2020-01-07)
-------------------

- Load binary data into Snowflake BINARY data type column

1.1.8 (2019-12-09)
-------------------

- Add missing module `python-dateutil`

1.1.7 (2019-12-09)
-------------------

- Review dates & timestamps and fix them before insert/update

1.1.6 (2019-11-05)
-------------------

- Pinned stable version of `urllib3`

1.1.5 (2019-11-04)
-------------------

- Pinned stable version of `botocore` and `boto3`

1.1.4 (2019-11-04)
-------------------

- Fixed issue when extracting bookmarks from the state messages sometimes failed
 
1.1.3 (2019-11-04)
-------------------

- Bump `snowflake-connector-python` to 2.0.3

1.1.2 (2019-10-25)
-------------------

- Fixed an issue when number of rows in buckets were not calculated correctly and caused flushing of data at the wrong time with degraded performance
 
1.1.1 (2019-10-18)
-------------------

- Fixed an issue when sometimes the last bucket of data was not flushed correctly 

1.1.0 (2019-10-14)
-------------------

- Bump `snowflake-connector-python` to 2.0.1
- Always use secure connection to Snowflake and force auto commit
- Add `flush_all_streams` option
- Add `parallelism` option
- Add `max_parallelism` option

1.0.9 (2019-10-01)
-------------------

- Emit new state message as soon as data flushed to Snowflake

1.0.8 (2019-08-16)
-------------------

- Log SQLs only in debug mode

1.0.7 (2019-08-06)
-------------------

- Further improvements in `information_schema.tables` caching

1.0.6 (2019-07-26)
-------------------

- Improved and optimised `information_schema.tables` caching

1.0.5 (2019-07-17)
-------------------

- Caching `information_schema.tables` to avoid long running SQLs in snowflake
- Instead of DROPPING exiting column RENAME it

1.0.4 (2019-07-01)
-------------------

- Add `data_flattening_max_level` option

1.0.3 (2019-06-29)
-------------------

- Optimised queries to `information_schema.tables`

1.0.2 (2019-06-11)
-------------------

- Create `_sdc_deleted_at` as `VARCHAR` to avoid issues caused by invalid formatted date-times received from taps

1.0.1 (2019-06-07)
-------------------

- Manage only three metadata columns: `_sdc_extracted_at`, `_sdc_batched_at` and `_sdc_deleted_at`

1.0.0 (2019-06-03)
-------------------

- Initial release
