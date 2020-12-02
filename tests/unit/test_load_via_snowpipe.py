import mock
import os
import tempfile
import unittest
import boto3
import botocore
from moto import mock_s3

from target_snowflake import load_via_snowpipe

# import target_snowflake


MY_BUCKET = "my_bucket"
MY_PREFIX = "mock_folder"

@mock_s3
class TestDownloadJsonFiles(unittest.TestCase):
    def setUp(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            )
        try:
            s3 = boto3.resource(
                "s3",
                region_name="eu-west-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
                )
            s3.meta.client.head_bucket(Bucket=MY_BUCKET)
        except botocore.exceptions.ClientError:
            pass
        else:
            err = "{bucket} should not exist.".format(bucket=MY_BUCKET)
            raise EnvironmentError(err)
        # client.create_bucket(Bucket=MY_BUCKET)
        # current_dir = os.path.dirname(__file__)
        # fixtures_dir = os.path.join(current_dir, "fixtures")
        # _upload_fixtures(MY_BU
        # CKET, fixtures_dir)
        return client
    def tearDown(self):
        s3 = boto3.resource(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            )
        bucket = s3.Bucket(MY_BUCKET)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    def connection_config(self):
        return dict(TARGET_SNOWFLAKE_ACCOUNT="Fake_account",
                    TARGET_SNOWFLAKE_DBNAME="fake_DBname",
                    TARGET_SNOWFLAKE_USER="fake_user",
                    TARGET_SNOWFLAKE_PASSWORD="fake_password",
                    TARGET_SNOWFLAKE_WAREHOUSE="fake_warehouse",
                    TARGET_SNOWFLAKE_SCHEMA="fake_snowflake-schema",
                    TARGET_SNOWFLAKE_AWS_ACCESS_KEY="fake_aws-access-key-id",
                    TARGET_SNOWFLAKE_AWS_SECRET_ACCESS_KEY="fake_aws-access-secret-access-key",
                    TARGET_SNOWFLAKE_S3_ACL="fake_s3-target-acl",
                    TARGET_SNOWFLAKE_S3_BUCKET="fake_s3-external-bucket",
                    TARGET_SNOWFLAKE_S3_KEY_PREFIX="fakebucket-directory",
                    TARGET_SNOWFLAKE_STAGE="fake_stage-object-with-schema-name",
                    TARGET_SNOWFLAKE_FILE_FORMAT="fake_file-format-object-with-schema-name",
                    CLIENT_SIDE_ENCRYPTION_MASTER_KEY="fake_client_side_encryption_master_key",
                    CLIENT_SIDE_ENCRYPTION_STAGE_OBJECT="client_side_encryption_stage_object",
                    s3_bucket="fake_bucket")


def _upload_fixtures(bucket: str, fixtures_dir: str) -> None:
    client = boto3.client("s3")
    fixtures_paths = [
        os.path.join(path,  filename)
        for path, _, files in os.walk(fixtures_dir)
        for filename in files
    ]
    for path in fixtures_paths:
        key = os.path.relpath(path, fixtures_dir)
        client.upload_file(Filename=path, Bucket=bucket, Key=key)


def test_upload_file():
    connection = TestDownloadJsonFiles()
    conn_config = connection.connection_config()
    load_snow = load_via_snowpipe.LoadViaSnowpipe(conn_config)
    fake_s3_key, fake_s3_foldername = load_snow.upload_file('fake_file.txt', 'fake stream')
    assert fake_s3_key == 0