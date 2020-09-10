import os
import boto3
import datetime
from singer import get_logger
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import SnowflakeFileEncryptionMaterial

class S3UploadClient: 
     
    def __init__(self, connection_config):
        self.connection_config = connection_config
        self.logger = get_logger('target_snowflake')
        self.s3 = self.create_s3_client()


    def create_s3_client(self, config=None):
        if not config:
            config = self.connection_config

        # Get the required parameters from config file and/or environment variables
        aws_profile = config.get('aws_profile') or os.environ.get('AWS_PROFILE')
        aws_access_key_id = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
        aws_session_token = config.get('aws_session_token') or os.environ.get('AWS_SESSION_TOKEN')

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        # Create the s3 client
        return aws_session.client('s3',
                                  region_name=config.get('s3_region_name'),
                                  endpoint_url=config.get('s3_endpoint_url'))

    def upload_file(self, file, stream, temp_dir=None):
        # Generating key in S3 bucket
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_key_prefix = self.connection_config.get('s3_key_prefix', '')
        s3_key = "{}pipelinewise_{}_{}.csv".format(s3_key_prefix, stream, datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f"))

        self.logger.info("Target S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, file, s3_key))

        # Encrypt csv if client side encryption enabled
        master_key = self.connection_config.get('client_side_encryption_master_key', '')
        if master_key != '':
            # Encrypt the file
            encryption_material = SnowflakeFileEncryptionMaterial(
                query_stage_master_key=master_key,
                query_id='',
                smk_id=0
            )
            encryption_metadata, encrypted_file = SnowflakeEncryptionUtil.encrypt_file(
                encryption_material,
                file,
                tmp_dir=temp_dir
            )

            # Upload to s3
            extra_args = {'ACL': s3_acl} if s3_acl else dict()

            # Send key and iv in the metadata, that will be required to decrypt and upload the encrypted file
            extra_args['Metadata'] = {
                'x-amz-key': encryption_metadata.key,
                'x-amz-iv': encryption_metadata.iv
            }
            self.s3.upload_file(encrypted_file, bucket, s3_key, ExtraArgs=extra_args)

            # Remove the uploaded encrypted file
            os.remove(encrypted_file)

        # Upload to S3 without encrypting
        else:
            extra_args = {'ACL': s3_acl} if s3_acl else None
            self.s3.upload_file(file, bucket, s3_key, ExtraArgs=extra_args)

        return s3_key

    def delete_object(self, stream, s3_key):
        self.logger.info("Deleting {} from external snowflake stage on S3".format(s3_key))
        bucket = self.connection_config['s3_bucket']
        self.s3.delete_object(Bucket=bucket, Key=s3_key)

