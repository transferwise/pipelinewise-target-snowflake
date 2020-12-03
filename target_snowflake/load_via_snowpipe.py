import os
import datetime
import botocore
from singer import get_logger
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import SnowflakeFileEncryptionMaterial
from snowflake.ingest import SimpleIngestManager, \
    StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from requests import HTTPError
from cryptography.hazmat.primitives.serialization import load_pem_private_key, \
    Encoding, \
    PrivateFormat, \
    NoEncryption
from cryptography.hazmat.backends import default_backend
from target_snowflake.s3_upload_client import S3UploadClient
from snowflake.connector.errors import ProgrammingError
# from snowflake.connector import DictCursor
class LoadViaSnowpipe: 

    def __init__(self, connection_config, dbLink):
        self.connection_config = connection_config
        self.logger = get_logger('target_snowflake')
        self.s3_upload_client = S3UploadClient(connection_config)
        self.dbLink = dbLink


    def create_s3_folder(self, stream):
        
        bucket = self.connection_config.get('s3_bucket', None)
        if not bucket:
            raise Exception("Cannot run load via snowpipe without s3 bucket")
        s3_folder_name = "{}/".format(self.dbLink.table_name(stream, None, True).lower().replace('"',''))
        if not s3_folder_name:
            raise Exception("S3 folder name is empty")
        # BUG: // in prefix, s3_key_prefix already has /
        s3_key_prefix = "{}{}".format(self.connection_config.get('s3_key_prefix', ''),s3_folder_name)
        try:
            self.s3_upload_client.s3.head_object(Bucket=bucket, Key=s3_key_prefix)
            self.logger.info(f"S3 key prefix: {s3_key_prefix} already exists!!")  
        except botocore.errorfactory.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.info(f"S3 key prefix: {s3_key_prefix} does not exist\nCreating s3 prefix...")
                self.s3_upload_client.s3.put_object(Bucket=bucket, Key=s3_key_prefix)

        self.s3_folder_name = s3_folder_name
        return  s3_folder_name

    def upload_file(self, file, stream, temp_dir=None):
        s3_folder_name = self.create_s3_folder(stream)
        # SAME bug
        s3_key_prefix = "{}{}".format(self.connection_config.get('s3_key_prefix', ''), s3_folder_name)
        s3_key = self.s3_upload_client.upload_file(file, stream, s3_key_prefix=s3_key_prefix)
        return s3_key

    def load_via_snowpipe(self,s3_key):
        obj_name = getattr(self, 's3_folder_name', None).replace('/', '')
        if not obj_name:
            raise Exception("S3 folder name cannot be empty.")
        pipe_name = "{0}.{1}.{2}_s3_pipe".format(self.connection_config['dbname'], self.dbLink.schema_name, obj_name)

        create_pipe_sql = """create pipe if not exists {0} as
                            copy into {1}.{2}.{3} from @{1}.{4}
                            file_format = (format_name =  {1}.{5} );""".format(
                                pipe_name,
                                self.connection_config['dbname'],
                                self.dbLink.schema_name,
                                obj_name,
                                self.connection_config['stage'],
                                self.connection_config['file_format'])
        breakpoint()
        pipe_status_sql = "select system$pipe_status('{}');".format(pipe_name)

        with self.dbLink.open_connection() as connection:
            with connection.cursor() as cur:
                # Create snowpipe if it does not exist
                try:
                    cur.execute(pipe_status_sql)
                    self.logger.info("""snowpipe "{}" already exists!!""".format(pipe_name))
                except ProgrammingError as excp:
                    self.logger.info("""snowpipe "{}" does not exist. Creating...""".format(pipe_name))
                    cur.execute(create_pipe_sql)
                finally:
                    cur.close()

        # If you generated an encrypted private key, implement this method to return
        # the passphrase for decrypting your private key.
        # def get_private_key_passphrase(): #os.getenv('') #cartridge template grab 
        #     return ''

        with open("/upd_rsa_key.p8", 'rb') as pem_in:
            # pemlines = pem_in.read()
            private_key_obj = load_pem_private_key(pem_in.read(),password=None,backend=default_backend())

        private_key_text = private_key_obj.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')
        # breakpoint()
        file_list=[s3_key]
        self.logger.info(file_list)

        ingest_manager = SimpleIngestManager(account=self.connection_config['account'].split('.')[0],
                                        host=self.connection_config['account']+'.snowflakecomputing.com',
                                        user=self.connection_config['user'],
                                        pipe=pipe_name,
                                        scheme='https',
                                        port=443,
                                        private_key=private_key_text)

        # List of files, but wrapped into a class
        staged_file_list = []
        for file_name in file_list:
            staged_file_list.append(StagedFile(file_name, None))

        self.logger.info(staged_file_list)

        try:
            resp = ingest_manager.ingest_files(staged_file_list)
        except HTTPError as e:
            # HTTP error, may need to retry
            self.logger.error(e)
            exit(1)

        # This means Snowflake has received file and will start loading
        assert(resp['responseCode'] == 'SUCCESS')