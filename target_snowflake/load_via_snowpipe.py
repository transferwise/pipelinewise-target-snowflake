import os
import datetime
import botocore
from snowflake.connector.encryption_util import SnowflakeEncryptionUtil
from snowflake.connector.remote_storage_util import SnowflakeFileEncryptionMaterial
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption

class LoadViaSnowpipe: 
     
    def __init__(self, connection_config):
        self.connection_config = connection_config
        #self.logger = get_logger('target_snowflake')

    # Load to s3 external stage
    def upload_file(self, file, stream, temp_dir=None):
        
        # Generating key in S3 bucket
        bucket = self.connection_config['s3_bucket']
        s3_acl = self.connection_config.get('s3_acl')
        s3_folder_name = "{}/".format(self.table_name(stream, None, True).lower().replace('"',''))
        s3_key_prefix = "{}/{}".format(self.connection_config.get('s3_key_prefix', ''),s3_folder_name)
        s3_key = "{}pipelinewise_{}_{}.csv".format(s3_key_prefix, stream, datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f"))

        self.logger.info("Target S3 bucket: {}, local file: {}, S3 key: {}".format(bucket, file, s3_key))

        # Create s3 folder if it does not exist
        try:
            self.s3.head_object(Bucket=bucket, Key=s3_key_prefix)
            self.logger.info(f"S3 key prefix: {s3_key_prefix} already exists!!")  
        except botocore.errorfactory.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.info(f"S3 key prefix: {s3_key_prefix} does not exist\nCreating s3 prefix...")
                self.s3.put_object(Bucket=bucket, Key=s3_key_prefix)                

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

        return s3_key,s3_folder_name

    def load_via_snowpipe(self,s3_key,s3_folder_name):
        obj_name = s3_folder_name.replace('/','')
        pipe_name = "{0}.{1}.{2}_s3pipe".format(self.connection_config['dbname'],self.schema_name,obj_name)

        create_pipe_sql = """create pipe if not exists {0} as
                            copy into {1}.{2}.{3} from @{1}.{4}
                            file_format = (format_name =  {1}.{5} );""".format(
                                pipe_name,
                                self.connection_config['dbname'],
                                self.schema_name,
                                obj_name,
                                self.connection_config['stage'],
                                self.connection_config['file_format'])

        pipe_status_sql = "select system$pipe_status('{}');".format(pipe_name)

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                # Create snowpipe if it does not exist
                try:
                    cur.execute(pipe_status_sql)
                    self.logger.info("""snowpipe "{}" already exists!!""".format(pipe_name))
                except snowflake.connector.errors.ProgrammingError as excp:
                    self.logger.info("""snowpipe "{}" does not exist. Creating...""".format(pipe_name))
                    cur.execute(create_pipe_sql)
                finally:
                    cur.close()
        
        logging.basicConfig(filename='/tmp/ingest.log',level=logging.DEBUG)
        
        logger = getLogger(__name__)

        # If you generated an encrypted private key, implement this method to return
        # the passphrase for decrypting your private key.
        def get_private_key_passphrase():
            return "CkYt-3fngCdm65P2"

        with open("/home/ec2-user/rsa_key.p8", 'rb') as pem_in:
            # pemlines = pem_in.read()
            private_key_obj = serialization.load_pem_private_key(pem_in.read(),password=get_private_key_passphrase().encode(),backend=default_backend())

        private_key_text = private_key_obj.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode('utf-8')

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
            logger.error(e)
            exit(1)

        # This means Snowflake has received file and will start loading
        assert(resp['responseCode'] == 'SUCCESS')