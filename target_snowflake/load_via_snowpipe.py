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
import target_snowflake.db_sync
from snowflake.connector.errors import ProgrammingError
# from snowflake.connector import DictCursor
class LoadViaSnowpipe:

    def __init__(self, connection_config, dbLink):
        self.connection_config = connection_config
        self.logger = get_logger('target_snowflake')
        self.dbLink = dbLink

    def load_via_snowpipe(self, s3_key):

        # Get list if columns with types
        columns_with_trans = [
            {
                "name": safe_column_name(name),
                "trans": column_trans(schema)
            }
            for (name, schema) in target_snowflake.db_sync.flatten_schema().items()
        ]

        pipe_name = "{0}.{1}.{2}_s3_pipe".format(self.connection_config['dbname'], self.dbLink.schema_name, obj_name)

        pipe_args = dict(
            pipe_name= pipe_name,
            db_name = self.connection_config['dbname'],
            schema_name = self.dbLink.schema_name,
            obj_name = obj_name,
            stage = self.connection_config['stage'],
            file_format = self.connection_config['file_format'],
            cols = ', '.join([c['name'] for c in columns_with_trans]),
        )

        create_pipe_sql = """create pipe {pipe_name} as
                            copy into {db_name}.{schema_name}.{obj_name} ({cols})
                            from @{db_name}.{stage}
                            file_format = (format_name = {db_name}.{file_format} );""".format(**pipe_args)

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

        key_path = getattr(self.connection_config, "private_key_path", "/rsa_key.p8")
        password = getattr(self.connection_config, "private_key_password", None)
        with open(key_path, 'rb') as pem_in:
            # pemlines = pem_in.read()
            private_key_obj = load_pem_private_key(pem_in.read(),password=password,backend=default_backend())

        private_key_text = private_key_obj.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')
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