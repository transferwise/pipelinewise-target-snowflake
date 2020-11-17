import os

from singer import get_logger


class SnowflakeUploadClient: 
     
    def __init__(self, connection_config, dbLink):
        self.connection_config = connection_config
        self.logger = get_logger('target_snowflake')
        self.dbLink = dbLink

    def upload_file(self, file, stream, temp_dir=None):
        # Generating key in S3 bucket
        key = os.path.basename(file)
        normFile = os.path.normpath(file).replace('\\', '/')
        
        compression = '' if self.connection_config.get('no_compression', '') else "SOURCE_COMPRESSION=GZIP"
        stage = self.dbLink.get_stage_name(stream)

        self.logger.info(f"Target internal stage: {stage}, local file: {normFile}, key: {key}")
        cmd = f"PUT 'file://{normFile}' '@{stage}' {compression}"
        self.logger.info(cmd)
        
        with self.dbLink.open_connection() as connection:
            connection.cursor().execute(cmd)

        return key

    def delete_object(self, stream, key):
        self.logger.info("Deleting {} from internal snowflake stage".format(key))
        stage = self.dbLink.get_stage_name(stream)

        with self.dbLink.open_connection() as connection:
            connection.cursor().execute(f"REMOVE '@{stage}/{key}'")
