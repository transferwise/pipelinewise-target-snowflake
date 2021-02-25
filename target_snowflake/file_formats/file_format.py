"""
S3 Upload Client
"""


# pylint: disable=too-few-public-methods
class FileFormat:
    """
    Static class with supported snowflake file format type constants
    """
    CSV = 'CSV'

    supported_types = (
        CSV
    )
