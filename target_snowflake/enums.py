"""Enums used by pipelinewise-target-snowflake"""
from enum import Enum, unique


# Supported types for file formats.
@unique
class FileFormatTypes(str, Enum):
    """Enum of supported file format types"""

    CSV = 'csv'
