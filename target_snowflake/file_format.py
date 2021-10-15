"""Enums used by pipelinewise-target-snowflake"""
import abc
from enum import Enum, unique
from types import ModuleType
from typing import Callable

import target_snowflake.file_formats
from target_snowflake.exceptions import FileFormatNotFoundException, InvalidFileFormatException

# Supported types for file formats.
@unique
class FileFormatTypes(str, Enum):
    """Enum of supported file format types"""

    CSV = 'csv'
    PARQUET = 'parquet'

    @staticmethod
    def list():
        """List of supported file type values"""
        return list(map(lambda c: c.value, FileFormatTypes))


class FileFormat(abc.ABCMeta):
    """File Format class (abstract)"""

    def __init__(self, file_format_type: FileFormatTypes):
        """Initialize type and file format specific functions."""
        self.file_format_type = file_format_type
        self.formatter = self._get_formatter(file_format_type)

    @classmethod
    def _get_formatter(cls, file_format_type: FileFormatTypes) -> ModuleType:
        """Get the corresponding file formatter implementation based
        on the FileFormatType parameter

        Params:
            file_format_type: FileFormatTypes enum item

        Returns:
            ModuleType implementation of the file formatter
        """
        formatter = None

        if file_format_type == FileFormatTypes.CSV:
            formatter = target_snowflake.file_formats.csv
        elif file_format_type == FileFormatTypes.PARQUET:
            formatter = target_snowflake.file_formats.parquet
        else:
            raise InvalidFileFormatException(f"Not supported file format: '{file_format_type}")

        return formatter

    @abc.abstractproperty
    def declaration_for_copy(self) -> str:
        """Return the format declaration text for a COPY INTO statement."""
        pass

    @abc.abstractproperty
    def declaration_for_merge(self) -> str:
        """Return the format declaration text for a MERGE statement."""
        pass


# pylint: disable=too-few-public-methods
class NamedFileFormat(FileFormat):
    """Named File Format class"""

    def __init__(
        self,
        file_format: str,
        query_fn: Callable,
        file_format_type: FileFormatTypes = None,
    ):
        """Find the file format in Snowflake, detect its type and
        initialise file format specific functions"""
        self.qualified_format_name = file_format
        if not file_format_type:
            # Detect file format type by querying it from Snowflake
            file_format_type = self._detect_file_format_type(file_format, query_fn)

        super().__init__(file_format_type)

    @classmethod
    def _detect_file_format_type(cls, file_format: str, query_fn: Callable) -> FileFormatTypes:
        """Detect the type of an existing snowflake file format object

        Params:
            file_format: File format name
            query_fn: A callable function that can run SQL queries in an active Snowflake session

        Returns:
            FileFormatTypes enum item
        """
        file_format_name = file_format.split('.')[-1]
        file_formats_in_sf = query_fn(f"SHOW FILE FORMATS LIKE '{file_format_name}'")

        if len(file_formats_in_sf) == 1:
            file_format = file_formats_in_sf[0]
            try:
                file_format_type = FileFormatTypes(file_format['type'].lower())
            except ValueError as ex:
                raise InvalidFileFormatException(
                    f"Not supported named file format {file_format_name}. Supported file formats: {FileFormatTypes}") \
                    from ex
        else:
            raise FileFormatNotFoundException(
                f"Named file format not found: {file_format}")

        return file_format_type

    def declaration_for_copy(self) -> str:
        """Return the format declaration text for a COPY INTO statement."""
        return f"FILE_FORMAT = (format_name='{self.qualified_format_name}')"

    def declaration_for_merge(self) -> str:
        return f"FILE_FORMAT => '{self.qualified_format_name}'"


class InlineFileFormat(FileFormat):
    def __init__(
        self,
        file_format_type: FileFormatTypes = None,
    ):
        """Find the file format in Snowflake, detect its type and
        initialise file format specific functions"""
        if file_format_type != FileFormatTypes.CSV:
            raise NotImplementedError("Only CSV is supported as an inline format type.")

        self.file_format_type = file_format_type
        self.formatter = self._get_formatter(self.file_format_type)

    @abc.abstractproperty
    def declaration_for_copy(self) -> str:
        """Return the format declaration text for a COPY INTO statement."""
        if self.file_format_type == FileFormatTypes.CSV:
            return (
                "FILE_FORMAT = (\n"
                "    TYPE = CSV\n"
                "    EMPTY_FIELD_AS_NULL = FALSE\n"
                "    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'\n"
                ")\n"
            )

        raise NotImplementedError("Only CSV is supported as an inline format type.")

    def declaration_for_merge(self) -> str:
        return (
            "FILE_FORMAT => (\n"
            "    TYPE = CSV\n"
            "    EMPTY_FIELD_AS_NULL = FALSE\n"
            "    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'\n"
            ")"
        )
