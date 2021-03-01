"""Exceptions used by pipelinewise-target-snowflake"""


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""


class UnexpectedValueTypeException(Exception):
    """Exception to raise when record value type doesn't match the expected schema type"""


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records"""