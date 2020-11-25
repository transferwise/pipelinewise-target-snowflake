from typing import Dict
from dateutil import parser
from datetime import datetime
from dateutil.parser import ParserError

from target_snowflake import (
    MAX_TIME, MAX_TIMESTAMP
)
from target_snowflake.errors import (
    InvalidValidationOperationException,
    RecordValidationException
)


def adjust_timestamps_in_record(record_message: Dict, schema: Dict) -> Dict:
    """
    Goes through every field that is of type date/datetime/time and if its value is out of range,
    resets it to MAX value accordingly
    Args:
        record_message: singer record_message containing properties and values
        schema: json schema that has types of each property
    """
    record = record_message.get('record')

    # creating this internal function to avoid duplicating code and too many nested blocks.
    def reset_new_value(record: Dict, key: str, format: str):
        try:
            parser.parse(record[key])
        except ParserError:
            record[key] = MAX_TIMESTAMP if format != 'time' else MAX_TIME

    for key, value in record.items():
        if value is not None and key in schema['properties']:
            if 'anyOf' in schema['properties'][key]:
                for type_dict in schema['properties'][key]['anyOf']:
                    if 'string' in type_dict['type'] and type_dict.get('format', None) in {'date-time', 'time', 'date'}:
                        reset_new_value(record, key, type_dict['format'])
                        break
            else:
                if 'string' in schema['properties'][key]['type'] and \
                        schema['properties'][key].get('format', None) in {'date-time', 'time', 'date'}:
                    reset_new_value(record, key, schema['properties'][key]['format'])
    return record_message


def validate_record(record_message, validator):
    try:
        validator.validate(
            float_to_decimal(record_message['record'])
        )
    except Exception as ex:
        if type(ex).__name__ == "InvalidOperation":
            raise InvalidValidationOperationException(
                f"Data validation failed and cannot load to destination. RECORD: {record_message['record']}\n"
                "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                "or more) Try removing 'multipleOf' methods from JSON schema."
            )
        else:
            raise RecordValidationException(
                f"Record does not pass schema validation. RECORD: {record_message['record']}"
            )


def add_metadata_values_to_record(record_message):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    record_message['record']['_sdc_extracted_at'] = record_message.get('time_extracted')
    record_message['record']['_sdc_batched_at'] = datetime.now().isoformat()
    record_message['record']['_sdc_deleted_at'] = record_message.get('record', {}).get('_sdc_deleted_at')
    return record_message
