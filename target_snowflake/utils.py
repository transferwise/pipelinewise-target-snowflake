from decimal import Decimal


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def check_message_has_stream(line, message):
    """ Check a given message has a corresponding stream.
    """
    if 'stream' not in message:
        raise Exception(
            f"Line is missing required key 'stream': {line}"
        )


def check_message_has_schema(line, message, schemas):
    """ Check a given message has a corresponding schema.
    """
    if message['stream'] not in schemas:
        raise Exception(
            f"A message for stream {message['stream']} "
            "was encountered before a corresponding schema"
        )
