

def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    schema_message['schema']['properties']['_sdc_extracted_at'] = {
        'type': ['null', 'string'],
        'format': 'date-time'
    }
    schema_message['schema']['properties']['_sdc_batched_at'] = {
        'type': ['null', 'string'],
        'format': 'date-time'
    }
    schema_message['schema']['properties']['_sdc_deleted_at'] = {
        'type': ['null', 'string']
    }
    return schema_message