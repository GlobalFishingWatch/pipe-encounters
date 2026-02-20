from apache_beam.io.gcp.internal.clients.bigquery import TableSchema, TableFieldSchema

class SchemaBuilder(object):

    allowed_types = {
        "INTEGER",
        "FLOAT",
        "TIMESTAMP",
        "STRING",
        "RECORD",
        "DATE"
    }

    def __init__(self):
        self.schema = TableSchema()

    def build(self, name, schema_type, mode='REQUIRED', description=None):
        is_record = isinstance(schema_type, (list, tuple))
        type_name = 'RECORD' if is_record else schema_type
        if type_name not in self.allowed_types:
            raise ValueError('"{}" not in allowed_types'.format(type_name))
        if description is None:
            field = TableFieldSchema()
        else:
            field = TableFieldSchema(description=description)
        field.name = name
        field.type = type_name
        field.mode = mode
        if is_record:
            for subfield in schema_type:
                field.fields.append(subfield)
        return field

    def add(self, name, schema_type, mode="REQUIRED", description=None):
        field = self.build(name, schema_type, mode, description)
        self.schema.fields.append(field)
        return field


def schema_field_to_dict(x):
    return {'name' : x.name,
            'mode' : x.mode,
            'type' : x.type,
            'decription': x.description,
            'fields' : [schema_field_to_dict(x) for x in x.fields]}

def schema_to_obj(x):
    return [schema_field_to_dict(x) for x in x.fields]
