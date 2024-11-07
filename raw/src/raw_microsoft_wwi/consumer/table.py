class Table:
    def __init__(self, table_data):
        self.table_data = table_data
        self.type_map = self.set_type_map()
        self.schema_name = self.set_schema_name()
        self.table_name = self.set_table_name()
        self.table_schema = self.set_table_schema()
        self.primary_key = self.set_primary_key()

    def set_type_map(self):
        self.type_map = {
            'bigint': 'long',
            'bit': 'boolean',
            'decimal': 'decimal',
            'int': 'integer',         
            'money': 'decimal',
            'numeric': 'decimal',
            'smallint': 'smallint',
            'smallmoney': 'decimal',
            'tinyint': 'smallint',
            'real': 'real',
            'float': 'float',
            'date': 'date',
            'datetime2': 'timestamp',
            'datetime': 'timestamp',
            'datetimeoffset': 'string',
            'smalldatetime': 'timestamp',
            'char': 'string',
            'varchar': 'string',
            'text': 'string',
            'nchar': 'string',
            'nvarchar': 'string',
            'ntext': 'string',
            'varbinary': 'binary',
            'binary': 'binary',
            'geography': 'string'
        }
        return self.type_map

    def set_schema_name(self):
        return self.table_data['source']['schema']

    def set_table_name(self):
        return self.table_data['source']['table']

    def set_table_schema(self):
        table_schema = {'fields': [], 'type': 'struct'}     
        for column in self.table_data['tableChanges'][0]['table']['columns']:
            column_schema = {'metadata': {'scale': column['scale'] if column['scale'] else 0},
                             'name': column['name'],
                             'nullable': True,
                             'type': self.type_map.get(column['typeName'], 'string')
                             }
            if column_schema['type'] == 'decimal':
                column_schema['type'] = f"decimal({column['length']},{column['scale']})"
            table_schema['fields'].append(column_schema)
        
        table_schema['fields'].append({
            "metadata": {},
            "name": "LoadDatetime",
            "nullable": False,
            "type": "timestamp"
        })
        return table_schema

    def set_primary_key(self):
        return self.table_data['tableChanges'][0]['table']['primaryKeyColumnNames']
