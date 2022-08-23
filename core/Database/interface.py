from typing import Any
from core.Database.data_types import DateType, GeoPointType, IntegerType, StringType, UUIDType, BOOLType
from core.Database.db import Column, Database, GeoColumn, Table
from helper import parse_db_results 


class DatabaseInterface:
    def __init__(self, table: str = None):
        self.db: Database = Database(host="localhost", database="test", user="postgres", password="admin")
        

    def select_table(self, table: str = None):
        self.table: Table = self.db.select_table(table)

    # create new databse instance and initailize with credentials
    def add_table_to_database(self, table: str):
        # # add a new table to databse and select the new table
        self.db.add_table(name=table)

    def initialize_table_with_fields(self, fields):
        cols = []

        # # prepare columns to add to table
        for field in fields:
            if field['key'] == 'location':
                cols.append(GeoColumn(key=field['key'], type=field['type']))
            else:
                cols.append(Column(key=field['key'], type=field['type']))

            # initialize selected table with the column properties
        self.table.initialize_table(cols)

    def update_record(self, data:dict=None, fields:str=None):
        update = self.table.update( data=data, version=1)
        return parse_db_results(fields, fields,update)

    def delete_record(self, field:dict=None):
        return self.table.delete_row(field=field)


    def add_record_to_table(self, records: list[dict[str, Any]]):

        # add new data to the created columns
        for record in records:
            cols = []
            for key in record:
                if key == 'location':
                    cols.append(GeoColumn(
                        key=key, longitude=record[key]['longitude'], latitude=record[key]['latitude']))
                else:
                    cols.append(Column(key=key, value=record[key]))

            return self.table.insert_column(cols)

    def select_table_field(self, field: str = "location", where:list=None):
        data = []
        key_list = field.split(',')
        table_field = self.table.select(keys=field, where=where)
        for result in table_field:
            obj = {}
            for key in range(0, len(key_list)):
                obj[key_list[key]] = result[key]
            data.append(obj)

    def add_column(self, fields: list[dict[str, Any]]):
        cols = []

        # # prepare columns to add to table
        for field in fields:
            if field['key'] == 'location':
                cols.append(GeoColumn(key=field['key'], type=field['type']))
            else:
                cols.append(Column(key=field['key'], type=field['type']))
        return self.table.add_column(cols)

        return data
    def commit(self):
        self.db.commit()
        self.db.close_cursor()
        self.db.close()



### EXAMLES

setup_table = [
    {
        'key': 'id',
        'type': IntegerType(not_null=True, primary_key=True, auto_increase=True)
    },
    {
        'key': 'date',
        'type': DateType(not_null=True)
    },
    {
        'key': 'name',
        'type': StringType(length=50, not_null=True)
    },
    {
        'key': 'height',
        'type': StringType(length=50, not_null=True)
    },
    {
        'key': 'location',
        'type': GeoPointType(not_null=True)
    },
    {
        'key': 'uuid',
        'type': UUIDType(not_null=True, default=True)
    },
    {
        'key': 'isactive',
        'type': BOOLType()
    }
]

users = [
    {
        'name': "Farah J Fresh",
        'height': "15",
        'date': "1996-08-16",
        'location': {
            'longitude': 69.3504869,
            'latitude': 24.9104797
        }

    },
    {
        'name': "Samuel Damilola Atiku",
        'height': "18.3",
        'date': "1998-11-6",
        'location': {
            'longitude': 29.3504669,
            'latitude': 14.9107797
        }

    }
]


db = DatabaseInterface()
db.select_table(table="hands")
db.initialize_table_with_fields(setup_table)
name = db.table.get_field_value('name', 'name', 'Farah J Fresh' )
db.add_record_to_table(users)
print(f'Users Name is {name}')

rad_search = db.table.select_by_radius(geo_column='location', lnglat={
                                    'lon': 29.0, 'lat': 14.5}, radius=1)
s = db.select_table_field('name, height, isactive')
# print(f' =================> {rad_search} ==== {s}')

# commit changes to database
db.commit()
# playground ---END
