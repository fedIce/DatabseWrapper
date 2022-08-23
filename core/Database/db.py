from calendar import day_abbr
from operator import indexOf
from typing import Any
from colorama import Cursor
import psycopg2 as postgres
import psycopg2.extras as postgres_extras
from ppygis3 import Point, LineString, Geometry
from core.Database.data_types import DataType, GeoPointType
from helper import serialize_objects, parse_where_clause
from errors import InvalidDataError

SRID_WGS84 = 4326


def extract_keys_values(arr):
    """
        Extract key and values from Column object and return them as coma seperated strings
        e.g. "key1, key2, key3" and "value1, value2, value3" for insert query.

        example :
        INSERT INTO table1 (key1, key2, key3) VALUES (value1, value2, value3)

        used in Table.insert_column method
    """
    keys = ""
    op = ''
    values = []
    for i in range(0, len(arr)):
        keys += arr[i].key
        if arr[i].type:
            keys += ' '+arr[i].type.value

        # if arr[i].type == GeoPointType().type and not values == None:

        keys += ', ' if len(arr) > 1 and not i+1 == len(arr) else ''
        op += '%s'
        op += ', ' if len(arr) > 1 and not i+1 == len(arr) else ''
        if hasattr(arr[i], 'longitude') and hasattr(arr[i], 'latitude'):
            point = Point(arr[i].longitude, arr[i].latitude)
            point.srid = SRID_WGS84
            arr[i].value = point
        values.append(arr[i].value)

    return keys, tuple(values), op


class Column:
    def __init__(self, key,  type: DataType = None, primary_key=False, serial: bool = False, value: Any = None):
        """
            Create a new table column instance

            for new columns specify:
                    Column(
                            key="column_name",
                            type=One_OF_Datatype_Types( see data_type.py ),
                            primary_key: bool = default is False,
                            serial: bool = default is False (use for Integer fields),
                             )
            to add to existing table:
                    Columns(
                        key="column_name",
                        value="value to be added"
                    )
        """
        self.key = key
        self.value = value
        self.primary_key = primary_key
        self.type = type
        self.serial = serial


class GeoColumn(Column):
    """
    Create a new table Geometric column instance

            for new GeoColumn specify:
                    Column(
                            key="column_name",
                            default=bool : True,
                            point: dict : {{longitude, latitude}},
                            not_null: bool : True
                             )
            to add to existing table:
                    GeoColumn(
                        key="column_name",
                        point=dict : {{longitude, latitude}}
                    )
    
    """
    def __init__(self, key, primary_key=False, value=None, type: GeoPointType = None, longitude: float = None, latitude: float = None):
        if longitude and latitude:
            self.value = None
        else:
            self.value = value

        self.key = key
        self.type = type
        self.primary_key = primary_key
        self.longitude = longitude
        self.latitude = latitude


class Table:
    def __init__(self, name, cursor=None):
        """
            create a new table instance
        """
        self.cursor = cursor
        self.name = name
        self.extras = postgres_extras

    def execute(self, *args):
        """
            Execute your query on the selected table
        """
        try:
            return self.cursor.execute(*args)
        except Exception as e:
            raise e

    def insert_column(self, column: list):
        '''
        formulate insert query
        executes strin in the format:
        INSERT INTO table_name (column1, column2, column3,..,column(n)) VALUES (value(1), value(2), value(3),...,value(n))
        '''
        # extract key value pairs
        keys, values, op = extract_keys_values(column)

        # TESTING - printing insert query
        print(
            f'INSERT INTO {self.name} ({keys}) VALUES ({op})) RETURNING id' % values)

        # execute insert query
        query = self.execute(
            f'INSERT INTO {self.name} ({keys}) VALUES ({op}) RETURNING id', values)
        return self.cursor.fetchone()[0]

    def initialize_table(self, column: list):
        keys, values, op = extract_keys_values(column)

        self.drop_table()
        query = f'CREATE TABLE IF NOT EXISTS {self.name} ({keys});'
        print(f'QUERY: {query}')
        return self.execute(query, values)

    def drop_table(self):
        return self.execute(f'DROP TABLE IF EXISTS {self.name};')

    def update(self, data: dict = None, version=1):
        '''
            updates field(s) in the table given the id
            input => list[ dict{'id':'cee6cc34...', 'field_id': 'new_value '}] 
            note: id is a required field

            returns => updated record object
        '''
        data = serialize_objects(data)
        has_id = any(['id' in list(m.keys()) for m in data])
        if(not has_id):
            raise(
                InvalidDataError(
                    error="one or more update list object(s) is missing an 'id' field ")
            )
            return None
        # try:
        if version == 1:

            variables = ''
            values = []
            for n in range(0, len(data)):
                for k in data[n].keys():
                    if not k == 'id':
                        variables += f' {k} = e.{k}'
                        x = data[n].keys()
                        if (len(x) > 1) and not k == list(x)[len(x)-1]:
                            variables += ', '
                values.append((tuple(data[n].values())))
                values = tuple(values)

            # TESTING - printing update query
            print(
                f'UPDATE {self.name} AS t SET {variables} FROM (VALUES %s) AS e({",".join(data[0].keys())}) WHERE e.id::UUID = t.id ')
            print('VALUES: ', values)
            query = f'UPDATE {self.name} AS t SET {variables} FROM (VALUES %s) AS e({",".join(data[0].keys())}) WHERE e.id::UUID = t.id RETURNING *;'
            self.extras.execute_values(
                self.cursor, query, values, page_size=100)
            return self.cursor.fetchall()

        elif version == 2:

            variables = ''
            for n in range(0, len(data)):
                for k in data[n].keys():
                    if not k == 'id':
                        variables += f' {k} = {data[n][k]}'

            # TESTING - printing update query
            print(
                f'UPDATE {self.name} SET {variables} WHERE id = {data[0]["id"]}')
            self.cursor.execute(
                f'UPDATE {self.name} SET {variables} WHERE id::UUID = {data[0]["id"]} RETURNING *;')
            return self.cursor.fetchall()

        # except Exception as e:
        #     print(f'UPDATE EXCEPTION {e}')

    def select(self, keys: str = None, where: list = None):
        '''
        keys = name of columns to return
        ========================================
        formulate select query
        returns array of tuple containing matching results:
        SELECT keys FROM table;
        '''

        key = keys
        if not keys:
            key = '*'
        query = f'SELECT {key} FROM {self.name}'
        q = ''
        if not where == None:
            q = ' WHERE '
            for w in range(0, len(where)):
                q += f' {where[w]["key"]} {where[w]["condition"]} {where[w]["value"]} {parse_where_clause(where[w].setdefault("clause",""))}'
            q += ';'
            query = f'{query} {q}'
        print(query)
        self.execute(query)
        return self.cursor.fetchall()

    def select_by_radius(self, geo_column: str = 'location', lnglat: dict = {'lon': 0.00, 'lat': 0.00}, radius: float = 0.00, keys: str = None):
        '''
        geo_column = name of geometry column
        lnglat = object of lon and lat
        radius = search radius in Km
        keys = name of columns to return
        ========================================
        formulate select by radius query 
        returns array of tuple containing matching results:
        SELECT keys FROM table WHERE ST_DistanceSphere(geometry_column, ST_MakePoint(longitude,latitude)) <= radius * 1609.34;
        '''
        keys = keys
        if not keys:
            keys = '*'

        query = f'SELECT {keys} FROM {self.name} WHERE ST_DistanceSphere({geo_column}, ST_MakePoint({lnglat["lon"]},{lnglat["lat"]})) <= {radius * 1000}* 1609.34;'
        print(query)
        self.execute(query)
        return self.cursor.fetchall()

    def get_field_value(self, row, field, field_value):
        query = f'SELECT {field} from {self.name} WHERE name = %(fv)s;'
        self.execute(query, {'r': row, 'f': field, 'fv': field_value})
        return self.cursor.fetchall()

    def delete_row(self, field: dict = None):
        '''
            Delete row from table
            e.g
            field = {
                key: id,
                value: ce234...
            }
        '''
        query = self.execute(
            f'DELETE FROM {self.name} WHERE {field["key"]} = {field["value"]} RETURNING *;')
        return self.cursor.fetchone()[0]

    def add_column(self, column: list):
        '''
        formulate alter query
        executes string in the format:
        INSERT INTO table_name (column1, column2, column3,..,column(n)) VALUES (value(1), value(2), value(3),...,value(n))
        '''
        keys, values, op = extract_keys_values(column)

        keys = keys.split(',')
        for indx in range(0, len(keys)):
            keys[indx] = 'ADD '+ keys[indx]
        
        keys = ', '.join(keys)


        print(f'ALTER TABLE {self.name} {keys}')

        # execute insert query
        query = self.execute(f'ALTER TABLE {self.name} {keys}')
        return query


class Database:
    def __init__(self, **kwargs):
        """
            initialize with credentails:
            Database(
                host=your_host_url,
                database=your_database_name,
                user=your_database_user_name,
                password=your_database_password
            )
        """
        self.db = self.__initialize(
            host=kwargs['host'], db=kwargs['database'], user=kwargs['user'], password=kwargs['password'])
        self.cursor = self.db.cursor()
        self.table = {}

    def __initialize(self, host, db, user, password):
        """
            establish connection to database
        """
        return postgres.connect(host=host, database=db, user=user, password=password)

    def add_table(self, name) -> dict[str, Table]:
        """
            add new tables to database
            returns a dict [table_name, Table instance]
        """
        self.table[name] = Table(name, self.cursor)
        return self.table[name]

    def select_table(self, name: str):
        self.table[name] = Table(name, self.cursor)
        return self.table[name]

    def commit(self):
        """
            commit changes to database
        """
        return self.db.commit()

    def close_cursor(self):
        """
            detatch cursor
        """
        self.cursor.close()

    def close(self):
        """
            close connection to database
        """
        self.db.close()
