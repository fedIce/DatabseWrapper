from abc import ABC, abstractmethod
from ppygis3 import Point, LineString, Geometry

SRID_WGS84 = 4326

class DataType(ABC):
    @abstractmethod
    def __init__(self):
        pass


class StringType(DataType):
    def __init__(self, length: int = 0, default: str = None, primary_key: bool = False, not_null: bool = False):
        self.value = ''
        if length > 0:
            self.type = f' VARCHAR ({length})'
            self.value += self.type
        else:
            self.type = ' VARCHAR'
            self.value += self.type

        if default:
            self.default = f' DEFAULT {default}'
            self.value += self.default

        if not_null:
            self.nullable = f' NOT NULL'
            self.value += self.nullable

        if primary_key:
            self.primary = f' PRIMARY KEY'
            self.value += self.primary


class IntegerType(DataType):

    def __init__(self, default: int = None, primary_key: bool = False, auto_increase: bool = False, not_null: bool = False):
        self.value = ''
        self.type = " INTEGER"

        if auto_increase:
            self.auto_increase = f' SERIAL'
            self.value += self.auto_increase
        else:
            self.value += self.type

        if default:
            self.default = f' DEFAULT {default}'
            self.value += self.default

        if primary_key:
            self.primary = f' PRIMARY KEY'
            self.value += self.primary

        if not_null:
            self.nullable = f' NOT NULL'
            self.value += self.nullable


class DateType(DataType):
    def __init__(self, not_null: bool = False, default=False, primary_key: bool = False):
        self.type = ' DATE'
        self.value = self.type
        if default:
            self.default = " DEFAULT CURRENT_DATE"
            self.value += self.default

        if primary_key:
            self.primary_key = " PRIMARY KEY"
            self.value += self.primary_key

        if not_null:
            self.nullable = f' NOT NULL'
            self.value += self.nullable


class TimeStampType(DataType):
    def __init__(self, not_null: bool = False, default=False, primary_key: bool = False):
        self.type = ' TIMESTAMP'
        self.value = self.type
        if default:
            self.default = " DEFAULT CURRENT_DATE"
            self.value += self.default

        if primary_key:
            self.primary_key = " PRIMARY KEY"
            self.value += self.primary_key

        if not_null:
            self.nullable = f' NOT NULL'
            self.value += self.nullable

class GeoPointType(DataType):
    def __init__(self, default=None, point: dict=None, not_null: bool = False):
        self.type = ' GEOMETRY(POINT, 4326)'
        self.value = self.type

        if default:
            self.default = ''
            self.type = self.default
            self.value += self.type

        if not_null:
            self.nullable = f' NOT NULL'
            self.value += self.nullable
        
        if point:
            self.point = Point(point['longitude'], point['latitude'])
            self.point.srid = SRID_WGS84
            self.value += f' {self.point}'


class UUIDType(DataType):
    def __init__(self, default=None, not_null: bool = False, primary_key: bool = False ):
        self.type = ' UUID'
        self.value = self.type

        if not_null:
            self.nullable = ' NOT NULL'
            self.value += self.nullable

        if primary_key:
            self.primary_key = " PRIMARY KEY"
            self.value += self.primary_key

        if default:
            self.default = ' DEFAULT uuid_generate_v4()'
            self.value += self.default


class JSONType(DataType):
    def __init__(self, not_null: bool = False, default=None):
        self.type = ' JSON'
        self.value = self.type

        if not_null:
            self.nullable = ' NOT NULL'
            self.value += self.nullable

        if default:
            self.default = f' DEFAULT {default}'
            self.value += self.default


class BOOLType(DataType):
    def __init__(self, not_null: bool = False, default=False):
        self.type = ' BOOLEAN'
        self.value = f'{self.type}'

        if not_null:
            self.nullable = ' NOT NULL'
            self.value += self.nullable

        self.default = f' DEFAULT {default}'
        self.value += self.default
