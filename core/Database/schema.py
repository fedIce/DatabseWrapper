#!/usr/bin/env python

from abc import ABC, abstractmethod
import os
from typing import Any
import psycopg2 as postgres

class Column(ABC):

    def where(self):
        pass
    
    def insert(self):
        pass


class Table(ABC):
    def __init__(self):
        # connect to db
        pass

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def add_column(self):
        pass

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod
    def drop_if_exists(self):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def columns(self, column: Column):
        pass

    @abstractmethod
    def rows(self, column: Column):
        pass



class Database(ABC):
    def __init__(self):
        self.cursor = self.__initialize().cursor

    def __initialize(self):
        return postgres.connect( host="localhost", database="test", user="postgres", password="admin")

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod
    def select(self, table: Any):
        pass







