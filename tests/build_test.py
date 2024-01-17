from logging import Logger
import unittest
import sqlite3
import os 
import pandas as pd
from time import sleep
from parallel_db.base import BaseTable
from parallel_db.db_connection.connection import Connection
from parallel_db.db_connection.connection_factory import connection_factory


class table_one(BaseTable):
    connection_name = "name"
    def __init__(self, __logger: Logger = None, /, db_connection: Connection = None, con_factory: connection_factory = None, log_consol=True, log_file=True, draw_progress=True, auto_build=False, parallel=False, file=None):
        super().__init__(__logger, db_connection, con_factory, log_consol, log_file, draw_progress, auto_build, parallel, file)
        self.stages = [self.put]
        
    def put(self):
        self.table = pd.DataFrame({"id": [1]})
        
class table_two(BaseTable):
    connection_name = "name"
    def __init__(self, __logger: Logger = None, /, db_connection: Connection = None, con_factory: connection_factory = None, log_consol=True, log_file=True, draw_progress=True, auto_build=False, parallel=False, file=None):
        super().__init__(__logger, db_connection, con_factory, log_consol, log_file, draw_progress, auto_build, parallel, file)
        self.stages = [self.put]
        
    def put(self):
        self.table = pd.DataFrame({"id": [3]})
        
class table_three(BaseTable):
    connection_name = "name"
    requirements = [table_one, table_two]
    def __init__(self, __logger: Logger = None, /, db_connection: Connection = None, con_factory: connection_factory = None, log_consol=True, log_file=True, draw_progress=True, auto_build=False, parallel=False, file=None):
        super().__init__(__logger, db_connection, con_factory, log_consol, log_file, draw_progress, auto_build, parallel, file)
        self.stages = [self.put]
        
    def put(self):
        print(table_one.table)
        print(table_two.table)
        self.table = pd.DataFrame.merge(self=table_one.table, right=table_two.table, on="id", how="outer")
        
        
class build_tests(unittest.TestCase):
    def test_build(self):        
        factory = connection_factory({"name": 1})
        table = factory.connect_table(table_three)
        table.build()
        self.assertEqual(table.table.shape, (2, 1))
        
    def test_build_paral(self):
        factory = connection_factory({"name": 1})
        table = factory.connect_table(table_three)
        table.build_paral()
        self.assertEqual(table.table.shape, (2, 1))
        
if __name__ == "__main__":
    unittest.main()