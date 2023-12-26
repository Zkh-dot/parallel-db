import unittest
import sqlite3

from parallel_db.base import base_table
from parallel_db.db_connection.connection import connection
from parallel_db.db_connection.connection_factory import connection_factory

class test_table_down_1(base_table):
    connection_name = "sqlite"
    def __init__(self, __logger=None, db_connection=None, connection_factory=None, log_consol=True, log_file=True, draw_progress=True):
        super().__init__(__logger, db_connection, connection_factory, log_consol, log_file, draw_progress)
        self.stages = [self.create_table, self.insert]
        
    def create_table(self):
        self.connection.exequte("CREATE TABLE test (id int, name varchar(255))")
        
    def insert(self):
        self.connection.exequte("INSERT INTO test VALUES (1, 'test')")
        self.connection.exequte("INSERT INTO test VALUES (2, 'test2')")
        print("insert")
    
    def clean(self):
        print("clean")
        self.connection.exequte("DROP TABLE test")

class test_table_down_2(base_table):
    connection_name = "sqlite"
    def __init__(self, __logger=None, db_connection=None, connection_factory=None, log_consol=True, log_file=True, draw_progress=True):
        super().__init__(__logger, db_connection, connection_factory, log_consol, log_file, draw_progress)
        self.stages = [self.create_table, self.insert, self.select]
    def create_table(self):
        self.connection.exequte("CREATE TABLE test_2 (id int, name varchar(255))")
        
    def insert(self):
        self.connection.exequte("INSERT INTO test_2 VALUES (3, 'test3')")
        self.connection.exequte("INSERT INTO test_2 VALUES (4, 'test4')")
        print("insert_2")
    
    def select(self):
        self._put(self.connection.exequte("SELECT * FROM test_2")) 
    
    def clean(self):
        self.connection.exequte("DROP TABLE test_2")
        
        
class test_table_up(base_table):
    requirements = [test_table_down_1, test_table_down_2]
    connection_name = "sqlite"
    def __init__(self, __logger=None, db_connection=None, connection_factory=None, log_consol=True, log_file=True, draw_progress=True):
        super().__init__(__logger, db_connection, connection_factory, log_consol, log_file, draw_progress)
        self.stages = [self.create, self.insert, self.select, self.clean]
    def create(self):
        self.connection.exequte("CREATE TABLE test_up (id int, name varchar(255))")
        
    def insert(self):
        self.connection.exequte("INSERT INTO test_up SELECT * FROM test")
        self.connection.exequte("INSERT INTO test_up SELECT * FROM test_2")
        
    def select(self):
        self._put(self.connection.exequte("SELECT * FROM test_up"))
        
    def clean(self):
        self.connection.exequte("DROP TABLE test_up")
        for table in self.requirements:
            table.clean()
        
class test_error_table(base_table):
    connection_name = "sqlite_error"
    
    
class TestConnectFactory(unittest.TestCase):
        
    def test_init(self):
        factory = connection_factory({"sqlite": connection(logger=None, df_connection=sqlite3.connect("test.db"))})
        self.assertIsInstance(factory.connections["sqlite"], connection)
        self.assertIsInstance(factory.connections["sqlite"].connection, sqlite3.Connection)
        self.assertIsInstance(factory, connection_factory)

    def test_init_tables(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({"sqlite": con})
        table = factory.connect_table(test_table_down_1)
        self.assertIsInstance(table.connection, connection)

    def test_init_tables_recursive(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({"sqlite": con})
        table = factory.connect_table(test_table_up)
        self.assertIsInstance(table.requirements[0], base_table)
 
    def test_error_connection_1(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({})
        with self.assertRaises(KeyError):
            factory.connect_table(test_table_down_1)
        
    def test_error_connection_2(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({"sqlite": con})
        # with self.assertRaises(KeyError):
        factory.connect_table(test_table_down_1)
        with self.assertRaises(KeyError):
            factory.connect_table(test_error_table)


class TestTable(unittest.TestCase): 
    # def test_sql_path(self):
    #     con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
    #     factory = connection_factory({"sqlite": con})
    #     table = factory.connect_table(test_table_down_1)
    #     self.assertEqual(table.command("insert_4.sql"), "INSERT INTO test VALUES (4, 'test4')")
     
    def test_one_table_exec(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({"sqlite": con})
        table = factory.connect_table(test_table_down_1)
        table.build()
        df = con.exequte("SELECT * FROM test")
        self.assertEqual(df.iloc[0, 0], 1)
        self.assertEqual(df.iloc[0, 1], "test")
        self.assertEqual(df.iloc[1, 0], 2)
        self.assertEqual(df.iloc[1, 1], "test2")
        table.clean()
        con.close()
        
    def test_paral_one_table_exec(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({"sqlite": con})
        table = factory.connect_table(test_table_down_1)
        table.build_paral()
        df = con.exequte("SELECT * FROM test")
        self.assertEqual(df.iloc[0, 0], 1)
        self.assertEqual(df.iloc[0, 1], "test")
        self.assertEqual(df.iloc[1, 0], 2)
        self.assertEqual(df.iloc[1, 1], "test2")
        table.clean()
        con.close()
        
    def test_all_exec(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        factory = connection_factory({"sqlite": con})
        table = factory.connect_table(test_table_up)
        table.build()
        df = table.table
        self.assertEqual(df.iloc[0, 0], 1)
        self.assertEqual(df.iloc[0, 1], "test")
        self.assertEqual(df.iloc[1, 0], 2)
        self.assertEqual(df.iloc[1, 1], "test2")
        self.assertEqual(df.iloc[2, 0], 3)
        self.assertEqual(df.iloc[2, 1], "test3")
        self.assertEqual(df.iloc[3, 0], 4)
        self.assertEqual(df.iloc[3, 1], "test4")
        con.close()
        
    # sqlite dont like multythreading, so this test is not working :(
    # def test_all_paral_exec(self):
    #     con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
    #     factory = connection_factory({"sqlite": con})
    #     table = factory.connect_table(test_table_up)
    #     table.build_paral()
    #     df = table.table
    #     # print(df)
    #     # self.assertEqual(df.iloc[0, 0], 1)
    #     # self.assertEqual(df.iloc[0, 1], "test")
    #     # self.assertEqual(df.iloc[1, 0], 2)
    #     # self.assertEqual(df.iloc[1, 1], "test2")
    #     # self.assertEqual(df.iloc[2, 0], 3)
    #     # self.assertEqual(df.iloc[2, 1], "test3")
    #     # self.assertEqual(df.iloc[3, 0], 4)
    #     # self.assertEqual(df.iloc[3, 1], "test4")
    #     con.close()
        
if __name__ == "__main__":
    unittest.main()