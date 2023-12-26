import unittest
import sqlite3
import pandas as pd

from parallel_db import connection, connection_factory
from logging import Logger
from parallel_db.time_predictor.time_predictor import TimePredictor

class TestConnection(unittest.TestCase):
    def setUp(self) -> None:
        con = sqlite3.connect("test.db")
        con.cursor().execute("DROP TABLE IF EXISTS test")
        
    
    def test_init(self):
        con = connection(logger=None, login="sa", password="Password123")
        self.assertEqual(con.login, "sa")
        self.assertEqual(con.password, "*" * len("Password123"))
        self.assertEqual(con.connection, None)
        self.assertEqual(con.cursor, None)
        self.assertEqual(con.connected, False)
        self.assertIsInstance(con.predictor, TimePredictor)
        
    def test_connection(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        self.assertIsInstance(con.connection, sqlite3.Connection)
        self.assertIsInstance(con.cursor, sqlite3.Cursor)
        self.assertEqual(con.connected, True)
        con.close()
        
    def test_execute_types(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        con.exequte("CREATE TABLE test (id int, name varchar(255))")
        con.exequte("INSERT INTO test VALUES (1, 'test')")
        con.exequte("INSERT INTO test VALUES (2, 'test2')")
        con.exequte("INSERT INTO test VALUES (3, 'test3')")
        df = con.exequte("SELECT * FROM test")
        self.assertIsInstance(df, pd.DataFrame)
        con.exequte("DROP TABLE test")
        con.close()
        
        
    # возможно ошибка из-за пустой таблицы =D
    def test_execute_empty_df(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        df = con.exequte("SELECT * FROM test")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)
        con.close()
        
    def test_execute_content(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        con.exequte("CREATE TABLE test (id int, name varchar(255))")
        con.exequte("INSERT INTO test VALUES (1, 'test')")
        con.exequte("INSERT INTO test VALUES (2, 'test2')")
        con.exequte("INSERT INTO test VALUES (3, 'test3')")
        df = con.exequte("SELECT * FROM test")
        self.assertEqual(df.iloc[0, 0], 1)
        self.assertEqual(df.iloc[0, 1], "test")
        self.assertEqual(df.iloc[1, 0], 2)
        self.assertEqual(df.iloc[1, 1], "test2")
        self.assertEqual(df.iloc[2, 0], 3)
        self.assertEqual(df.iloc[2, 1], "test3")
        con.exequte("DROP TABLE test")
        con.close()
        
    def test_execute_many_commands(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        con.exequte("CREATE TABLE test (id int, name varchar(255))")
        df = con.exequte("""
                         INSERT INTO test VALUES (1, 'test');
                         INSERT INTO test VALUES (2, 'test2');
                         INSERT INTO test VALUES (3, 'test3');
                         SELECT * FROM test
                        """)
        self.assertIsInstance(df, pd.DataFrame)
        con.exequte("DROP TABLE test")
        con.close()
        
    def test_execute_many_commands_content(self):
        con = connection(logger=None, df_connection=sqlite3.connect("test.db"))
        con.exequte("CREATE TABLE test (id int, name varchar(255))")
        df = con.exequte("""
                         INSERT INTO test VALUES (1, 'test');
                         INSERT INTO test VALUES (2, 'test2');
                         INSERT INTO test VALUES (3, 'test3');
                         SELECT * FROM test
                        """)
        self.assertEqual(df.iloc[0, 0], 1)
        self.assertEqual(df.iloc[0, 1], "test")
        self.assertEqual(df.iloc[1, 0], 2)
        self.assertEqual(df.iloc[1, 1], "test2")
        self.assertEqual(df.iloc[2, 0], 3)
        self.assertEqual(df.iloc[2, 1], "test3")
        con.exequte("DROP TABLE test")
        con.close()
        
    
if __name__ == "__main__":
    unittest.main()