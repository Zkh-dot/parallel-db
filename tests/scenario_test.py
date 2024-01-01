from parallel_db.base.base_scenario import base_scenario
import sqlite3
from parallel_db import Connection, connection_factory
from unittest import TestCase

class test_scenario(base_scenario):
    def __init__(self):
        super().__init__()
        
class TestScenario(TestCase):
    def test_init(self):
        tst = test_scenario()
        self.assertIsInstance(tst, test_scenario)
        
    # def test_connections(self):
    #     con = Connection(logger=None, df_connection=sqlite3.connect("test_scenario.db"))
    #     tst = test_scenario()
    #     tst.connections = {"test_connection", con}
    #     self.assertEqual(tst.connections, {"test_connection", con})
        
    # def test_factory(self):
    #     con = Connection(logger=None, df_connection=sqlite3.connect("test_scenario.db"))
    #     tst = test_scenario()
    #     tst.connections = {"test_connection", con}
    #     self.assertIsInstance(tst.con_factory, connection_factory)