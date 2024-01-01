import os
import functools
from ..db_connection.connection import Connection
from ..decorators import utils
from .. import logger
from .abstract_table import AbstractTable
from ..db_connection.connection_factory import connection_factory
import warnings
from abc import ABC
from ..logger import get_logger
from typing import Union

class base_scenario(ABC):
    """
    This class represents a base scenario for data proccessing. 
    Basicly, it gives few more comfortable interfaces for simpler work with sql databases

    Returns:
        _type_: _description_
    """
    def __connect_all_req(self):
        pass
    
    def __init__(self):
        if logger:
            self.__logger = logger
        else:
            self.__logger = get_logger(self.__class__.__name__, log_consol=False, log_file=False, draw_progress=False)
        self.__start_loggers()
    
    def __start_loggers(self):
        """
        Starts the loggers for function tracing.

        Returns:
            None
        """

        decorator_with_logger = functools.partial(logger.trace_call, self.__logger)
        utils.decorate_function_by_name(decorator_with_logger, "read_sql", "pandas")
        utils.decorate_function_by_name(decorator_with_logger, "Cursor.execute", "pyodbc")
        utils.decorate_function_by_name(decorator_with_logger, "DataFrame.to_sql", "pandas")
    
    @property
    def connections(self) -> dict[str, Connection]:
        return self.__con_factory.connections
    
    @connections.setter
    def connections(self, connections: Union(connection_factory, dict[str, Connection])):
        self.__con_factory = connections
    
    # not shure, if con_factory should be public
    @property
    def con_factory(self) -> connection_factory:
        return self.__con_factory
    
    @con_factory.setter
    def con_factory(self, connections: Union(connection_factory, dict[str, Connection])):
        if isinstance(connections, dict):
            self.__con_factory = connection_factory(connections, self.__logger)
        elif isinstance(connections, connection_factory):
            self.__con_factory = connections
        else:
            raise TypeError

    @con_factory.deleter
    def con_factory(self):
        self.close_connections()
        
    @property
    def tables(self):
        return self.tables
    
    @tables.setter
    def tables(self, tables = dict[str, AbstractTable]):
        self.tables = tables
        for table in self.tables:
            table = self.__con_factory.connect_table(table)
        
    def close_connections(self):
        self.__con_factory.close_all()

class old_base_scenario:
    """
    This class represents a base scenario for data processing.

    Attributes:
        sql_scripts (str): The path to the directory containing SQL scripts.
    """

    sql_scripts = ""
    warnings.filterwarnings("ignore")

    def __init__(self, login: str = os.getenv('LOGIN'), password: str = os.getenv('PASSWORD'), required_connections: list[str] = ['MS', "PL", "oracle"]):
        """
        Initializes a base_scenario object.

        Args:
            login (str): The login for database connections.
            password (str): The password for database connections.
            required_connections (list): The list of required database connections. Pass only if you need new connection (basicly, never)

        Returns:
            None
        """
        self.logger = logger.get_logger(self.__class__.__name__)

        self.credentials(login, password)
        # создание подключений к базам данных
        self.logger.info('going to connect {}'.format(*required_connections))

     
        self.connections = {}
        for connection_name in required_connections:
            self.connections[connection_name.lower()] = Connection(db=connection_name, logger=self.logger, login=self.login, password=password)
        self.connection_factory = connection_factory(self.connections)

        # userfriendly
        if "ms" in self.connections:
            self.ms_con = self.connections["ms"]
        if "pl" in self.connections:
            self.pl_con = self.connections["pl"]
        if "oracle" in self.connections:
            self.oracle_con = self.connections["oracle"]

        Connection.set_predictor(self.logger)
        self.__start_loggers()
        

    def __start_loggers(self):
        """
        Starts the loggers for function tracing.

        Returns:
            None
        """

        decorator_with_logger = functools.partial(logger.trace_call, self.logger)
        utils.decorate_function_by_name(decorator_with_logger, "read_sql", "pandas")
        utils.decorate_function_by_name(decorator_with_logger, "Cursor.execute", "pyodbc")
        utils.decorate_function_by_name(decorator_with_logger, "DataFrame.to_sql", "pandas")
        # utils.decorate_function_by_name(decorator_with_logger, "cursor.execute")
    

    def credentials(self, login: str, password: str):
        """
        Sets the login and password for database connections.

        Args:
            login (str): The login for database connections.
            password (str): The password for database connections.

        Returns:
            None
        """
        self.login = login
        self.password = password

    
    @staticmethod
    def sql_scripts_path(file):
        """
        Returns the path to the directory containing SQL scripts.

        Args:
            file (str): The path to the current file.

        Returns:
            str: The path to the directory containing SQL scripts.
        """
        return os.path.join(os.path.dirname(os.path.abspath(file)), "sql_scripts")


    def command(self, script_name: str) -> str:
        """
        Returns the content of an SQL script.

        Args:
            script_name (str): The name of the SQL script.

        Returns:
            str: The content of the SQL script.
        """
        with open(os.path.join(self.sql_scripts, script_name), "r", encoding="utf-8") as sql_script:
            return sql_script.read()
    

    def init_recursively(self, table: AbstractTable) -> AbstractTable:
        """
        function for recursiv initialisation of table and all it's required
        need to be done before any work with table
        
        Args:
            table (table): table wich should be initialised
        Returns:
            table: initialised table instance
        """
        self.logger.info("init table {}".format(table.__name__))
        table = self.connection_factory.connect_table(self.logger, table)
        for i, el in enumerate(table.requirements):
            table.requirements[i] = self.init_recursively(el)
        return table
