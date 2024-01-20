import logging
from typing import Union
import pyodbc, oracledb, sqlalchemy
from abc import ABC, abstractmethod

class AbstractConnection(ABC):
    """
    A class representing a database connection.

    Args:
        * logger (logging.Logger, optional): The logger object for logging messages.
        * df_connection (Union(pyodbc.Connection, oracledb.Connection), optional): The database connection object.
        * login (str, optional): The login username.
        * password (str, optional): The login password.
    """
    def __init__(self, logger: logging.Logger = None, df_connection: Union[pyodbc.Connection, oracledb.Connection] = None, login: str = None, password: str = None) -> None:

        """
        Initializes a connection object.

        Args:
            * logger (logging.Logger, optional): The logger object for logging messages.
            * df_connection (Union(pyodbc.Connection, oracledb.Connection), optional): The database connection object.
            * login (str, optional): The login username.
            * password (str, optional): The login password.
        """
        self.__login = login
        self.__password = password
    
    @property
    def login(self):
        """
        str: The login username.
        """
        return self.__login
    
    @login.setter
    def login(self, login: str):
        """
        Sets the login username.

        Args:
            * login (str): The login username.
        """
        self.__login = login
        
    @property
    def password(self): 
        """
        str: The masked login password.
        """
        return "*" * len(self.__password)
    
    @password.setter
    def password(self, value):
        """
        Sets the login password.

        Args:
            * value (str): The login password.
        """
        self.__password = value
        
    @abstractmethod
    @property
    def cursor(self):
        """
        The database cursor object.
        """
        pass
    
    @abstractmethod
    @cursor.setter
    def cursor(self, value):
        """
        Sets the database cursor object.

        Args:
            * value: The database cursor object.
        """
        
    @cursor.deleter
    def cursor(self):
        """
        Closes the database cursor.
        """
    
    @property
    def connection(self):
        """
        The database connection object.
        """
        
    @abstractmethod
    def __connect_class(self, db_connection: Union[pyodbc.Connection, oracledb.Connection]):
        """
        Connects to the database using a class-based connection object.

        Args:
            * db_connection (Union(pyodbc.Connection, oracledb.Connection)): The class-based database connection object.
        """
        
    @abstractmethod
    def __connect_instance(self, db_connection: Union[pyodbc.Connection, oracledb.Connection]):
        """
        Connects to the database using an instance-based connection object.

        Args:
            * db_connection (Union(pyodbc.Connection, oracledb.Connection)): The instance-based database connection object.
        """
        
    @abstractmethod
    def __connect_engine(self, db_connection: sqlalchemy.Engine):
        """
        Connects to the database using a SQLAlchemy engine.

        Args:
            * db_connection (sqlalchemy.Engine): The SQLAlchemy engine object.
        """
        
    @abstractmethod
    @connection.setter
    def connection(self, db_connection: Union[pyodbc.Connection, oracledb.Connection, sqlalchemy.Engine]):
        """
        Connects to the database based on the type of connection object.

        Args:
            * db_connection (Union(pyodbc.Connection, oracledb.Connection, sqlalchemy.Engine)): The database connection object.
        """
        
    @abstractmethod
    @property
    def engine(self):
        """
        sqlalchemy.Engine: The SQLAlchemy engine object.
        """
        
    @abstractmethod
    @engine.setter
    def engine(self, prefix: str = None):
        """
        Sets the SQLAlchemy engine object.

        Args:
            * line (str): The connection string.
        """
        
    @abstractmethod
    @connection.deleter
    def connection(self):
        """
        Closes the database connection.
        """