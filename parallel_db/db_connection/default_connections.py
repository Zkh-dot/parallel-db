import pyodbc
import cx_Oracle as cx
import oracledb
import sqlite3
import sqlalchemy
from .connection import Connection

def oracle_connection(username: str, password: str, con_line: str, encoding: str = 'UTF-8'):
    """
    Establishes a connection to an Oracle database.

    Args:
        * username (str): The username for the database connection.
        * password (str): The password for the database connection.
        * con_line (str): The connection string for the database.
        * encoding (str, optional): The encoding to use for the connection. Defaults to 'UTF-8'.

    Returns:
        * cx_Oracle.Connection: The connection object.

    """
    return oracledb.connect(username, password, f'{con_line}', encoding=encoding)

def mssql_connection(username: str = None, password: str = None, driver: str = 'SQL Server', server: str = '', database: str = "", thusted_connection: str = "yes", encoding: str = 'utf-16le', *args):
    """
    Establishes a connection to a Microsoft SQL Server database.

    Args:
        * username (str, optional): The username for the database connection. Defaults to None.
        * password (str, optional): The password for the database connection. Defaults to None.
        * driver (str, optional): The driver to use for the connection. Defaults to 'SQL Server'.
        * server (str, optional): The server name or IP address. Defaults to ''.
        * database (str, optional): The name of the database. Defaults to "".
        * thusted_connection (str, optional): Whether to use trusted connection. Defaults to "yes".
        * encoding (str, optional): The encoding to use for the connection. Defaults to 'utf-16le'.
        * *args: Additional arguments for the connection string.

    Returns:
        * pyodbc.Connection: The connection object.

    """
    if username is None and password is None:
        return pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection={thusted_connection}', encoding=encoding)
    return pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection={thusted_connection}{";" if args else ""}{";".join(*args)}', encoding=encoding)

def sqlalchemy_engine(con_type: str, username: str, password: str, driver: str, server: str, database: str, encoding: str = 'utf-16le', *args):
    """
    Creates a SQLAlchemy engine for connecting to a database.

    Args:
        * con_type (str): The type of database connection.
        * username (str): The username for the database connection.
        * password (str): The password for the database connection.
        * driver (str): The driver to use for the connection.
        * server (str): The server name or IP address.
        * database (str): The name of the database.
        * encoding (str, optional): The encoding to use for the connection. Defaults to 'utf-16le'.
        * *args: Additional arguments for the connection string.

    Returns:
        * sqlalchemy.engine.Engine: The SQLAlchemy engine object.

    """
    return sqlalchemy.create_engine(f'{con_type}://{username}:{password}@{server}/{database}?driver={driver};Trusted_Connection=yes{";" if args else ""}{";".join(*args)}', encoding=encoding)