# _*_ coding: utf-8 _*_
import cx_Oracle as cx
import pyodbc
import os
import copy
import logging
import pandas as pd
import sqlalchemy
import time
import datetime
from ..logger import get_logger
from datetime import timedelta
from ..time_predictor.time_predictor import TimePredictor
from typing import Union
from inspect import isclass


class connection:
    """
    A class representing a database connection.

    Args:
        logger (logging.Logger, optional): The logger object for logging messages.
        df_connection (Union(pyodbc.Connection, cx.connect), optional): The database connection object.
        login (str, optional): The login username.
        password (str, optional): The login password.
    """
    predictor = TimePredictor
    connected = False

    def __init__(self, logger: logging.Logger = None, df_connection: Union[pyodbc.Connection, cx.connect] = None, login: str = None, password: str = None) -> None:
        """
        Initializes a connection object.

        Args:
            logger (logging.Logger, optional): The logger object for logging messages.
            df_connection (Union(pyodbc.Connection, cx.connect), optional): The database connection object.
            login (str, optional): The login username.
            password (str, optional): The login password.
        """
        self.__login = login
        self.__password = password
        if logger:
            self.__logger = logger
        else:
            self.__logger = get_logger(self.__class__.__name__, log_consol=False, log_file=False, draw_progress=False)
        
        self.__connection = None
        self.__cursor = None
        self.predictor = TimePredictor(logger)
        self.connection = df_connection
        
    def __switch_state(self):
        self.connected = not self.connected
    
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
            login (str): The login username.
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
            value (str): The login password.
        """
        self.__password = value
        
    @property
    def cursor(self):
        """
        The database cursor object.
        """
        return self.__cursor
    
    @cursor.setter
    def cursor(self, value):
        """
        Sets the database cursor object.

        Args:
            value: The database cursor object.
        """
        self.__cursor = value
    
    @cursor.deleter
    def cursor(self):
        """
        Closes the database cursor.
        """
        try:
            self.__cursor.close()
        except (pyodbc.ProgrammingError, cx.InterfaceError) as error:
            self.__logger.warning("cursor already closed (•-•)⌐")
        except AttributeError:
            self.__logger.error('cursor does not exist')
        except Exception as e:
            self.__logger.error(e)
            self.__logger.error("Most likely, you are working with unusual db")
        
    @property
    def connection(self):
        """
        The database connection object.
        """
        return self.__connection    
        
    def __connect_class(self, db_connection: Union[pyodbc.Connection, cx.connect]):
        """
        Connects to the database using a class-based connection object.

        Args:
            db_connection (Union(pyodbc.Connection, cx.connect)): The class-based database connection object.
        """
        try:
            self.__connection = db_connection(self.__login, self.__password)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            self.__logger.error(e)
            raise e
            
    def __connect_instance(self, db_connection: Union[pyodbc.Connection, cx.connect]):
        """
        Connects to the database using an instance-based connection object.

        Args:
            db_connection (Union(pyodbc.Connection, cx.connect)): The instance-based database connection object.
        """
        try:
            self.__connection = db_connection
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            self.__logger.error(e)
            raise e 
        
    def __connect_engine(self, db_connection: sqlalchemy.Engine):
        """
        Connects to the database using a SQLAlchemy engine.

        Args:
            db_connection (sqlalchemy.Engine): The SQLAlchemy engine object.
        """
        try:
            self.__connection = db_connection.connect()
            self.__cursor = self.__connection
        except Exception as e:
            self.__logger.error(e)
            raise e
    
    @connection.setter
    def connection(self, db_connection: Union[pyodbc.Connection, cx.connect, sqlalchemy.Engine]):
        """
        Connects to the database based on the type of connection object.

        Args:
            db_connection (Union(pyodbc.Connection, cx.connect, sqlalchemy.Engine)): The database connection object.
        """
        if db_connection is None:
            self.__logger.debug("db_connection is None")
            return
        elif isinstance(db_connection, sqlalchemy.Engine):
            self.__connect_engine(db_connection)
        elif isclass(db_connection):
            self.__connect_class(db_connection)
        else:
            self.__connect_instance(db_connection)
        self.__switch_state()
            
    @connection.deleter
    def connection(self):
        """
        Closes the database connection.
        """
        try:
            self.__connection.close()
        except (pyodbc.ProgrammingError, cx.InterfaceError) as error:
            self.__logger.error("connection already closed \\(°Ω°)/")
        except AttributeError:
            self.__logger.error('connection does not exist')
            
    def __copy__(self):
        """
        Returns a copy of the connection object.
        """
        new = connection(self.__logger, None, self.__login, self.__password)
        new.connection = self.connection
        new.__cursor = copy.deepcopy(self.__cursor)
        return new
    
    def __get_table(self, sql_request: str, go_next: bool = True, *args):
        """
        Executes a SQL query and returns the result as a pandas DataFrame.

        Args:
            sql_request (str): The SQL query to execute.
            go_next (bool, optional): Whether to continue executing commands after an error. Defaults to True.
            *args: Additional keyword arguments to be passed to the SQL query.

        Returns:
            pd.DataFrame: The result of the query as a pandas DataFrame.
        """
        try:
            result = pd.read_sql(sql_request.format(*args), self.__connection)
        except Exception as e:
            self.__logger.error(e)
            if not go_next:
                raise e
        try:
            return result
        except:
            return pd.DataFrame()
    
    def __exequte(self, sql_request: str, really_try: bool = True, go_next: bool = True, commit: bool = True, *args):
        """
        Executes a SQL command.

        Args:
            sql_request (str): The SQL command to execute.
            really_try (bool, optional): Whether to raise errors. Defaults to True.
            go_next (bool, optional): Whether to continue executing commands after an error. Defaults to True.
            *args: Additional keyword arguments to be passed to the SQL command.
        """
        try:
            self.__cursor.execute(sql_request.format(*args))
        except Exception as e:
            if really_try:
                raise e
            else:
                self.__logger.error(e)
            if not go_next:
                raise e
        if commit:
            self.commit(really_try)
    
    def new_cursor(self):
        """
        Returns same connection with a new cursor.
        """
        new = self.__copy__()
        new.__cursor = new.__connection.cursor()
        return new
    
    def commit(self, really_try=True):
        """
        Commits the current transaction.

        Args:
            really_try (bool, optional): Whether to raise errors. Defaults to True.

        Returns:
            bool: True if the commit was successful, False otherwise.
        """
        try:
            self.__connection.commit()
            self.__logger.info("commited! \\(^ᵕ^ )/ ")
        except Exception as e:
            if really_try:
                raise(e)
            else:
                self.__logger.error(e)
            return False
        
    def exequte(self, sql_requests: str = None, command_name: str = None, really_try=True, go_next=True, commit=True, *args):
        """
        Executes one or more SQL commands. The commands must be separated by semicolons.

        Args:
            sql_requests (str): The SQL commands to execute, separated by semicolons.
            command_name (str, optional): The name of the command to time prediction. Defaults to None.
            really_try (bool, optional): Whether to raise errors. Defaults to True.
            go_next (bool, optional): Whether to continue executing commands after an error. Defaults to True.
            *args: Additional keyword arguments to be passed to the SQL commands.

        Returns:
            pd.DataFrame: The combined result of the queries as a pandas DataFrame. If no SELECT statement is present in the SQL commands, an empty DataFrame is returned.
        """
        self.__logger.debug(sql_requests)
        sql_requests = sql_requests.split(";")
        result = pd.DataFrame()
        
        self.__logger.info("executing {} sql command(s):".format(len(sql_requests) - sql_requests.count("")))

        for req in sql_requests:
            if command_name is None:
                local_name = req[:20]
            else:
                local_name = command_name
                
            self.__logger.info("{}... predicted runtime = {}".format(req[:20], str(
                    timedelta(seconds=self.predictor.predict(file_name=local_name)[-1]))))
            start = time.time()
            
            if req.lower().startswith("select"):
                self.__logger.debug("select!")
                result = self.__get_table(req, go_next, *args)
                
            else:
                self.__exequte(req, really_try, go_next, commit, *args)
                      
            self.predictor.remember(file_name=local_name, time=time.time() - start)
        return result
                
    @staticmethod
    def to_date(table: pd.DataFrame, date: str, mapping):
        """
        Converts a column in a pandas DataFrame to datetime format.

        Args:
            table (pd.DataFrame): The DataFrame containing the column to be converted.
            date (str): The name of the column to be converted.
            mapping: A dictionary mapping column names to their respective data types.

        Returns:
            pd.DataFrame: The modified DataFrame with the column converted to datetime format.
            mapping: The updated mapping dictionary.
        """
        if date in mapping:
            for i, row in enumerate(table[date]):
                if table[date][i] == None:
                    continue
                table[date][i] = datetime.datetime.strptime(table[date][i], '%Y-%m-%d')
            mapping[date] = sqlalchemy.types.Date
        else:
            pass

        return table, mapping
    
    def __disconnect(self):
        """
        Disconnects from .the database.
        """
        del self.cursor
        del self.connection
        self.__switch_state()
        
    def close(self):
        self.__disconnect()

    def __del__(self):
        """
        Destructor that disconnects from .the database.
        """
        self.__logger.debug("saving history...")
        self.predictor.save()
        self.__disconnect()
        

class old_connecion:
    """
    A class representing a database connection (old version).

    Args:
        logger (logging.Logger): The logger object for logging messages.
        db (str): The type of database to connect to ('ms', 'pl', 'oracle').
        login (str, optional): The login username. Defaults to the value of the 'LOGIN' environment variable.
        password (str, optional): The login password. Defaults to the value of the 'PASSWORD' environment variable.
    """
    predictor = TimePredictor

    def __init__(self, logger: logging.Logger, db: str, login: str = os.getenv('LOGIN'), password: str = os.getenv('PASSWORD')):
        """
        Initializes a connection object.

        Args:
            logger (logging.Logger): The logger object for logging messages.
            db (str): The type of database to connect to ('ms', 'pl', 'oracle').
            login (str, optional): The login username. Defaults to the value of the 'LOGIN' environment variable.
            password (str, optional): The login password. Defaults to the value of the 'PASSWORD' environment variable.
        """
        self.credentials(login, password)
        self.logger = logger
        try:
            logger.info("connecting to {} as {}...".format(db.lower(), login))
            if db.lower() == 'ms':
                self.connect_MS()
            elif db.lower() == 'pl':
                self.connect_PL()
            elif db.lower() == "oracle":
                self.connect_Oracle()
            else:
                logger.error('not supported db type!')
        except Exception as e:
            logger.error("cant connect to {}! {}".format(db.lower(), e))
        else:
            logger.info('success! (ﾉ^ヮ^)ﾉ*:・ﾟ✧')

    @classmethod
    def set_predictor(cls, logger: logging.Logger):
        """
        Sets the time predictor for the connection class.

        Args:
            logger (logging.Logger): The logger object for logging messages.
        """
        cls.predictor = TimePredictor(logger)

    def credentials(self, login: str, password: str):
        """
        Sets the login credentials.

        Args:
            login (str): The login username.
            password (str): The login password.
        """
        self.login = login
        self.password = password

    def connect_MS(self):
        """
        Connects to a Microsoft SQL Server database using pyodbc.
        """
        self.conn = pyodbc.connect('wonfweone3ovnr3onroj;')
        self.cursor = self.conn.cursor()
        return True

    def connect_PL(self):
        """
        Connects to a PostgreSQL database using cx_Oracle.
        """
        self.conn = cx.connect(self.login, self.password,
                               "jenvrweojvnrov", encoding="UTF-8")
        self.cursor = self.conn.cursor()
        return True

    def connect_Oracle(self):
        """
        Connects to an Oracle database using SQLAlchemy. Not recommended.
        """
        conn_str = 'oracle+cx_oracle://' + self.login + \
            ':' + self.password + '@' + 'efjvnerovneb3'
        engine = sqlalchemy.create_engine(conn_str, max_identifier_length=128)
        self.conn = engine.connect()
        return True

    def copy(self):
        """
        Returns a copy of the connection object.
        """
        new = old_connecion(self.logger, "", self.login, self.password)
        new.conn = self.conn
        new.cursor = copy.deepcopy(self.cursor)
        return new

    def new_cursor(self):
        """
        Returns a new connection object with a new cursor.
        """
        new = self.copy()
        new.cursor = new.conn.cursor()
        return new

    def get_table(self, sql_requests: str, command_name: str = None, *args):
        """
        Executes one or more SQL queries and returns the results as a pandas DataFrame.

        Args:
            sql_requests (str): The SQL queries to execute, separated by semicolons.
            command_name (str, optional): The name of the command to time prediction. Defaults to None.
            *args: Additional keyword arguments to be passed to the SQL queries.

        Returns:
            pd.DataFrame: The combined result of the queries as a pandas DataFrame.
        """
        self.logger.debug(sql_requests)
        sql_requests = sql_requests.split(";")
        result = pd.DataFrame()
        for req in sql_requests:
            try:
                self.logger.debug("--->" + req.format(*args))

                # TODO: переписать этот кринж
                local_name = command_name
                if local_name == None:
                    local_name = sql_requests[:20]
                    local_name = req[:20]
                start = time.time()
                self.logger.info("{}... predicted runtime = {}".format(req[:20], str(
                    timedelta(seconds=self.predictor.predict(file=local_name)[-1]))))
                # dtype_backend="pyarrow"
                result = pd.read_sql(req.format(*args), self.conn)
                self.logger.info("executed! \\(^ᵕ^ )/")
                runtime = time.time() - start
                self.predictor.remember(file=local_name, time=runtime)

            except Exception as e:
                self.logger.error(e)
        return result

    def execute(self, sql_requests: str = None, command_name: str = None, really_try=True, go_next=True):
        """
        Executes one or more SQL commands. The commands must be separated by semicolons.

        Args:
            sql_requests (str): The SQL commands to execute, separated by semicolons.
            command_name (str, optional): The name of the command to time prediction. Defaults to None.
            really_try (bool, optional): Whether to raise errors. Defaults to True.
            go_next (bool, optional): Whether to continue executing commands after an error. Defaults to True.

        Returns:
            bool: True if the commands were executed successfully, False otherwise.
        """
        self.logger.debug(sql_requests)
        sql_requests = sql_requests.split(";")
        col = len(sql_requests)
        if "" in sql_requests:
            col -= 1
        self.logger.info("executing {} sql command(s):".format(col))

        for req in sql_requests:
            # try:
            # TODO: переписать этот кринж
            local_name = command_name
            if local_name == None:
                local_name = req[:20]
            start = time.time()
            self.logger.info("{}... predicted runtime = {}".format(req[:20], str(
                timedelta(seconds=self.predictor.predict(file=local_name)[-1]))))
            self.cursor.execute(req)
            self.logger.info("executed! \\(^ᵕ^ )/")
            runtime = time.time() - start
            self.predictor.remember(file=local_name, time=runtime)

            if self.commit(really_try=False):
                self.logger.debug("commited!")

            # except Exception as e:
            #     if really_try:
            #         self.logger.error(e)
            #         # выводит первые 100 знаков сфейлившего запроса
            #         self.logger.error(req[:100])
            #     if not go_next:
            #         return False
        return True

    def commit(self, really_try=True):
        """
        Commits the current transaction.

        Args:
            really_try (bool, optional): Whether to raise errors. Defaults to True.

        Returns:
            bool: True if the commit was successful, False otherwise.
        """
        try:
            self.cursor.commit()
            return True
        except Exception as e:
            if really_try:
                raise e
            else:
                self.logger.error(e)
            return False

    def __disconnect(self):
        """
        Disconnects from the database.
        """
        try:
            self.conn.close()
        except (pyodbc.ProgrammingError, cx.InterfaceError) as error:
            self.logger.error("conn already closed \\(°Ω°)/")
        except AttributeError:
            self.logger.warning('conn does not exist')
        try:
            self.cursor.close()
        except (pyodbc.ProgrammingError, cx.InterfaceError) as error:
            self.logger.warning("cur already closed (•-•)⌐")
        except AttributeError:
            self.logger.warning('cur does not exist (•-•)⌐')

    def __del__(self):
        """
        Destructor that disconnects from the database.
        """
        self.__disconnect()
