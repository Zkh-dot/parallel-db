# _*_ coding: utf-8 _*_
import cx_Oracle as cx
import pyodbc
import os
import copy
import logging
import pandas as pd
import sqlalchemy
import time
from datetime import timedelta
from core.time_predictor.time_predictor import TimePredictor

# класс для создания подключений к базам данных


class connecion:
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
        Connects to an Oracle database using SQLAlchemy. Not recomended.
        """
        conn_str = 'oracle+cx_oracle://' + self.login + \
            ':' + self.password + '@' + 'efjvnerovneb3'
        engine = sqlalchemy.create_engine(conn_str, max_identifier_length=128)
        self.conn = engine.connect()
        return True

    def copy(self):
        """
        returns copy of obj
        """
        new = connecion(self.logger, "", self.login, self.password)
        new.conn = self.conn
        new.cursor = copy.deepcopy(self.cursor)
        return new

    def new_cursor(self):
        """
        Returns connection obj with new cursor
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

    # def get_table(self, sql_request: str):
    #     """
    #     Executes a SQL query and returns the result as a pandas DataFrame.

    #     Args:
    #         sql_request (str): The SQL query to execute.

    #     Returns:
    #         pd.DataFrame: The result of the query as a pandas DataFrame.
    #     """
    #     self.logger.debug(sql_request)

    #     return pd.read_sql(sql_request, self.conn)

    def execute(self, sql_requests: str = None, command_name: str = None, really_try=True, go_next=True):
        """
        Executes one or more SQL commands. The commands must be separated by semicolons.

        Args:
            sql_requests (str): The SQL commands to execute, separated by semicolons.
            command_name (str, optional): The name of the command to time prediction. Defaults to None.
            really_try (bool, optional): Whether to log errors. Defaults to True.
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
        try:
            self.cursor.commit()
            return True
        except Exception as e:
            if really_try:
                self.logger.error(e)
            return False

    def __disconnect(self):
        """
        Disconnects from the database
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
