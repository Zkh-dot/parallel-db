from ..db_connection.connection import connection
import os
import threading
import functools
from rich.progress import Progress
from .. import logger
from ..db_connection.connection_factory import connection_factory
from logging import Logger
from ..decorators import utils as utils

class base_table():
    table = None
    connection_name = str
    requirements = []
    stages = []
    log_consol = True
    log_file = True
    draw_progress = True

    def __init__(self, __logger: Logger = None, db_connection: connection = None, connection_factory: connection_factory = None, log_consol = True, log_file = True, draw_progress = True):
        """
        Initializes the base_table object.

        Args:
            __logger (Logger, optional): The logger object. Defaults to None.
            db_connection (connection, optional): The database connection object. Defaults to None.
            connection_factory (connection_factory, optional): The connection factory object. Defaults to None.

        Raises:
            TypeError: If db_connection is not of type connection.
        """
        self.log_consol = log_consol
        self.log_file = log_file
        self.draw_progress = draw_progress
        
        if __logger == None:
            self.__create_logger()
        else:
            self.__logger = __logger
            
        if isinstance(db_connection, connection):
            self.connection = connection
        else:
            self.__logger.warning("db_connection is not connection!")
        
        if not isinstance(connection_factory, connection_factory):
            raise TypeError("connection_factory is not connection_factory")
        
        for i, table in enumerate(self.requirements):
            self.requirements[i] = connection_factory.connect_table(__logger, table)
            
    def __init_subclass__(cls):
        cls.__set_sql_scripts_path()
        
    def __create_logger(self):
        """
        Creates the logger object and decorates specific functions with logging.

        Returns:
            None
        """
        self.__logger = logger.get_logger(self.__class__.__name__, self.log_consol, self.log_file, self.draw_progress)
        decorator_with_logger = functools.partial(logger.trace_call, self.__logger)
        utils.decorate_function_by_name(decorator_with_logger, "read_sql", "pandas")
        utils.decorate_function_by_name(decorator_with_logger, "Cursor.execute", "pyodbc")
        utils.decorate_function_by_name(decorator_with_logger, "DataFrame.to_sql", "pandas")
            
    def __set_sql_scripts_path(self):
        """
        Sets the path to the directory containing SQL scripts.

        Returns:
            None
        """
        self.__sql_path =  os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql_scripts")

    def command(self, script_name: str) -> str:
        """
        Returns the content of an SQL script.

        Args:
            script_name (str): The name of the SQL script.

        Returns:
            str: The content of the SQL script.
        """
        with open(os.path.join(self.__sql_path, script_name), "r", encoding="utf-8") as sql_script:
            return sql_script.read()

    def build(self):
        """
        Builds the base_table by building its requirements and executing its stages.

        Returns:
            None
        """
        for r in self.requirements:
            r.build()

        for stage in self.stages:
            stage()


    def build_paral(self, progress: Progress = None):
        """
        Builds the base_table in parallel by building its requirements and executing its stages.

        Args:
            progress (Progress, optional): The progress object. Defaults to None.

        Returns:
            None
        """
        if progress:
            self.task = progress.add_task(self.__class__.__name__, total=len(self.stages) * 2)
        threads = []
        for i, r in enumerate(self.requirements):
            x = threading.Thread(target=r.build_paral, args=(progress,))
            threads.append(x)
            self.__logger.info(f"Start thread for {r.__class__.__name__} ( ˶ˆ꒳ˆ˵ )")
            x.start()
        for index, thread in enumerate(threads):
            self.__logger.info(f"successfully calculated {self.requirements[index].__class__.__name__} ˶ᵔ ᵕ ᵔ˶")
            thread.join()
        for stage in self.stages:
            if progress:
                progress.update(self.task, advance=1)
                stage()
                progress.update(self.task, advance=1)                
          
    @classmethod
    def _put(cls, table):
        """
        Sets the table attribute of the base_table class.

        Args:
            table: The table object.

        Returns:
            None
        """
        cls.table = table
