from .db_connection.connection import connection
import os
import threading
import functools
from .rich.progress import Progress
import logger
from .db_connection.connecion_factory import connecion_factory
from .logging import Logger
import decorators.utils as utils

class base_table():
    table = None
    connection_name = str
    requirements = []
    stages = []

    def __init__(self, __logger: Logger = None, db_connection: connection = None, connection_factory: connecion_factory = None):
        if __logger == None:
            self.__create_logger()
        else:
            self.__logger = __logger
            
        if isinstance(db_connection, connection):
            self.connection = connection
        else:
            raise TypeError("db_connection is not connection")
        for i, table in enumerate(self.requirements):
            self.requirements[i] = connection_factory.connect_table(__logger, table)
            
    def __init_subclass__(cls):
        cls.__set_sql_scripts_path()
        
    def __create_logger(self):
        self.__logger = logger.get_logger(self.__class__.__name__)
        decorator_with_logger = functools.partial(logger.trace_call, self.__logger)
        utils.decorate_function_by_name(decorator_with_logger, "read_sql", "pandas")
        utils.decorate_function_by_name(decorator_with_logger, "Cursor.execute", "pyodbc")
        utils.decorate_function_by_name(decorator_with_logger, "DataFrame.to_sql", "pandas")
            
    def __set_sql_scripts_path(self):
        """
        Returns the path to the directory containing SQL scripts.

        Args:
            file (str): The path to the current file.

        Returns:
            str: The path to the directory containing SQL scripts.
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
        for r in self.requirements:
            r.build()

        for stage in self.stages:
            stage()


    def build_paral(self, progress: Progress):
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
                progress.update(self.task, advance=1)
                stage()
                progress.update(self.task, advance=1)                
          
    @classmethod
    def _put(cls, table):
        cls.table = table
