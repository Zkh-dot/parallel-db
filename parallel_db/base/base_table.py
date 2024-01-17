from ..db_connection.connection import Connection
import os
import threading
import pandas as pd
import functools
from .abstract_table import AbstractTable
from .. import logger
from ..db_connection.connection_factory import connection_factory
from logging import Logger
from ..decorators import utils as utils

class classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)
    
class BaseTable(AbstractTable):
    __table = pd.DataFrame
    connection_name = str
    connection = Connection
    requirements = []
    stages = []
    log_consol = True
    log_file = True
    draw_progress = True
    __sql_path = os.path.dirname(os.path.abspath(__file__))

    def __init__(self, __logger: Logger = None, db_connection: Connection = None, con_factory: connection_factory = None, log_consol = True, log_file = True, draw_progress = True, auto_build = False, parallel = False, file = None):
        self.log_consol = log_consol
        self.log_file = log_file
        self.draw_progress = draw_progress
        
        if __logger == None:
            self.__create_logger()
        else:
            self.__logger = __logger
            
        if isinstance(db_connection, Connection):
            self.connection = db_connection
        else:
            self.__logger.warning("db_connection is not connection!")
        
        if not isinstance(con_factory, connection_factory):
            raise TypeError("con_factory is not connection_factory")
        
        for i, table in enumerate(self.requirements):
            self.requirements[i] = con_factory.connect_table(table)
            
        if file:
            self.sql_path = file
        
        if auto_build:
            if parallel:
                self.build_paral()
            else:
                self.build()
        
    def __create_logger(self):
        self.__logger = logger.get_logger(self.__class__.__name__, self.log_consol, self.log_file, self.draw_progress)
        decorator_with_logger = functools.partial(logger.trace_call, self.__logger)
        utils.decorate_function_by_name(decorator_with_logger, "read_sql", "pandas")
        utils.decorate_function_by_name(decorator_with_logger, "Cursor.execute", "pyodbc")
        utils.decorate_function_by_name(decorator_with_logger, "DataFrame.to_sql", "pandas")
        
    @property
    def sql_path(self):
        return self.__sql_path
    
    @sql_path.setter
    def sql_path(self, file):
        self.__sql_path = os.path.join(os.path.dirname(os.path.abspath(file)), "sql_scripts")
        
    @property
    def logger(self):
        return self.__logger

    def command(self, script_name: str) -> str:
        with open(os.path.join(self.__sql_path, script_name), "r", encoding="utf-8") as sql_script:
            return sql_script.read()

    def build(self, custom_stages = [], custom_requirements = [], full = True):
        if custom_requirements == []:
            custom_requirements = self.requirements
        if custom_stages == []:
            custom_stages = self.stages
        if full:
            for r in custom_requirements:
                r.build()

        for stage in custom_stages:
            stage()


    def build_paral(self, custom_stages = [], custom_requirements = []):
        if custom_requirements == []:
            custom_requirements = self.requirements
        if custom_stages == []:
            custom_stages = self.stages 
        self.__logger.debug(f"{self.__class__.__name__} requirements: {custom_requirements}")
        self.__logger.debug(f"{self.__class__.__name__} stages: {custom_stages}")
            
        if self.__logger.progress:
            self.task = self.__logger.progress.add_task(self.__class__.__name__, total=len(custom_stages) * 2)
        threads = []
        for i, r in enumerate(custom_requirements):
            x = threading.Thread(target=r.build_paral, args=())
            threads.append(x)
            self.__logger.info(f"Start thread for {r.__class__.__name__} ( ˶ˆ꒳ˆ˵ )")
            x.start()
        for index, thread in enumerate(threads):
            self.__logger.info(f"successfully calculated {custom_requirements[index].__class__.__name__} ˶ᵔ ᵕ ᵔ˶")
            thread.join()
        for stage in custom_stages:
            if self.__logger.progress:
                self.__logger.progress.update(self.task, advance=1)
            stage()
            if self.__logger.progress:
                self.__logger.progress.update(self.task, advance=1)   
                
    @classmethod
    def set_reqs(cls, reqs: list):
        cls.requirements = reqs     
          
    @classmethod
    def _put(cls, table):
        cls.__table = table
        
    @classproperty
    def table(cls) -> pd.DataFrame:
        return cls.__table
    
    @table.setter
    def table(self, table):
        self._put(table)
