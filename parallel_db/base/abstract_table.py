from abc import ABC, abstractmethod
from ..db_connection.connection import connection
from logging import Logger
from rich.progress import Progress
import pandas as pd

class BaseTable(ABC):
    table = pd.DataFrame
    connection_name = str
    requirements = []
    stages = []
    log_consol = True
    log_file = True
    draw_progress = True
    @abstractmethod
    def __init__(self, __logger: Logger = None, db_connection: connection = None, con_factory = None, log_consol = True, log_file = True, draw_progress = True):
        """
        Initializes the base_table object.

        Args:
            __logger (Logger, optional): The logger object. Defaults to None.
            db_connection (connection, optional): The database connection object. Defaults to None.
            connection_factory (connection_factory, optional): The connection factory object. Defaults to None.

        Raises:
            TypeError: If db_connection is not of type connection.
        """
        pass

    @abstractmethod
    def command(self, script_name: str) -> str:
        """
        Returns the content of an SQL script.

        Args:
            script_name (str): The name of the SQL script.

        Returns:
            str: The content of the SQL script.
        """
        pass

    @abstractmethod
    def build(self):
        """
        Builds the base_table by building its requirements and executing its stages.

        Returns:
            None
        """
        pass
    
    @abstractmethod
    def build_paral(self, progress: Progress = None):
        """
        Builds the base_table in parallel by building its requirements and executing its stages.

        Args:
            progress (Progress, optional): The progress object. Defaults to None.

        Returns:
            None
        """
        pass
    @abstractmethod
    def _put(cls, table):
        """
        Sets the table attribute of the base_table class.

        Args:
            table: The table object.

        Returns:
            None
        """
        pass