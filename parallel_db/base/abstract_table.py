from abc import ABC, abstractmethod
from ..db_connection.connection import Connection
from logging import Logger
import pandas as pd

class AbstractTable(ABC):
    """
    Base tables class. Is used to create tables in a database.
    """
    table = pd.DataFrame
    connection_name = str
    requirements = []
    stages = []
    log_consol = True
    log_file = True
    draw_progress = True
    @abstractmethod
    def __init__(self, __logger: Logger = None, db_connection: Connection = None, con_factory = None, log_consol = True, log_file = True, draw_progress = True):
        """
        Initializes the BaseTable object.

        Args:
            * __logger (Logger, optional): The logger object. Defaults to None.
            * db_connection (connection, optional): The database connection object. Defaults to None.
            * connection_factory (connection_factory, optional): The connection factory object. Defaults to None.

        Raises:
            TypeError: If db_connection is not of type connection.
        """
        pass

    @abstractmethod
    def command(self, script_name: str) -> str:
        """
        Returns the content of an SQL script.

        Args:
            * script_name (str): The name of the SQL script.

        Returns:
            str: The content of the SQL script.
        """
        pass

    @abstractmethod
    def build(self):
        """
        Builds the BaseTable by building its requirements and executing its stages.

        Returns:
            None
        """
        pass
    
    @abstractmethod
    def build_paral(self):
        """
        Builds the BaseTable in parallel by building its requirements and executing its stages.

        Returns:
            None
        """
        pass

    @classmethod
    @abstractmethod
    def _put(cls, table):
        """
        Sets the table attribute of the BaseTable class.

        Args:
            * table: The table object.

        Returns:
            None
        """
        pass
    
    @classmethod
    @abstractmethod
    def set_reqs(cls, reqs: list):
        """
        Sets the requirements attribute of the BaseTable class.

        Args:
            reqs: The requirements.

        Returns:
            None
        """
        pass