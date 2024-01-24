from .connection import Connection
from logging import Logger
from ..logger import get_logger
from ..base.abstract_table import AbstractTable

class connection_factory:
    """
    this class is used to initialise tables with connections
    """
    def __init__(self, connections: dict[str, Connection], logger: Logger = None):
        self.connections = connections
        if logger:
            print(logger.handlers)
        else:
            self.__logger = get_logger(self.__class__.__name__, False, False, False)
            
    def add_connection(self, connection_name: str, connection: Connection):
        self.connections[connection_name] = connection

    def connect_table(self,  table) -> AbstractTable:
        if not table.connection_name:
            self.__logger.error(
                "Not declared connection in table {}!".format(table.__class__.__name__))
            raise KeyError
        try:
            table = table(self.__logger, self.connections[table.connection_name.lower()], self)
        except KeyError:
            if Connection == None:
                self.__logger.error(
                    "No connection in table {}!".format(table.__class__.__name__))
            else:
                self.__logger.error(
                    "Not supported connection in table {}: {}!".format(table.__class__.__name__, table.connection_name))
                raise KeyError
        except Exception as e:
            self.__logger.error("something went wrong")
            raise NotImplementedError(e)
        return table

    def close_all(self):
        for name in self.connections:
            self.connections[name].close()