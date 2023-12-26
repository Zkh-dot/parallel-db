from .connection import connection
from logging import Logger
from ..logger import get_logger
from ..base.abstract_table import BaseTable

class connection_factory:
    def __init__(self, connections: dict[str, connection], logger: Logger = None):
        self.connections = connections
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(self.__class__.__name__, False, False, False)

    def connect_table(self,  table) -> BaseTable:
        if not table.connection_name:
            self.logger.error(
                "Not declared connection in table {}!".format(table.__name__))
            raise KeyError
        try:
            table = table(self.logger, self.connections[table.connection_name.lower()], self)
        except KeyError:
            if connection == None:
                self.logger.error(
                    "No connection in table {}!".format(table.__name__))
            else:
                self.logger.error(
                    "Not supported connection in table {}: {}!".format(table.__name__, table.connection_name))
                raise KeyError
        except Exception as e:
            self.logger.error("something went wrong")
            raise NotImplementedError(e)
        return table
