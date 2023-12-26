from .connection import connection
from ..base.base_table import base_table
from logging import Logger


class connection_factory:
    def __init__(self, connections: dict[str, connection]):
        self.connections = connections

    def connect_table(self, logger: Logger,  table: base_table):
        if not table.connection_name:
            logger.error(
                "Not declared connection in table {}!".format(table.__name__))
        try:
            table = table(logger, self.connections[table.connection_name.lower()], self)
        except KeyError:
            if connection == None:
                logger.error(
                    "No connection in table {}!".format(table.__name__))
            else:
                logger.error(
                    "Not supported connection in table {}: {}!".format(table.__name__, table.connection_name))
        except Exception:
            logger.error("something went wrong")
            raise
        return table
