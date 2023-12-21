from .connection import connecion
from core.base_table import base_table


class connecion_factory:
    def __init__(self, connections: dict[str, connecion]):
        self.connections = connections

    def connect_table(self, logger,  table: base_table):
        if not table.connecion:
            logger.error(
                "Not declared connection in table {}!".format(table.__name__))
        try:
            table = table(logger, self.connections[table.connecion.lower()])
        except KeyError:
            if connecion == None:
                logger.error(
                    "No connection in table {}!".format(table.__name__))
            else:
                logger.error(
                    "Not supported connection in table {}: {}!".format(table.__name__, table.connecion))
        except Exception:
            logger.error("something went wrong")
            raise
        return table
