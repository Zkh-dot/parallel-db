from core.db_connection.connection import connecion
import os
import threading
from rich.progress import Progress

class base_table():
    connecion = None
    table = None
    requirements = []

    def __init__(self, logger, conn: connecion):
        self.logger = logger
        if isinstance(conn, connecion):
            self.conn = conn
        else:
            print("not supported connection!")
            raise
        self.stages = []

    @staticmethod
    def sql_scripts_path(file):
        """
        Returns the path to the directory containing SQL scripts.

        Args:
            file (str): The path to the current file.

        Returns:
            str: The path to the directory containing SQL scripts.
        """
        return os.path.join(os.path.dirname(os.path.abspath(file)), "sql_scripts")

    def command(self, script_name: str) -> str:
        """
        Returns the content of an SQL script.

        Args:
            script_name (str): The name of the SQL script.

        Returns:
            str: The content of the SQL script.
        """
        with open(os.path.join(self.sql_scripts, script_name), "r", encoding="utf-8") as sql_script:
            return sql_script.read()

    def build(self):
        for r in self.requirements:
            r.build()

        for stage in self.stages:
            stage()


    # def get_free_position():
    #     """ Return the minimum possible position """
    #     with threading.lock:
    #         free_position = heapq.heappop(free_positions)
    #     return free_position

    # def return_free_position(position):
    #     with lock:
    #         heapq.heappush(free_positions, position)


    def build_paral(self, progress: Progress):
            self.task = progress.add_task(self.__class__.__name__, total=len(self.stages) * 2)
            threads = []
            for i, r in enumerate(self.requirements):
                x = threading.Thread(target=r.build_paral, args=(progress,))
                threads.append(x)
                self.logger.info(f"Start thread for {r.__class__.__name__} ( ˶ˆ꒳ˆ˵ )")
                x.start()
            for index, thread in enumerate(threads):
                self.logger.info(f"successfully calculated {self.requirements[index].__class__.__name__} ˶ᵔ ᵕ ᵔ˶")
                thread.join()
            for stage in self.stages:
                progress.update(self.task, advance=1)
                stage()
                progress.update(self.task, advance=1)                
                
    @classmethod
    def _put(cls, table):
        cls.table = table
