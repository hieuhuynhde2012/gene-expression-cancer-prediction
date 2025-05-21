from typing import Optional, Tuple, List, Any
import psycopg2
from psycopg2 import sql, OperationalError, extensions
import logging


class PostgresConnect:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        autocommit: bool = True
    ):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password
        }
        if database:
            self.config['dbname'] = database

        self.autocommit = autocommit
        self.connection: Optional[extensions.connection] = None
        self.cursor: Optional[extensions.cursor] = None

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("PostgresConnect")

    def connect(self) -> Tuple[Optional[extensions.connection], Optional[extensions.cursor]]:
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            self.connection.autocommit = self.autocommit
            self.logger.info("Connected to PostgreSQL.")
            return self.connection, self.cursor
        except OperationalError as e:
            self.logger.error(f"Error connecting to PostgreSQL: {e}")
            return None, None

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.logger.info("PostgreSQL cursor closed.")
        if self.connection:
            self.connection.close()
            self.logger.info("PostgreSQL connection closed.")

    def execute_query(self, query: str, params: Optional[Tuple[Any]] = None) -> Optional[List[Tuple]]:
        if not self.cursor:
            self.logger.warning("No active cursor. Did you forget to connect?")
            return None
        try:
            self.cursor.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                return self.cursor.fetchall()
            return True
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
