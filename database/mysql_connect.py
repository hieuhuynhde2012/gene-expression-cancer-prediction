from typing import Optional, Tuple
import mysql.connector
from mysql.connector import Error, MySQLConnection
import logging

import mysql.connector.cursor

class MySQLConnect:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        charset: str = 'utf8mb4',
        autocommit: bool = True,
    ):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset,
        }
        
        if database:
            self.config['database'] = database
            
        self.autocommit = autocommit
        self.connection: Optional[MySQLConnection] = None
        self.cursor = None
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("MySQLConnect")
        
    def connect(self) -> Tuple[Optional[MySQLConnection], Optional[mysql.connector.cursor.MySQLCursor]]:
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                self.connection.autocommit = self.autocommit
                self.logger.info("MySQL connection established.")
                return self.connection, self.cursor
        except Error as e:
            self.logger.error(f"Error connecting to MySQL: {e}")
            return None, None
        
    def close(self):
        if self.cursor:
            self.cursor.close()
            self.logger.info("MySQL cursor closed.")
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("MySQL connection closed.")
            
    def execute_query(self, query: str, params: Optional[Tuple] = None):
        if not self.cursor:
            self.logger.warning("No activate cursor. Did you forget to connect?")
            return None
        try:
            self.cursor.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                return self.cursor.fetchall()
            return True
        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            return None
        
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        