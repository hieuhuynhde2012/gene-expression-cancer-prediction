from dotenv import load_dotenv
import os
from typing import Dict
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"Missing environment variable: {key}")
            
@dataclass
class MySQLConfig(DatabaseConfig):
    host: str
    user: str
    password: str
    port: int
    database: str
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
    
    @property
    def connection_properties(self) -> Dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
@dataclass
class PostgreSQLConfig(DatabaseConfig):
    host: str
    user: str
    password: str
    port: int
    database: str
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    @property
    def connection_properties(self) -> Dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
        
        
def get_database_config() -> Dict[str, DatabaseConfig]:
    load_dotenv()
    
    mysql_config = MySQLConfig(
        host = os.getenv("MYSQL_HOST"),
        user = os.getenv("MYSQL_USER"),
        password = os.getenv("MYSQL_PASSWORD"),
        port = int(os.getenv("MYSQL_PORT" or 3306)),
        database = os.getenv("MYSQL_DATABASE")
    )
    
    postgres_config = PostgreSQLConfig(
        host= os.getenv("POSTGRES_HOST"),
        user= os.getenv("POSTGRES_USER"),
        password= os.getenv("POSTGRES_PASSWORD"),
        port= int(os.getenv("POSTGRES_PORT" or 5432)),
        database= os.getenv("POSTGRES_DATABASE")
    )
    
    config = {
        "mysql": mysql_config,
        "postgres": postgres_config
    }
    
    for db, settings in config.items():
        try:
            settings.validate()
        except ValueError as e:
            print(f"Configuration error for {db}: {e}")
            
    return config
    
    
# if __name__ == "__main__":
#     configs = get_database_config()
#     for name, conf in configs.items():
#         print(f"{name} config: {conf}")