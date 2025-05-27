from dotenv import load_dotenv
import os
from typing import Dict
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"Missing environment variable for {key}")

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
            "driver": "com.mysql.cj.jdbc.Driver",
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
            "driver": "org.postgresql.Driver",
        }


def get_database_config() -> Dict[str, Dict]:
    load_dotenv()

    # Đọc và convert port đúng cách, có default nếu biến env không set
    mysql_port = int(os.getenv("MYSQL_PORT", 3306))
    postgres_port = int(os.getenv("POSTGRES_PORT", 5432))

    mysql_config = MySQLConfig(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        port=mysql_port,
        database=os.getenv("MYSQL_DATABASE"),
    )
    postgres_config = PostgreSQLConfig(
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=postgres_port,
        database=os.getenv("POSTGRES_DATABASE"),
    )

    # Validate configs
    for conf_name, conf in [("mysql", mysql_config), ("postgres", postgres_config)]:
        try:
            conf.validate()
        except ValueError as e:
            print(f"Configuration error for {conf_name}: {e}")

    # Trả về dict theo dạng dễ dùng, jdbc url + properties riêng cho mỗi db
    config = {
        "mysql": {
            "config": mysql_config,
            "jdbc_url": mysql_config.jdbc_url,
            "connection_properties": mysql_config.connection_properties,
        },
        "postgres": {
            "config": postgres_config,
            "jdbc_url": postgres_config.jdbc_url,
            "connection_properties": postgres_config.connection_properties,
        }
    }

    return config


# Test run
if __name__ == "__main__":
    configs = get_database_config()
    for name, conf in configs.items():
        print(f"{name.upper()} JDBC URL: {conf['jdbc_url']}")
        print(f"{name.upper()} Connection Properties: {conf['connection_properties']}") 