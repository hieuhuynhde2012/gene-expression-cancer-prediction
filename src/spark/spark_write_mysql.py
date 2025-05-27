from pyspark.sql import SparkSession, DataFrame
import sys
import os

# Add parent directory to sys.path to import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.schema_manager import (
    create_mysql_schema, validate_mysql_schema
)

from typing import Dict
from database.mysql_connect import MySQLConnect


class SparkWriteDatabases:
    def __init__(self, spark: SparkSession, db_config: Dict):
        """
        Initialize with SparkSession and database configuration dictionary.
        
        Args:
            spark (SparkSession): Spark session instance.
            db_config (Dict): Dictionary containing database configurations returned by get_database_config().
        """
        self.spark = spark
        self.db_config = db_config
        # Extract MySQLConfig instance
        self.mysql_config = db_config["mysql"]["config"]
        # Extract MySQL JDBC URL string
        self.mysql_jdbc_url = db_config["mysql"]["jdbc_url"]

    def spark_write_mysql(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        primary_key: str = None,
        ignore_duplicates: bool = False,
    ):
        """
        Write a Spark DataFrame to MySQL table with optional duplicate ignoring based on primary key.
        
        Args:
            df (DataFrame): Spark DataFrame to write.
            table_name (str): Target MySQL table name.
            mode (str, optional): Write mode. Defaults to "append".
            primary_key (str, optional): Primary key column(s) to check duplicates, comma-separated if multiple.
            ignore_duplicates (bool, optional): Whether to ignore duplicates. Defaults to False.
        
        Raises:
            Exception: If connection to MySQL fails.
        """
        try:
            # Test MySQL connection before write
            mysql_client = MySQLConnect(**self.mysql_config.__dict__)
            mysql_client.connect()
            mysql_client.close()
        except Exception as e:
            raise Exception(f"Error connecting to MySQL: {e}")

        if ignore_duplicates and primary_key:
            # Handle multiple primary keys
            primary_keys = [key.strip() for key in primary_key.split(",")]

            # Read existing primary keys from the target table
            existing_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.mysql_jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.mysql_config.user) \
                .option("password", self.mysql_config.password) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load() \
                .select(*primary_keys)

            # Remove rows from df that already exist in MySQL based on primary key
            df = df.join(existing_df, on=primary_keys, how="left_anti")

            if df.rdd.isEmpty():
                print(f"No new records to insert into {table_name}.")
                return

        # Write DataFrame to MySQL via JDBC
        df.write \
            .format("jdbc") \
            .option("url", self.mysql_jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.mysql_config.user) \
            .option("password", self.mysql_config.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()

        print(f"Data written to {table_name} table in MySQL database successfully.")
