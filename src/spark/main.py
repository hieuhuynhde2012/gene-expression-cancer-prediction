import sys
import os

# Append the root directory to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))  # Go up 2 levels from src/spark/
sys.path.append(ROOT_DIR)

from config.spark_config import SparkConnect
from config.database_config import get_database_config
from pyspark.sql.types import *
from pyspark.sql.functions import col
from src.spark.spark_write_mysql import SparkWriteDatabases


def main():
    # Load database configuration
    db_config = get_database_config()

    # Define Spark JARs and configuration
    jars = [
        r"C:\Users\PC\Desktop\data-sync-pipeline\lib\mysql-connector-j-9.2.0.jar"
    ]
    spark_conf = {
        "spark.jar.packages": "mysql:mysql-connector-java:9.2.0",
    }

    # Initialize Spark session
    spark_session = SparkConnect(
        app_name="GeneExpressionApp",
        master_url="local[*]",
        executor_memory="2g",
        executor_cores=1,
        driver_memory="1g",
        num_executors=1,
        jars=jars,
        spark_conf=spark_conf,
        log_level="ERROR"
    )

    # Define schema for gene info CSV file
    gene_schema = StructType([
        StructField("gene_id", StringType(), True),
        StructField("gene_name", StringType(), True),
        StructField("gene_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("seqname", StringType(), True),
        StructField("start", IntegerType(), True),
        StructField("end", IntegerType(), True),
        StructField("strand", StringType(), True)
    ])

    # Load the CSV file into DataFrame
    df_gene = spark_session.read.csv(
        r"F:\gene-expression-cancer-prediction\data\processed\genes_info.csv",
        header=True,
        schema=gene_schema
    ).dropna(subset=["gene_id"])  # Ensure primary key is not null

    df_gene = df_gene.dropDuplicates(["gene_id"]).repartition(1)

    # Initialize SparkWriteDatabases utility with updated config structure
    df_writer = SparkWriteDatabases(spark_session, db_config)

    # Write data to MySQL with primary key and ignoring duplicates enabled
    df_writer.spark_write_mysql(
        df=df_gene,
        table_name="genes",
        mode="append",
        primary_key="gene_id",
        ignore_duplicates=True
    )


if __name__ == "__main__":
    main()
