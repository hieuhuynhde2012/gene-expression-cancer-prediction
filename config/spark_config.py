from typing import Optional, List, Dict
from pyspark.sql import SparkSession
import os

from config.database_config import get_database_config

class SparkConnect:
    def __init__(
        self,
        app_name: str,
        master_url: str = "local[*]",
        executor_memory: Optional[str] = '2g',
        executor_cores: Optional[int] = 2,
        driver_memory: Optional[str] = '2g',
        num_executors: Optional[int] = 3,
        jars: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "WARN",
    ) : 
        self.app_name = app_name
        self.spark = self.create_spark_session(
            master_url=master_url,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            driver_memory=driver_memory,
            num_executors=num_executors,
            jars=jars,
            spark_conf=spark_conf,
            log_level=log_level
        )
        
    def create_spark_session(
        self,
        master_url: str = "local[*]",
        executor_memory: Optional[str] = '2g',
        executor_cores: Optional[int] = 2,
        driver_memory: Optional[str] = '2g',
        num_executors: Optional[int] = 3,
        jars: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "WARN",
    ) -> SparkSession:
        
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)
            
        if executor_memory:
            builder = builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder = builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder = builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder = builder.config("spark.executor.instances", num_executors)
        if jars:
            jars_path = ",".join([os.path.abspath(jar) for jar in jars])
            builder = builder.config("spark.jars", jars_path)
        if spark_conf:
            for key, value in spark_conf.items():
                builder = builder.config(key, value)
                
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        
        return spark
    
    def __getattr__(self, attr):
        return getattr(self.spark, attr)
    
    def __enter__(self):
        self.spark = self.create_spark_session()
        return self.spark
    
    def stop(self):
        if self.spark:
            self.spark.stop()
            print(f"Spark session '{self.app_name}' stopped.")
            