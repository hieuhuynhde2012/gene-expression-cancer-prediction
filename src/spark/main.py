import sys
import os

# Append the root directory to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
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

    # Initialize SparkWriteDatabases utility
    df_writer = SparkWriteDatabases(spark_session, db_config)

    # ------------------- Load and write genes table -------------------
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

    df_gene = spark_session.read.csv(
        r"F:\gene-expression-cancer-prediction\data\processed\genes_info.csv",
        header=True,
        schema=gene_schema
    ).dropna(subset=["gene_id"])

    df_gene = df_gene.dropDuplicates(["gene_id"])

    print("✅ [genes] Loaded rows:", df_gene.count())
    df_gene.show(3)

    df_writer.spark_write_mysql(
        df=df_gene,
        table_name="genes",
        mode="append",
        primary_key="gene_id",
        ignore_duplicates=True
    )

    # ------------------- Load and write patients table -------------------
    patients_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("case_id", StringType(), True),
        StructField("age_at_diagnosis", FloatType(), True),
        StructField("gender", StringType(), True),
    ])

    df_patients = spark_session.read.csv(
        r"F:\gene-expression-cancer-prediction\data\processed\clinical_data_extracted.csv",
        header=True,
        schema=patients_schema
    ).dropna(subset=["patient_id"])

    df_patients = df_patients.dropDuplicates(["patient_id"])

    print("✅ [patients] Loaded rows:", df_patients.count())
    df_patients.show(3)

    df_writer.spark_write_mysql(
        df=df_patients,
        table_name="patients",
        mode="overwrite",
        primary_key="patient_id",
        ignore_duplicates=True
    )

    # ------------------- Load and write treatment_outcomes table -------------------
    treatment_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("treatment_outcome", StringType(), True),
        StructField("survival_time_months", FloatType(), True),
    ])

    df_treatment = spark_session.read.csv(
        r"F:\gene-expression-cancer-prediction\data\processed\clinical_data_extracted.csv",
        header=True,
        schema=treatment_schema
    ).dropna(subset=["patient_id", "treatment_outcome"])

    df_treatment_cleaned = df_treatment.dropDuplicates(
        ["patient_id", "treatment_outcome", "survival_time_months"]
    )

    print("✅ [treatment_outcomes] Loaded rows:", df_treatment_cleaned.count())
    df_treatment_cleaned.show(3)

    df_writer.spark_write_mysql(
        df=df_treatment_cleaned,
        table_name="treatment_outcomes",
        mode="append",
        primary_key="patient_id,treatment_outcome",
        ignore_duplicates=True
    )

    # ------------------- Load and write gene_expression table -------------------
    gene_expr_schema = StructType([
        StructField("gene_id", StringType(), True),
        StructField("gene_name", StringType(), True),
        StructField("gene_type", StringType(), True),
        StructField("unstranded", IntegerType(), True),
        StructField("stranded_first", IntegerType(), True),
        StructField("stranded_second", IntegerType(), True),
        StructField("tpm_unstranded", FloatType(), True),
        StructField("fpkm_unstranded", FloatType(), True),
        StructField("fpkm_uq_unstranded", FloatType(), True),
        StructField("sample_id", StringType(), True),
        StructField("case_id", StringType(), True)
    ])

    df_gene_expr = spark_session.read.csv(
        r"F:\gene-expression-cancer-prediction\data\processed\cleaned_gene_expression_data_with_case_id.csv",
        header=True,
        schema=gene_expr_schema
    ).dropna(subset=["sample_id", "gene_id"])

    # Log gene_id distinct từ df_gene_expr
    print("🔍 Distinct gene_id in gene_expression:")
    df_gene_expr.select("gene_id").distinct().show(5, truncate=False)

    # Log gene_id distinct từ bảng genes
    gene_ids_in_genes = df_gene.select("gene_id").distinct()
    print("🔍 Distinct gene_id in genes:")
    gene_ids_in_genes.show(5, truncate=False)

    # Join để lọc chỉ những gene_id có trong bảng genes
    df_gene_expr = df_gene_expr.join(gene_ids_in_genes, on="gene_id", how="inner")

    print(f"✅ [gene_expression] Rows after filtering with genes: {df_gene_expr.count()}")
    df_gene_expr.show(3)

    df_writer.spark_write_mysql(
        df=df_gene_expr,
        table_name="gene_expression",
        mode="append",
        ignore_duplicates=True
    )


if __name__ == "__main__":
    main()
