import sys
import os

# Add root directory to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(ROOT_DIR)

from config.spark_config import SparkConnect
from config.database_config import get_database_config
from pyspark.sql.types import *
from pyspark.sql.functions import col
from src.spark.spark_write_mysql import SparkWriteDatabases

def main():
    # Load DB config
    db_config = get_database_config()

    # Spark JARs and config
    jars = [r"C:\Users\PC\Desktop\data-sync-pipeline\lib\mysql-connector-j-9.2.0.jar"]
    spark_conf = {
        "spark.jar.packages": "mysql:mysql-connector-java:9.2.0"
    }

    # Create Spark Session
    spark_session = SparkConnect(
        app_name="GeneExpressionApp",
        master_url="local[*]",
        executor_memory="3g",
        executor_cores=2,
        driver_memory="2g",
        num_executors=2,
        jars=jars,
        spark_conf=spark_conf,
        log_level="ERROR"
    )

    df_writer = SparkWriteDatabases(spark_session, db_config)

    # ------------------- Load & Write genes -------------------
    # gene_schema = StructType([
    #     StructField("gene_id", StringType(), True),
    #     StructField("gene_name", StringType(), True),
    #     StructField("gene_type", StringType(), True),
    #     StructField("source", StringType(), True),
    #     StructField("seqname", StringType(), True),
    #     StructField("start", IntegerType(), True),
    #     StructField("end", IntegerType(), True),
    #     StructField("strand", StringType(), True)
    # ])

    # df_gene = spark_session.read.csv(
    #     r"F:\gene-expression-cancer-prediction\data\processed\genes_info.csv",
    #     header=True,
    #     schema=gene_schema
    # ).dropna(subset=["gene_id"]).dropDuplicates(["gene_id"])

    # print("[genes] Loaded rows:", df_gene.count())
    # df_writer.spark_write_mysql(df_gene, "genes", mode="append", primary_key="gene_id", ignore_duplicates=True)

    # ------------------- Load & Write patients -------------------
    full_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("age_at_diagnosis", FloatType(), True),
    StructField("gender", StringType(), True),
    StructField("treatment_outcome", StringType(), True),
    StructField("survival_time_months", FloatType(), True),
])

    # Đọc dữ liệu với schema đầy đủ
    df_full = spark_session.read.csv(
        r"F:\gene-expression-cancer-prediction\data\processed\clinical_data_extracted.csv",
        header=True,
        schema=full_schema
    )

    # df_patients = df_full.select(
    #     "patient_id", "case_id", "age_at_diagnosis", "gender"
    # ).dropna(subset=["patient_id"]).dropDuplicates(["patient_id"])

    # print("[patients] Loaded rows:", df_patients.count())

    # df_writer.spark_write_mysql(
    #     df_patients,
    #     "patients",
    #     mode="append",  
    #     primary_key="patient_id",
    #     ignore_duplicates=True
    # )

    # ------------------- Load & Write treatment_outcomes -------------------
    
    df_treatment = df_full.select(
        "patient_id", "treatment_outcome", "survival_time_months"
    ).dropna(subset=["patient_id", "treatment_outcome"])


    print("[treatment_outcomes] Loaded rows:", df_treatment.count())

    df_writer.spark_write_mysql(
        df_treatment,
        "treatment_outcomes",
        mode="append",
        primary_key="patient_id,treatment_outcome",  
        ignore_duplicates=True
    ) 

    # # ------------------- Load & Write samples -------------------
    # samples_schema = StructType([
    #     StructField("sample_id", StringType(), True),
    #     StructField("case_id", StringType(), True)
    # ])

    # df_samples = spark_session.read.csv(
    #     r"F:\gene-expression-cancer-prediction\data\processed\cleaned_gene_expression_data_with_case_id.csv",
    #     header=True,
    #     schema=samples_schema
    # ).select("sample_id", "case_id").dropna(subset=["sample_id"]).dropDuplicates(["sample_id"])

    # print("[samples] Loaded rows:", df_samples.count())
    # df_writer.spark_write_mysql(df_samples, "samples", mode="append", primary_key="sample_id", ignore_duplicates=True)

    # # ------------------- Load & Write gene_expression -------------------
    # expr_schema = StructType([
    #     StructField("gene_id", StringType(), True),
    #     StructField("gene_name", StringType(), True),
    #     StructField("gene_type", StringType(), True),
    #     StructField("unstranded", IntegerType(), True),
    #     StructField("stranded_first", IntegerType(), True),
    #     StructField("stranded_second", IntegerType(), True),
    #     StructField("tpm_unstranded", FloatType(), True),
    #     StructField("fpkm_unstranded", FloatType(), True),
    #     StructField("fpkm_uq_unstranded", FloatType(), True),
    #     StructField("sample_id", StringType(), True),
    #     StructField("case_id", StringType(), True)
    # ])

    # df_gene_expr = spark_session.read.csv(
    #     r"F:\gene-expression-cancer-prediction\data\processed\cleaned_gene_expression_data_with_case_id.csv",
    #     header=True,
    #     schema=expr_schema
    # ).dropna(subset=["sample_id", "gene_id"])

    # # Filter by valid gene_id
    # valid_genes = df_gene.select("gene_id").distinct()
    # df_gene_expr = df_gene_expr.join(valid_genes, on="gene_id", how="inner")

    # print("[gene_expression] Loaded rows:", df_gene_expr.count())
    # df_writer.spark_write_mysql(df_gene_expr, "gene_expression", mode="append", ignore_duplicates=True)

    # # ------------------- Load & Write transcripts -------------------
    # transcript_schema = StructType([
    #     StructField("transcript_id", StringType(), True),
    #     StructField("gene_id", StringType(), True),
    #     StructField("strand", StringType(), True)
    # ])

    # df_transcripts = spark_session.read.csv(
    #     r"F:\gene-expression-cancer-prediction\data\processed\transcripts.csv",
    #     header=True,
    #     schema=transcript_schema
    # ).dropna(subset=["transcript_id", "gene_id"]).dropDuplicates(["transcript_id"])

    # print("[transcripts] Loaded rows:", df_transcripts.count())
    # df_writer.spark_write_mysql(df_transcripts, "transcripts", mode="append", primary_key="transcript_id", ignore_duplicates=True)

    # # ------------------- Load & Write exons -------------------
    # exons_schema = StructType([
    #     StructField("transcript_id", StringType(), True),
    #     StructField("start", IntegerType(), True),
    #     StructField("end", IntegerType(), True)
    # ])

    # df_exons = spark_session.read.csv(
    #     r"F:\gene-expression-cancer-prediction\data\processed\exons.csv",
    #     header=True,
    #     schema=exons_schema
    # ).dropna(subset=["transcript_id"]).dropDuplicates()

    # print("[exons] Loaded rows:", df_exons.count())
    # df_writer.spark_write_mysql(df_exons, "exons", mode="append", ignore_duplicates=True)


if __name__ == "__main__":
    main()
