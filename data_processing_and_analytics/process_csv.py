import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from gcp_model import GCStorage
from logger import logger
from spark_processors import (dataframe_from_csv, dataframe_to_parquet,
                              get_files_by_extension, init_spark)
from validators import is_valid_email, is_valid_text

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"

BUCKET_NAME = "processed_parquet_job_2"
LOCAL_FOLDER = "data"
FILE_TO_PROCESS = "users.csv"
VALID_PARQUET_FOLDER = "valid"
INVALID_PARQUET_FOLDER = "invalid"
VALID_PARQUET_RDD_FOLDER = "valid_rdd"
INVALID_PARQUET_RDD_FOLDER = "invalid_rdd"


def users_processing_pipeline(spark: SparkSession):

    logger.info("Processing data")
    df = dataframe_from_csv(spark, LOCAL_FOLDER, FILE_TO_PROCESS)

    logger.info("Check text and email validity")
    udf_validator = udf(lambda fname, lname, email:
                        is_valid_text(fname) and is_valid_text(lname) and is_valid_email(email))

    temp_df = df.withColumn("is_valid", udf_validator(col("fname"), col("lname"), col("email")))

    valid_df = temp_df.filter(col("is_valid") == True).drop(col("is_valid"))
    invalid_df = temp_df.filter(col("is_valid") == False).drop(col("is_valid"))

    dataframe_to_parquet(valid_df, LOCAL_FOLDER, VALID_PARQUET_FOLDER)
    dataframe_to_parquet(invalid_df, LOCAL_FOLDER, INVALID_PARQUET_FOLDER)


def users_processing_pipeline_rdd(spark: SparkSession):
    logger.info("Starting process data by rdd")
    df = dataframe_from_csv(spark, LOCAL_FOLDER, FILE_TO_PROCESS)

    rdd_df = df.rdd
    src_schema = df.schema
    logger.info("Mapping Data")
    mapped_rdd = rdd_df.map(
        lambda x: ((is_valid_text(x["fname"]) and is_valid_text(x["lname"]) and is_valid_email(x["email"])), x))

    valid_rdd = mapped_rdd.filter(lambda x: x[0]).map(lambda x: x[1])
    valid_df = valid_rdd.toDF()

    invalid_rdd = mapped_rdd.filter(lambda x: not x[0]).map(lambda x: x[1])
    invalid_df = invalid_rdd.toDF(src_schema)

    dataframe_to_parquet(valid_df, LOCAL_FOLDER, VALID_PARQUET_RDD_FOLDER)
    dataframe_to_parquet(invalid_df, LOCAL_FOLDER, INVALID_PARQUET_RDD_FOLDER)


def main():
    gcs = GCStorage()
    bucket = gcs.create_bucket(BUCKET_NAME)

    spark = init_spark()
    users_processing_pipeline(spark)
    users_processing_pipeline_rdd(spark)

    parquet_files = get_files_by_extension(LOCAL_FOLDER, logger, "parquet")
    gcs.upload_files(bucket, parquet_files, by_folder=True)


if __name__ == "__main__":
    main()
