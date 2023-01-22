import logging

from pyspark.sql.functions import col, udf

from logger import my_logger
from pyspark.sql import SparkSession
from spark_based_functions import dataframe_from_csv, dataframe_to_parquet, init_spark
from transform_functions import is_valid_email, is_valid_text
from upload_data_to_gcp import GCStorage, get_files_by_extension

BUCKET_NAME = "processed_parquet_job_2"
LOCAL_FOLDER = "data"
FILE_TO_PROCESS = "users.csv"
VALID_PARQUET_FOLDER = "valid"
INVALID_PARQUET_FOLDER = "invalid"
VALID_PARQUET_RDD_FOLDER = "valid_rdd"
INVALID_PARQUET_RDD_FOLDER = "invalid_rdd"


def users_processing_pipeline(spark: SparkSession, logger: logging):

    logger.info("Processing data")
    df = dataframe_from_csv(spark, LOCAL_FOLDER, FILE_TO_PROCESS, logger)

    logger.info("Check text and email validity")
    udf_validator = udf(lambda fname, lname, email:
                        is_valid_text(fname) and is_valid_text(lname) and is_valid_email(email))

    temp_df = df.withColumn("is_valid", udf_validator(col("fname"), col("lname"), col("email")))

    valid_df = temp_df.filter(col("is_valid") == True).drop(col("is_valid"))
    invalid_df = temp_df.filter(col("is_valid") == False).drop(col("is_valid"))

    dataframe_to_parquet(valid_df, LOCAL_FOLDER, VALID_PARQUET_FOLDER, logger)
    dataframe_to_parquet(invalid_df, LOCAL_FOLDER, INVALID_PARQUET_FOLDER, logger)


def users_processing_pipeline_rdd(spark: SparkSession, logger: logging):
    logger.info("Starting process data by rdd")
    df = dataframe_from_csv(spark, LOCAL_FOLDER, FILE_TO_PROCESS, logger)

    rdd_df = df.rdd
    src_schema = df.schema
    logger.info("Mapping Data")
    mapped_rdd = rdd_df.map(
        lambda x: ((is_valid_text(x["fname"]) and is_valid_text(x["lname"]) and is_valid_email(x["email"])), x))

    valid_rdd = mapped_rdd.filter(lambda x: x[0]).map(lambda x: x[1])
    valid_df = valid_rdd.toDF()

    invalid_rdd = mapped_rdd.filter(lambda x: not x[0]).map(lambda x: x[1])
    invalid_df = invalid_rdd.toDF(src_schema)

    dataframe_to_parquet(valid_df, LOCAL_FOLDER, VALID_PARQUET_RDD_FOLDER, logger)
    dataframe_to_parquet(invalid_df, LOCAL_FOLDER, INVALID_PARQUET_RDD_FOLDER, logger)


def main():
    logger = my_logger()
    gcs = GCStorage(logger)
    bucket = gcs.create_bucket(BUCKET_NAME)

    spark = init_spark()
    users_processing_pipeline(spark, logger)
    users_processing_pipeline_rdd(spark, logger)

    parquet_files = get_files_by_extension(LOCAL_FOLDER, logger, "parquet")
    gcs.upload_files(bucket, parquet_files, by_folder=True)


if __name__ == "__main__":
    main()

