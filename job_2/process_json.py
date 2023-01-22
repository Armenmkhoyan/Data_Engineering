import logging

import pyspark.sql.types as t
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

from logger import my_logger
from spark_based_functions import (dataframe_from_json, dataframe_to_parquet,
                                   init_spark)
from transform_functions import (convert_timestamp, del_elem_by_key,
                                 set_element_to_none)
from upload_data_to_gcp import GCStorage, get_files_by_extension

BUCKET_NAME = "processed_parquet_job_2"
LOCAL_FOLDER = "data"
FILE_TO_PROCESS = "events.jsonl"
PATH_TO_SAVE = "events"


def events_processing_pipeline(spark: SparkSession, logger: logging):
    logger.info("Starts processing json")
    df = dataframe_from_json(spark, LOCAL_FOLDER, FILE_TO_PROCESS, logger)

    logger.info("Creating new DF from multi lines, converting timestamp, filtering")
    events_inner = (
                    df.where(col("events").isNotNull())
                    .select("events")
                    .withColumn("events", f.explode("events"))
                    .select("events.*")
                    .withColumn("timestamp", col("timestamp").cast(t.TimestampType()))
    )

    logger.info("Filtering null elements, converting timestamp")
    df = (
            df.where(col("events").isNull())
            .select("comment", "event", "tags", "timestamp", "user_id", "video_id")
            .withColumn("timestamp", col("timestamp").cast(t.TimestampType()))
    )

    logger.info("Merging two dataframes")
    df = df.unionByName(events_inner, allowMissingColumns=True)

    df.show(truncate=False)

    dataframe_to_parquet(df, LOCAL_FOLDER, PATH_TO_SAVE, logger)


def events_processing_pipeline_rdd(spark: SparkSession, logger: logging):
    logger.info("Starts processing JSON")
    rdd_dataframe = dataframe_from_json(spark, LOCAL_FOLDER, FILE_TO_PROCESS, logger)

    logger.info("Converting to rdd")
    rdd_dataframe = rdd_dataframe.rdd

    logger.info("Converting Row to Dict")
    rdd_dataframe = rdd_dataframe.map(lambda x: x.asDict())

    logger.info("Creating new DF from multi lines, converting timestamp, filtering")
    rdd_inner_df = (
            rdd_dataframe
            .filter(lambda x: x["events"])
            .map(lambda x: x["events"])
            .flatMap(lambda x: x)
            .map(lambda x: x.asDict())
            .map(lambda x: convert_timestamp(x, "timestamp"))
            .map(lambda x: set_element_to_none(x, "tags"))
    )

    logger.info("Converting timestamp, filtering")
    rdd_dataframe = (
            rdd_dataframe
            .filter(lambda x: x["timestamp"])
            .map(lambda x: del_elem_by_key(x, "events"))
            .map(lambda x: convert_timestamp(x, "timestamp"))
    )

    logger.info("Merging two rdd")
    rdd_df = rdd_dataframe.union(rdd_inner_df)

    logger.info("Converting  rdd to dataframe")
    df = rdd_df.toDF()

    df.show(truncate=False)
    dataframe_to_parquet(df, LOCAL_FOLDER, PATH_TO_SAVE, logger)


def main():
    logger = my_logger()
    gcs = GCStorage(logger)
    bucket = gcs.create_bucket(BUCKET_NAME)

    spark = init_spark()
    events_processing_pipeline(spark, logger)
    events_processing_pipeline_rdd(spark, logger)

    parquet_files = get_files_by_extension(LOCAL_FOLDER, logger, "parquet")
    gcs.upload_files(bucket, parquet_files, by_folder=True)


if __name__ == "__main__":
    main()
