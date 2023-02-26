from pyspark.sql import SparkSession

from schema_and_structures.elt_logger import logger
from utils.gcp_model import GCStorage
from utils.utils import convert_timestamp, del_elem_by_key, set_element_to_none
from validators_and_processors.spark_processors import (clean_transform_df,
                                                        dataframe_from_json,
                                                        dataframe_to_parquet,
                                                        get_files_by_extension,
                                                        init_spark)

BUCKET_NAME = "processed_parquet_job_2"
LOCAL_FOLDER = "/home/universe.dart.spb/amkhoyan/Documents/DataEngeener/TASKS/data_processing_and_analytics/data/pandas"
FILE_TO_PROCESS = "events.jsonl"
PATH_TO_SAVE = "events"


def events_processing_pipeline(spark: SparkSession):
    logger.info("Starts processing json")
    df = dataframe_from_json(spark, LOCAL_FOLDER, FILE_TO_PROCESS)
    df = clean_transform_df(df)
    dataframe_to_parquet(df, LOCAL_FOLDER, PATH_TO_SAVE)


def events_processing_pipeline_rdd(spark: SparkSession):
    logger.info("Starts processing JSON")
    rdd_dataframe = dataframe_from_json(spark, LOCAL_FOLDER, FILE_TO_PROCESS)

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
    dataframe_to_parquet(df, LOCAL_FOLDER, PATH_TO_SAVE)


def main():
    gcs = GCStorage()
    bucket = gcs.create_bucket(BUCKET_NAME)

    spark = init_spark()
    events_processing_pipeline(spark)
    events_processing_pipeline_rdd(spark)

    parquet_files = get_files_by_extension(LOCAL_FOLDER, logger, "parquet")
    gcs.upload_files(bucket, parquet_files, by_folder=True)


if __name__ == "__main__":
    main()
