import glob
import logging
import os

from pyspark.sql import DataFrame, SparkSession


def init_spark() -> SparkSession:
    return SparkSession.builder.master("local[4]").getOrCreate()


def dataframe_from_json(spark: SparkSession, path: str, file: str, logger: logging) -> DataFrame:
    logger.info("Creating Dataframe from JSON")
    path = os.path.join(path, file)
    return spark.read.format("json").load(path)


def dataframe_from_csv(spark: SparkSession, path: str, file: str, logger: logging) -> DataFrame:
    logger.info("Creating Dataframe from CSV")
    path = os.path.join(path, file)
    return spark.read.option("header", "true").format("csv").load(path).repartition(4)


def dataframe_to_parquet(df: DataFrame, local_folder: str, dir_name: str, logger: logging):
    logger.info("Writing dataframe to parquet")

    df.coalesce(1).write.mode("overwrite").format("parquet").save(os.path.join(local_folder, dir_name))


def get_files_by_extension(path: str, logger: logging, *args: str) -> list:
    logger.info(f"Getting {args} files from directory")
    all_files = []
    for extension in args:
        files = glob.glob(f'{path}/**/*.{extension}', recursive=True)
        for file in files:
            all_files.append(file)
    return all_files
