import glob
import logging
import os

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.session import SparkSession

from validators import (is_digit, is_valid_address, is_valid_coordinate,
                        is_valid_email, is_valid_text, is_validate_url)

FIELDS_TO_VALIDATE = {
    "id": is_digit,
    "url": is_validate_url,
    "latitude": is_valid_coordinate,
    "longitude": is_valid_coordinate,
    "address": is_valid_address,
    "email": is_valid_email,
    "name": is_valid_text,
    "fname": is_valid_text,
    "lname": is_valid_text,
}


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


def validate_dataframe(df: DataFrame, logger: logging) -> DataFrame:
    logger.info("Validating Dataframe")
    fields_to_validate = {col_name: func for column in df.columns
                          for col_name, func in FIELDS_TO_VALIDATE.items() if column == col_name}
    if len(fields_to_validate) >= 1:
        for col_name, validator_function in fields_to_validate.items():
            udf_validator = udf(lambda x: validator_function(x))
            if "is_valid" in df.columns:
                column_by_condition = f.when(col("is_valid") == False, col("is_valid"))\
                    .when(col("is_valid") == True, udf_validator(col(col_name))).otherwise(0)
                df = df.withColumn("is_valid", column_by_condition)
            else:
                df = df.withColumn("is_valid", udf_validator(col_name))
            df = df.filter(col("is_valid") == True).drop("is_valid")
    return df


def clean_transform_df(df: DataFrame, logger: logging) -> DataFrame:
    df = unpack_nested_df(df, logger)
    df = convert_timestamp(df, logger)
    df = remove_none_rows(df, logger)
    return df


def convert_timestamp(df: DataFrame, logger: logging) -> DataFrame:
    logger.info("Converting timestamp columns")
    timestamp_cols = [column for column, dtype in df.dtypes if column == "timestamp" and isinstance(dtype, str)]
    if timestamp_cols:
        for timestamp_col in timestamp_cols:
            df = df.withColumn(timestamp_col, col(timestamp_col).cast(t.TimestampType()))
    return df


def unpack_nested_df(df: DataFrame, logger: logging) -> DataFrame:
    logger.info("Unpacking nested dataframe")
    columns = _get_nested_columns(df)
    for column in columns:
        nested_df = df.withColumn(column, f.explode(column)).select(f"{column}.*")
        df = df.drop(column).unionByName(nested_df, allowMissingColumns=True)
    return df


def remove_none_rows(df: DataFrame, logger: logging) -> DataFrame:
    logger.info("Removing rows with None values")
    return df.na.drop("all")


def _get_nested_columns(df: DataFrame) -> list:

    return [field[0] for field in df.dtypes if "struct" in field[1]]
