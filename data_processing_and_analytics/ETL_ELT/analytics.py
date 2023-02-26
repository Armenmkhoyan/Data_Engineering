import os
from datetime import datetime

from pyspark.sql.functions import DataFrame, col, count
from pyspark.sql.session import SparkSession

from schema_and_structures.elt_logger import logger
from utils.utils import parse_date
from validators_and_processors.spark_processors import (clean_transform_df,
                                                        dataframe_from_csv,
                                                        dataframe_from_json,
                                                        init_spark,
                                                        validate_dataframe)

LOCAL_FOLDER = "data"
EVENTS = "events.jsonl"
USERS = "users.csv"
LOCATIONS = "locations.csv"
PARTNERS = "partners.csv"
VIDEOS = "videos.csv"


def analytics_processing(spark: SparkSession):

    start_date = parse_date("1990-01-01")
    end_date = datetime.now()

    events = dataframe_from_json(spark, LOCAL_FOLDER, EVENTS)
    events = clean_transform_df(events)

    users = dataframe_from_csv(spark, LOCAL_FOLDER, USERS)
    users = validate_dataframe(users)

    videos = dataframe_from_csv(spark, LOCAL_FOLDER, VIDEOS)
    videos = validate_dataframe(videos)

    partners = dataframe_from_csv(spark, LOCAL_FOLDER, PARTNERS)
    partners = validate_dataframe(partners)

    top_users = top_n_users(events, users, start_date, end_date)
    top_partners = top_n_partners(
        events, videos, partners, start_date, end_date
    )
    most_engaged = most_engaged_video_by_partners(
        events, videos, partners, start_date, end_date, "like")
    top_users.write.csv(os.path.join(LOCAL_FOLDER, "top_users"), mode="overwrite")
    top_partners.write.csv(os.path.join(LOCAL_FOLDER, "top_partners"), mode="overwrite")
    most_engaged.write.csv(os.path.join(LOCAL_FOLDER, "most_engaged_videos"), mode="overwrite")


def most_engaged_video_by_partners(
    events: DataFrame,
    videos: DataFrame,
    partners: DataFrame,
    start_date: datetime,
    end_date: datetime,
    key: str,
   ) -> DataFrame:

    logger.info("Most liked, disliked video by partners and time markers")

    most_engaged = (
        events.select("*")
        .where(col("timestamp").between(start_date, end_date))
        .where(col("event") == key)
        .groupBy("video_id", "event")
        .agg(count("event").alias("event_count"))
        .orderBy(col("event_count").desc())
        .join(videos, events["video_id"] == videos["id"], "left")
        .select("video_id", "name", "event_count", "partner_id")
        .join(partners, videos["partner_id"] == partners["id"], "left")
        .drop("id")
    )
    most_engaged.show()
    return most_engaged


def top_n_partners(
    events: DataFrame,
    videos: DataFrame,
    partners: DataFrame,
    start_date: datetime,
    end_date: datetime,
) -> DataFrame:

    logger.info("Top N partners by time marker")

    top_partners = (
        events.select("*")
        .where(col("timestamp").between(start_date, end_date))
        .groupBy("video_id")
        .agg(count("event").alias("event_count"))
        .orderBy(col("event_count").desc())
        .join(videos, events["video_id"] == videos["id"], "left")
        .join(partners, videos["partner_id"] == partners["id"], "left")
        .select(
            partners["id"],
            "fname",
            "lname",
            "email",
            "address",
            "phone",
            "video_id",
            "name",
            "event_count",
        )
    )
    top_partners.show()
    return top_partners


def top_n_users(
    events: DataFrame,
    users: DataFrame,
    start_date: datetime,
    end_date: datetime,
) -> DataFrame:

    logger.info("Top N users by time markers")

    top_n_workers = (
        events.select("*")
        .where(col("timestamp").between(start_date, end_date))
        .groupBy("user_id")
        .agg(count(col("event")).alias("event_count"))
        .orderBy(col("event_count").desc())
        .limit(3)
        .join(users, events["user_id"] == users["id"], "left")
        .select("id", "fname", "lname", "email", "event_count")
    )
    top_n_workers.show()
    return top_n_workers


if __name__ == "__main__":
    spark_session = init_spark()
    analytics_processing(spark_session)
