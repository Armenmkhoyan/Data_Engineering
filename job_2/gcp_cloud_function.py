import csv
from urllib.parse import urlparse

import pandas as pd
from google.cloud import storage
from pandas import DataFrame

FILE_NAME = "videos.csv"
VALID_DATA = "valid_data"
INVALID_DATA = "invalid_data"
BUCKET_FOR_PROCESSED_DATA = "processed_data_job_2"


def process_data(event, context):
    bucket_name = event["bucket"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    gcs_file = bucket.get_blob(FILE_NAME)

    file_contents = gcs_file.download_as_string().decode("utf-8").splitlines()
    valid_df, invalid_df = process_file(file_contents)

    bucket_for_processed = get_bucket(client, BUCKET_FOR_PROCESSED_DATA)

    save_dataframe(valid_df, bucket_for_processed, VALID_DATA)
    save_dataframe(invalid_df, bucket_for_processed, INVALID_DATA)


def process_file(file_contents: list) -> tuple[DataFrame, DataFrame]:
    temp_for_valid = []
    temp_for_invalid = []

    reader = csv.DictReader(file_contents)
    for line in reader:
        if validate_line(line):
            temp_for_valid.append(line)
        else:
            temp_for_invalid.append(line)

    valid_df = pd.DataFrame(temp_for_valid)
    invalid_df = pd.DataFrame(temp_for_invalid)

    return valid_df, invalid_df


def validate_line(line: dict):
    return all(
        [
            validate_url(line["url"]),
            is_digit(line["id"]),
            is_digit(line["creation_timestamp"]),
            is_digit(line["creator_id"]),
        ]
    )


def validate_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def is_digit(text: str) -> bool:
    return text.isdigit()


def save_dataframe(df: DataFrame, bucket: storage.Bucket, file_name: str):
    df.to_parquet(f"/tmp/{file_name}.parquet", index=False)
    blob = bucket.blob(f"{file_name}.parquet")
    blob.upload_from_filename(f"/tmp/{file_name}.parquet")


def get_bucket(client: storage.Client, bucket_name: str) -> storage.Bucket:
    if is_bucket_exist(client, bucket_name):
        return client.get_bucket(bucket_name)
    return create_bucket(client, bucket_name)


def create_bucket(client: storage.Client, bucket_name: str, location: str = "US") -> storage.Bucket:
    try:
        return client.create_bucket(
            bucket_name, location=location, requester_pays=False
        )
    except Exception as ex:
        print("An error occurred: %s", ex)


def is_bucket_exist(client: storage.Client, bucket_name: str) -> bool:
    buckets = client.list_buckets()
    return bucket_name in [bucket.name for bucket in buckets]
