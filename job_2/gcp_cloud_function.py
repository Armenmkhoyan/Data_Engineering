import csv
from urllib.parse import urlparse

import pandas as pd
from google.cloud import storage

file_name = "videos.csv"
valid_data = "valid_data"
invalid_data = "invalid_data"
bucket_for_processed_data = "processed_data_job_2"


def process_data(event, context):
    bucket_name = event["bucket"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    gcs_file = bucket.get_blob(file_name)

    file_contents = gcs_file.download_as_string().decode("utf-8").splitlines()
    valid_df, invalid_df = process_file(file_contents)

    bucket_for_processed = create_bucket(client, bucket_for_processed_data)

    save_dataframe(valid_df, bucket_for_processed, valid_data)
    save_dataframe(invalid_df, bucket_for_processed, invalid_data)


def process_file(file):
    temp_for_valid = []
    temp_for_invalid = []

    reader = csv.DictReader(file)
    for line in reader:
        if validate_line(line):
            temp_for_valid.append(line)
        else:
            temp_for_invalid.append(line)

    valid_df = pd.DataFrame(temp_for_valid)
    invalid_df = pd.DataFrame(temp_for_invalid)

    return valid_df, invalid_df


def validate_line(line):
    return all(
        [
            validate_url(line["url"]),
            is_digit(line["id"]),
            is_digit(line["creation_timestamp"]),
            is_digit(line["creator_id"]),
        ]
    )


def validate_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def is_digit(text):
    return text.isdigit()


def save_dataframe(df, bucket, file_name):
    df.to_parquet(f"/tmp/{file_name}.parquet", index=False)
    blob = bucket.blob(f"{file_name}.parquet")
    blob.upload_from_filename(f"/tmp/{file_name}.parquet")


def create_bucket(client, bucket_name, location="US"):

    if not is_bucket_exist(client, bucket_name):
        try:
            return client.create_bucket(
                bucket_name, location=location, requester_pays=False
            )
        except Exception as ex:
            print("An error occurred: %s", ex)


def is_bucket_exist(client, bucket_name):
    buckets = client.list_buckets()
    return bucket_name in [bucket.name for bucket in buckets]
