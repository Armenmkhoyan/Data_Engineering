import os

from google.cloud import storage

from gcp_model import GCStorage
from logger import my_logger
from spark_processors import get_files_by_extension

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"

BUCKET_NAME = "raw_files_job_2"
LOCAL_FOLDER = "data"


def main():
    logger = my_logger()
    client = storage.Client()
    gcs = GCStorage(client, logger)
    bucket = gcs.create_bucket(BUCKET_NAME)
    files = get_files_by_extension(LOCAL_FOLDER, logger, "json", "csv", "jsonl")
    gcs.upload_files(bucket, files)


if __name__ == "__main__":
    main()
