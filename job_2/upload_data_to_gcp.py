import logging
import os

from google.cloud import storage

from logger import my_logger
from spark_based_functions import get_files_by_extension

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"

BUCKET_NAME = "raw_files_job_2"
LOCAL_FOLDER = "data"


class GCStorage:
    def __init__(self, logger: logging):
        self.logger = logger
        self.client = storage.Client()
        self.logger.info("Creating GCP object")

    def create_bucket(
        self, bucket: str, location: str = "US", requester_pays: bool = False
    ) -> storage.bucket:
        if not self.is_bucket_exist(bucket):
            self.logger.warning("Try to create new bucket")
            try:
                bucket = self.client.bucket(bucket)
                return self.client.create_bucket(
                    bucket, location=location, requester_pays=requester_pays
                )

            except Exception as ex:
                self.logger.error("An error occurred: %s", ex)
        return self.get_bucket(bucket)

    def get_bucket(self, bucket: str) -> storage.bucket:
        self.logger.info("Getting bucket")
        return self.client.get_bucket(bucket)

    def is_bucket_exist(self, bucket: str) -> bool:
        self.logger.info("Check is bucket exist")
        buckets = self.client.list_buckets()
        return bucket in [bucket.name for bucket in buckets]

    def upload_files(self, bucket: storage.bucket, files: list, by_folder: bool = False) -> None:

        self.logger.info("Uploading files to bucket")

        for file in files:
            if by_folder:
                dirname = os.path.basename(os.path.dirname(file))
                file_name = os.path.basename(file)
                blob_name = os.path.join(dirname, file_name)
            else:
                blob_name = os.path.basename(file)

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file)


def main():
    logger = my_logger()
    gcs = GCStorage(logger)
    bucket = gcs.create_bucket(BUCKET_NAME)
    files = get_files_by_extension(LOCAL_FOLDER, logger, "json", "csv", "jsonl")
    gcs.upload_files(bucket, files)


if __name__ == "__main__":
    main()
