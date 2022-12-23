import os

from google.cloud import storage

from logger import my_logger

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "data-n-analytics-edu-345714-658a4f6e1c6d.json"

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"

BUCKET_NAME = "raw_files_job_2"
LOCAL_FOLDER = "data"


class GCStorage:
    def __init__(self, storage_client, logger):
        self.logger = logger
        self.client = storage_client
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

    def upload_file(self, bucket: storage.bucket, folder: str, file_name: str) -> None:

        self.logger.info("Uploading file to bucket")
        file_to_upload = os.path.join(folder, file_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_to_upload)


def main():
    logger = my_logger()
    storage_client = storage.Client()
    gcs = GCStorage(storage_client, logger)
    bucket = gcs.create_bucket(BUCKET_NAME)

    logger.info("Getting all files from directory to be uploaded ")
    files = [
        f
        for f in os.listdir(LOCAL_FOLDER)
        if os.path.isfile(os.path.join(LOCAL_FOLDER, f))
    ]

    for file in files:
        gcs.upload_file(bucket, LOCAL_FOLDER, file)


if __name__ == "__main__":
    main()
