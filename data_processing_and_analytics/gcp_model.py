import os

from google.cloud import storage
from logger import logger
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"


class GCStorage:
    client = storage.Client()

    def __init__(self):
        logger.info("Creating GCP object")

    def create_bucket(
            self, bucket: str, location: str = "US", requester_pays: bool = False
    ) -> storage.bucket:
        if not self.is_bucket_exist(bucket):
            logger.warning("Creating new bucket")
            try:
                bucket = self.client.bucket(bucket)
                return self.client.create_bucket(
                    bucket, location=location, requester_pays=requester_pays
                )

            except Exception as ex:
                logger.error("An error occurred: %s", ex)
        return self.get_bucket(bucket)

    def get_bucket(self, bucket: str) -> storage.bucket:
        logger.info("Getting bucket")
        return self.client.get_bucket(bucket)

    def is_bucket_exist(self, bucket: str) -> bool:
        logger.info("Check is bucket exist")
        buckets = self.client.list_buckets()
        return bucket in [bucket.name for bucket in buckets]

    def upload_files(self, bucket: storage.bucket, files: list, by_folder: bool = False) -> None:
        logger.info("Uploading files to bucket")
        for file in files:
            if by_folder:
                dirname = os.path.basename(os.path.dirname(file))
                file_name = os.path.basename(file)
                blob_name = os.path.join(dirname, file_name)
            else:
                blob_name = os.path.basename(file)

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file)

    @staticmethod
    def get_files(bucket: str, file_extension: str = "") -> list[str]:
        logger.info("Getting all files from bucket by extension")
        bucket = GCStorage.client.get_bucket(bucket)
        files = [blob.name for blob in bucket.list_blobs() if blob.name.endswith(file_extension)]
        return files

    def move_files(self, source_bucket: str, destination_bucket: str) -> None:
        logger.info("Moving files in bucket")

        source = self.client.get_bucket(source_bucket)
        if not self.is_bucket_exist(destination_bucket):
            self.create_bucket(destination_bucket)

        for obj in source.list_blobs():
            new_obj = source.copy_blob(obj, self.client.bucket(destination_bucket), obj.name)
            logger.info(f"Moving file: {obj.name} to {destination_bucket} bucket")
            obj.delete()

        logger.info("All files Moved successfully.")
