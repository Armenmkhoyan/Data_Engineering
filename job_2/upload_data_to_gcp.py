import logging
import os

from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data-n-analytics-edu-345714-658a4f6e1c6d.json"

bucket_name = 'raw_files_job_2'
local_folder = "data"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler()
ch.setFormatter(formatter)

logger.addHandler(ch)


class GCStorage:

    def __init__(self, storage_client):
        self.client = storage_client
        logger.info("Creating GCP object")

    def create_bucket(self, bucket: str, location: str = 'US', requester_pays: bool = False) -> storage.bucket:
        if not self.is_bucket_exist(bucket):
            logger.warning("Try to create new bucket")
            try:
                bucket = self.client.bucket(bucket)
                return self.client.create_bucket(bucket, location=location, requester_pays=requester_pays)

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

    @staticmethod
    def upload_file(bucket: storage.bucket,
                    folder: str,
                    file_name: str
                    ) -> None:

        logger.info('Uploading file to bucket')
        file_to_upload = os.path.join(folder, file_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_to_upload)


def main():

    storage_client = storage.Client()
    gcs = GCStorage(storage_client)
    bucket = gcs.create_bucket(bucket_name)

    logger.info("Getting all files from directory to be uploaded ")
    files = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]

    for file in files:
        gcs.upload_file(bucket, local_folder, file)


if __name__ == "__main__":
    main()
