from schema_and_structures.elt_logger import logger
from utils.gcp_model import GCStorage
from validators_and_processors.spark_processors import get_files_by_extension

BUCKET_NAME = "raw_files_job_2"
LOCAL_FOLDER = "/home/universe.dart.spb/amkhoyan/Documents/DataEngeener/TASKS/data_processing_and_analytics/data/pandas"


def main():
    gcs = GCStorage()
    bucket = gcs.create_bucket(BUCKET_NAME)
    files = get_files_by_extension(LOCAL_FOLDER, logger, "json", "csv", "jsonl")
    gcs.upload_files(bucket, files)


if __name__ == "__main__":
    main()
