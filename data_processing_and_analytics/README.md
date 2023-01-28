### Spark-based Data Platform foundation
#### Integration Job 1-3 will process Input Data and put it into GCS as Parquet files. Consider partitioning for Parquet files.
#### [DWH Schema](https://dbdiagram.io/d/638269c7c9abfc6111755c94)
* [upload_data_to_gcp ](./upload_data_to_gcp.py) - for create bucket and upload files to the cloud.

1. Ingestion Job 1. GCP Cloud Function to read “videos” data, validate (move invalid records to separate file) and save to GCS as Parquet files. The Function should be triggered when new files land to GCS.
* [gcp_cloud_function.py](./gcp_cloud_function.py) the full copy of the script from the cloud function it triggers when new files arrives.


2. Ingestion Job 2. Create Spark job to read “users.csv” data, validate (move invalid records to separate file) and save to GCS as Parquet files. Create two versions of the job: based on RDD and Datasets.
    [Solution](./process_csv.py)


3. Ingestion Job 3. Create Spark job to read “events.jsonl” data, validate (move invalid records to separate file) and save to GCS as Parquet files. Consider that a single line may contain multiple events. Create two versions of the job: based on RDD and Datasets. Test and run the job locally.
    [Solution](./process_json.py)