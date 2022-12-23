### Spark-based Data Platform foundation (no analytical use cases)
#### Integration Job 1-3 will process Input Data and put it into S3 (GCS) as Parquet files. Consider partitioning for Parquet files. It should be pure PySpark jobs, do not use Jupyter Notebooks.
* [upload_data_to_gcp ](./upload_data_to_gcp.py) - for create bucket and upload files to cloud locally
1. Ingestion Job 1. Create AWS Lambda Function (GCP Cloud Function) to read “videos” data, validate (move invalid records to separate file) and save to S3 (GCS) as Parquet files. The Function should be triggered when new files land to S3 (GCS).
* [gcp_cloud_function.py](./gcp_cloud_function.py) the full copy of the script from the cloud function from CSP with [requirements.txt](./requirements.txt)
##### to be continued)))
2. Ingestion Job 2. Create Spark job to read “users” data, validate (move invalid records to separate file) and save to S3 (GCS) as Parquet files. Create two versions of the job: based on RDD and Datasets. Test and run the job locally.
3. Ingestion Job 3. Create Spark job to read “events” data, validate (move invalid records to separate file) and save to S3 (GCS) as Parquet files. Consider that a single line may contain multiple events. Create two versions of the job: based on RDD and Datasets. Test and run the job locally.