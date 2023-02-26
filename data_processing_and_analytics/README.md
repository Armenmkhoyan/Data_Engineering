### Spark-based Data Platform foundation
#### Integration Job 1-3 will process Input Data and put it into GCS as Parquet files. Consider partitioning for Parquet files.
#### [DWH Schema](https://dbdiagram.io/d/638269c7c9abfc6111755c94)
* [upload_data_to_gcp ](ETL_ELT/upload_data_to_gcp.py) - for create bucket and upload files to the cloud.

1. Ingestion Job 1. GCP Cloud Function to read “videos” data, validate (move invalid records to separate file) and save to GCS as Parquet files. The Function should be triggered when new files land to GCS.
* [gcp_cloud_function.py](schema_and_structures/gcp_cloud_function.py) the full copy of the script from the cloud function it triggers when new files arrives.


2. Ingestion Job 2. Create Spark job to read “users.csv” data, validate (move invalid records to separate file) and save to GCS as Parquet files. Create two versions of the job: based on RDD and Datasets.
    [Solution](ETL_ELT/process_csv.py)


3. Ingestion Job 3. Create Spark job to read “events.jsonl” data, validate (move invalid records to separate file) and save to GCS as Parquet files. Consider that a single line may contain multiple events. Create two versions of the job: based on RDD and Datasets. Test and run the job locally.
    [Solution](ETL_ELT/process_json.py)




2. Spark-based Data Platform foundation (no analytical use cases)
Integration Job 1-3 will process Input Data and put it into S3 (GCS) as Parquet files. Consider partitioning for Parquet files. It should be pure PySpark jobs, do not use Jupyter Notebooks.
Ingestion Job 1. Create AWS Lambda Function (GCP Cloud Function) to read “videos” data, validate (move invalid records to separate file) and save to S3 (GCS) as Parquet files. The Function should be triggered when new files land to S3 (GCS).
Ingestion Job 2. Create Spark job to read “users” data, validate (move invalid records to separate file) and save to S3 (GCS) as Parquet files. Create two versions of the job: based on RDD and Datasets. Test and run the job locally.
Ingestion Job 3. Create Spark job to read “events” data, validate (move invalid records to separate file) and save to S3 (GCS) as Parquet files. Consider that a single line may contain multiple events. Create two versions of the job: based on RDD and Datasets. Test and run the job locally.

3.Implement your business use cases with Spark
Implement your business use cases, what you have developed and presented to the lector, with Spark. Implement cases on top of Parquet files in blob storage that you have got after completion of 3.2.

4.SQL/Airflow-based Data Platform foundation (no analytical use cases)
Model Star Schema for Input Data (for Snowflake or BigQuery storages). Consider that the user’s data may be updated, you should keep all updates. Consider optimization techniques for Snowflake (Clustering) and BigQuery (partitioning and clustering). 
Put manually source csv files into S3 (GCS).
Develop data ingestion jobs. It should be SQL orchestrated via Airflow. No data should be moved directly truth Airflow, it should only run SQL queries.
Injection job 1. Develop data injection from Blob Storage to DWH Raw tables. 
For BigQuery all jobs should be SQL batch load jobs.
(optionally) Develop Dataflow streaming job
For Snowflake develop: 
Continuous/Streaming load for users and videos data.
Batch load for event data (for instance once per 3 hours).
Injection Job 2. Create SQL jobs (orchestrated by Airflow, for instance, to run once per 3 hours) to process Raw Tables data and save it to Core tables. Jobs should validate records (move invalid records to separate tables). The Core tables should be a Star Schema that was modeled before.