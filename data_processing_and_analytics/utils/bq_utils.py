import os
from typing import Callable, List, Optional

from google.api_core import page_iterator
from google.cloud import bigquery, storage

from schema_and_structures.elt_logger import logger
from schema_and_structures.tables_structure import (QUERY_DICT_FOR_INVALID_RECORDS,
                                                    QUERY_DICT_FOR_VALID_RECORDS, TABLES_STRUCTURE)

DATASET_ID = 'job_4_star'

json_key_path = "/opt/airflow/dags/json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"
json_key_path = json_key_path if os.path.isfile(json_key_path) else \
    "json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path


def execute_tables(client: bigquery.Client, func_executor: Callable, tables: List[str]) -> None:
    """Executes a function on one or more tables in a BigQuery dataset.
    Args:
        client (bigquery.Client): A BigQuery client instance.
        func_executor (Callable): A function that takes two arguments: a BigQuery client
            instance and a table ID, and performs a task on the table.
        tables (List[str]): A list of table names to be processed. If the list is
            empty, this function returns immediately without doing anything.
            If the list contains the string 'all', the function applies `func_executor`
            to all tables in the dataset. Otherwise, the function applies `func_executor`
            to the specified tables.
    Returns:
        None.
    """
    if not tables:
        return

    from ETL_ELT.elt_pipeline import get_tables_from_bq

    for table in get_tables_from_bq(client):

        if len(tables) == 2 and tables[0].lower() == "all":
            if tables[1].lower() in table.table_id:
                func_executor(client, table.table_id)
        elif len(tables) == 1 and tables[0].lower() == "all":
            func_executor(client, table.table_id)
        else:
            for table_id in tables:
                func_executor(client, table_id)


def get_field_list(table_id: str) -> Optional[List[str]]:
    return TABLES_STRUCTURE.get(table_id)["fields"]


def _create_query_part_for_valid_or_invalid_records(table_id: str, for_valid: bool = False) -> str:
    fields_list = get_field_list(table_id)
    if for_valid:
        query_dict_for_records = QUERY_DICT_FOR_VALID_RECORDS
    else:
        query_dict_for_records = QUERY_DICT_FOR_INVALID_RECORDS
    return f" ".join([query_dict_for_records.get(field, "") for field in fields_list])


def _execute_query(client: bigquery.Client, query: str) -> None:
    try:
        client.query(query)
    except Exception as e:
        logger.error(e)


def get_fields(table: str) -> str:
    fields = TABLES_STRUCTURE[table].get("fields")
    return ", ".join(fields)


def move_files_from_source_to_destination_bucket(bucket_name: str, destination_bucket: str) -> None:

    source_client = storage.Client()
    source_bucket = source_client.get_bucket(bucket_name)
    destination_client = storage.Client()
    destination_bucket = destination_client.get_bucket(destination_bucket)

    for blob in source_bucket.list_blobs():
        logger.info(f"Moving File: {blob.name} from: {bucket_name} to: {destination_bucket.name} bucket")
        destination_blob = destination_bucket.blob(blob.name)
        destination_blob.upload_from_string(blob.download_as_string())
        blob.delete()
    logger.info("All files are moved successful")


def truncate_table(client: bigquery.Client, table_id: str) -> None:
    query = f"""TRUNCATE TABLE {DATASET_ID}.{table_id}"""
    logger.info(f"Deleting data from {table_id} table")
    _execute_query(client, query)


def get_tables_from_bq(client: bigquery.Client) -> page_iterator.Iterator:
    logger.info("Getting all tables")
    # dataset = client.dataset(DATASET_ID)

    dataset_id = f"{client.project}.{DATASET_ID}"
    dataset = client.get_dataset(dataset_id)
    return client.list_tables(dataset)
