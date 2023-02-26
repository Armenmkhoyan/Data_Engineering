import os
from typing import List, Optional

from google.cloud import bigquery

from schema_and_structures.elt_logger import logger
from schema_and_structures.tables_structure import (FIELD_TYPES,
                                                    TABLES_STRUCTURE)
from utils.bq_utils import execute_tables

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = '/home/universe.dart.spb/amkhoyan/Documents/DataEngeener' \
    '/TASKS/data_processing_and_analytics/json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json'

DATASET_ID = "job_4_star"
BUCKET_NAME = "raw_files_job_2"


def init_bq_client() -> bigquery.Client:
    return bigquery.Client()


def generate_schema(table_name: str) -> List[bigquery.SchemaField]:
    schema_fields = TABLES_STRUCTURE[table_name].get("fields")
    nested_fields = TABLES_STRUCTURE[table_name].get("nested_fields")

    if schema_fields is None:
        raise ValueError(f"Unknown table name: {table_name}")
    return [
        bigquery.SchemaField(
            field, FIELD_TYPES.get(field).get("type"), mode=FIELD_TYPES[field]["mode"],
            fields=[bigquery.SchemaField(nested_field, FIELD_TYPES.get(nested_field).get("type"),
                                         mode=FIELD_TYPES[nested_field]["mode"]) for nested_field in nested_fields or []
                    if FIELD_TYPES[field]["mode"] == "REPEATED"]
        )
        for field in schema_fields
    ]


def create_all_tables(client: bigquery.Client) -> None:
    for table_id in TABLES_STRUCTURE:
        schema = generate_schema(table_id)
        partition_by = TABLES_STRUCTURE[table_id].get("partitioning_by")
        create_table(client, table_id, schema, partitioning_by=partition_by)


def create_table(
    client: bigquery.Client,
    table_id: str,
    schema: List[bigquery.SchemaField],
    partitioning_by: Optional[str] = None,
) -> None:
    # dataset = client.dataset(DATASET_ID)
    dataset_id = f"{client.project}.{DATASET_ID}"
    dataset = client.get_dataset(dataset_id)
    table = bigquery.Table(dataset.table(table_id), schema=schema)
    if partitioning_by is not None:
        table.time_partitioning = bigquery.TimePartitioning(type_=partitioning_by)
    try:
        client.create_table(table)
        logger.info(f"Table: {table_id} created")
    except Exception as e:
        logger.error(e)


def drop_table(client: bigquery.Client, table_id: str) -> None:
    dataset_id = f"{client.project}.{DATASET_ID}"
    dataset = client.get_dataset(dataset_id)
    table_ref = dataset.table(table_id)
    try:
        client.delete_table(table_ref)
        logger.info(f"Table: {table_id} deleted")
    except Exception as e:
        logger.error(e)


def main():
    execute_tables(init_bq_client(), drop_table, ["All"])
    create_all_tables(init_bq_client())


if __name__ == "__main__":
    main()
