from google.cloud import bigquery

from DDL.manage_tables import init_bq_client
from schema_and_structures.elt_logger import logger
from schema_and_structures.tables_structure import TABLES
from utils.bq_utils import (_create_query_part_for_valid_or_invalid_records,
                            _execute_query, execute_tables, get_field_list,
                            get_tables_from_bq,
                            move_files_from_source_to_destination_bucket,
                            truncate_table)
from utils.gcp_model import GCStorage
from utils.utils import get_corresponding_table_name, get_file_extension

DATASET_ID = "job_4_star"
BUCKET_NAME = "raw_files_job_2"
DESTINATION_BUCKET = "processed_files_for_job_4"


def load_files_from_bucket_to_raw_tables(client: bigquery.Client) -> None:
    tables = [
        table.table_id
        for table in get_tables_from_bq(client)
        if "raw" in table.table_id
    ]
    files_to_upload = GCStorage.get_files(BUCKET_NAME)
    for file in files_to_upload:
        file_extension = get_file_extension(file)
        raw_table = get_corresponding_table_name(file, tables)

        if raw_table:
            query = f"""
            LOAD DATA INTO {DATASET_ID}.{raw_table}
            FROM FILES (
            max_bad_records=100,
            format = '{file_extension}',
            uris = ['gs://{BUCKET_NAME}/{file}']);
            """
            _execute_query(client, query)
            logger.info("File {} loaded to {} table ".format(file, raw_table))


def load_invalid_records_from_raw_to_error(client: bigquery.Client) -> None:
    for table in TABLES:
        error_table = table.get("error", "")
        raw_table = table["raw"]

        if not error_table:
            continue

        fields_list = get_field_list(raw_table)
        query_part = _create_query_part_for_valid_or_invalid_records(raw_table, False)

        query = f"""
        INSERT INTO {DATASET_ID}.{error_table} ({", ".join(fields_list)})
        SELECT *
        FROM {DATASET_ID}.{raw_table}
        WHERE
            _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM {DATASET_ID}.{raw_table})
            AND {query_part}
        """
        logger.info(f"Load invalid records from {raw_table} to {error_table} table")
        _execute_query(client, query)


def load_valid_records_from_raw_to_stage(client: bigquery.Client) -> None:
    for table in TABLES:
        raw_table = table.get("raw", "")
        stage_table = table.get("stage", "")
        fields_list = get_field_list(raw_table)
        query_part = _create_query_part_for_valid_or_invalid_records(raw_table, True)

        if raw_table == "raw_events":
            fields_list = get_field_list(stage_table)
            query = f"""
            CREATE OR REPLACE TABLE {DATASET_ID}.{stage_table} AS
            SELECT {", ".join(fields_list)}
            FROM {DATASET_ID}.{raw_table}
            WHERE user_id IS NOT NULL
            OR video_id IS NOT NULL
            OR event IS NOT NULL
            OR timestamp IS NOT NULL
            OR latitude IS NOT NULL
            OR longitude IS NOT NULL
            OR comment IS NOT NULL
            UNION ALL
            SELECT e.user_id, e.video_id, e.event, e.timestamp, e.latitude, e.longitude, e.comment
            FROM {DATASET_ID}.{raw_table}, UNNEST(events) as e
            """
        else:
            query = f"""
            CREATE OR REPLACE TABLE {DATASET_ID}.{stage_table} AS
            SELECT MD5({" || ".join(fields_list)}) AS key,
            {", ".join(fields_list)},
            {raw_table}.timestamp AS effective_start,
            TIMESTAMP_TRUNC(TIMESTAMP("9999-12-31"), DAY) AS effective_end
            FROM {DATASET_ID}.{raw_table}
            WHERE
              _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM {DATASET_ID}.{raw_table})
              {query_part}
            """
        logger.info(f"Load valid records from {raw_table} to {stage_table} table")
        _execute_query(client, query)


def load_records_from_stage_to_dm(client: bigquery.Client) -> None:
    for table in TABLES:
        stage_table = table.get("stage", "")
        dm_table = table.get("dm", "")

        if not dm_table:
            continue

        fields_list = get_field_list(stage_table)
        fields = ", ".join(fields_list)
        values = ", ".join(["source." + field for field in fields_list])

        query = f"""
        INSERT INTO {DATASET_ID}.{dm_table}
        ({fields}, is_current)
        SELECT 
        {values.replace('source.effective_end', 'TIMESTAMP_ADD(source.effective_start, INTERVAL 1 MINUTE)')}, FALSE
        FROM {DATASET_ID}.{stage_table}  AS source
        INNER JOIN {DATASET_ID}.{dm_table} AS target
        ON source.id = target.id And target.effective_start > source.effective_start
        """
        _execute_query(client, query)

        query = f"""
        MERGE {DATASET_ID}.{dm_table} AS target
        USING
        (
            SELECT src.id as pseudo_id, src.*
            FROM {DATASET_ID}.{stage_table} AS src
            UNION ALL
            SELECT NULL as pseudo_id, dup.*
            FROM {DATASET_ID}.{stage_table} AS dup
            INNER JOIN {DATASET_ID}.{dm_table} AS target 
            ON dup.id = target.id
            WHERE target.key <> dup.key
        )
        AS source
        ON source.pseudo_id = target.ID
        WHEN NOT MATCHED THEN
          INSERT ({fields}, is_current)
          VALUES ({values}, TRUE)
        WHEN MATCHED AND target.key <> source.key AND target.effective_start > source.effective_start THEN
          UPDATE SET
            target.effective_end = DATE_SUB(source.effective_start, INTERVAL 1 HOUR), target.is_current = FALSE
        """
        _execute_query(client, query)
        logger.info(f"Load records from {stage_table} to {dm_table} table")


def processing_fact_table(client: bigquery.Client) -> None:

    query = f"""
        INSERT INTO {DATASET_ID}.fact_events (
          id,
          event, 
          timestamp, 
          comment, 
          dm_videos_key, 
          dm_partners_key,
          dm_users_key, 
          dm_locations_key
        )
        SELECT
         FARM_FINGERPRINT(GENERATE_UUID()) AS id,
          E.event,
          E.timestamp,
          E.comment,
          V.key AS dm_videos_key,
          P.key AS dm_partners_key,
          U.key AS dm_users_key,
          L.key AS dm_locations_key,

        FROM job_4_star.stage_events AS E
        LEFT JOIN job_4_star.dm_videos AS V
        ON E.video_id = V.id AND V.is_current = True
        LEFT JOIN job_4_star.dm_partners AS P
        ON P.id = partner_id AND P.is_current = True
        LEFT JOIN job_4_star.dm_users AS U
        ON E.user_id = U.id AND U.is_current = True
        LEFT JOIN `job_4_star.dm_locations` AS L
        ON E.latitude = L.latitude AND E.longitude = l.longitude AND L.is_current = True
    """

    logger.info("Processing fact_events table")
    _execute_query(client, query)


def main():
    load_files_from_bucket_to_raw_tables(init_bq_client())
    load_invalid_records_from_raw_to_error(init_bq_client())
    load_valid_records_from_raw_to_stage(init_bq_client())
    load_records_from_stage_to_dm(init_bq_client())
    processing_fact_table(init_bq_client())
    move_files_from_source_to_destination_bucket(BUCKET_NAME, DESTINATION_BUCKET)
    execute_tables(init_bq_client(), truncate_table, ["all"])


if __name__ == "__main__":
    main()
