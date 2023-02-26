
USERS = {
    "raw": "raw_users",
    "error": "error_users",
    "stage": "stage_users",
    "dm": "dm_users",
}

PARTNERS = {
    "raw": "raw_partners",
    "error": "error_partners",
    "stage": "stage_partners",
    "dm": "dm_partners",
}

VIDEOS = {
    "raw": "raw_videos",
    "error": "error_videos",
    "stage": "stage_videos",
    "dm": "dm_videos",
}

LOCATIONS = {
    "raw": "raw_locations",
    "error": "error_locations",
    "stage": "stage_locations",
    "dm": "dm_locations",
}

EVENTS = {
    "raw": "raw_events",
    "stage": "stage_events",
    "fact": "fact_events",
}

FILE_EXTENSIONS = {"json": "JSON", "jsonl": "JSON", "csv": "CSV"}

TABLES = [
    PARTNERS,
    VIDEOS,
    LOCATIONS,
    USERS,
    EVENTS,
]

FIELD_TYPES = {
    "key": {"type": "BYTES", "mode": "NULLABLE"},
    "id": {"type": "INTEGER", "mode": "NULLABLE"},
    "unique_id": {"type": "STRING", "mode": "NULLABLE"},
    "partner_id": {"type": "INTEGER", "mode": "NULLABLE"},
    "phone": {"type": "INTEGER", "mode": "NULLABLE"},
    "latitude": {"type": "FLOAT64", "mode": "NULLABLE"},
    "longitude": {"type": "FLOAT64", "mode": "NULLABLE"},
    "fname": {"type": "STRING", "mode": "NULLABLE"},
    "lname": {"type": "STRING", "mode": "NULLABLE"},
    "email": {"type": "STRING", "mode": "NULLABLE"},
    "name": {"type": "STRING", "mode": "NULLABLE"},
    "url": {"type": "STRING", "mode": "NULLABLE"},
    "address": {"type": "STRING", "mode": "NULLABLE"},
    "effective_start": {"type": "TIMESTAMP", "mode": "NULLABLE"},
    "effective_end": {"type": "TIMESTAMP", "mode": "NULLABLE"},
    "is_current": {"type": "BOOLEAN", "mode": "NULLABLE"},
    "user_id": {"type": "INTEGER", "mode": "NULLABLE"},
    "video_id": {"type": "INTEGER", "mode": "NULLABLE"},
    "event": {"type": "STRING", "mode": "NULLABLE"},
    "timestamp": {"type": "TIMESTAMP", "mode": "NULLABLE"},
    "comment": {"type": "STRING", "mode": "NULLABLE"},
    "events": {"type": "RECORD", "mode": "REPEATED"},
    "dm_users_key": {"type": "BYTES", "mode": "NULLABLE"},
    "dm_videos_key": {"type": "BYTES", "mode": "NULLABLE"},
    "dm_locations_key": {"type": "BYTES", "mode": "NULLABLE"},
    "dm_partners_key": {"type": "BYTES", "mode": "NULLABLE"},
}

TABLES_STRUCTURE = {
    "raw_events": {
        "partitioning_by": "HOUR",
        "fields": ["events", "user_id", "video_id", "event", "timestamp", "latitude", "longitude", "comment"],
        "nested_fields": ["user_id", "video_id", "event", "timestamp", "latitude", "longitude", "comment"],
    },
    "stage_events": {
        "fields": ["user_id", "video_id", "event", "timestamp", "latitude", "longitude", "comment"],
    },
    "fact_events": {
        "fields": ["id", "dm_users_key", "dm_videos_key", "dm_locations_key", "dm_partners_key",
                   "timestamp", "comment", "event"],
    },

    "raw_locations": {
        "partitioning_by": "HOUR",
        "fields": ["id", "latitude", "longitude", "address", "timestamp"],
    },
    "error_locations": {
        "fields": ["id", "latitude", "longitude", "address", "timestamp"],
    },
    "dm_locations": {
        "fields": ["key", "id", "latitude", "longitude", "address", "effective_start", "effective_end", "is_current"],
    },
    "stage_locations": {
        "fields": ["key", "id", "latitude", "longitude", "address", "effective_start", "effective_end"],
    },


    "raw_partners": {
        "partitioning_by": "HOUR",
        "fields": ["id", "fname", "lname", "email", "address", "phone", "timestamp"],
    },
    "error_partners": {
        "fields": ["id", "fname", "lname", "email", "address", "phone", "timestamp"],
    },
    "dm_partners": {
        "fields": ["key", "id", "fname", "lname", "email", "address", "phone", "timestamp",
                   "effective_start", "effective_end", "is_current"],
    },
    "stage_partners": {
        "fields": ["key", "id", "fname", "lname", "email", "address", "phone", "timestamp",
                   "effective_start", "effective_end"],
    },


    "raw_videos": {
        "partitioning_by": "HOUR",
        "fields": ["id", "name", "url", "timestamp", "partner_id"],
    },
    "error_videos": {
        "fields": ["id", "name", "url", "timestamp", "partner_id"],
    },
    "dm_videos": {
        "fields": ["key", "id", "name", "url", "timestamp", "partner_id",
                   "effective_start", "effective_end", "is_current"],
    },
    "stage_videos": {
        "fields": ["key", "id", "name", "url", "timestamp", "partner_id",
                   "effective_start", "effective_end"],
    },


    "raw_users": {
        "partitioning_by": "HOUR",
        "fields": ["id", "fname", "lname", "email", "timestamp", "phone"],
    },
    "error_users": {
        "fields": ["id", "fname", "lname", "email", "timestamp", "phone"],
    },
    "dm_users": {
        "fields": ["key", "id", "fname", "lname", "email", "timestamp", "phone", "effective_start",
                   "effective_end", "is_current"]
    },
    "stage_users": {
        "fields": ["key", "id", "fname", "lname", "email", "timestamp", "phone", "effective_start",
                   "effective_end"]
    },
}

QUERY_DICT_FOR_INVALID_RECORDS = {
    "id": "id IS NULL",
    "partner_id": "OR partner_id IS NULL",
    "latitude": "OR latitude IS NULL",
    "longitude": "OR longitude IS NULL",
    "fname": "OR fname IS NULL",
    "lname": "OR lname IS NULL",
    "name": "OR name IS NULL",
    "email": "OR NOT REGEXP_CONTAINS(email, r'[^@]+@[^@]+[^@]+')",
    "url": "OR NOT REGEXP_CONTAINS(url, r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+')",
    "address": "OR address IS NULL",
    "timestamp": "OR timestamp < TIMESTAMP('2020-01-01 00:00:00')",

}

QUERY_DICT_FOR_VALID_RECORDS = {
    "id": "AND id IS NOT NULL",
    "partner_id": "AND partner_id IS NOT NULL",
    "latitude": "AND latitude IS NOT NULL",
    "longitude": "AND longitude IS NOT NULL",
    "fname": "AND fname IS NOT NULL",
    "lname": "AND lname IS NOT NULL",
    "name": "AND name IS NOT NULL",
    "email": "AND REGEXP_CONTAINS(email, r'[^@]+@[^@]+[^@]+')",
    "url": "AND REGEXP_CONTAINS(url, r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+')",
    "address": "AND address IS NOT NULL",
    "timestamp": "AND timestamp > TIMESTAMP('2020-01-01 00:00:00')",
    "events": "AND events IS NOT NULL",
    "user_id": "AND user_id IS NOT NULL",
}
