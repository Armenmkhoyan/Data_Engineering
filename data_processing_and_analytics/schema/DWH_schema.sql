CREATE TABLE "dim_locations" (
  "id" int,
  "key" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  "attitude" varchar,
  "Latitude" varchar,
  "effective_start" timestamp,
  "effective_end" timestamp
);

CREATE TABLE "dim_walkers" (
  "key" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  "id" int,
  "first_name" varchar,
  "last_name" varchar,
  "created_at" timestamp,
  "phone" int,
  "email" varchar,
  "effective_start" timestamp,
  "effective_end" timestamp,
  "is_active" bool
);

CREATE TABLE "dim_videos" (
  "key" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  "id" int,
  "dim_partners_key" int,
  "name" varchar,
  "created_at" varchar,
  "duration" varchar,
  "link" varchar,
  "effective_start" timestamp,
  "effective_end" timestamp,
  "is_active" bool
);

CREATE TABLE "dim_partners" (
  "id" int,
  "key" INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  "name" varchar,
  "phone" int,
  "address" varchar,
  "effective_start" timestamp,
  "effective_end" timestamp,
  "is_active" boolean
);

CREATE TABLE "fact_event_walkers" (
  "id" int,
  "dim_walkers_key" int,
  "dim_videos_key" int,
  "dim_locations_key" int,
  "dim_partners_key" int,
  "video_start_playing_date" timestamp,
  "video_stop_playing_date" timestamp
);

CREATE TABLE "fact_walkers_online_status" (
  "id" int,
  "dim_walkers_key" int,
  "is_online" boolean,
  "date" timestamp
);

ALTER TABLE "dim_videos" ADD FOREIGN KEY ("dim_partners_key") REFERENCES "dim_partners" ("key");

ALTER TABLE "fact_event_walkers" ADD FOREIGN KEY ("dim_walkers_key") REFERENCES "dim_walkers" ("key");

ALTER TABLE "fact_event_walkers" ADD FOREIGN KEY ("dim_videos_key") REFERENCES "dim_videos" ("key");

ALTER TABLE "fact_event_walkers" ADD FOREIGN KEY ("dim_locations_key") REFERENCES "dim_locations" ("key");

ALTER TABLE "fact_event_walkers" ADD FOREIGN KEY ("dim_partners_key") REFERENCES "dim_partners" ("key");

ALTER TABLE "fact_walkers_online_status" ADD FOREIGN KEY ("dim_walkers_key") REFERENCES "dim_walkers" ("key");