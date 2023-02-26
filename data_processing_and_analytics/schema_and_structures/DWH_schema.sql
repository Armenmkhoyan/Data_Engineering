Table dim_locations {
  id int
  key int [pk, increment]
  latitude float8
  longitude float8
  address varchar
  effective_start timestamp
  effective_end timestamp

}

Table dim_users {
  key int [pk, increment]
  id int
  first_name varchar
  last_name varchar
  email varchar
  registered_at timestamp
  phone int
  effective_start timestamp
  effective_end timestamp
  is_active bool
}

Table dim_videos {
  key int [pk, increment]
  id int
  partner_id int
  name varchar
  url varchar
  created_at varchar
  effective_start timestamp
  effective_end timestamp
  is_active bool

  }

Table dim_partners {
  id int
  key int [pk, increment]
  fname varchar
  lname varchar
  email varchar
  address varchar
  phone int
  registered_at timestamp
  effective_start timestamp
  effective_end timestamp
  is_active boolean
}

Table fact_event_users {
  id int
  dim_users_key int
  dim_videos_key int
  dim_locations_key int
  dim_partners_key int
  timestamp timestamp
  event varchar
  comment varchar

}

Ref: fact_event_users.dim_users_key > dim_users.key
Ref: fact_event_users.dim_videos_key > dim_videos.key
Ref: fact_event_users.dim_locations_key > dim_locations.key
Ref: fact_event_users.dim_partners_key > dim_partners.key
Ref: dim_videos.partner_id > dim_partners.id
