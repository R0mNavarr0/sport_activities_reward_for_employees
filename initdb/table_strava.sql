DROP TABLE IF EXISTS strava_activities CASCADE;

CREATE TABLE strava_activities (
    activity_id         bigint PRIMARY KEY,            
    employee_id         integer REFERENCES rh_employees(id),
    name                text,
    distance_m          integer,
    moving_time_s       integer,
    elapsed_time_s      integer,
    total_elev_gain_m   integer,
    type                text,
    sport_type          text,
    start_date_utc      timestamptz,
    start_date_local    timestamptz,
    timezone            text,
    utc_offset_s        integer,
    commute             boolean,
    manual              boolean,
    private             boolean,
    flagged             boolean,
    calories            integer,
    raw_payload         jsonb,
    created_at          timestamp default now()
);