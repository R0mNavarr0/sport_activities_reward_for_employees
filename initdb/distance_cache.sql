DROP TABLE IF EXISTS distance_cache CASCADE;

CREATE TABLE distance_cache (
    id              SERIAL PRIMARY KEY,
    origin_address  text NOT NULL,
    destination_address text NOT NULL,
    distance_m      integer,
    duration_s      integer,
    provider        text NOT NULL DEFAULT 'google_maps',
    status          text NOT NULL,
    error_message   text,
    last_updated    timestamp without time zone DEFAULT now(),
    UNIQUE(origin_address, destination_address, provider)
);