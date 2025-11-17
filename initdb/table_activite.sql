DROP TABLE IF EXISTS sport_activities CASCADE;

CREATE TABLE sport_activities (
    id integer PRIMARY KEY,
    pratique_sport text,
    created_at timestamp default now()
);