CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.prime_sportive_input 
USING DELTA 
LOCATION '/data/delta/gold/prime_sportive_input';

CREATE TABLE IF NOT EXISTS gold.wellbeing_days_eligibility 
USING DELTA 
LOCATION '/data/delta/gold/wellbeing_days_eligibility';

USE gold;

CREATE OR REPLACE VIEW v_prime_sportive AS
SELECT 
    CAST(employee_id AS INT) AS employee_id,
    CAST(year AS INT) AS year,
    CAST(business_unit AS STRING) AS business_unit,
    CAST(salaire_brut AS DOUBLE) AS salaire_brut,
    CAST(moyen_deplacement AS STRING) AS moyen_deplacement,
    CAST(distance_km AS DOUBLE) AS distance_km
FROM prime_sportive_input;

CREATE OR REPLACE VIEW v_wellbeing AS
SELECT 
    CAST(employee_id AS INT) AS employee_id,
    CAST(year AS INT) AS year,
    CAST(nb_activities_year AS LONG) AS nb_activities_year,
    CAST(first_activity_date AS TIMESTAMP) AS first_activity_date,
    CAST(last_activity_date AS TIMESTAMP) AS last_activity_date
FROM wellbeing_days_eligibility;