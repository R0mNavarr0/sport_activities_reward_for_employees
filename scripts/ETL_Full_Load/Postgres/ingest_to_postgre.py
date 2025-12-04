import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from sqlalchemy import create_engine
import sys

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "rh_sport"

CSV_RH = "./data/output/data_rh_rdy.csv"
CSV_SPORT = "./data/output/data_sport_rdy.csv"

TABLE_RH = "rh_employees"
TABLE_SPORT = "sport_activities"

# Schema pour RH : Vérifie types, valeurs nulles et règles métiers simples
schema_rh_validator = DataFrameSchema({
    "id": Column(int, Check.greater_than(0), coerce=True),
    "nom": Column(str, Check.str_length(min_value=1)),
    "salaire_brut": Column(int, Check.greater_than(0), nullable=True),
    "date_naissance": Column(pd.Timestamp, nullable=True),
    "nb_conges_payes": Column(int, Check.in_range(0, 100), nullable=True),
    "adresse_complete": Column(str, nullable=True)
})

# Schema pour Sport
schema_sport_validator = DataFrameSchema({
    "id": Column(int, Check.greater_than(0), coerce=True),
    "pratique_sport": Column(str, nullable=True)
})


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def ingest_rh(engine):
    print(f"Lecture de {CSV_RH} ...")
    df_rh = pd.read_csv(CSV_RH, sep=';')

    print("Colonnes du CSV RH :", list(df_rh.columns))    

    if "date_naissance" in df_rh.columns:
        df_rh["date_naissance"] = pd.to_datetime(df_rh["date_naissance"], dayfirst=True, errors="coerce")
    if "date_embauche" in df_rh.columns:
        df_rh["date_embauche"] = pd.to_datetime(df_rh["date_embauche"], dayfirst=True, errors="coerce")
    print("[DQ] Validation des données RH en cours...")
    try:
        schema_rh_validator.validate(df_rh, lazy=True) 
        print("[DQ] Données RH valides.")
        
        if len(df_rh) == 0:
            raise ValueError("Le fichier RH est vide !")
        print(f"[DQ] Volume RH : {len(df_rh)} lignes à ingérer.")

    except pa.errors.SchemaErrors as err:
        print("[DQ] Echec de validation RH :")
        print(err.failure_cases)
        raise err

    print("Aperçu des données RH :")
    print(df_rh.head())

    if "date_naissance" in df_rh.columns:
        df_rh["date_naissance"] = df_rh["date_naissance"].dt.date
    if "date_embauche" in df_rh.columns:
        df_rh["date_embauche"] = df_rh["date_embauche"].dt.date

    df_rh.to_sql(
        TABLE_RH,
        engine,
        schema='public',
        if_exists="append",
        index=False,
        method="multi")
    
    print(f"Ingestion RH terminée : {len(df_rh)} lignes insérées dans {TABLE_RH}.")


def ingest_sport(engine):
    print(f"Lecture de {CSV_SPORT} ...")
    df_sport = pd.read_csv(CSV_SPORT, sep=';')

    print("[DQ] Validation des données Sport...")
    try:
        schema_sport_validator.validate(df_sport, lazy=True)
        print("[DQ] Données Sport valides.")
    except pa.errors.SchemaErrors as err:
        print("[DQ] Echec validation Sport.")
        raise err

    print("Colonnes du CSV SPORT :", list(df_sport.columns))

    print("Aperçu des données SPORT :")
    print(df_sport.head())

    df_sport.to_sql(
        TABLE_SPORT, 
        engine, 
        schema='public',
        if_exists="append", 
        index=False, 
        method="multi")
    print(f"Ingestion sport terminée : {len(df_sport)} lignes insérées dans {TABLE_SPORT}.")


def main():
    engine = get_engine()
    try:
        ingest_rh(engine)
        ingest_sport(engine)
        print("Ingestion terminée pour RH et sport avec validation DQ.")
    except Exception as e:
        print(f"ERREUR CRITIQUE INGESTION : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()