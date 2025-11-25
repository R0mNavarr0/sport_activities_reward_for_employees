import pandas as pd
from sqlalchemy import create_engine

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "rh_sport"

CSV_RH = "./data/output/data_rh_rdy.csv"
CSV_SPORT = "./data/output/data_sport_rdy.csv"

TABLE_RH = "rh_employees"
TABLE_SPORT = "sport_activities"


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def ingest_rh(engine):
    print(f"Lecture de {CSV_RH} ...")
    df_rh = pd.read_csv(CSV_RH, sep=';')

    print("Colonnes du CSV RH :", list(df_rh.columns))    

    if "date_naissance" in df_rh.columns:
        df_rh["date_naissance"] = pd.to_datetime(df_rh["date_naissance"], errors="coerce").dt.date
    if "date_embauche" in df_rh.columns:
        df_rh["date_embauche"] = pd.to_datetime(df_rh["date_embauche"], errors="coerce").dt.date
    if 'code_postal' in df_rh.columns:
        df_rh['code_postal'] = df_rh['code_postal'].astype(str).str[:5]

    print("Aperçu des données RH :")
    print(df_rh.head())

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
    ingest_rh(engine)
    ingest_sport(engine)
    print("Ingestion terminée pour RH et sport avec schéma explicite.")

if __name__ == "__main__":
    main()