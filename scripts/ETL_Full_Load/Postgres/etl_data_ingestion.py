import pandas as pd
import datetime as dt
import re

INPUT_RH = "./data/input/data_rh.csv"
INPUT_SPORT = "./data/input/data_sport.csv"

OUTPUT_RH = "./data/output/data_rh_rdy.csv"
OUTPUT_SPORT = "./data/output/data_sport_rdy.csv"

df_rh = pd.read_csv(INPUT_RH)
df_sport = pd.read_csv(INPUT_SPORT)

dict_col_rh = {
    "ID salarié":"id",
    "Nom" : "nom",
    "Prénom" : "prenom",
    'Date de naissance' : 'date_naissance',
    'Date d\'embauche' : 'date_embauche',
    "BU" : "business_unit",
    "Adresse du domicile" : 'adresse_complete',
    "Salaire brut" : "salaire_brut",
    "Type de contrat" : "contrat",
    "Nombre de jours de CP" : "nb_conges_payes",
    "Moyen de déplacement" : "moyen_deplacement"
}

df_rh = df_rh.rename(columns=dict_col_rh)

dict_col_sport = {
    "ID salarié":"id",
    "Pratique d'un sport" : "pratique_sport"
}

df_sport = df_sport.rename(columns=dict_col_sport)

df_rh.to_csv(OUTPUT_RH, encoding='utf-8', sep=';', index=False)
df_sport.to_csv(OUTPUT_SPORT, encoding='utf-8', sep=';', index=False)