import pandas as pd
import datetime as dt
import re

INPUT_RH = "./data/input/data_rh.csv"
INPUT_SPORT = "./data/input/data_sport.csv"

OUTPUT_RH = "./data/staging/data_rh_transformed.csv"
OUTPUT_SPORT = "./data/output/data_sport_rdy.csv"

df_rh = pd.read_csv(INPUT_RH)
df_sport = pd.read_csv(INPUT_SPORT)

def split_adresse(serie: pd.Series) -> pd.DataFrame:
    """
    Prend une série de chaînes d'adresses et renvoie
    un DataFrame avec adresse, code_postal, ville.
    """
    adresses = []
    codes_postaux = []
    villes = []

    for val in serie.fillna(""):
        # Découper avant et après la virgule
        if "," in val:
            adr, reste = val.split(",", 1)
            adr = adr.strip()
            reste = reste.strip()
        else:
            adr = val.strip()
            reste = ""

        # Extraire le code postal (premier bloc de 5 chiffres)
        match_cp = re.search(r"\b\d{5}\b", reste)
        if match_cp:
            cp = match_cp.group(0)
            # Ville = le reste après le CP
            ville = reste.replace(cp, "").strip()
        else:
            cp = None
            ville = reste.strip()

        adresses.append(adr)
        codes_postaux.append(cp)
        villes.append(ville)

    return pd.DataFrame({
        "adresse": adresses,
        "code_postal": codes_postaux,
        "ville": villes
    })

def extraire_jour_mois_annee(serie_dates: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({
        "jour_naissance": serie_dates.dt.day,
        "mois_naissance": serie_dates.dt.month,
        "annee_naissance": serie_dates.dt.year
    })

df_rh['Date de naissance'] = pd.to_datetime(df_rh['Date de naissance'], format='%d/%m/%Y', errors='coerce')
df_rh[["jour_naissance", "mois_naissance", "annee_naissance"]] = extraire_jour_mois_annee(df_rh["Date de naissance"])

df_rh['Date d\'embauche'] = pd.to_datetime(df_rh['Date d\'embauche'], format='%d/%m/%Y', errors='coerce')
df_rh[["jour_embauche", "mois_embauche", "annee_embauche"]] = extraire_jour_mois_annee(df_rh["Date d\'embauche"])

df_rh[["adresse", "code_postal", "ville"]] = split_adresse(df_rh['Adresse du domicile'])

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