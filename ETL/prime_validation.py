import pandas as pd
import json

INPUT_RH = './data/staging/data_rh_with_distance.csv'
OUTPUT_RH = "./data/output/data_rh_rdy.csv"
OUTPUT_ERROR = "./data/output/erreurs_deplacement.json"

df=pd.read_csv(INPUT_RH, sep=";")

prime_validation = []
errors = []

for idx, row in df.iterrows():

    mode = row['moyen_deplacement']
    dist = row['distance_domicile_travail']

    if mode == 'Vélo/Trottinette/Autres' and dist <= 25000:
        prime_validation.append(True)

    elif mode == 'Marche/running' and dist <= 15000:
        prime_validation.append(True)

    else:
        prime_validation.append(False)

    if mode == 'Vélo/Trottinette/Autres' and dist >= 25000:
        errors.append({
            "employee_id": row["employee_id"],
            "moyen_deplacement": mode,
            "distance": dist,
            "error_type": "distance_trop_elevee",
            "message": "Distance incompatible avec 'Vélo/Trottinette/Autres' (>= 25 km)"
        })

    elif mode == 'Marche/running' and dist >= 15000:
        errors.append({
            "employee_id": row["employee_id"],
            "moyen_deplacement": mode,
            "distance": dist,
            "error_type": "distance_trop_elevee",
            "message": "Distance incompatible avec 'Marche/running' (>= 15 km)"
        })

df['prime_validation'] = prime_validation

df.to_csv(OUTPUT_RH, encoding='utf-8', sep=';', index=False)

with open(OUTPUT_ERROR, "w", encoding="utf-8") as f:
    json.dump(errors, f, ensure_ascii=False, indent=4)