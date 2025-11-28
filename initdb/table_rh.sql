DROP TABLE IF EXISTS rh_employees CASCADE;

CREATE TABLE rh_employees (
    id integer PRIMARY KEY,
    nom text NOT NULL,
    prenom text NOT NULL,
    date_naissance date,
    date_embauche date,
    business_unit text,
    salaire_brut numeric(10,2),
    contrat text,
    nb_conges_payes smallint,
    adresse_complete text,
    moyen_deplacement text
);