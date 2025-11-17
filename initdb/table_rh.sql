DROP TABLE IF EXISTS rh_employees CASCADE;

CREATE TABLE rh_employees (
    id integer PRIMARY KEY,
    nom text NOT NULL,
    prenom text NOT NULL,
    date_naissance date,
    jour_naissance smallint,
    mois_naissance smallint,
    annee_naissance smallint,
    date_embauche date,
    jour_embauche smallint,
    mois_embauche smallint,
    annee_embauche smallint,
    business_unit text,
    salaire_brut numeric(10,2),
    contrat text,
    nb_conges_payes smallint,
    adresse_complete text,
    adresse text,
    code_postal varchar,
    ville text,
    moyen_deplacement text,         
    distance_domicile_travail integer,       
    duree_domicile_travail integer,
    prime_validation boolean,
    created_at timestamp default now()
);
