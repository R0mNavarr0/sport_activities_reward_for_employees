# Sport Activities Reward - Data Lakehouse

Ce projet est un **Proof of Concept (POC)** d'une infrastructure Data Engineering moderne ("Lakehouse"). Il automatise la collecte, le traitement et l'analyse des activités sportives des employés pour calculer des primes RH et des avantages bien-être.

-----

## Vue d'ensemble du projet

L'objectif est de consolider des données RH (salariés) et des données d'activités sportives (type Strava) pour répondre à trois besoins métiers :

1.  **Prime Sportive** : Calculer l'impact financier d'une prime de 5% du salaire brut pour les employés venant au travail en mobilité douce (sous condition de distance).
2.  **Jours Bien-Être** : Identifier les employés éligibles à 5 jours de congés supplémentaires (ceux ayant réalisé au moins 15 activités sportives dans l'année).
3.  **Engagement Salarié** : Notifier en temps réel sur Slack les nouvelles performances sportives.

### Architecture Médaillon (Bronze / Silver / Gold)

L'architecture est optimisée pour le **Temps Réel** avec un traitement en cascade (Waterfall) :

  * **Ingestion & Transformation (Bronze → Silver → Gold)** : Un **Master Stream** unique gère la capture des changements (CDC), le nettoyage, l'enrichissement (API Google Maps) et les agrégations métiers en continu.
  * **Serving** : Les données sont immédiatement disponibles pour le reporting.

-----

## Architecture Technique

Le pipeline de données se décompose comme suit :

1.  **Source & Simulation** :
      * **PostgreSQL** héberge les données opérationnelles (`rh_employees`, `sport_activities`, `strava_activities`).
      * Un script Python simule l'arrivée de nouvelles activités sportives.
2.  **Capture & Streaming** :
      * **Debezium** capture les transactions Postgres (CDC) et les envoie dans **Redpanda** (Kafka).
      * Un **Slack Notifier** écoute Kafka et envoie une alerte pour chaque nouvelle activité.
3.  **Traitement Unifié (Spark Structured Streaming)** :
      * Le script `continuous_master_stream.py` orchestre 4 flux séquentiels en boucle infinie :
        1.  **RH** : Ingestion Bronze → Demande calcul distance → Transformation Silver → Calcul Prime (Gold).
        2.  **Sport** : Ingestion Bronze → Normalisation Silver.
        3.  **Strava** : Ingestion Bronze (avec gestion de types) → Transformation Silver.
        4.  **Bien-Être** : Recalcul incrémental des éligibilités aux congés (Gold) dès qu'une activité arrive.
4.  **Enrichissement** :
      * Un **Distance Worker** asynchrone calcule la distance Domicile-Travail via l'API Google Maps sur demande du stream RH.
5.  **Serving** :
      * **Spark Thrift Server** expose les tables Gold via JDBC/ODBC.
      * **Power BI** consomme ces vues pour le tableau de bord final.

-----

## Pré-requis

  * **Docker Desktop** installé (avec au moins **8 Go de RAM** alloués, 12 Go recommandés).
  * Une clé API **Google Maps** (Distance Matrix API).
  * Un Webhook **Slack** pour les notifications.

-----

## Installation et Démarrage

### 1\. Configuration de l'environnement

Créez un fichier `.env` à la racine du projet avec vos clés :

```env
API_KEY_MAPS="VOTRE_CLE_GOOGLE_MAPS"
DESTINATION="1362 Av. des Platanes, 34970 Lattes"
KAFKA_BOOTSTRAP_SERVERS="redpanda:9092"
SLACK_WEBHOOK_URL="https://hooks.slack.com/services/VOTRE/WEBHOOK/ICI"
# Config BDD
DB_HOST=postgres
DB_PORT=5432
DB_NAME=rh_sport
DB_USER=postgres
DB_PASSWORD=postgres
```

### 2\. Lancement de l'infrastructure

Démarrez l'ensemble des services avec Docker Compose :

```bash
docker-compose up -d --build
```

**Ce qui se passe au démarrage :**

1.  PostgreSQL s'initialise et les données RH initiales (CSV) sont chargées (avec validation **Pandera**).
2.  Les connecteurs Debezium sont configurés automatiquement.
3.  Le **Master Stream Processor** démarre et active la boucle de traitement (Bronze/Silver/Gold).
4.  Le **Spark Thrift Server** démarre et un script d'init crée automatiquement les vues SQL Gold pour Power BI.

-----

## Guide d'Utilisation (Scénarios)

### Scénario 1 : Simulation de Vie (Nouvelles Activités)

Simulez l'activité des employés pour voir le pipeline réagir.

Depuis votre terminal local :

```bash
# Génère 20 activités aléatoires
python simulate_new_strava_activities.py
```

> **Résultat Immédiat :**
>
> 1.  Notification Slack 🔔.
> 2.  La donnée traverse Bronze -\> Silver.
> 3.  Le compteur "Activités Annuelles" de l'employé est mis à jour dans la table Gold (visible dans les logs du stream processor).

### Scénario 2 : Visualisation Power BI

1.  Ouvrez Power BI Desktop.
2.  Connectez-vous à **Spark** (`localhost:10000`), protocole **Standard**, mode **Importer**.
3.  Utilisateur : `admin` / Mdp : (vide).
4.  Sélectionnez les **Vues** dans le dossier `gold` :
      * `v_prime_sportive`
      * `v_wellbeing`
5.  Cliquez sur **Actualiser** : les données sont à jour instantanément.

-----

## Règles Métiers (Power BI)

Les indicateurs suivants sont pré-calculés dans la couche Gold :

  * **Montant Prime Sportive** :
      * *Condition* : Déplacement en "Marche/Running" (\<= 15km) OU "Vélo/Trottinette" (\<= 25km).
      * *Calcul* : `Salaire Brut * 5%`.
  * **Éligibilité Bien-Être** :
      * *Condition* : Avoir réalisé \>= 15 activités dans l'année en cours.
      * *Avantage* : 5 jours de congés.

-----

## Structure du Projet

```bash
.
├── data/
│   ├── input/          # Fichiers CSV sources
│   ├── output/         # Fichiers CSV prêts pour ingestion
│   └── delta/          # STOCKAGE DATA LAKE (Bronze/Silver/Gold + Checkpoints)
├── debezium/           # Config connecteur Postgres
├── initdb/             # Scripts SQL (Création tables Postgres)
├── scripts/
│   ├── ETL_Full_Load/  # Scripts d'initialisation & Transformations
│   ├── Jobs_Spark/     # Scripts Streaming & Workers
│   │   ├── continuous_master_stream.py  # LE CERVEAU : Orchestre tout le flux
│   │   ├── distance_worker.py           # Appel API Google Maps
│   │   ├── slack_new_activity_notifier.py # Notifications Slack
│   └── Dockerfile      # Image Python pour les workers
├── spark/              # Config Spark Thrift Server
├── simulate_new_strava_activities.py # Générateur de données
└── docker-compose.yml  # Orchestration globale
```

## Commandes de Maintenance

  * **Suivre le traitement temps réel :**

    ```bash
    docker logs -f stream-processor
    ```

    *(Vous verrez les logs "Batch X : Traitement de Y activités" et "Gold Wellbeing mis à jour")*

  * **Vérifier les données brutes via SQL :**

    ```bash
    docker exec -it spark-thrift /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n admin -e "SELECT COUNT(*) FROM delta.\`/data/delta/bronze/strava_activities\`;"
    ```

  * **Reset complet (Attention : supprime toutes les données) :**

    ```bash
    docker-compose down -v
    # Sur Linux/Mac/WSL
    rm -rf data/delta/*
    docker-compose up -d
    ```