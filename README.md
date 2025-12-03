# Sport Activities Reward - Data Lakehouse

Ce projet est un **Proof of Concept (POC)** d'une infrastructure Data Engineering moderne ("Lakehouse"). Il automatise la collecte, le traitement et l'analyse des activités sportives des employés pour calculer des primes RH et des avantages bien-être.

-----

## Vue d'ensemble du projet

L'objectif est de consolider des données RH (salariés) et des données d'activités sportives (type Strava) pour répondre à trois besoins métiers :

1.  **Prime Sportive** : Calculer l'impact financier d'une prime de 5% du salaire brut pour les employés venant au travail en mobilité douce (sous condition de distance).
2.  **Jours Bien-Être** : Identifier les employés éligibles à 5 jours de congés supplémentaires (ceux ayant réalisé au moins 15 activités sportives dans l'année).
3.  **Engagement Salarié** : Notifier en temps réel sur Slack les nouvelles performances sportives.

### Architecture Médaillon (Bronze / Silver / Gold)

L'architecture est hybride pour optimiser les ressources :

  * **Ingestion Temps Réel (Bronze)** : Capture des changements (CDC) et écriture immédiate dans le Data Lake.
  * **Transformation (Silver/Gold)** : Traitement par batch déclenché à la demande pour recalculer les indicateurs complexes.

-----

## Architecture Technique

Le pipeline de données se décompose comme suit :

1.  **Source & Simulation** :
      * **PostgreSQL** héberge les données opérationnelles (`rh_employees`, `sport_activities`, `strava_activities`).
      * Un script Python simule l'arrivée de nouvelles activités sportives.
2.  **Capture & Streaming** :
      * **Debezium** capture les transactions Postgres (CDC) et les envoie dans **Redpanda** (Kafka).
      * Un **Stream Processor (Spark)** écoute Kafka et écrit les données brutes dans le **Delta Lake (Couche Bronze)**.
      * Un **Slack Notifier** écoute Kafka et envoie une alerte pour chaque nouvelle activité.
3.  **Enrichissement** :
      * Un **Distance Worker** calcule la distance Domicile-Travail via l'API Google Maps dès qu'un nouvel employé est détecté.
4.  **Transformation & Serving** :
      * Un script d'orchestration (`batch_refresh_silver_gold.py`) nettoie les données (**Silver**) et agrège les KPIs (**Gold**).
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
```

### 2\. Lancement de l'infrastructure

Démarrez l'ensemble des services avec Docker Compose :

```bash
docker-compose up -d --build
```

**Ce qui se passe au démarrage :**

1.  PostgreSQL s'initialise et les données RH initiales (CSV) sont chargées.
2.  Les connecteurs Debezium sont configurés automatiquement.
3.  Le **Stream Processor** démarre et commence à ingérer le Bronze en temps réel.
4.  Le **Spark Thrift Server** démarre et les vues SQL Gold sont créées automatiquement pour Power BI.

-----

## Guide d'Utilisation (Scénarios)

### Scénario 1 : Simulation de Vie (Nouvelles Activités)

Simulez l'activité des employés pour voir le pipeline réagir.

Depuis votre terminal local :

```bash
# Génère 20 activités aléatoires
python simulate_new_strava_activities.py
```

> **Résultat :** Vous recevrez instantanément des notifications sur votre canal Slack 🔔.

### Scénario 2 : Mise à jour des Dashboards (Silver & Gold)

Une fois les nouvelles données ingérées dans le Bronze (automatique), lancez le traitement Batch pour mettre à jour les tables de reporting.

```bash
docker-compose run --rm pipeline python scripts/Jobs_Spark/batch_refresh_silver_gold.py
```

> **Résultat :** Les tables `silver.*` et `gold.*` sont mises à jour avec les dernières données.

### Scénario 3 : Visualisation Power BI

1.  Ouvrez Power BI Desktop.
2.  Connectez-vous à **Spark** (`localhost:10000`), protocole **Standard**, mode **Importer**.
3.  Utilisateur : `admin` / Mdp : (vide).
4.  Sélectionnez les **Vues** dans le dossier `gold` :
      * `v_prime_sportive`
      * `v_wellbeing`
5.  Actualisez pour voir les KPIs changer suite à votre simulation.

-----

## Structure du Projet

```bash
.
├── data/
│   ├── input/          # Fichiers CSV sources
│   ├── output/         # Fichiers CSV prêts pour ingestion dans Postgres
│   └── delta/          # STOCKAGE DATA LAKE (Bronze/Silver/Gold + Checkpoints)
├── debezium/           # Config connecteur Postgres
├── initdb/             # Scripts SQL (Création tables Postgres)
├── scripts/
│   ├── ETL_Full_Load/  # Scripts Batch (Init, Silver, Gold)
│   ├── Jobs_Spark/     # Scripts Streaming & Workers
│   │   ├── continuous_master_stream.py  # Ingestion Bronze (4 flux parallèles)
│   │   ├── batch_refresh_silver_gold.py # Orchestrateur Batch Silver->Gold
│   │   ├── distance_worker.py           # Appel API Google Maps
│   │   ├── slack_new_activity_notifier.py # Notifications Slack
│   └── Dockerfile      # Image Python pour les workers
├── spark/              # Config Spark Thrift Server
├── simulate_new_strava_activities.py # Générateur de données
└── docker-compose.yml  # Orchestration globale
```

## Commandes de Maintenance

  * **Vérifier les logs du streaming :** `docker logs -f stream-processor`
  * **Vérifier les données brutes via SQL :**
    ```bash
    docker exec -it spark-thrift /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n admin -e "SELECT COUNT(*) FROM delta.\`/data/delta/bronze/strava_activities\`;"
    ```
  * **Reset complet (Attention : supprime toutes les données) :**
    ```bash
    docker-compose down -v
    rm -rf data/delta/*
    docker-compose up -d
    ```