# Sport Activities Reward - Data Lakehouse

Ce projet est un **Proof of Concept (POC)** d'une infrastructure Data Engineering moderne ("Lakehouse"). Il vise à automatiser la collecte et l'analyse des activités sportives des employés pour calculer des primes et avantages RH.

## Objectifs du Projet

L'objectif est d'implémenter les règles métier suivantes à partir de données RH et de flux d'activités sportives (type Strava) :

1.  **Prime Sportive (Impact Financier)** : Accorder 5% de prime annuelle aux employés venant au travail en mobilité douce (Vélo, Marche, Course), sous condition de distance.
2.  **Jours Bien-Être** : Accorder 5 jours de congés supplémentaires aux employés ayant réalisé au moins 15 activités sportives dans l'année.

## Architecture Technique

Le projet repose sur une architecture **Medallion (Bronze / Silver / Gold)** entièrement conteneurisée.

### Flux de données

1.  **Ingestion (Sources)** :
      * Données RH & Référentiel Sport (CSV) chargées dans **PostgreSQL**.
      * Activités sportives simulées (Python) injectées dans PostgreSQL.
2.  **Capture (CDC & Streaming)** :
      * **Debezium** capture les changements PostgreSQL en temps réel.
      * **Redpanda** (Kafka compatible) transmet les événements.
3.  **Lakehouse (Spark & Delta Lake)** :
      * **Bronze Layer (Streaming)** : Ingestion brute depuis Kafka via Spark Structured Streaming (Script unifié `continuous_master_stream.py`).
      * **Enrichissement (Micro-batch)** : Un worker Python calcule la distance Domicile-Travail via l'API Google Maps sur demande Kafka.
      * **Silver Layer (Batch)** : Nettoyage, typage (cast Timestamp), et jointures.
      * **Gold Layer (Batch)** : Agrégations métiers et calcul des KPIs.
4.  **Visualisation** :
      * **Spark Thrift Server** expose les tables Gold.
      * **Power BI** se connecte en JDBC/ODBC pour le reporting.

## Structure du Projet

```bash
.
├── data/
│   ├── input/          # Fichiers CSV sources (RH, Sport)
│   ├── output/         # Fichiers générés (JSONL Strava, CSV transformés)
│   └── delta/          # STOCKAGE DATA LAKE (Bronze/Silver/Gold + Checkpoints)
├── debezium/           # Configuration du connecteur Postgres
├── initdb/             # Scripts SQL d'initialisation des tables (Schemas)
├── pipeline/           # (Ancien dossier, migré vers scripts)
├── scripts/
│   ├── Dockerfile      # Image Python pour les workers Spark/ETL
│   ├── requirements.txt
│   ├── ETL_Full_Load/  # Scripts Batch (Init Postgres, Silver->Gold)
│   │   ├── Postgres/   # Ingestion CSV -> DB
│   │   ├── Bronze/     # (Obsolète, remplacé par Streaming)
│   │   ├── Silver/     # Transformations logiques
│   │   └── Gold/       # Agrégations finales
│   └── Jobs_Spark/     # Scripts de Streaming & Workers
│       ├── continuous_master_stream.py  # MASTER JOB : Ingestion Bronze (3 flux)
│       ├── batch_refresh_silver_gold.py # MASTER JOB : Propagation Silver/Gold
│       ├── distance_worker.py           # Appel API Google Maps
│       └── ...
├── spark/              # Configuration Spark Thrift Server (Dockerfile)
└── docker-compose.yml  # Orchestration de l'infrastructure
```

## Installation et Démarrage

### Pré-requis

  * **Docker Desktop** (avec au moins 8Go de RAM alloués, 12Go recommandés).
  * **Clé API Google Maps** (Distance Matrix API).

### 1\. Configuration

Créez un fichier `.env` à la racine du projet :

```env
API_KEY_MAPS=VOTRE_CLE_GOOGLE_MAPS_ICI
DESTINATION="1362 Av. des Platanes, 34970 Lattes"
KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
```

### 2\. Démarrage de l'infrastructure

Lancez l'ensemble des conteneurs (Base de données, Kafka, Spark, Workers) :

```bash
docker-compose up -d
```

*Cette commande va :*

1.  Initialiser PostgreSQL et créer les tables.
2.  Charger les données CSV initiales.
3.  Lancer le **Stream Processor** qui écoute les changements en temps réel.
4.  Lancer le **Spark Thrift Server** pour Power BI.
5.  Créer automatiquement les Vues SQL pour Power BI une fois les données prêtes.

### 3\. Mise à jour de la couche Bronze

Pour voir le pipeline réagir, générez de nouvelles activités sportives :

```bash
python simulate_new_strava_activities.py
```

### 4\. Mise à jour des couches Silver & Gold

Les couches supérieures sont mises à jour par un job Batch planifié (toutes les 5 min) ou manuel.
Pour forcer une mise à jour immédiate :

```bash
docker-compose run --rm pipeline python scripts/Jobs_Spark/batch_refresh_silver_gold.py
```

## 📊 Connexion Power BI

Pour visualiser les résultats :

1.  Ouvrez **Power BI Desktop**.
2.  Cliquez sur **Obtenir les données** \> **Spark**.
3.  Configuration :
      * **Serveur** : `localhost:10000`
      * **Protocole** : `Standard` (HTTP n'est pas activé).
      * **Mode** : `Importer` (Recommandé).
4.  Identifiants : `admin` / (mot de passe vide).
5.  Dans le navigateur, allez dans `spark_catalog` \> `gold`.
6.  **IMPORTANT** : Sélectionnez les VUES (`v_prime_sportive`, `v_wellbeing`) et non les tables brutes pour éviter les erreurs de typage.

## 🛠 Commandes Utiles

**Vérifier les données brutes (Bronze) via SQL :**

```bash
docker exec -it spark-thrift /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n admin -e "SELECT COUNT(*) FROM delta.\`/data/delta/bronze/strava_activities\`;"
```

**Voir les logs du Streaming :**

```bash
docker logs -f stream-processor
```

**Redémarrer proprement (Reset complet des données) :**

```bash
docker-compose down
# Sur Linux/Mac (ou WSL)
rm -rf data/delta/*
docker-compose up -d
```