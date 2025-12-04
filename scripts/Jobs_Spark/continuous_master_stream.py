import os
import sys
import time
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, DoubleType
from dotenv import load_dotenv

load_dotenv()

# --- IMPORTS ---
sys.path.append("/app/scripts/ETL_Full_Load/Silver_layer")
try:
    from silver_transforms import (
        transform_rh_bronze_to_silver_employee, 
        transform_rh_bronze_to_silver_commute,
        transform_sport_bronze_to_silver,
        transform_strava_act_bronze_to_silver
    )
except ImportError as e:
    print(f"Warning: Impossible d'importer silver_transforms: {e}")

# ==============================
# CONFIGURATION
# ==============================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
BASE_DELTA = "./data/delta"
CHECKPOINT_BASE = f"{BASE_DELTA}/checkpoints"

DB_URL = os.getenv("DB_URL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configs
CONF_STRAVA = {
    "topic": "rh_sport.public.strava_activities",
    "path_bronze": f"{BASE_DELTA}/bronze/strava_activities",
    "path_silver": f"{BASE_DELTA}/silver/silver.activity",
    "checkpoint": f"{CHECKPOINT_BASE}/strava_cdc"
}
CONF_RH = {
    "topic": "rh_sport.public.rh_employees",
    "path_bronze": f"{BASE_DELTA}/bronze/rh_employees",
    "path_silver_emp": f"{BASE_DELTA}/silver/silver.employee",
    "path_silver_com": f"{BASE_DELTA}/silver/silver.commute",
    "checkpoint": f"{CHECKPOINT_BASE}/rh_cdc"
}
CONF_SPORT = {
    "topic": "rh_sport.public.sport_activities",
    "path_bronze": f"{BASE_DELTA}/bronze/sport_activities",
    "path_silver": f"{BASE_DELTA}/silver/silver.sport_profile",
    "checkpoint": f"{CHECKPOINT_BASE}/sport_cdc"
}
CONF_DISTANCE_TOPIC = "distance_requests"
COMPANY_ADDR = os.getenv("DESTINATION", "1362 Av. des Platanes, 34970 Lattes")

CONF_GOLD_PRIME = { "path": f"{BASE_DELTA}/gold/prime_sportive_input" }
CONF_GOLD_WELLBEING = { 
    "path": f"{BASE_DELTA}/gold/wellbeing_days_eligibility",
    "checkpoint": f"{CHECKPOINT_BASE}/gold_wellbeing_agg"
}

# ==============================
# INIT SPARK
# ==============================
builder = (
    SparkSession.builder.appName("Master_Continuous_Stream_Optimized")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar,jars/spark-sql-kafka-0-10_2.13-4.0.1.jar,jars/kafka-clients-3.8.0.jar,jars/commons-pool2-2.12.0.jar,jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar")
    .config("spark.driver.memory", "512m")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ==============================
# FONCTIONS UTILITAIRES
# ==============================
def read_kafka_stream(topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load())

def parse_debezium(df_raw, schema):
    return (df_raw.select(F.col("value").cast("string").alias("json_str"))
            .select(
                F.get_json_object("json_str", "$.payload.op").alias("op"),
                F.from_json(F.get_json_object("json_str", "$.payload.after"), schema).alias("after")
            )
            .filter(F.col("op").isin("c", "u", "r"))
            .select("op", "after.*"))

def merge_delta(df, path, key_col, update_cols=None):
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("append").save(path)
    else:
        dt = DeltaTable.forPath(spark, path).alias("t")
        merge = dt.merge(df.alias("s"), f"t.{key_col} = s.{key_col}")
        if update_cols:
             merge.whenMatchedUpdate(set=update_cols).whenNotMatchedInsertAll().execute()
        else:
             merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f" Merge effectué : {path.split('/')[-1]}")

# ==============================
# STREAM 1 : RH
# ==============================
def process_rh():
    print("[1/4] Traitement RH (Bronze -> Silver -> Gold)...")
    schema_rh = StructType([
        StructField("id", IntegerType(), True),
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("date_naissance", LongType(), True), 
        StructField("date_embauche", LongType(), True),
        StructField("business_unit", StringType(), True),
        StructField("salaire_brut", DoubleType(), True),
        StructField("contrat", StringType(), True),
        StructField("nb_conges_payes", IntegerType(), True), 
        StructField("adresse_complete", StringType(), True),
        StructField("moyen_deplacement", StringType(), True)
    ])

    def upsert_rh(micro_df, batch_id):
        if micro_df.isEmpty(): return
        print(f"--- [RH] Batch {batch_id}: {micro_df.count()} lignes ---")

        # Préparation Bronze
        upserts = micro_df.dropDuplicates(["id"]).withColumn(
            "date_naissance", F.expr("date_from_unix_date(date_naissance)")
        ).withColumn(
            "date_embauche", F.expr("date_from_unix_date(date_embauche)")
        )
        
        # Écriture Bronze
        bronze_cols = {c: f"s.{c}" for c in upserts.columns if c not in ["created_at", "op"]}
        merge_delta(upserts.select([c for c in upserts.columns if c != "op"]), CONF_RH["path_bronze"], "id", bronze_cols)

        print(f" Envoi demandes de distance...")
        kafka_payload = upserts.select(
            F.to_json(F.struct(
                F.col("adresse_complete").alias("origin_address"), 
                F.lit(COMPANY_ADDR).alias("destination_address")
            )).alias("value")
        ).filter(F.col("value").isNotNull())
        
        kafka_payload.write.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", CONF_DISTANCE_TOPIC).save()
        
        print(f" Pause pour calcul distance...")
        time.sleep(5)
        print(f" Calcul des distances effectué...")

        # Silver Employee
        df_silver_emp = transform_rh_bronze_to_silver_employee(upserts)
        merge_delta(df_silver_emp, CONF_RH["path_silver_emp"], "employee_id")

        # Silver Commute
        df_dist = (spark.read.format("jdbc")
                   .option("url", DB_URL).option("dbtable", "distance_cache")
                   .option("user", DB_USER).option("password", DB_PASSWORD).option("driver", "org.postgresql.Driver")
                   .load().filter(F.col("status") == "OK")
                   .select(F.col("origin_address"), F.col("distance_m"), F.col("duration_s")))
        
        df_base_commute = transform_rh_bronze_to_silver_commute(upserts)
        
        df_silver_commute = (df_base_commute.alias("c")
                             .join(df_dist.alias("d"), F.col("c.adresse_complete") == F.col("d.origin_address"), "left")
                             .select("c.employee_id", "c.adresse_complete", "c.code_postal", "c.ville", "c.moyen_deplacement",
                                     F.col("d.distance_m").alias("distance_domicile_travail_m"),
                                     F.col("d.duration_s").alias("duree_domicile_travail_s")))
        
        merge_delta(df_silver_commute, CONF_RH["path_silver_com"], "employee_id")

        # Gold Prime
        df_gold_prime = (df_silver_emp.alias("e").join(df_silver_commute.alias("c"), "employee_id")
            .select(F.col("e.employee_id"), F.year(F.current_date()).alias("year"), F.col("e.business_unit"), F.col("e.salaire_brut"), F.col("c.moyen_deplacement"), F.col("c.distance_domicile_travail_m"))
            .withColumn("mode_normalise", F.lower(F.trim(F.col("moyen_deplacement"))))
            .withColumn("distance_km", (F.col("distance_domicile_travail_m") / 1000.0).cast("double")))
        merge_delta(df_gold_prime, CONF_GOLD_PRIME["path"], "employee_id")

    query = (parse_debezium(read_kafka_stream(CONF_RH["topic"]), schema_rh)
             .writeStream.foreachBatch(upsert_rh)
             .option("checkpointLocation", CONF_RH["checkpoint"])
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()

# ==============================
# 2. TRAITEMENT SPORT
# ==============================
def process_sport():
    print("[2/4] Traitement Sport...")
    schema_sport = StructType([StructField("id", IntegerType(), True), StructField("pratique_sport", StringType(), True)])

    def upsert_sport(micro_df, batch_id):
        if micro_df.isEmpty(): return
        upserts = (micro_df.dropDuplicates(["id"]).select("id","pratique_sport"))
        merge_delta(upserts, CONF_SPORT["path_bronze"], "id")
        df_silver = transform_sport_bronze_to_silver(upserts)
        merge_delta(df_silver, CONF_SPORT["path_silver"], "employee_id")

    query = (parse_debezium(read_kafka_stream(CONF_SPORT["topic"]), schema_sport)
             .writeStream.foreachBatch(upsert_sport)
             .option("checkpointLocation", CONF_SPORT["checkpoint"])
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()

# ==============================
# 3. TRAITEMENT STRAVA
# ==============================
def process_strava():
    print("[3/4] Traitement Strava...")
    schema_strava = StructType([
        StructField("activity_id", LongType(), True), StructField("employee_id", IntegerType(), True), StructField("name", StringType(), True),
        StructField("distance_m", IntegerType(), True), StructField("moving_time_s", IntegerType(), True), StructField("elapsed_time_s", IntegerType(), True),
        StructField("total_elev_gain_m", IntegerType(), True), StructField("type", StringType(), True), StructField("sport_type", StringType(), True),
        StructField("start_date_utc", StringType(), True), StructField("start_date_local", StringType(), True), StructField("timezone", StringType(), True),
        StructField("utc_offset_s", IntegerType(), True), StructField("commute", BooleanType(), True), StructField("manual", BooleanType(), True),
        StructField("private", BooleanType(), True), StructField("flagged", BooleanType(), True), StructField("calories", IntegerType(), True),
        StructField("raw_payload", StringType(), True), StructField("created_at", StringType(), True)
    ])

    def upsert_strava(micro_df, batch_id):
        if micro_df.isEmpty(): return
        upserts = micro_df.dropDuplicates(["activity_id"]).withColumn(
            "start_date_utc", F.coalesce((F.expr("try_cast(start_date_utc as long)") / 1000000).cast("timestamp"), F.col("start_date_utc").cast("timestamp"))
        ).withColumn(
            "start_date_local", F.coalesce((F.expr("try_cast(start_date_local as long)") / 1000000).cast("timestamp"), F.col("start_date_local").cast("timestamp"))
        ).withColumn(
            "created_at", F.coalesce((F.expr("try_cast(created_at as long)") / 1000000).cast("timestamp"), F.col("created_at").cast("timestamp"))
        )
        cols_bronze = {c: f"s.{c}" for c in upserts.columns if c not in ["created_at", "op"]}
        merge_delta(upserts.select([c for c in upserts.columns if c != "op"]), CONF_STRAVA["path_bronze"], "activity_id", cols_bronze)
        
        df_silver = transform_strava_act_bronze_to_silver(upserts)
        merge_delta(df_silver, CONF_STRAVA["path_silver"], "activity_id")

    query = (parse_debezium(read_kafka_stream(CONF_STRAVA["topic"]), schema_strava)
             .writeStream.foreachBatch(upsert_strava)
             .option("checkpointLocation", CONF_STRAVA["checkpoint"])
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()

# ==============================
# 4. TRAITEMENT GOLD WELLBEING
# ==============================
def process_gold_wellbeing():
    print("[4/4] Traitement Gold Wellbeing...")
    if not os.path.exists(CONF_STRAVA["path_silver"]): return
    
    def update_gold(micro_df, batch_id):
        if not micro_df.isEmpty():
             micro_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(CONF_GOLD_WELLBEING["path"])
             print(f"Gold Wellbeing mis à jour ({micro_df.count()} lignes)")

    df_silver_stream = spark.readStream.format("delta").option("ignoreChanges", "true").load(CONF_STRAVA["path_silver"])
    current_year = F.year(F.current_date())
    df_agg = (df_silver_stream.filter(F.col("activity_year") == current_year)
              .groupBy("employee_id", "activity_year")
              .agg(F.count("*").alias("nb_activities_year"), F.min("activity_date").alias("first_activity_date"), F.max("activity_date").alias("last_activity_date"))
              .withColumnRenamed("activity_year", "year"))
    
    query = (df_agg.writeStream.outputMode("complete").foreachBatch(update_gold)
            .option("checkpointLocation", CONF_GOLD_WELLBEING["checkpoint"])
            .trigger(availableNow=True)
            .start())
    query.awaitTermination()

# ==============================
# BOUCLE INFINIE
# ==============================
print("Lancement de la boucle de traitement en cascade...")

while True:
    print(f"\n--- CYCLE DE TRAITEMENT : {time.strftime('%H:%M:%S')} ---")
    
    process_rh()
    process_sport()
    process_strava()
    process_gold_wellbeing()
    
    time.sleep(5)