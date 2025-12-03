import os
import sys
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, ShortType, DoubleType

# ==============================
# CONFIG GLOBALE
# ==============================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
BASE_DELTA = "./data/delta"
CHECKPOINT_BASE = f"{BASE_DELTA}/checkpoints"

# Configs spécifiques
CONF_STRAVA = {
    "topic": "rh_sport.public.strava_activities",
    "path": f"{BASE_DELTA}/bronze/strava_activities",
    "checkpoint": f"{CHECKPOINT_BASE}/strava_cdc"
}
CONF_RH = {
    "topic": "rh_sport.public.rh_employees",
    "path": f"{BASE_DELTA}/bronze/rh_employees",
    "checkpoint": f"{CHECKPOINT_BASE}/rh_cdc"
}
CONF_SPORT = {
    "topic": "rh_sport.public.sport_activities",
    "path": f"{BASE_DELTA}/bronze/sport_activities",
    "checkpoint": f"{CHECKPOINT_BASE}/sport_cdc"
}
CONF_DISTANCE = {
    "source_path": CONF_RH["path"],
    "topic_out": "distance_requests",
    "checkpoint": f"{CHECKPOINT_BASE}/req_distance_producer",
    "company_addr": os.getenv("DESTINATION", "1362 Av. des Platanes, 34970 Lattes")
}

# ==============================
# INIT SPARK
# ==============================
builder = (
    SparkSession.builder.appName("Master_Continuous_Stream_Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar,jars/spark-sql-kafka-0-10_2.13-4.0.1.jar,jars/kafka-clients-3.8.0.jar,jars/commons-pool2-2.12.0.jar,jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar")
    .config("spark.driver.memory", "512m")
    .config("spark.sql.streaming.trigger.processingTime", "10 seconds")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Démarrage du Master Stream (4 Flux parallèles)...")

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

# ==============================
# STREAM 1 : RH EMPLOYEES
# ==============================
def process_rh():
    schema_rh = StructType([
        StructField("id", IntegerType(), True),
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("date_naissance", LongType(), True), # Int pour Debezium dates
        StructField("date_embauche", LongType(), True),
        StructField("business_unit", StringType(), True),
        StructField("salaire_brut", DoubleType(), True),
        StructField("contrat", StringType(), True),
        StructField("nb_conges_payes", IntegerType(), True), # Int pour éviter conflit smallint
        StructField("adresse_complete", StringType(), True),
        StructField("moyen_deplacement", StringType(), True)
    ])

    def upsert_rh(micro_df, batch_id):
        if micro_df.isEmpty(): return

        count = micro_df.count()
        print(f"--- [RH] Batch {batch_id}: Traitement de {count} employés ---")

        upserts = micro_df.dropDuplicates(["id"]).withColumn(
            "date_naissance", F.expr("date_from_unix_date(date_naissance)")
        ).withColumn(
            "date_embauche", F.expr("date_from_unix_date(date_embauche)")
        )
        update_columns = {col: f"s.{col}" for col in upserts.columns if col not in ["created_at", "op"]}
        insert_columns = {col: f"s.{col}" for col in upserts.columns if col not in ["op"]}
        
        if not DeltaTable.isDeltaTable(spark, CONF_RH["path"]):
            cols = [c for c in upserts.columns if c != "op"]
            upserts.select(cols).write.format("delta").mode("append").save(CONF_RH["path"])
        else:
            (DeltaTable.forPath(spark, CONF_RH["path"]).alias("t")
             .merge(upserts.alias("s"), "t.id = s.id")
             .whenMatchedUpdate(set=update_columns)
             .whenNotMatchedInsert(values=insert_columns)
             .execute())
            print("Merge rh_employees effectué avec succès.")

    return (parse_debezium(read_kafka_stream(CONF_RH["topic"]), schema_rh)
            .writeStream.foreachBatch(upsert_rh)
            .option("checkpointLocation", CONF_RH["checkpoint"])
            .start())

# ==============================
# STREAM 2 : SPORT ACTIVITIES
# ==============================
def process_sport():
    schema_sport = StructType([
        StructField("id", IntegerType(), True),
        StructField("pratique_sport", StringType(), True)
    ])

    def upsert_sport(micro_df, batch_id):
        if micro_df.isEmpty(): return

        count = micro_df.count()
        print(f"--- [SPORT] Batch {batch_id}: Traitement de {count} sports ---")

        upserts = (micro_df.dropDuplicates(["id"]).select("id","pratique_sport"))
        
        if not DeltaTable.isDeltaTable(spark, CONF_SPORT["path"]):
            cols = [c for c in upserts.columns if c != "op"]
            upserts.select(cols).write.format("delta").mode("append").save(CONF_SPORT["path"])
        else:
            (DeltaTable.forPath(spark, CONF_SPORT["path"]).alias("t")
             .merge(upserts.alias("s"), "t.id = s.id")
             .whenMatchedUpdate(set={"pratique_sport": "s.pratique_sport"})
             .whenNotMatchedInsertAll()
             .execute())
            print("Merge sport_profile effectué avec succès.")

    return (parse_debezium(read_kafka_stream(CONF_SPORT["topic"]), schema_sport)
            .writeStream.foreachBatch(upsert_sport)
            .option("checkpointLocation", CONF_SPORT["checkpoint"])
            .start())

# ==============================
# STREAM 3 : STRAVA ACTIVITIES
# ==============================
def process_strava():
    schema_strava = StructType([
        StructField("activity_id", LongType(), True),
        StructField("employee_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("distance_m", IntegerType(), True),
        StructField("moving_time_s", IntegerType(), True),
        StructField("elapsed_time_s", IntegerType(), True),
        StructField("total_elev_gain_m", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("sport_type", StringType(), True),
        StructField("start_date_utc", StringType(), True),
        StructField("start_date_local", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("utc_offset_s", IntegerType(), True),
        StructField("commute", BooleanType(), True),
        StructField("manual", BooleanType(), True),
        StructField("private", BooleanType(), True),
        StructField("flagged", BooleanType(), True),
        StructField("calories", IntegerType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("created_at", StringType(), True)
    ])

    def upsert_strava(micro_df, batch_id):
        if micro_df.isEmpty(): return

        count = micro_df.count()
        print(f"--- [STRAVA] Batch {batch_id}: Traitement de {count} activités ---")

        upserts = micro_df.dropDuplicates(["activity_id"]).withColumn(
            "start_date_utc", 
            F.coalesce(
                (F.expr("try_cast(start_date_utc as long)") / 1000000).cast("timestamp"), 
                F.col("start_date_utc").cast("timestamp")
            )
        ).withColumn(
            "start_date_local", 
            F.coalesce(
                (F.expr("try_cast(start_date_local as long)") / 1000000).cast("timestamp"),
                F.col("start_date_local").cast("timestamp")
            )
        ).withColumn(
            "created_at", 
            F.coalesce(
                (F.expr("try_cast(created_at as long)") / 1000000).cast("timestamp"),
                F.col("created_at").cast("timestamp")
            )
        )
        
        if not DeltaTable.isDeltaTable(spark, CONF_STRAVA["path"]):
            cols = [c for c in upserts.columns if c != "op"]
            upserts.select(cols).write.format("delta").mode("append").save(CONF_STRAVA["path"])
        else:
            update_cols = {c: f"s.{c}" for c in upserts.columns if c not in ["created_at", "op"]}
            insert_columns = {col: f"s.{col}" for col in upserts.columns if col not in ["op"]}
            (DeltaTable.forPath(spark, CONF_STRAVA["path"]).alias("t")
             .merge(upserts.alias("s"), "t.activity_id = s.activity_id")
             .whenMatchedUpdate(set=update_cols)
             .whenNotMatchedInsert(values=insert_columns)
             .execute())
            print("Merge Strava effectué avec succès.")

    return (parse_debezium(read_kafka_stream(CONF_STRAVA["topic"]), schema_strava)
            .writeStream.foreachBatch(upsert_strava)
            .option("checkpointLocation", CONF_STRAVA["checkpoint"])
            .start())

# ==============================
# STREAM 4 : DISTANCE REQUESTS (Delta -> Kafka)
# ==============================
def process_distance_req():
    # Fonction pour envoyer à Kafka
    def send_to_kafka(df_batch, batch_id):
        if df_batch.isEmpty(): return

        count = df_batch.count()
        print(f"--- [DISTANCE] Batch {batch_id}: Envoi de {count} demandes ---")

        (df_batch.select("value")
         .write.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", CONF_DISTANCE["topic_out"])
         .save())

    if not os.path.exists(CONF_DISTANCE["source_path"]):
        print("Path Bronze RH introuvable pour le moment. Le stream Distance attendra.")
        return None

    df_stream = spark.readStream.format("delta").option("ignoreChanges", "true").load(CONF_DISTANCE["source_path"])
    
    df_payload = df_stream.select(
        F.to_json(F.struct(
            F.col("adresse_complete").alias("origin_address"),
            F.lit(CONF_DISTANCE["company_addr"]).alias("destination_address")
        )).alias("value")
    ).filter(F.col("value").isNotNull())

    return (df_payload.writeStream
            .foreachBatch(send_to_kafka)
            .option("checkpointLocation", CONF_DISTANCE["checkpoint"])
            .start())

# ==============================
# MAIN LAUNCHER
# ==============================
s1 = process_rh()
s2 = process_sport()
s3 = process_strava()
s4 = process_distance_req()

print("TOUS LES STREAMS SONT LANCÉS.")
spark.streams.awaitAnyTermination()