import os
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC_RH = "rh_sport.public.rh_employees"
BRONZE_RH_PATH = "./data/delta/bronze/rh_employees"
CHECKPOINT_RH = "./data/delta/checkpoints/cdc_rh"

builder = (
    SparkSession.builder.appName("Continuous_Integration_rh_employees_bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "spark.jars",
        ",".join([
            "jars/postgresql-42.7.4.jar",
            "jars/spark-sql-kafka-0-10_2.13-4.0.1.jar",
            "jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
            "jars/kafka-clients-3.8.0.jar",
            "jars/commons-pool2-2.12.0.jar"
        ])
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema_rh = StructType([
    StructField("id", IntegerType(), True),
    StructField("nom", StringType(), True),
    StructField("prenom", StringType(), True),
    StructField("date_naissance", DateType(), True), 
    StructField("date_embauche", DateType(), True),
    StructField("business_unit", StringType(), True),
    StructField("salaire_brut", DoubleType(), True),
    StructField("contrat", StringType(), True),
    StructField("nb_conges_payes", IntegerType(), True),
    StructField("adresse_complete", StringType(), True),
    StructField("moyen_deplacement", StringType(), True)
])


# ==============================
# Lecture du topic Kafka (Redpanda)
# ==============================

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_RH)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss","false")
    .load()
)

# Parsing des messages JSON Debezium
parsed_df = (
    raw_df
    .select(
        F.col("value").cast("string").alias("json_str"), 
        "timestamp"
    )
    .select(
        F.get_json_object("json_str", "$.payload.op").alias("op"),
        F.from_json(
            F.get_json_object("json_str", "$.payload.after"), 
            schema_rh
        ).alias("after")
    )
    # On filtre pour ne garder que Création (c), Update (u) et Snapshot (r)
    .filter(F.col("op").isin("c", "u", "r"))
    .select("op", "after.*")
)

# ==============================
# foreachBatch = MERGE dans Delta
# ==============================

def apply_cdc_to_delta(micro_df, batch_id: int):
    if micro_df.rdd.isEmpty():
        return

    print(f"--- Batch {batch_id} : {micro_df.count()} lignes reçues ---")
    
    # Dédoublonnage sur la clé primaire (id) dans le micro-batch
    # On garde la dernière version reçue pour chaque ID
    upserts_df = micro_df.dropDuplicates(["id"])

    # 1. Cas Table Inexistante : Initialisation
    if not DeltaTable.isDeltaTable(spark, BRONZE_RH_PATH):
        print(f"⚠️ Table inexistante à {BRONZE_RH_PATH}. Création initiale...")
        upserts_df.write.format("delta").mode("append").save(BRONZE_RH_PATH)
        return

    # 2. Cas Normal : Merge (Upsert)
    delta_table = DeltaTable.forPath(spark, BRONZE_RH_PATH)
    
    update_columns = {col: f"s.{col}" for col in upserts_df.columns if col not in ["created_at", "op"]}
    insert_columns = {col: f"s.{col}" for col in upserts_df.columns if col not in ["op"]}

    (delta_table.alias("t")
        .merge(
            upserts_df.alias("s"),
            "t.id = s.id"
        )
        .whenMatchedUpdate(set=update_columns)
        .whenNotMatchedInsert(values=insert_columns)
        .execute()
    )
    print("✅ Merge effectué avec succès.")

query = (
    parsed_df.writeStream
    .foreachBatch(apply_cdc_to_delta)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_RH)
    .start()
)

print(f"Streaming RH démarré (Topic: {TOPIC_RH})")

query.awaitTermination()