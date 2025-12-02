import time
import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

# Import de vos transformations existantes
from silver_transforms import (
    transform_strava_act_bronze_to_silver,
    transform_rh_bronze_to_silver_employee,
    transform_rh_bronze_to_silver_commute
)

# --- Configuration des chemins ---
BASE_DELTA = "./data/delta"
CHECKPOINT_BASE = f"{BASE_DELTA}/checkpoints"

BRONZE_STRAVA = f"{BASE_DELTA}/bronze/strava_activities"
BRONZE_RH = f"{BASE_DELTA}/bronze/rh_employees"

SILVER_ACTIVITY = f"{BASE_DELTA}/silver/silver.activity"
SILVER_EMP = f"{BASE_DELTA}/silver/silver.employee"
SILVER_COMMUTE = f"{BASE_DELTA}/silver/silver.commute"

GOLD_WELLBEING = f"{BASE_DELTA}/gold/wellbeing_days_eligibility"
GOLD_PRIME = f"{BASE_DELTA}/gold/prime_sportive_input"

# --- Init Spark ---
builder = (
    SparkSession.builder.appName("StreamCDC_Strava_To_DeltaBronze")
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

print("🚀 Démarrage du moteur de Streaming Continu...")

# ==========================================
# STREAM 1 : Bronze Strava -> Silver Activity
# ==========================================
def process_silver_activity():
    # On attend que la table existe
    while not os.path.exists(BRONZE_STRAVA):
        print("En attente de la table Bronze Strava...")
        time.sleep(10)

    df_bronze = spark.readStream.format("delta").option("ignoreChanges", "true").load(BRONZE_STRAVA)
    df_silver = transform_strava_act_bronze_to_silver(df_bronze)
    
    return (
        df_silver.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_activity")
        .option("mergeSchema", "true")
        .start(SILVER_ACTIVITY)
    )

# ==========================================
# STREAM 2 : Silver Activity -> Gold Wellbeing
# ==========================================
def process_gold_wellbeing():
    while not os.path.exists(SILVER_ACTIVITY):
        print("En attente de la table Silver Activity...")
        time.sleep(10)

    df_silver = spark.readStream.format("delta").load(SILVER_ACTIVITY)
    current_year = F.year(F.current_date())
    
    df_agg = (
        df_silver
        .filter(F.col("activity_year") == current_year)
        .groupBy("employee_id", "activity_year")
        .agg(
            F.count("*").alias("nb_activities_year"),
            F.min("activity_date").alias("first_activity_date"),
            F.max("activity_date").alias("last_activity_date"),
        )
        .withColumnRenamed("activity_year", "year")
    )
    
    return (
        df_agg.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_wellbeing")
        .start(GOLD_WELLBEING)
    )

# ==========================================
# STREAM 3 : Bronze RH -> Silver & Gold Prime
# ==========================================
def update_rh_layers(df_batch, batch_id):
    if df_batch.count() == 0: return
    
    # 1. Mise à jour Silver
    transform_rh_bronze_to_silver_employee(df_batch).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_EMP)
    transform_rh_bronze_to_silver_commute(df_batch).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_COMMUTE)
    
    # 2. Recalcul Gold Prime
    silver_e = spark.read.format("delta").load(SILVER_EMP).alias("e")
    silver_c = spark.read.format("delta").load(SILVER_COMMUTE).alias("c")
    
    df_gold = (
        silver_e.join(silver_c, F.col("e.employee_id") == F.col("c.employee_id"))
        .select(
            F.col("e.employee_id"),
            F.year(F.current_date()).alias("year"),
            F.col("e.business_unit"),
            F.col("e.salaire_brut"),
            F.col("c.moyen_deplacement"),
        )
    )
    
    # Gestion de la distance (si dispo)
    if "distance_domicile_travail_m" in silver_c.columns:
        df_gold = df_gold.withColumn("distance_domicile_travail_m", F.col("c.distance_domicile_travail_m")) \
                         .withColumn("distance_km", (F.col("distance_domicile_travail_m") / 1000.0).cast("double"))
    else:
        df_gold = df_gold.withColumn("distance_km", F.lit(None).cast("double"))

    df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PRIME)

def process_rh_streams():
    while not os.path.exists(BRONZE_RH):
        print("En attente de la table Bronze RH...")
        time.sleep(10)

    return (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(BRONZE_RH)
        .writeStream
        .foreachBatch(update_rh_layers)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/rh_propagation")
        .start()
    )

# Lancement
q1 = process_silver_activity()
q2 = process_gold_wellbeing()
q3 = process_rh_streams()

print("✅ Tous les streams sont actifs.")
spark.streams.awaitAnyTermination()