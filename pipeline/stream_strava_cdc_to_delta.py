import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
)

# ==============================
# Config
# ==============================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = "rh_sport.public.strava_activities"

# Chemin de la table Delta Bronze strava_activities
DELTA_BRONZE_STRAVA_PATH = "./data/delta/bronze/strava_activities"

# ==============================
# SparkSession + Delta + Kafka
# ==============================

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

# ==============================
# Schéma Debezium pour strava_activities
# ==============================

after_schema = StructType([
    StructField("activity_id",        IntegerType(), True),
    StructField("employee_id",        IntegerType(), True),
    StructField("name",               StringType(),  True),
    StructField("distance_m",         IntegerType(), True),
    StructField("moving_time_s",      IntegerType(), True),
    StructField("elapsed_time_s",     IntegerType(), True),
    StructField("total_elev_gain_m",  IntegerType(), True),
    StructField("type",               StringType(),  True),
    StructField("sport_type",         StringType(),  True),
    StructField("start_date_utc",     TimestampType(), True),
    StructField("start_date_local",   TimestampType(), True),
    StructField("timezone",           StringType(),  True),
    StructField("utc_offset_s",       IntegerType(), True),
    StructField("commute",            BooleanType(), True),
    StructField("manual",             BooleanType(), True),
    StructField("private",            BooleanType(), True),
    StructField("flagged",            BooleanType(), True),
    StructField("calories",           IntegerType(), True),
    # Si tu stockes le payload brut en JSON, tu peux ajuster ce champ
    StructField("raw_payload",        StringType(),  True),
])

# ==============================
# Lecture du topic Kafka (Redpanda)
# ==============================

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss","false")
    .load()
)

json_df = raw_df.select(
    F.col("value").cast("string").alias("json"),
    "offset",
    "timestamp"
)

parsed_df = json_df.select(
    F.get_json_object("json", "$.payload.op").alias("op"),
    F.get_json_object("json", "$.payload.after").alias("after_json")
    )

parsed_after_df = parsed_df.select(
    "op",
    F.from_json("after_json", after_schema).alias("after")
)

# On garde { op, after.* } uniquement
cdc_df = (
    parsed_after_df
    .where(F.col("op").isin("c","u","d"))
    .select("op","after.*")
)

# ==============================
# foreachBatch = MERGE dans Delta
# ==============================

def apply_cdc_to_delta(micro_df, batch_id: int):
    if micro_df.rdd.isEmpty():
        print(f"=== Batch {batch_id} (vide) ===")
        return

    print(f"\n=== Batch {batch_id} ===")
    print("micro_df.count() =", micro_df.count())
    micro_df.show(truncate=False)

    # Séparer upserts et deletes
    upserts_df = micro_df.filter(F.col("op").isin("c", "u")).drop("op")
    deletes_df = micro_df.filter(F.col("op") == "d").select("activity_id")

    delta_table = DeltaTable.forPath(spark, DELTA_BRONZE_STRAVA_PATH)
    target_df = delta_table.toDF()

    target_cols = [f.name for f in target_df.schema.fields]
    source_cols = upserts_df.columns
    common_cols = [c for c in source_cols if c in target_cols and c != "created_at"]

    # Mapping pour UPDATE et INSERT
    update_set = {c: F.col(f"s.{c}") for c in common_cols}
    insert_values = {c: F.col(f"s.{c}") for c in common_cols}

    # UPSERT (c, u)
    if not upserts_df.rdd.isEmpty():
        (
            delta_table.alias("t")
            .merge(
                upserts_df.alias("s"),
                "t.activity_id = s.activity_id"
            )
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
        print(f"Upserts appliqués sur {upserts_df.count()} lignes")

    # DELETE (d)
    if not deletes_df.rdd.isEmpty():
        ids_to_delete = [row["activity_id"] for row in deletes_df.collect()]
        if ids_to_delete:
            (
                delta_table.alias("t")
                .delete(F.col("t.activity_id").isin(ids_to_delete))
            )
            print(f"Deletes appliqués sur activity_id in {ids_to_delete}")


query = (
    cdc_df.writeStream
    .foreachBatch(apply_cdc_to_delta)
    .outputMode("update")
    .option("checkpointLocation", "./data/delta/checkpoints/strava_cdc_to_bronze")
    .start()
)

print("Streaming query started, waiting for data...")
print("Is active:", query.isActive)

query.awaitTermination()