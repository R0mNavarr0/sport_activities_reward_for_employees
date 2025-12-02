import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from dotenv import load_dotenv

load_dotenv()

SILVER_BASE = "./data/delta/silver"
KAFKA_BOOTSTRAP = "redpanda:9092"
REQUEST_TOPIC = "distance_requests"

COMPANY_ADDRESS = os.getenv("DESTINATION")

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

dim_commute_path = f"{SILVER_BASE}/silver.commute"
df_commute = spark.read.format("delta").load(dim_commute_path)

# Construire origin/destination
df_pairs = (
    df_commute
    .select(
        "employee_id",
        F.col("adresse_complete").alias("origin_address"),
        F.lit(COMPANY_ADDRESS).alias("destination_address"),
    )
    .dropDuplicates(["origin_address", "destination_address"])
)

# Lire le cache de distances (via JDBC Postgres)
distance_cache_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/rh_sport")
    .option("dbtable", "distance_cache")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .load()
    .select("origin_address", "destination_address")
    .dropDuplicates()
)

df_missing = (
    df_pairs
    .join(
        distance_cache_df,
        on=["origin_address", "destination_address"],
        how="left_anti"   # garde seulement ce qui n'est PAS dans le cache
    )
)

# Encoder en JSON pour envoyer dans Kafka
df_requests = df_missing.select(
    F.to_json(
        F.struct(
            "origin_address",
            "destination_address"
        )
    ).alias("value")
)

df_requests.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", REQUEST_TOPIC) \
    .save()

spark.stop()
print("\n=== Demandes de distance envoyées dans Kafka ===")