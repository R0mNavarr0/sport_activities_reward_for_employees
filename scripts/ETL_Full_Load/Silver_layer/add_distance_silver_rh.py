from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

SILVER_BASE = "./data/delta/silver"

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

distance_cache_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/rh_sport")
    .option("dbtable", "distance_cache")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .load()
    .filter(F.col("status") == "OK")
)

df_commute_enriched = (
    df_commute.alias("c")
    .join(
        distance_cache_df.alias("d"),
        F.col("c.adresse_complete") == F.col("d.origin_address"),
        "left"
    )
    .select(
        "c.employee_id",
        "c.adresse_complete",
        "c.code_postal",
        "c.ville",
        "c.moyen_deplacement",
        F.col("d.distance_m").alias("distance_domicile_travail_m"),
        F.col("d.duration_s").alias("duree_domicile_travail_s"),
    )
)

df_commute_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(dim_commute_path)

spark.stop()