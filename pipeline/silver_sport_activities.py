from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===========================
# Config chemins Delta
# ===========================
BRONZE_BASE = "./data/delta/bronze"
SILVER_BASE = "./data/delta/silver"

# ===========================
# SparkSession avec Delta
# ===========================
builder = (
    SparkSession.builder.appName("Silver_RH")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze_sport_path = f"{BRONZE_BASE}/sport_activities"
df_sport = spark.read.format("delta").load(bronze_sport_path)

df_sport.printSchema()
df_sport.show(5, truncate=False)

df_dim_sport_profile = (
    df_sport
    .select(
        F.col("id").alias("employee_id"),
        "pratique_sport",
    )
)

silver_dim_sport_profile_path = f"{SILVER_BASE}/dim_sport_profile"

df_dim_sport_profile.write.format("delta").mode("overwrite").save(silver_dim_sport_profile_path)

print("\n=== Écrit : silver.dim_sport_profile ===")
df_dim_sport_profile.show(5, truncate=False)

# ===========================
# Fin
# ===========================
spark.stop()
print("\n=== Silver RH terminé ===")