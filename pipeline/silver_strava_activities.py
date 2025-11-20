from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_BASE = "./data/delta/bronze"
SILVER_BASE = "./data/delta/silver"

builder = (
    SparkSession.builder.appName("Silver_Activities")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronze_strava_path = f"{BRONZE_BASE}/strava_activities"
df_strava = spark.read.format("delta").load(bronze_strava_path)

df_strava.printSchema()
df_strava.show(5, truncate=False)

df_fact_activity = (
    df_strava
    .select(
        "activity_id",
        "employee_id",
        F.col("start_date_utc").alias("activity_datetime_utc"),
        F.to_date("start_date_utc").alias("activity_date"),
        F.year("start_date_utc").alias("activity_year"),
        "sport_type",
        "type",
        "distance_m",
        "moving_time_s",
        "elapsed_time_s",
    )
)

silver_fact_activity_path = f"{SILVER_BASE}/fact_activity"
df_fact_activity.write.format("delta").mode("overwrite").save(silver_fact_activity_path)

df_fact_activity.show(5, truncate=False)

spark.stop()