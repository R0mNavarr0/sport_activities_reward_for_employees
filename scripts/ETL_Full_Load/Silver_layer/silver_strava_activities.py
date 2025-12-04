from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from silver_transforms import transform_strava_act_bronze_to_silver
import sys
sys.path.append("/app/scripts/ETL_Full_Load/Silver_layer")
from data_quality import DataQualityManager

BRONZE_PATH = "./data/delta/bronze/strava_activities"
SILVER_PATH = "./data/delta/silver/silver.activity"

builder = (
    SparkSession.builder.appName("Silver_Activities")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_strava = spark.read.format("delta").load(BRONZE_PATH)
df_strava.printSchema()
df_strava.show(5, truncate=False)

df_fact_activity = transform_strava_act_bronze_to_silver(df_strava)

# DQ
dq_strava = DataQualityManager(df_fact_activity, "Silver Strava")
# Quantitatif
dq_strava.check_volume()
# Unicité
dq_strava.check_unique("activity_id")
# Validation
dq_strava.validate()

df_fact_activity.write.format("delta").mode("overwrite").save(SILVER_PATH)

df_fact_activity.show(5, truncate=False)

spark.stop()