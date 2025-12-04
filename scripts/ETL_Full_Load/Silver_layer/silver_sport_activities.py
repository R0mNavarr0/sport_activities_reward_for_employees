from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from silver_transforms import transform_sport_bronze_to_silver
import sys
sys.path.append("/app/scripts/ETL_Full_Load/Silver_layer")
from data_quality import DataQualityManager

BRONZE_PATH = "./data/delta/bronze/sport_activities"
SILVER_PATH = "./data/delta/silver/silver.sport_profile"

builder = (
    SparkSession.builder.appName("Silver_RH")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_sport = spark.read.format("delta").load(BRONZE_PATH)
df_sport.printSchema()
df_sport.show(10, truncate=False)


df_dim_sport_profile = transform_sport_bronze_to_silver(df_sport)

# DQ
dq_sport = DataQualityManager(df_dim_sport_profile, "Silver Sport Profile")
# Quantitatif
dq_sport.check_volume()
# Unicité
dq_sport.check_unique("employee_id")
# Validation
dq_sport.validate()

df_dim_sport_profile.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("\n=== Écrit : silver.sport_profile ===")
df_dim_sport_profile.show(10, truncate=False)

spark.stop()
print("\n=== Silver Sport terminé ===")