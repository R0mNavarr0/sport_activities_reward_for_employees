from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from silver_transforms import (
    transform_rh_bronze_to_silver_employee,
    transform_rh_bronze_to_silver_commute,
)

BRONZE_PATH = "./data/delta/bronze/rh_employees"
SILVER_EMP_PATH = "./data/delta/silver/dim_employee"
SILVER_COM_PATH = "./data/delta/silver/dim_commute"

builder = (
    SparkSession.builder.appName("Silver_RH")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Lecture Bronze rh_employees

df_rh = spark.read.format("delta").load(BRONZE_PATH)

df_rh.printSchema()
df_rh.show(5, truncate=False)

# Construction silver.dim_employee

df_dim_employee = transform_rh_bronze_to_silver_employee(df_rh)

df_dim_employee.write.format("delta").mode("overwrite").save(SILVER_EMP_PATH)

print("\n=== Écrit : silver.dim_employee ===")
df_dim_employee.show(5, truncate=False)

# Construction silver.dim_commute

df_dim_commute = transform_rh_bronze_to_silver_commute(df_rh)

df_dim_commute.write.format("delta").mode("overwrite").save(SILVER_COM_PATH)

print("\n=== Écrit : silver.dim_commute ===")
df_dim_commute.show(5, truncate=False)

# Fin

spark.stop()
print("\n=== Silver RH terminé ===")