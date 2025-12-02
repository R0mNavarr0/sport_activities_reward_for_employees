from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_BASE = "./data/delta/silver"
GOLD_BASE = "./data/delta/gold"

# SparkSession Delta

builder = (
    SparkSession.builder.appName("Gold_Layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Création de la database 'gold'...")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# GOLD – prime_sportive_eligibility

# Lecture Silver
dim_employee_path = f"{SILVER_BASE}/silver.employee"
dim_commute_path = f"{SILVER_BASE}/silver.commute"

df_emp = spark.read.format("delta").load(dim_employee_path)
df_commute = spark.read.format("delta").load(dim_commute_path)

# Jointure employee + commute
df_ec = (
    df_emp.alias("e")
    .join(
        df_commute.alias("c"),
        on=F.col("e.employee_id") == F.col("c.employee_id"),
        how="inner"
    )
)

current_year = F.year(F.current_date())

df_gold_prime_input = (
    df_ec
    .select(
        F.col("e.employee_id"),
        current_year.alias("year"),
        F.col("e.business_unit"),
        F.col("e.salaire_brut"),
        F.col("c.moyen_deplacement"),
        F.col("c.distance_domicile_travail_m"),
    )
    .withColumn(
        "mode_normalise",
        F.lower(F.trim(F.col("moyen_deplacement")))
    )
    .withColumn(
        "distance_km",
        (F.col("distance_domicile_travail_m") / 1000.0).cast("double")
    )
)

gold_prime_path = f"{GOLD_BASE}/prime_sportive_input"

df_gold_prime_input.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", gold_prime_path) \
    .saveAsTable("gold.prime_sportive_input")

print("\n=== GOLD : prime_sportive_input ===")
df_gold_prime_input.show(10, truncate=False)

# GOLD – wellbeing_days_eligibility

fact_activity_path = f"{SILVER_BASE}/silver.activity"
df_act = spark.read.format("delta").load(fact_activity_path)

# Filtre sur l'année courante
df_act_year = df_act.filter(F.col("activity_year") == current_year)

# Agrégation par salarié + année
df_gold_wellbeing = (
    df_act_year
    .groupBy("employee_id", "activity_year")
    .agg(
        F.count("*").alias("nb_activities_year"),
        F.min("activity_date").alias("first_activity_date"),
        F.max("activity_date").alias("last_activity_date"),
    )
    .withColumnRenamed("activity_year", "year")
)

gold_wellbeing_path = f"{GOLD_BASE}/wellbeing_days_eligibility"

df_gold_wellbeing.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", gold_wellbeing_path) \
    .saveAsTable("gold.wellbeing_days_eligibility")

print("\n=== GOLD : wellbeing_days_eligibility ===")
df_gold_wellbeing.show(10, truncate=False)

spark.stop()
print("\n=== Construction couche GOLD terminée ===")