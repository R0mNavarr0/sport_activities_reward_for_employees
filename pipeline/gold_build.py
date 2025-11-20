from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_BASE = "./data/delta/silver"
GOLD_BASE = "./data/delta/gold"

# ===========================
# SparkSession Delta
# ===========================
builder = (
    SparkSession.builder.appName("Gold_Layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# =============================================================================
# 1. GOLD – prime_sportive_eligibility
# =============================================================================

# Lecture Silver
dim_employee_path = f"{SILVER_BASE}/dim_employee"
dim_commute_path = f"{SILVER_BASE}/dim_commute"

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

# Normalisation mode de déplacement
mode_norm = F.lower(F.trim(F.col("c.moyen_deplacement")))

# Seuils métiers (en mètres)
WALK_MAX = 15000   # Marche / running : 15 km
BIKE_MAX = 25000   # Vélo / trottinette / autres : 25 km

distance = F.col("c.distance_domicile_travail_m")

condition_walk = (
    (mode_norm.like("%marche%")) | (mode_norm.like("%run%"))
) & (distance <= WALK_MAX)

condition_bike = (
    mode_norm.like("%vélo%") | mode_norm.like("%velo%") | mode_norm.like("%trot%") | mode_norm.like("%autres%")
) & (distance <= BIKE_MAX)

elig_prime = condition_walk | condition_bike

# Année de référence = année courante (adaptable)
current_year = F.year(F.current_date())

df_gold_prime = (
    df_ec
    .select(
        F.col("e.employee_id"),
        current_year.alias("year"),
        F.col("c.moyen_deplacement"),
        F.col("c.distance_domicile_travail_m"),
        F.col("e.salaire_brut"),
    )
    .withColumn("elig_prime_sportive", elig_prime)
    .withColumn(
        "montant_prime",
        F.when(F.col("elig_prime_sportive"), F.col("salaire_brut") * F.lit(0.05))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "elig_rule_description",
        F.when(condition_walk, F.lit("Marche/Running et distance <= 15 km"))
         .when(condition_bike, F.lit("Vélo/Trottinette/Autres et distance <= 25 km"))
         .otherwise(F.lit("Conditions non remplies"))
    )
)

gold_prime_path = f"{GOLD_BASE}/prime_sportive_eligibility"

df_gold_prime.write.format("delta").mode("overwrite").save(gold_prime_path)

print("\n=== GOLD : prime_sportive_eligibility ===")
df_gold_prime.show(10, truncate=False)

# =============================================================================
# 2. GOLD – wellbeing_days_eligibility (5 journées bien-être)
#    Règle : >= 15 activités physiques dans l'année
# =============================================================================

fact_activity_path = f"{SILVER_BASE}/fact_activity"
df_act = spark.read.format("delta").load(fact_activity_path)

# Filtre sur l'année courante (adaptable)
df_act_year = df_act.filter(F.col("activity_year") == current_year)

# Agrégation par salarié + année
df_agg = (
    df_act_year
    .groupBy("employee_id", "activity_year")
    .agg(
        F.count("*").alias("nb_activities_year"),
        F.min("activity_date").alias("first_activity_date"),
        F.max("activity_date").alias("last_activity_date"),
    )
)

# Règle d'éligibilité : >= 15 activités
df_gold_wellbeing = (
    df_agg
    .withColumn(
        "elig_5_jours_bien_etre",
        F.col("nb_activities_year") >= F.lit(15)
    )
    .withColumnRenamed("activity_year", "year")
)

gold_wellbeing_path = f"{GOLD_BASE}/wellbeing_days_eligibility"

df_gold_wellbeing.write.format("delta").mode("overwrite").save(gold_wellbeing_path)

print("\n=== GOLD : wellbeing_days_eligibility ===")
df_gold_wellbeing.show(10, truncate=False)

# =============================================================================
# 3. GOLD – hr_costs_summary par BU
# =============================================================================

df_gold_hr = (
    df_gold_prime.alias("p")
    .join(df_emp.alias("e"), on="employee_id", how="left")
    .groupBy("year", "business_unit")
    .agg(
        F.countDistinct("employee_id").alias("nb_salaries"),
        F.sum(F.when(F.col("elig_prime_sportive"), F.lit(1)).otherwise(F.lit(0))).alias("nb_eligibles_prime"),
        F.sum("montant_prime").alias("cout_total_primes"),
    )
)

gold_hr_path = f"{GOLD_BASE}/hr_costs_summary"
df_gold_hr.write.format("delta").mode("overwrite").save(gold_hr_path)

print("\n=== GOLD : hr_costs_summary ===")
df_gold_hr.show(10, truncate=False)

# =============================================================================
# Fin
# =============================================================================
spark.stop()
print("\n=== Construction couche GOLD terminée ===")