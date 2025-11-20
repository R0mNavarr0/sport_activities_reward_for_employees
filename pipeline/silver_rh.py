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

# ===========================
# 1. Lecture Bronze rh_employees
# ===========================
bronze_rh_path = f"{BRONZE_BASE}/rh_employees"
df_rh = spark.read.format("delta").load(bronze_rh_path)

df_rh.printSchema()
df_rh.show(5, truncate=False)

# ===========================
# 2. Construction silver.dim_employee
# ===========================

# Calcul âge et ancienneté (approximation en années)
today = F.current_date()

df_dim_employee = (
    df_rh
    .select(
        F.col("id").alias("employee_id"),
        "nom",
        "prenom",
        "date_naissance",
        "date_embauche",
        "business_unit",
        "salaire_brut",
        "contrat",
        "nb_conges_payes",
    )
    .withColumn(
        "age_annees",
        F.when(
            F.col("date_naissance").isNotNull(),
            F.floor(F.datediff(today, F.col("date_naissance")) / F.lit(365.25))
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "anciennete_annees",
        F.when(
            F.col("date_embauche").isNotNull(),
            F.floor(F.datediff(today, F.col("date_embauche")) / F.lit(365.25))
        ).otherwise(F.lit(None).cast("int"))
    )
)

silver_dim_employee_path = f"{SILVER_BASE}/dim_employee"

df_dim_employee.write.format("delta").mode("overwrite").save(silver_dim_employee_path)

print("\n=== Écrit : silver.dim_employee ===")
df_dim_employee.show(5, truncate=False)

# ===========================
# 3. Construction silver.dim_commute
# ===========================

df_dim_commute = (
    df_rh
    .select(
        F.col("id").alias("employee_id"),
        "adresse",
        "code_postal",
        "ville",
        "moyen_deplacement",
        F.col("distance_domicile_travail").alias("distance_domicile_travail_m"),
        F.col("duree_domicile_travail").alias("duree_domicile_travail_s"),
    )
)

silver_dim_commute_path = f"{SILVER_BASE}/dim_commute"

df_dim_commute.write.format("delta").mode("overwrite").save(silver_dim_commute_path)

print("\n=== Écrit : silver.dim_commute ===")
df_dim_commute.show(5, truncate=False)

# ===========================
# Fin
# ===========================
spark.stop()
print("\n=== Silver RH terminé ===")