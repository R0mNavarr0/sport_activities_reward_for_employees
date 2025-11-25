from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "rh_sport"
TABLES = {
    "rh_employees": "public.rh_employees",
    "sport_activities": "public.sport_activities",
    "strava_activities": "public.strava_activities",
}

jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

connection_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

builder = (
    SparkSession.builder.appName("BronzeFromPostgres")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars", "jars/postgresql-42.7.4.jar")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

for delta_name, dbtable in TABLES.items():

    print(f"\n=== Lecture Postgres : {dbtable} ===")

    df = (
        spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", dbtable)
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
    )

    df.printSchema()
    df.show(5)

    bronze_path = f"./data/delta/bronze/{delta_name}"

    print(f"=== Écriture Delta Bronze : {bronze_path} ===")
    df.write.format("delta").mode("overwrite").save(bronze_path)

spark.stop()

print("\n=== Bronze Delta terminé ===")