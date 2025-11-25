from pyspark.sql import DataFrame, functions as F

def transform_rh_bronze_to_silver_employee(df: DataFrame) -> DataFrame:
    df_emp = (
        df.select(
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
                F.floor(
                    F.datediff(F.current_date(), F.col("date_naissance")) / F.lit(365.25)
                ),
            ).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "anciennete_annees",
            F.when(
                F.col("date_embauche").isNotNull(),
                F.floor(
                    F.datediff(F.current_date(), F.col("date_embauche")) / F.lit(365.25)
                ),
            ).otherwise(F.lit(None).cast("int")),
        )
    )

    return df_emp

def transform_rh_bronze_to_silver_commute(df: DataFrame) -> DataFrame:
    df_commute = df.select(
        F.col("id").alias("employee_id"),
        "adresse",
        "code_postal",
        "ville",
        "moyen_deplacement",
        F.col("distance_domicile_travail").alias("distance_domicile_travail_m"),
        F.col("duree_domicile_travail").alias("duree_domicile_travail_s"),
    )

    return df_commute

def transform_sport_bronze_to_silver(df: DataFrame) -> DataFrame:
    df_sport = df.select(
        F.col("id").alias("employee_id"),
        "pratique_sport",
    )

    return df_sport

def transform_strava_act_bronze_to_silver(df: DataFrame) -> DataFrame:
    df_strava = df.select(
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

    return df_strava