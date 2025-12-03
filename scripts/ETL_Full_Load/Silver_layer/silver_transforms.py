from pyspark.sql import DataFrame, functions as F

def transform_rh_bronze_to_silver_employee(df: DataFrame) -> DataFrame:
    
    # Conversion date_naissance (Int -> Date)
    if "LongType" in str(df.schema["date_naissance"].dataType):
        df_clean = df_clean.withColumn("date_naissance", F.expr("date_add('1970-01-01', date_naissance)"))
    
    # Conversion date_embauche (Int -> Date)
    if "LongType" in str(df.schema["date_embauche"].dataType):
        df_clean = df_clean.withColumn("date_embauche", F.expr("date_add('1970-01-01', date_embauche)"))
    
    df_emp = (
        df
        # Décomposition dates de naissance
        .withColumn("jour_naissance",  F.dayofmonth("date_naissance"))
        .withColumn("mois_naissance",  F.month("date_naissance"))
        .withColumn("annee_naissance", F.year("date_naissance"))
        # Décomposition dates d'embauche
        .withColumn("jour_embauche",   F.dayofmonth("date_embauche"))
        .withColumn("mois_embauche",   F.month("date_embauche"))
        .withColumn("annee_embauche",  F.year("date_embauche"))
        # Décomposition adresse_complete -> code_postal / ville
        .withColumn(
            "code_postal",
            F.regexp_extract("adresse_complete", r"(\d{5})", 1)
        )
        .withColumn(
            "ville",
            F.trim(
                F.regexp_extract("adresse_complete", r"\d{5}\s+(.*)$", 1)
            )
        )
        # Sélection des colonnes de la dim_employee Silver
        .select(
            F.col("id").alias("employee_id"),
            "nom",
            "prenom",
            "date_naissance",
            "jour_naissance",
            "mois_naissance",
            "annee_naissance",
            "date_embauche",
            "jour_embauche",
            "mois_embauche",
            "annee_embauche",
            "business_unit",
            "salaire_brut",
            "contrat",
            "nb_conges_payes",
            "adresse_complete",
            "code_postal",
            "ville",
            "moyen_deplacement",
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
    df_commute = (
        df
        .withColumn(
            "code_postal",
            F.regexp_extract("adresse_complete", r"(\d{5})", 1)
        )
        .withColumn(
            "ville",
            F.trim(
                F.regexp_extract("adresse_complete", r"\d{5}\s+(.*)$", 1)
            )
        )
        .select(
            F.col("id").alias("employee_id"),
            "adresse_complete",
            "code_postal",
            "ville",
            "moyen_deplacement",
        )
    )

    return df_commute

def transform_sport_bronze_to_silver(df: DataFrame) -> DataFrame:
    df_sport = (
        df.select(
            F.col("id").alias("employee_id"),
            F.col("pratique_sport")
        )
        .withColumn(
            "pratique_sport",
            F.when(
                F.col("pratique_sport").isNull() | (F.col("pratique_sport") == ""),
                F.lit("Aucune")
            ).otherwise(F.col("pratique_sport"))
        )
    )

    return df_sport

def transform_strava_act_bronze_to_silver(df: DataFrame) -> DataFrame:
    
    df_clean = df
    
    # Pour start_date_utc
    if "LongType" in str(df.schema["start_date_utc"].dataType):
        df_clean = df_clean.withColumn(
            "start_date_utc", 
            (F.col("start_date_utc") / 1000000).cast("timestamp")
        )
    
    if "LongType" in str(df.schema["start_date_local"].dataType):
        df_clean = df_clean.withColumn(
            "start_date_local", 
            (F.col("start_date_local") / 1000000).cast("timestamp")
        )

    # 2. Sélection et Renommage
    df_strava = df_clean.select(
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