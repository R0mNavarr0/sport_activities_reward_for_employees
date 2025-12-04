from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataQualityManager:
    """
    Classe utilitaire pour exécuter des tests de qualité sur les DataFrames Spark.
    """
    
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.errors = []

    def run_check(self, check_name, condition_expr, error_msg):
        """
        Exécute un test qualitatif. Si des lignes ne respectent pas la condition, c'est une erreur.
        condition_expr : Expression Spark SQL (ex: "age > 18") qui doit être VRAIE.
        """
        bad_rows = self.df.filter(f"NOT ({condition_expr})")
        count = bad_rows.count()
        
        if count > 0:
            print(f"[DQ] {self.table_name} - {check_name} : ÉCHEC ({count} lignes en erreur)")
            self.errors.append(f"{check_name}: {count} erreurs - {error_msg}")
        else:
            print(f"[DQ] {self.table_name} - {check_name} : SUCCÈS")

    def check_unique(self, col_name):
        """Test d'unicité (ex: clé primaire)"""
        dup_count = self.df.groupBy(col_name).count().filter("count > 1").count()
        if dup_count > 0:
            msg = f"Doublons détectés sur la colonne {col_name}"
            print(f"[DQ] {self.table_name} - Unicité {col_name} : ÉCHEC ({dup_count} doublons)")
            self.errors.append(msg)
        else:
            print(f"[DQ] {self.table_name} - Unicité {col_name} : SUCCÈS")

    def check_volume(self, min_rows=1):
        """Test Quantitatif : Vérifie que la table n'est pas vide"""
        count = self.df.count()
        if count < min_rows:
            msg = f"Volume insuffisant : {count} lignes (attendu >= {min_rows})"
            print(f"[DQ] {self.table_name} - Volume : ÉCHEC ({msg})")
            self.errors.append(msg)
        else:
            print(f"[DQ] {self.table_name} - Volume : {count} lignes.")

    def validate(self, raise_error=False):
        """Lève une exception si des erreurs ont été détectées"""
        if self.errors:
            print(f"\n[DQ SUMMARY] {len(self.errors)} tests échoués sur {self.table_name}.")
            if raise_error:
                raise ValueError(f"DQ FAILED for {self.table_name}: {self.errors}")
        else:
            print(f"[DQ SUMMARY] Tous les tests sont passés pour {self.table_name}.")