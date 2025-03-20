import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json


def parse_description(audit_df: DataFrame) -> DataFrame:
    schema_connexion = T.StructType(
        [
            T.StructField("heure de connexion", T.StringType(), True),
            T.StructField("id Tiers", T.StringType(), True),
            T.StructField("canal", T.StringType(), True),
            T.StructField("Date de connexion", T.StringType(), True),
        ]
    )

    return (
        audit_df.drop("canal")
        .withColumn("description", from_json("description", schema_connexion))
        .select(
            col("_id").alias("ID"),
            col("date").alias("DATE"),
            col("application").alias("APPLICATION"),
            col("user").alias("UTILISATEUR"),
            col("userIP").alias("USER_IP"),
            col("action").alias("ACTION"),
            col("description.*"),
            col("_class").alias("CLASSE"),
            col("partitiondate"),
        )
        .withColumnRenamed("heure de connexion", "HEURE_DE_CONNEXION")
        .withColumnRenamed("id Tiers", "ID_TIERS")
        .withColumnRenamed("Date de connexion", "DATE_DE_CONNEXION")
        .withColumnRenamed("canal", "CANAL")
    )


def audit_connexion_build_daily(df: DataFrame) -> DataFrame:
    df_filtered = df.filter(col("ACTION") == "Authentification d'un client")
    return parse_description(df_filtered)
