from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    least,
    lit,
    max,
)
from pyspark.sql.types import IntegerType


def build_transaction_score(
    compte: DataFrame,
    mouvement_comptable: DataFrame,
    ref_operation: DataFrame,
    transaction_monetique: DataFrame,
) -> DataFrame:
    is_tpe_internet = col("tra_type_terminal").isin(["TPE", "INTERNET"])

    transaction_monetique = transaction_monetique.select(
        "numerodecompte",
        "tra_type_terminal",
    )

    compte = (
        compte.filter(
            col("naturecompte").isin(
                ["001", "002", "003", "004", "005", "006", "010", "011", "014"]
            )
        )
        .join(transaction_monetique, ["numerodecompte"], how="left")
        .withColumn("score_carte", is_tpe_internet)
        .select(
            "numerotiers",
            "numerodecompte",
            "score_carte",
        )
    )

    ref_operation = ref_operation.withColumnRenamed("cod", "codeoperation").select(
        "codeoperation",
        "canal",
        "sous_catégorie",
        "famille",
    )

    is_agence = col("canal") == "Agence"
    is_virement = col("sous_catégorie").contains("Virement")
    is_mise_a_disposition = col("sous_catégorie").contains("Mise à Disposition")
    is_operation = col("famille") == "Operation"
    is_agence_gab = col("canal").isin(["Agence", "Agence/GAB", "GAB"])
    is_retrait = col("sous_catégorie") == "Retrait"
    is_cheque_lcn = col("sous_catégorie") == "Cheque/LCN"

    mouvement_comptable = (
        mouvement_comptable.select(
            "numerodecompte",
            "codeoperation",
        )
        .join(ref_operation, ["codeoperation"], how="left")
        .withColumn("score_virement", (is_agence & is_virement))
        .withColumn("score_mad", (is_agence & is_mise_a_disposition & is_operation))
        .withColumn("score_retrait", (is_agence_gab & is_retrait & is_operation))
        .withColumn("score_cheque", (is_agence & is_cheque_lcn & is_operation))
        .groupBy("numerodecompte")
        .agg(
            max("score_virement").alias("score_virement"),
            max("score_mad").alias("score_mad"),
            max("score_retrait").alias("score_retrait"),
            max("score_cheque").alias("score_cheque"),
        )
        .select(
            "numerodecompte",
            "score_virement",
            "score_mad",
            "score_retrait",
            "score_cheque",
        )
    )

    return (
        compte.join(mouvement_comptable, ["numerodecompte"], how="left")
        .groupBy("numerotiers")
        .agg(
            max("score_virement").alias("score_virement"),
            max("score_mad").alias("score_mad"),
            max("score_retrait").alias("score_retrait"),
            max("score_cheque").alias("score_cheque"),
            max("score_carte").alias("score_carte"),
        )
        .withColumn("score_virement", col("score_virement").cast(IntegerType()) / 2)
        .withColumn("score_mad", col("score_mad").cast(IntegerType()) / 2)
        .withColumn("score_retrait", col("score_retrait").cast(IntegerType()) / 2)
        .withColumn("score_cheque", col("score_cheque").cast(IntegerType()) / 2)
        .withColumn("score_carte", col("score_carte").cast(IntegerType()))
        .withColumn(
            "scores_sum",
            coalesce(col("score_virement"), lit(0))
            + coalesce(col("score_mad"), lit(0))
            + coalesce(col("score_retrait"), lit(0))
            + coalesce(col("score_cheque"), lit(0))
            + coalesce(col("score_carte"), lit(0)),
        )
        .withColumn("score_transaction", least(col("scores_sum"), lit(1.5)))
    )
