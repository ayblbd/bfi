from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    round,
    expr,
    lit,
)
from src.transformation.common import create_primary_key


def stack_columns(objectif_tro: DataFrame, columns_to_drop: list) -> DataFrame:
    columns_to_stack = objectif_tro.drop(*columns_to_drop)

    stack_expr = "stack({}, {})".format(
        len(columns_to_stack.columns),
        ", ".join([f"'{col}', {col}" for col in columns_to_stack.columns]),
    )
    return objectif_tro.withColumn("code_marche", lit("R")).select(
        *columns_to_drop,
        expr(stack_expr).alias("produit", "objectif"),
        "code_marche",
    )


def build_objectif_fdc_retail(
    objectif_tro: DataFrame,
    saisionnalite: DataFrame,
    mois_trimestre_semaine: DataFrame,
    columns_to_drop: list,
) -> DataFrame:
    objectif_tro = stack_columns(objectif_tro, columns_to_drop)
    return (
        objectif_tro.crossJoin(saisionnalite)
        .join(
            mois_trimestre_semaine,
            saisionnalite["numero_semaine"] == mois_trimestre_semaine["semaine"],
            "left",
        )
        .withColumn("objectif_annuel", round(col("objectif"), 2).cast("float"))
        .withColumn(
            "objectif_hebdo",
            round(
                (
                    col("objectif_annuel").cast("float")
                    * col("pourc_semaine").cast("float")
                ),
                2,
            ).cast("float"),
        )
        .withColumn(
            "objectif_cumul",
            round(
                (
                    col("objectif_annuel").cast("float")
                    * col("pourc_cumul").cast("float")
                ),
                2,
            ).cast("float"),
        )
        .withColumn("code_produit", create_primary_key("produit"))
        .select(
            "code_fdc",
            "produit",
            "code_produit",
            "numero_semaine",
            "code_marche",
            "trimestre",
            "mois",
            "annee",
            expr(
                """
                stack(
                2,
                'S', objectif_hebdo,
                'C', objectif_cumul
                ) as (frequence, objectif)
            """
            ),
        )
    )


def build_dim_objectif_fdc(
    objectif_fdc: DataFrame,
    saisonnalite: DataFrame,
    mois_trimestre_semaine: DataFrame,
    columns_to_drop: list,
):
    objectif_fdc = build_objectif_fdc_retail(
        objectif_fdc, saisonnalite, mois_trimestre_semaine, columns_to_drop
    )
    return objectif_fdc.select("produit", "code_produit").dropDuplicates()


def build_objectif_fdc(
    objectif_tro: DataFrame,
    saisionnalite: DataFrame,
    mois_trimestre_semaine: DataFrame,
    columns_to_drop: list,
) -> DataFrame:
    objectif_fdc = build_objectif_fdc_retail(
        objectif_tro, saisionnalite, mois_trimestre_semaine, columns_to_drop
    )
    return objectif_fdc.select(
        "code_fdc",
        "code_produit",
        "numero_semaine",
        "code_marche",
        "trimestre",
        "mois",
        "annee",
        "frequence",
        "objectif",
    )
