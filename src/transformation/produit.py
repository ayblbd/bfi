from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def build_type_produit(
    contratelt: DataFrame,
    type_elt_contrat: DataFrame,
    type_contrat: DataFrame,
    partitiondate,
) -> DataFrame:
    contratelt_fitered = contratelt.filter(col("dfval") > partitiondate).select(
        col("typedeproduit"), col("typecontrat")
    )

    type_elt_contrat_fitered = type_elt_contrat.filter(
        col("dfval") > partitiondate
    ).select(col("typedeproduit"), col("libproduit"))
    type_contrat_filtered = type_contrat.filter(col("dfval") > partitiondate).select(
        col("typecontrat"), col("libcontrat")
    )

    join_elt_contrat = ["typedeproduit"]
    join_contrat = ["typecontrat"]

    type_produit_processed = (
        contratelt_fitered.join(type_elt_contrat_fitered, join_elt_contrat, "left")
        .join(type_contrat_filtered, join_contrat, "left")
        .select(
            col("typedeproduit"),
            col("libproduit"),
            col("typecontrat"),
            col("libcontrat"),
        )
    )
    return type_produit_processed
