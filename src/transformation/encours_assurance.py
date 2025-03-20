from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    max,
    substring,
    trim,
    when,
)
from pyspark.sql.types import FloatType

from src.transformation.common import (
    get_last_recorded_sold_per_client_from_last_month,
    get_last_recorded_sold_per_client_from_last_quarter,
    get_last_recorded_sold_per_client_from_last_week,
    get_last_recorded_sold_per_client_from_last_year,
)


def select_etat_contrat(df: DataFrame) -> DataFrame:
    """
    Sélectionne les états des contrats.

    Cette fonction extrait les colonnes principales des états des contrats, telles que le code
    et la description de l'état.
    """
    return df.select("code_etat_contrat", "etat_contrat")


def select_contrat_produit(df: DataFrame) -> DataFrame:
    """
    Sélectionne les informations des contrats produits.

    Cette fonction renomme certaines colonnes des contrats produits pour harmoniser
    les noms et extrait les colonnes principales nécessaires.
    """
    return (
        df.withColumnRenamed("numerocarte", "numero_adhesion")
        .withColumnRenamed("codeproduit", "code_produit")
        .withColumnRenamed("etatproduit", "code_etat_contrat")
        .withColumnRenamed("datesouscription", "date_souscription")
        .select(
            "numerodecompte",
            "numero_adhesion",
            "date_souscription",
            "code_produit",
            "code_etat_contrat",
        )
    )


def select_axa_assurance(df: DataFrame) -> DataFrame:
    """
    Sélectionne les données d'assurance pour AXA.

    Cette fonction transforme les données spécifiques à AXA Assurance, notamment en ajoutant
    un code produit, en convertissant les montants d'encours en type float et en renommant
    certaines colonnes.

    Arguments :
        df : DataFrame contenant les données d'assurance pour AXA.

    Retourne :
        Un DataFrame avec les informations formatées des assurances AXA.
    """
    return (
        df.withColumn(
            "code_produit",
            when(col("produit").isin("223", "2223"), "SE01").when(
                col("produit").isin("320", "2320"), "SR01"
            ),
        )
        .withColumn("encours", col("encours").cast(FloatType()))
        .withColumnRenamed("partitiondate", "date_encours")
        .withColumn("numero_adhesion", concat(lit("0"), col("field2")))
        .select(
            "entreprise",
            "numero_adhesion",
            "code_produit",
            "encours",
            "date_encours",
        )
    )


def select_wafa_assurance(df: DataFrame) -> DataFrame:
    """
    Sélectionne les données d'assurance pour Wafa Assurance.

    Cette fonction filtre les données de Wafa Assurance, convertit les montants
    d'encours, ajoute un code produit basé sur certaines conditions et extrait
    les colonnes principales.

    Arguments :
        df : DataFrame contenant les données d'assurance pour Wafa.

    Retourne :
        Un DataFrame avec les informations formatées des assurances Wafa.
    """
    return (
        df.filter(col("TYPE_D") != "Z")
        .withColumn("encours", col("epargne").cast(FloatType()) / 100)
        .withColumnRenamed("partitiondate", "date_encours")
        .withColumn(
            "code_produit",
            when(substring(col("code_produit"), 1, 3) == "100", "WR01").when(
                substring(col("code_produit"), 1, 3) == "101", "WE01"
            ),
        )
        .filter(col("code_produit").isin(["WR01", "WE01"]))
        .select(
            "entreprise",
            "numero_adhesion",
            "code_produit",
            "encours",
            "date_encours",
        )
    )


def select_rma_watanya(df: DataFrame) -> DataFrame:
    """
    Sélectionne les données d'assurance pour RMA Watanya.

    Cette fonction formate les données spécifiques à RMA Watanya, en ajustant
    le numéro d'adhésion et les montants d'encours, puis en extrayant les colonnes
    principales.

    Arguments :
        df : DataFrame contenant les données d'assurance pour RMA Watanya.

    Retourne :
        Un DataFrame avec les informations formatées des assurances RMA Watanya.
    """
    return (
        df.withColumn("numero_adhesion", trim(col("numero_contrat")))
        .withColumn("encours", col("encours").cast(FloatType()) / 100)
        .withColumnRenamed("partitiondate", "date_encours")
        .select(
            "entreprise",
            "numero_adhesion",
            "encours",
            "date_encours",
        )
    )


def select_saham_assurance(df: DataFrame) -> DataFrame:
    """
    Sélectionne les données d'assurance pour Saham.

    Cette fonction formate les données spécifiques à Saham Assurance, notamment
    en extrayant le code produit, en ajustant les montants d'encours, et en renommant
    certaines colonnes.

    Arguments :
        df : DataFrame contenant les données d'assurance pour Saham.

    Retourne :
        Un DataFrame avec les informations formatées des assurances Saham.
    """
    return (
        df.withColumn("numero_adhesion", trim(col("contrat")))
        .withColumn("code_produit", substring(col("numero_adhesion"), 4, 4))
        .withColumn("encours", col("encours_net").cast(FloatType()) / 100.0)
        .withColumnRenamed("partitiondate", "date_encours")
        .select(
            "entreprise",
            "numero_adhesion",
            "code_produit",
            "encours",
            "date_encours",
        )
    )


def build_encours_assurances(
    axa_assurance: DataFrame,
    rma_watanya: DataFrame,
    saham_assurance: DataFrame,
    wafa_assurance: DataFrame,
    contrat_produit: DataFrame,
    etat_contrat: DataFrame,
) -> DataFrame:
    """
    Construit l'historique des encours des assurances.

    Cette fonction combine les données des différentes assurances (AXA, RMA, Saham, Wafa) avec les
    contrats valides et agrège les montants d'encours pour produire un historique consolidé.

    Arguments :
        axa_assurance : DataFrame contenant les données d'assurance pour AXA.
        rma_watanya : DataFrame contenant les données d'assurance pour RMA Watanya.
        saham_assurance : DataFrame contenant les données d'assurance pour Saham.
        wafa_assurance : DataFrame contenant les données d'assurance pour Wafa.
        contrat_produit : DataFrame contenant les informations des contrats produits.
        etat_contrat : DataFrame contenant les états des contrats.

    Retourne :
        Un DataFrame consolidé avec les historiques d'encours des assurances.
    """
    axa_assurance = select_axa_assurance(axa_assurance)
    rma_watanya = select_rma_watanya(rma_watanya)
    saham_assurance = select_saham_assurance(saham_assurance)
    wafa_assurance = select_wafa_assurance(wafa_assurance)

    is_valid_state = col("etat_contrat") == "VALIDE"
    etat_contrat = select_etat_contrat(etat_contrat)
    valid_state = etat_contrat.filter(is_valid_state)

    contrat_produit = select_contrat_produit(contrat_produit)
    valid_contracts = (
        contrat_produit.join(valid_state, ["code_etat_contrat"])
        .select(
            "numerodecompte",
            "numero_adhesion",
            "date_souscription",
            "code_produit",
        )
        .drop_duplicates()
    )

    encours_axa_assurance = (
        axa_assurance.join(valid_contracts.drop("code_produit"), ["numero_adhesion"])
        .groupBy(
            "entreprise",
            "numerodecompte",
            "numero_adhesion",
            "code_produit",
            "date_souscription",
            "date_encours",
        )
        .agg(max("encours").alias("encours"))
    )
    encours_rma_watanya = (
        rma_watanya.join(valid_contracts, ["numero_adhesion"])
        .groupBy(
            "entreprise",
            "numerodecompte",
            "numero_adhesion",
            "code_produit",
            "date_souscription",
            "date_encours",
        )
        .agg(max("encours").alias("encours"))
    )

    encours_saham_assurance = (
        saham_assurance.join(valid_contracts, ["numero_adhesion", "code_produit"])
        .groupBy(
            "entreprise",
            "numerodecompte",
            "numero_adhesion",
            "code_produit",
            "date_souscription",
            "date_encours",
        )
        .agg(max("encours").alias("encours"))
    )

    encours_wafa_assurance = (
        wafa_assurance.join(valid_contracts, ["numero_adhesion", "code_produit"])
        .groupBy(
            "entreprise",
            "numerodecompte",
            "numero_adhesion",
            "code_produit",
            "date_souscription",
            "date_encours",
        )
        .agg(max("encours").alias("encours"))
    )

    return (
        encours_axa_assurance.unionByName(encours_rma_watanya)
        .unionByName(encours_saham_assurance)
        .unionByName(encours_wafa_assurance)
    )


def select_compte_client(df: DataFrame) -> DataFrame:
    return df.select("numerodecompte", "numerotiers")


def build_encours_assurance(
    compte: DataFrame,
    axa_assurance: DataFrame,
    rma_watanya: DataFrame,
    saham_assurance: DataFrame,
    wafa_assurance: DataFrame,
    contrat_produit: DataFrame,
    etat_contrat: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit les encours actuels et historiques des assurances.

    Cette fonction extrait les encours actuels pour une date donnée et calcule les
    encours historiques (semaine, mois, trimestre, année) en utilisant les données consolidées
    des assurances et des contrats.

    Arguments :
        axa_assurance : DataFrame contenant les données d'assurance pour AXA.
        rma_watanya : DataFrame contenant les données d'assurance pour RMA Watanya.
        saham_assurance : DataFrame contenant les données d'assurance pour Saham.
        wafa_assurance : DataFrame contenant les données d'assurance pour Wafa.
        contrat_produit : DataFrame contenant les informations des contrats produits.
        etat_contrat : DataFrame contenant les états des contrats.
        partition_date : Date de partition utilisée pour les calculs temporels.

    Retourne :
        Un DataFrame avec les encours actuels et historiques des assurances.
    """
    compte = select_compte_client(compte)
    assurances_history = build_encours_assurances(
        axa_assurance,
        rma_watanya,
        saham_assurance,
        wafa_assurance,
        contrat_produit,
        etat_contrat,
    )

    encours_assurances = assurances_history.filter(
        col("date_encours") == partition_date
    ).select(
        "entreprise",
        "numerodecompte",
        "numero_adhesion",
        "code_produit",
        "date_souscription",
        "date_encours",
        "encours",
    )

    encours_assurances_history = (
        assurances_history.transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="encours",
            partition_date=partition_date,
            weeks=1,
            partition_columns=[
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
            ],
            order_column="date_encours",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="encours",
            partition_date=partition_date,
            weeks=2,
            partition_columns=[
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
            ],
            order_column="date_encours",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="encours",
            partition_date=partition_date,
            months=1,
            partition_columns=[
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
            ],
            order_column="date_encours",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="encours",
            partition_date=partition_date,
            months=2,
            partition_columns=[
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
            ],
            order_column="date_encours",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="encours",
            partition_columns=[
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
            ],
            order_column="date_encours",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="encours",
            partition_date=partition_date,
            years=1,
            partition_columns=[
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
            ],
            order_column="date_encours",
        )
        .select(
            "entreprise",
            "numerodecompte",
            "numero_adhesion",
            "code_produit",
            "date_souscription",
            "date_encours",
            "last_week_encours",
            "last_2_weeks_encours",
            "last_month_encours",
            "last_2_months_encours",
            "last_quarter_encours",
            "last_year_encours",
        )
    )

    kpis = [
        "encours_s",
        "encours_s_1",
        "encours_s_2",
        "encours_m_1",
        "encours_m_2",
        "encours_t_1",
        "encours_y_1",
    ]

    return (
        encours_assurances.join(
            encours_assurances_history,
            [
                "entreprise",
                "numerodecompte",
                "numero_adhesion",
                "code_produit",
                "date_souscription",
                "date_encours",
            ],
            "left_outer",
        )
        .join(compte, ["numerodecompte"], "left")
        .withColumnRenamed("encours", "encours_s")
        .withColumnRenamed("last_week_encours", "encours_s_1")
        .withColumnRenamed("last_2_weeks_encours", "encours_s_2")
        .withColumnRenamed("last_month_encours", "encours_m_1")
        .withColumnRenamed("last_2_months_encours", "encours_m_2")
        .withColumnRenamed("last_quarter_encours", "encours_t_1")
        .withColumnRenamed("last_year_encours", "encours_y_1")
        .na.fill(0, kpis)
        .select(
            "entreprise",
            "numerotiers",
            "numerodecompte",
            "numero_adhesion",
            "code_produit",
            "date_souscription",
            *kpis,
        )
    )
