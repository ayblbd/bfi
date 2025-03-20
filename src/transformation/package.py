from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max

from src.transformation.common import (
    get_is_during_a_period_for_equipement,
    get_status_relative_to_last_day_of_period,
)


def get_contract_status_from_history(
    contrat_produit: DataFrame, partition_date: str, name: str = "valid"
) -> DataFrame:
    """
    Calcule l'état des contrats à partir de leur historique.

    Cette fonction analyse les données des contrats pour déterminer leur état (valide)
    au cours de différentes périodes (semaine, mois, trimestre, année) à partir d'une
    date de référence.

    Utilisation métier :
    - Suivi des contrats valides sur des périodes spécifiques.
    - Analyse historique des performances des contrats pour le reporting.
    - Aide à la prise de décision en fonction des tendances de validité des contrats.
    """

    return (
        contrat_produit.select(
            "numero_contrat_interne", "etat_produit", "partitiondate"
        )
        .filter(col("etat_produit") == "VALIDE")
        .transform(
            get_status_relative_to_last_day_of_period,
            by="partitiondate",
            partition_date=partition_date,
            name=name,
        )
        .groupBy("numero_contrat_interne")
        .agg(
            max(f"is_{name}_s").alias(f"is_{name}_s"),
            max(f"is_{name}_s_1").alias(f"is_{name}_s_1"),
            max(f"is_{name}_s_2").alias(f"is_{name}_s_2"),
            max(f"is_{name}_m").alias(f"is_{name}_m"),
            max(f"is_{name}_m_1").alias(f"is_{name}_m_1"),
            max(f"is_{name}_m_2").alias(f"is_{name}_m_2"),
            max(f"is_{name}_t").alias(f"is_{name}_t"),
            max(f"is_{name}_t_1").alias(f"is_{name}_t_1"),
            max(f"is_{name}_y").alias(f"is_{name}_y"),
            max(f"is_{name}_y_1").alias(f"is_{name}_y_1"),
        )
        .select(
            "numero_contrat_interne",
            f"is_{name}_s",
            f"is_{name}_s_1",
            f"is_{name}_s_2",
            f"is_{name}_m",
            f"is_{name}_m_1",
            f"is_{name}_m_2",
            f"is_{name}_t",
            f"is_{name}_t_1",
            f"is_{name}_y",
            f"is_{name}_y_1",
        )
    )


def build_package(
    contrat_produit: DataFrame,
    rel_package_contrat: DataFrame,
    etat_contrat: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue consolidée des packages et contrats associés.

    Ces fonctions traitent les données des contrats et packages pour produire une vue
    complète et enrichie, en intégrant les informations historiques et les indicateurs
    clés de souscription et de validité.

    - Filtre et transforme les données des packages pour isoler les produits pertinents.
    - Enrichit les contrats avec les informations d'état et de période de souscription.
    - Calcule les indicateurs de souscription et de validité en fonction des périodes
        définies (par exemple, souscrit_s pour la semaine, souscrit_m pour le mois).
    - Produit une vue consolidée des packages et contrats avec leurs indicateurs.


    Utilisation métier :
    - Analyse des souscriptions aux packages et leur validité au fil du temps.
    - Gestion des états des contrats et des packages associés.
    - Suivi des indicateurs (KPI) pour évaluer les performances des produits.
    """
    only_package = (
        rel_package_contrat.filter(col("dfval") > col("partitiondate"))
        .withColumnRenamed("typepackage", "code_produit")
        .select("code_produit")
        .drop_duplicates()
    )

    etat_contrat = (
        etat_contrat.withColumnRenamed("code_etat_contrat", "code_etat_produit")
        .withColumnRenamed("etat_contrat", "etat_produit")
        .select("code_etat_produit", "etat_produit")
    )

    contrat_produit = (
        contrat_produit.withColumnRenamed("numerotiers", "numero_tiers")
        .withColumnRenamed("numerodecompte", "numero_compte")
        .withColumnRenamed("numerocarte", "numero_contrat_externe")
        .withColumnRenamed("numerocontratinterne", "numero_contrat_interne")
        .withColumnRenamed("codeproduit", "code_produit")
        .withColumnRenamed("datesouscription", "date_souscription")
        .withColumnRenamed("dateecheance", "date_echeance")
        .withColumnRenamed("etatproduit", "code_etat_produit")
        .join(etat_contrat, ["code_etat_produit"], "left")
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_contrat_externe",
            "numero_contrat_interne",
            "code_produit",
            "date_souscription",
            "date_echeance",
            "etat_produit",
            "partitiondate",
        )
    )

    valid_contract_from_history = get_contract_status_from_history(
        contrat_produit, partition_date=partition_date
    )

    kpis = [
        "is_souscrit_s",
        "is_souscrit_s_1",
        "is_souscrit_s_2",
        "is_souscrit_m",
        "is_souscrit_m_1",
        "is_souscrit_m_2",
        "is_souscrit_t",
        "is_souscrit_t_1",
        "is_souscrit_y",
        "is_souscrit_y_1",
        "is_valid_s",
        "is_valid_s_1",
        "is_valid_s_2",
        "is_valid_m",
        "is_valid_m_1",
        "is_valid_m_2",
        "is_valid_t",
        "is_valid_t_1",
        "is_valid_y",
        "is_valid_y_1",
    ]

    return (
        contrat_produit.join(only_package, ["code_produit"])
        .transform(
            get_is_during_a_period_for_equipement,
            by="date_souscription",
            name="souscrit",
            partition_date=partition_date,
        )
        .join(
            valid_contract_from_history,
            ["numero_contrat_interne"],
            "left",
        )
        .na.fill(False, kpis)
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_contrat_externe",
            "numero_contrat_interne",
            "code_produit",
            "date_souscription",
            "date_echeance",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
        .drop_duplicates()
    )
