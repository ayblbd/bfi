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
    Analyse l'historique des contrats pour déterminer leur statut sur différentes périodes.

    Cette fonction:
    - Ne conserve que les contrats à l'état 'VALIDE'
    - Calcule des indicateurs de validité pour différentes périodes:
        * Semaine en cours et 2 semaines précédentes (s, s_1, s_2)
        * Mois en cours et 2 mois précédents (m, m_1, m_2)
        * Trimestre en cours et précédent (t, t_1)
        * Année en cours et précédente (y, y_1)
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


def build_assurance(
    contrat_produit: DataFrame,
    gamme_assurance: DataFrame,
    assurance_only: DataFrame,
    etat_contrat: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue complète des contrats d'assurance avec leurs caractéristiques et états.

    Cette fonction:
    - Unifie les informations des contrats d'assurance provenant de différentes sources
    - Enrichit les données avec:
        * La gamme d'assurance
        * L'état du contrat
        * Les dates clés (souscription, échéance)
    - Calcule des indicateurs de suivi pour:
        * La souscription du contrat sur différentes périodes
        * La validité du contrat sur différentes périodes

    Le résultat permet de:
    - Suivre l'évolution du portefeuille d'assurance
    - Analyser les tendances de souscription
    - Monitorer la validité des contrats dans le temps
    """

    gamme_assurance = gamme_assurance.withColumnRenamed(
        "gamme", "gamme_assurance"
    ).select(
        "code_produit",
        "libelle_assurance",
        "gamme_assurance",
    )

    assurance_only = (
        assurance_only.filter(col("dfval") > col("partitiondate"))
        .filter(col("codecompagnie").isNotNull())
        .withColumnRenamed("typecontrat", "code_produit")
        .select("code_produit", "partitiondate")
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
        contrat_produit.join(assurance_only, ["code_produit", "partitiondate"])
        .join(gamme_assurance, ["code_produit"], "left")
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
            "gamme_assurance",
            "date_souscription",
            "date_echeance",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
    )
