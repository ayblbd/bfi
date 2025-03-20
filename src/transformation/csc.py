from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    max,
    min,
    to_date,
)

from src.common.table_loader import get_history_from
from src.transformation.common import (
    get_is_during_a_period_for_equipement,
)


def build_csc(
    compte: DataFrame,
    donnees_compte: DataFrame,
    ressource_emploi: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue consolidée des comptes CSC.

    Cette fonction rassemble et analyse les comptes CSC (Compte sur Carnet) en combinant
    différentes sources de données pour produire des indicateurs clé de performance (KPI)
    liés à l'état, la souscription et l'encours des comptes sur des périodes définies.

    Utilisation métier :
    - Suivi des comptes CSC sur des périodes spécifiques (semaine, mois, trimestre, année).
    - Analyse de la souscription, des ouvertures et des encours des comptes CSC.
    - Identification des comptes actifs et des comportements historiques.

    Étapes principales :
    1. Préparation des données :
    - Filtrage des comptes CSC par type ("010", "011", "014").
    - Récupération de l'historique des comptes pour différentes périodes.
    - Identification des comptes ouverts avec `is_ouvert`.

    2. Calcul des indicateurs :
    - Indicateurs de souscription (`is_souscrit_*`) pour chaque période.
    - Indicateurs de comptes CSC spécifiques (`is_csc_sec_*`) sur différentes périodes.
    - Indicateurs d'encours ≥ 1000 (`is_encours_1000_*`).

    3. Consolidation :
    - Jointure des données préparées pour chaque indicateur.
    - Remplissage des valeurs manquantes avec des valeurs par défaut (False).
    - Sélection des colonnes clés pour l'analyse métier.

    Résultat :
    Un DataFrame contenant :
    - Les informations des comptes CSC : type de compte, numéro de compte, numéro de tiers.
    - Les indicateurs KPI calculés pour chaque période (semaine, mois, trimestre, année) :
    - Souscription (`is_souscrit_*`).
    - État ouvert (`is_ouvert_*`).
    - Spécificité CSC (`is_csc_sec_*`).
    - Encours ≥ 1000 (`is_encours_1000_*`).

    Ce tableau consolidé permet une vue complète et structurée des performances des comptes CSC.
    """
    donnees_compte = is_ouvert(donnees_compte, partition_date)

    compte_end_of_periods = get_history_from(
        compte,
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
    )

    compte = compte.filter(col("naturecompte").isin("010", "011", "014"))

    compte_is_souscrit = (
        compte.transform(
            get_is_during_a_period_for_equipement,
            by="date_ouverture_compte",
            name="souscrit",
            partition_date=partition_date,
        )
        .groupBy("numerodecompte")
        .agg(
            max("is_souscrit_s_1").alias("is_souscrit_s_1"),
            max("is_souscrit_s").alias("is_souscrit_s"),
            max("is_souscrit_s_2").alias("is_souscrit_s_2"),
            max("is_souscrit_s").alias("is_souscrit_m"),
            max("is_souscrit_m_1").alias("is_souscrit_m_1"),
            max("is_souscrit_m_2").alias("is_souscrit_m_2"),
            max("is_souscrit_s").alias("is_souscrit_t"),
            max("is_souscrit_t_1").alias("is_souscrit_t_1"),
            max("is_souscrit_s").alias("is_souscrit_y"),
            max("is_souscrit_y_1").alias("is_souscrit_y_1"),
        )
    )

    ressource_emploi = is_encours_1000(ressource_emploi, partition_date)

    compte_csc_flaged = is_csc_sec_from_history(
        compte_end_of_periods, partition_date
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
        "is_ouvert_s",
        "is_ouvert_s_1",
        "is_ouvert_s_2",
        "is_ouvert_m",
        "is_ouvert_m_1",
        "is_ouvert_m_2",
        "is_ouvert_t",
        "is_ouvert_t_1",
        "is_ouvert_y",
        "is_ouvert_y_1",
        "is_csc_sec_s",
        "is_csc_sec_s_1",
        "is_csc_sec_s_2",
        "is_csc_sec_m",
        "is_csc_sec_m_1",
        "is_csc_sec_m_2",
        "is_csc_sec_t",
        "is_csc_sec_t_1",
        "is_csc_sec_y",
        "is_csc_sec_y_1",
        "is_encours_1000_s",
        "is_encours_1000_s_1",
        "is_encours_1000_s_2",
        "is_encours_1000_m",
        "is_encours_1000_m_1",
        "is_encours_1000_m_2",
        "is_encours_1000_t",
        "is_encours_1000_t_1",
        "is_encours_1000_y",
        "is_encours_1000_y_1",
    ]

    return (
        compte.filter(col("partitiondate") == to_date(lit(partition_date)))
        .join(compte_is_souscrit, ["numerodecompte"], "left")
        .join(donnees_compte, ["numerodecompte"], "left")
        .join(compte_csc_flaged, ["numerotiers"], "left")
        .join(ressource_emploi, ["numerodecompte"], "left")
        .na.fill(False, kpis)
        .select(
            "naturecompte",
            "numerodecompte",
            "numerotiers",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
    )


def is_csc_sec_from_history(
    compte: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Identifie les comptes CSC (Compte sur Carnet) dans l'historique.

    Cette fonction vérifie les comptes non clôturés appartenant aux types CSC spécifiques
    ("010", "011", "014") et calcule des indicateurs montrant la présence de ces comptes
    sur différentes périodes (semaine, mois, trimestre, année).

    Retourne :
        Un DataFrame avec les indicateurs de présence des comptes CSC par période.
    """
    csc_natures = ["010", "011", "014"]

    is_compte_non_cloture = col("code_etatcompte") != "3"
    is_compte_csc = col("naturecompte").isin(csc_natures)

    return (
        compte.filter(is_compte_non_cloture)
        .withColumn("is_csc", is_compte_csc)
        .groupBy("numerotiers", "partitiondate")
        .agg(
            min(col("is_csc")).alias("is_csc"),
        )
        .filter(col("is_csc"))
        .transform(
            get_is_during_a_period_for_equipement,
            by="partitiondate",
            name="csc_sec",
            partition_date=partition_date,
        )
        .groupBy("numerotiers")
        .agg(
            max(col("is_csc_sec_s")).alias("is_csc_sec_s"),
            max(col("is_csc_sec_s_1")).alias("is_csc_sec_s_1"),
            max(col("is_csc_sec_s_2")).alias("is_csc_sec_s_2"),
            max(col("is_csc_sec_s")).alias("is_csc_sec_m"),
            max(col("is_csc_sec_m_1")).alias("is_csc_sec_m_1"),
            max(col("is_csc_sec_m_2")).alias("is_csc_sec_m_2"),
            max(col("is_csc_sec_s")).alias("is_csc_sec_t"),
            max(col("is_csc_sec_t_1")).alias("is_csc_sec_t_1"),
            max(col("is_csc_sec_s")).alias("is_csc_sec_y"),
            max(col("is_csc_sec_y_1")).alias("is_csc_sec_y_1"),
        )
    )


def is_encours_1000(
    ressource_emploi: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Détecte les comptes avec un solde supérieur ou égal à 1000.

    Cette fonction filtre les comptes ayant un solde supérieur ou égal à 1000 et calcule
    des indicateurs montrant leur présence sur des périodes définies (semaine, mois, trimestre, année).

    Retourne :
        Un DataFrame avec les indicateurs de présence des encours ≥ 1000 par période.
    """
    return (
        ressource_emploi.select(
            "partitiondate",
            "numerodecompte",
            "soldejourcr_devise",
        )
        .filter(col("soldejourcr_devise") >= 1000)
        .transform(
            get_is_during_a_period_for_equipement,
            by="partitiondate",
            name="encours_1000",
            partition_date=partition_date,
        )
        .groupBy("numerodecompte")
        .agg(
            max("is_encours_1000_s_1").alias("is_encours_1000_s_1"),
            max("is_encours_1000_s").alias("is_encours_1000_s"),
            max("is_encours_1000_s_2").alias("is_encours_1000_s_2"),
            max("is_encours_1000_s").alias("is_encours_1000_m"),
            max("is_encours_1000_m_1").alias("is_encours_1000_m_1"),
            max("is_encours_1000_m_2").alias("is_encours_1000_m_2"),
            max("is_encours_1000_s").alias("is_encours_1000_t"),
            max("is_encours_1000_t_1").alias("is_encours_1000_t_1"),
            max("is_encours_1000_s").alias("is_encours_1000_y"),
            max("is_encours_1000_y_1").alias("is_encours_1000_y_1"),
        )
    )


def is_ouvert(donnees_compte: DataFrame, partition_date: str) -> DataFrame:
    """
    Identifie les comptes non clôturés dans l'historique.

    Cette fonction filtre les comptes qui ne sont pas marqués comme "CLOTURE" (clôturés)
    et calcule des indicateurs montrant si le compte était ouvert sur différentes périodes
    (semaine, mois, trimestre, année).

    Retourne :
        Un DataFrame avec les indicateurs de comptes ouverts par période.
    """
    return (
        donnees_compte.select(
            "numerodecompte",
            "statutcompte",
            "partitiondate",
        )
        .filter(col("statutcompte") != "CLOTURE")
        .transform(
            get_is_during_a_period_for_equipement,
            by="partitiondate",
            name="ouvert",
            partition_date=partition_date,
        )
        .groupBy("numerodecompte")
        .agg(
            max("is_ouvert_s").alias("is_ouvert_s"),
            max("is_ouvert_s_1").alias("is_ouvert_s_1"),
            max("is_ouvert_s_2").alias("is_ouvert_s_2"),
            max("is_ouvert_s").alias("is_ouvert_m"),
            max("is_ouvert_m_1").alias("is_ouvert_m_1"),
            max("is_ouvert_m_2").alias("is_ouvert_m_2"),
            max("is_ouvert_s").alias("is_ouvert_t"),
            max("is_ouvert_t_1").alias("is_ouvert_t_1"),
            max("is_ouvert_s").alias("is_ouvert_y"),
            max("is_ouvert_y_1").alias("is_ouvert_y_1"),
        )
        .select(
            "numerodecompte",
            "is_ouvert_s",
            "is_ouvert_s_1",
            "is_ouvert_s_2",
            "is_ouvert_m",
            "is_ouvert_m_1",
            "is_ouvert_m_2",
            "is_ouvert_t",
            "is_ouvert_t_1",
            "is_ouvert_y",
            "is_ouvert_y_1",
        )
    )
