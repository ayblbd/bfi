from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    max,
    substring,
    to_date,
    expr,
    last_day,
    date_trunc,
    add_months,
    date_add,
    min,
)

from src.transformation.common import (
    get_is_during_a_period_for_equipement,
)


from src.transformation.common import (
    get_is_during_a_period_for_equipement,
    get_last_day_of_week,
)


def get_subscribed_autorisation(
    autorisation: DataFrame,
    elt_contrat: DataFrame,
    partition_date: str,
    name: str = "souscrit",
) -> DataFrame:
    """
    Identifie les autorisations souscrites durant une période donnée.

    Cette fonction détermine si une autorisation a été souscrite au cours d'une période spécifiée,
    basée sur les données des contrats associés.

    Utilisation métier:
    - Suivi des nouvelles souscriptions
    - Analyse des tendances de souscription
    - Rapport sur les autorisations actives par période
    """

    elt_contrat = elt_contrat.select("dateeffet", "numeroelementinterne")

    return (
        autorisation.join(elt_contrat, ["numeroelementinterne"], "left")
        .transform(
            get_is_during_a_period_for_equipement,
            by="dateeffet",
            name=name,
            partition_date=partition_date,
        )
        .groupBy(
            "numero_autorisation",
        )
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
            "numero_autorisation",
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


def get_autorisation_status_from_history(
    autorisation: DataFrame,
    elt_contrat: DataFrame,
    partition_date: str,
    name: str = "notifie",
) -> DataFrame:
    """
    Évalue le statut des autorisations notifiées dans l'historique.

    Cette fonction identifie les autorisations qui ont été notifiées et leur statut
    au cours des semaines, mois, trimestres et années précédents.

    Utilisation métier:
    - Analyse des notifications pour les engagements
    - Suivi des modifications d'état des autorisations
    - Rapport des statuts notifiés par période
    """

    elt_contrat = elt_contrat.select("numeroelementinterne", "dateecheance")

    is_notified = col("etat") == 2

    dt = to_date(lit(partition_date))
    last_day_1_week_ago = get_last_day_of_week(dt - expr("INTERVAL 1 WEEK"))
    last_day_2_weeks_ago = get_last_day_of_week(dt - expr("INTERVAL 2 WEEK"))
    last_day_1_month_ago = last_day(add_months(dt, -1))
    last_day_2_months_ago = last_day(add_months(dt, -2))
    last_day_1_quarter_ago = date_add(date_trunc("quarter", dt), -1)
    last_day_1_year_ago = date_add(date_trunc("year", dt), -1)

    return (
        autorisation.select(
            "numero_autorisation", "etat", "dateetat", "numeroelementinterne"
        )
        .filter(is_notified)
        .join(elt_contrat, ["numeroelementinterne"], "left")
        .withColumn(
            f"is_{name}_s", (col("dateecheance") > dt) & (col("dateetat") <= dt)
        )
        .withColumn(
            f"is_{name}_s_1",
            (col("dateecheance") > last_day_1_week_ago)
            & (col("dateetat") <= last_day_1_week_ago),
        )
        .withColumn(
            f"is_{name}_s_2",
            (col("dateecheance") > last_day_2_weeks_ago)
            & (col("dateetat") <= last_day_2_weeks_ago),
        )
        .withColumn(
            f"is_{name}_m", (col("dateecheance") > dt) & (col("dateetat") <= dt)
        )
        .withColumn(
            f"is_{name}_m_1",
            (col("dateecheance") > last_day_1_month_ago)
            & (col("dateetat") <= last_day_1_month_ago),
        )
        .withColumn(
            f"is_{name}_m_2",
            (col("dateecheance") > last_day_2_months_ago)
            & (col("dateetat") <= last_day_2_months_ago),
        )
        .withColumn(
            f"is_{name}_t", (col("dateecheance") > dt) & (col("dateetat") <= dt)
        )
        .withColumn(
            f"is_{name}_t_1",
            (col("dateecheance") > last_day_1_quarter_ago)
            & (col("dateetat") <= last_day_1_quarter_ago),
        )
        .withColumn(
            f"is_{name}_y", (col("dateecheance") > dt) & (col("dateetat") < dt)
        )
        .withColumn(
            f"is_{name}_y_1",
            (col("dateecheance") > last_day_1_year_ago)
            & (col("dateetat") <= last_day_1_year_ago),
        )
        .groupBy("numero_autorisation")
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
            "numero_autorisation",
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


def get_utilised_autorisation_from_history(
    bfi: DataFrame,
    map_ressource_emploi: DataFrame,
    partition_date: str,
    name: str = "utilise",
) -> DataFrame:
    """
    Identifie les autorisations utilisées durant une période spécifique.

    Cette fonction détermine si des autorisations ont été utilisées dans un cadre spécifique,
    en croisant les données de transactions et les types de ressources.

    Utilisation métier:
    - Suivi des autorisations utilisées
    - Gestion des engagements bancaires
    - Analyse des comportements d'utilisation des autorisations
    """

    is_decouvert = col("type_ressource_emploi") == "DEC"
    bfi = (
        bfi.withColumnRenamed("numerodecompte", "numero_compte")
        .withColumnRenamed("fk_mapressourceemploi", "pk_mapressourceemploi")
        .filter(col("soldejourdb_devise") > 0)
        .select("numero_compte", "dttrtm", "pk_mapressourceemploi")
    )

    map_ressource_emploi = map_ressource_emploi.filter(is_decouvert).select(
        "pk_mapressourceemploi"
    )

    return (
        bfi.join(map_ressource_emploi, ["pk_mapressourceemploi"])
        .transform(
            get_is_during_a_period_for_equipement,
            by="dttrtm",
            name=name,
            partition_date=partition_date,
        )
        .groupBy("numero_compte")
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
            "numero_compte",
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


def build_decouvert(
    autorisation: DataFrame,
    elt_contrat: DataFrame,
    compte: DataFrame,
    bfi: DataFrame,
    map_ressource_emploi: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit la table de gestion des découverts.

    Cette fonction regroupe les données des autorisations souscrites, notifiées, et utilisées,
    en intégrant également les informations des comptes clients.

    Utilisation métier:
    - Suivi global des engagements de type découvert
    - Analyse et reporting des KPIs liés aux autorisations
    - Gestion des risques et des engagements clients
    """

    code_engagement = lit("1317")

    autorisation = (
        autorisation.filter(col("codeengagement") == code_engagement)
        .withColumnRenamed("numeroautorisation", "numero_autorisation")
        .select("numero_autorisation", "dateetat", "etat", "numeroelementinterne")
    )

    compte = (
        compte.withColumnRenamed("numerodecompte", "numero_compte")
        .withColumnRenamed("numerotiers", "numero_tiers")
        .select("numero_compte", "numero_tiers")
    )

    subscribed_autorisation = get_subscribed_autorisation(
        autorisation, elt_contrat, partition_date
    )

    notified_autorisations = get_autorisation_status_from_history(
        autorisation, elt_contrat, partition_date
    )

    utilised_autorisations = get_utilised_autorisation_from_history(
        bfi, map_ressource_emploi, partition_date
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
        "is_notifie_s",
        "is_notifie_s_1",
        "is_notifie_s_2",
        "is_notifie_m",
        "is_notifie_m_1",
        "is_notifie_m_2",
        "is_notifie_t",
        "is_notifie_t_1",
        "is_notifie_y",
        "is_notifie_y_1",
        "is_utilise_s",
        "is_utilise_s_1",
        "is_utilise_s_2",
        "is_utilise_m",
        "is_utilise_m_1",
        "is_utilise_m_2",
        "is_utilise_t",
        "is_utilise_t_1",
        "is_utilise_y",
        "is_utilise_y_1",
    ]

    return (
        subscribed_autorisation.withColumn(
            "numero_compte", substring("numero_autorisation", 1, 11)
        )
        .join(compte, ["numero_compte"], "left")
        .join(notified_autorisations, ["numero_autorisation"], "left")
        .join(utilised_autorisations, ["numero_compte"], "left")
        .na.fill(False, kpis)
        .withColumn("code_engagement", code_engagement)
        .withColumn("is_utilise_s", col("is_notifie_s") & col("is_utilise_s"))
        .withColumn("is_utilise_s_1", col("is_notifie_s_1") & col("is_utilise_s_1"))
        .withColumn("is_utilise_s_2", col("is_notifie_s_2") & col("is_utilise_s_2"))
        .withColumn("is_utilise_m", col("is_notifie_m") & col("is_utilise_m"))
        .withColumn("is_utilise_m_1", col("is_notifie_m_1") & col("is_utilise_m_1"))
        .withColumn("is_utilise_m_2", col("is_notifie_m_2") & col("is_utilise_m_2"))
        .withColumn("is_utilise_t", col("is_notifie_t") & col("is_utilise_t"))
        .withColumn("is_utilise_t_1", col("is_notifie_t_1") & col("is_utilise_t_1"))
        .withColumn("is_utilise_y", col("is_notifie_y") & col("is_utilise_y"))
        .withColumn("is_utilise_y_1", col("is_notifie_y_1") & col("is_utilise_y_1"))
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_autorisation",
            "code_engagement",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
    )
