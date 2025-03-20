from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    add_months,
    col,
    date_add,
    date_trunc,
    expr,
    floor,
    lit,
    month,
    quarter,
    round,
    sum,
    to_date,
    trim,
    when,
)

from src.transformation.common import (
    filter_on_max_partition_date,
    get_first_day_of_week,
    get_is_during_a_period_for_equipement,
    get_last_recorded_sold_per_client_from_last_month,
    get_last_recorded_sold_per_client_from_last_quarter,
    get_last_recorded_sold_per_client_from_last_week,
    get_last_recorded_sold_per_client_from_last_year,
)


def get_active_conquest_from_history(
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    compte: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Calcule les agrégats financiers par client à partir des historiques de mouvements comptables.

    Cette fonction permet d'agréger et de transformer des données financières provenant de
    différentes sources, telles que les mouvements comptables, les taux de change BAM et les informations
    sur les comptes clients. Elle calcule les montants cumulés sur différentes périodes de temps
    (semaine, mois, trimestre, année) pour chaque client.

    Utilisation métier :
    - Génération de rapports financiers pour le suivi des transactions des clients.
    - Analyse des tendances financières sur plusieurs périodes pour des besoins de reporting.
    - Soutien à la prise de décision grâce à la consolidation des données comptables et financières.

    Détails des étapes :
    1. Préparation et normalisation des données des mouvements comptables (noms de colonnes, sélection de champs clés).
    2. Traitement des données des taux de change BAM pour les aligner sur les mouvements comptables.
    3. Détermination des premières dates de différentes périodes (semaine, mois, trimestre, année) pour le partitionnement des données.
    4. Calcul des montants cumulés par devise en fonction des périodes choisies.
    5. Agrégation finale des données au niveau des clients pour obtenir un aperçu synthétique.
    """

    mouvement_comptable = mouvement_comptable.withColumnRenamed(
        "numerodecompte", "numero_compte"
    ).select(
        "numero_compte",
        "datetraitement",
        "devise",
        "sens",
        "montantecriture",
    )

    taux_change_bam = (
        taux_change_bam.withColumn("devise", trim(col("cd_dev_oper")))
        .withColumnRenamed("date_cours", "datetraitement")
        .select(
            "devise",
            "midbam",
            "datetraitement",
        )
    )

    dt = to_date(lit(partition_date))
    first_day_of_week = get_first_day_of_week(dt)
    first_day_of_last_week = get_first_day_of_week(dt - expr("INTERVAL 1 WEEK"))
    first_day_of_month = date_trunc("month", dt)
    first_day_of_last_month = date_trunc("month", add_months(dt, -1))
    first_day_of_last_2_months = date_trunc("month", add_months(dt, -2))
    first_day_of_quarter = date_trunc("quarter", dt)
    first_day_of_last_quarter = date_trunc(
        "quarter", date_add(first_day_of_quarter, -1)
    )
    first_day_of_year = date_trunc("year", dt)
    first_day_of_last_quarter_of_last_year = date_trunc(
        "quarter", add_months(dt, -3 * (floor((month(dt) - 1) / 3) + 1))
    )

    def is_later_than(column: Column) -> Column:
        return when(col("datetraitement") >= column, col("montant_mad")).otherwise(
            0.0
        )

    sum_per_accout = (
        mouvement_comptable.withColumn("devise", trim("devise"))
        .filter(col("sens") == "C")
        .join(
            taux_change_bam,
            ["devise", "datetraitement"],
            how="left",
        )
        .withColumn(
            "midbam",
            when(col("devise") == lit("000C"), 1)
            .otherwise(col("midbam"))
            .cast("double"),
        )
        .withColumn("montantecriture", col("montantecriture").cast("double") / 100)
        .withColumn("montant_mad", col("midbam") * col("montantecriture"))
        .groupBy("numero_compte")
        .agg(
            sum(is_later_than(first_day_of_week)).alias("sum_s"),
            sum(is_later_than(first_day_of_last_week)).alias("sum_s_1"),
            sum(is_later_than(first_day_of_month)).alias("sum_m"),
            sum(is_later_than(first_day_of_last_month)).alias("sum_m_1"),
            sum(is_later_than(first_day_of_last_2_months)).alias("sum_m_2"),
            sum(is_later_than(first_day_of_quarter)).alias("sum_t"),
            sum(is_later_than(first_day_of_last_quarter)).alias("sum_t_1"),
            sum(is_later_than(first_day_of_year)).alias("sum_y"),
            sum(is_later_than(first_day_of_last_quarter_of_last_year)).alias(
                "sum_y_1"
            ),
        )
        .select(
            "numero_compte",
            "sum_s",
            "sum_s_1",
            "sum_m",
            "sum_m_1",
            "sum_m_2",
            "sum_t",
            "sum_t_1",
            "sum_y",
            "sum_y_1",
        )
    )

    compte = (
        compte.withColumnRenamed("numerodecompte", "numero_compte")
        .withColumnRenamed("numerotiers", "numero_tiers")
        .select("numero_compte", "numero_tiers")
    )

    return (
        compte.join(sum_per_accout, ["numero_compte"], "left")
        .groupBy("numero_tiers")
        .agg(
            sum(col("sum_s")).alias("sum_s"),
            sum(col("sum_s_1")).alias("sum_s_1"),
            sum(col("sum_m")).alias("sum_m"),
            sum(col("sum_m_1")).alias("sum_m_1"),
            sum(col("sum_m_2")).alias("sum_m_2"),
            sum(col("sum_t")).alias("sum_t"),
            sum(col("sum_t_1")).alias("sum_t_1"),
            sum(col("sum_y")).alias("sum_y"),
            sum(col("sum_y_1")).alias("sum_y_1"),
        )
        .select(
            "numero_tiers",
            "sum_s",
            "sum_s_1",
            "sum_m",
            "sum_m_1",
            "sum_m_2",
            "sum_t",
            "sum_t_1",
            "sum_y",
            "sum_y_1",
        )
    )


def get_actif_client_from_history(
    activite_client: DataFrame, partition_date: str
) -> DataFrame:
    """
    Identifie les clients actifs sur des périodes passées.

    Cette fonction détermine l'activité des clients sur des périodes variées
    (semaine, mois, trimestre, année) à partir des données d'historique.

    Utilisation métier:
    - Suivi de l'activité client
    - Analyse des segments de clients actifs
    - Gestion de la rétention client
    """

    return (
        activite_client.select("numerotiers", "is_actif", "partitiondate")
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="is_actif",
            partition_date=partition_date,
            weeks=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="is_actif",
            partition_date=partition_date,
            weeks=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="is_actif",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="is_actif",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="is_actif",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="is_actif",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .withColumnRenamed("numerotiers", "numero_tiers")
        .withColumn("is_actif_s", col("is_actif"))
        .withColumnRenamed("last_week_is_actif", "is_actif_s_1")
        .withColumnRenamed("last_2_weeks_is_actif", "is_actif_s_2")
        .withColumnRenamed("last_month_is_actif", "is_actif_m_1")
        .withColumnRenamed("last_2_months_is_actif", "is_actif_m_2")
        .withColumnRenamed("last_quarter_is_actif", "is_actif_t_1")
        .withColumnRenamed("last_year_is_actif", "is_actif_y_1")
        .transform(filter_on_max_partition_date)
        .select(
            "numero_tiers",
            "is_actif_s",
            "is_actif_s_1",
            "is_actif_s_2",
            "is_actif_m_1",
            "is_actif_m_2",
            "is_actif_t_1",
            "is_actif_y_1",
        )
    )


def get_inactif_client_from_history(
    activite_client: DataFrame, partition_date: str
) -> DataFrame:
    """
    Identifie les clients inactifs sur des périodes passées.

    Cette fonction détecte les périodes d'inactivité des clients sur des périodes données,
    en se basant sur les données historiques.

    Utilisation métier:
    - Suivi des clients à risque d'attrition
    - Analyse des segments de clients inactifs
    - Développement de campagnes de réengagement
    """

    return (
        activite_client.select("numerotiers", "is_inactif", "partitiondate")
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="is_inactif",
            partition_date=partition_date,
            weeks=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="is_inactif",
            partition_date=partition_date,
            weeks=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="is_inactif",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="is_inactif",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="is_inactif",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="is_inactif",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .withColumnRenamed("numerotiers", "numero_tiers")
        .withColumn("is_inactif_s", col("is_inactif"))
        .withColumnRenamed("last_week_is_inactif", "is_inactif_s_1")
        .withColumnRenamed("last_2_weeks_is_inactif", "is_inactif_s_2")
        .withColumnRenamed("last_month_is_inactif", "is_inactif_m_1")
        .withColumnRenamed("last_2_months_is_inactif", "is_inactif_m_2")
        .withColumnRenamed("last_quarter_is_inactif", "is_inactif_t_1")
        .withColumnRenamed("last_year_is_inactif", "is_inactif_y_1")
        .transform(filter_on_max_partition_date)
        .select(
            "numero_tiers",
            "is_inactif_s",
            "is_inactif_s_1",
            "is_inactif_s_2",
            "is_inactif_m_1",
            "is_inactif_m_2",
            "is_inactif_t_1",
            "is_inactif_y_1",
        )
    )


def get_score_pallier_from_history(
    score_intensite_bancaire: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Calcule les scores d'intensité bancaire des clients par période.

    Cette fonction analyse l'évolution des scores (équipement, transaction, activité) des clients
    sur plusieurs périodes (mois, trimestre, année).

    Utilisation métier:
    - Évaluation de la performance bancaire des clients
    - Identification des opportunités de vente croisée et montée en gamme
    - Suivi des indicateurs clés liés aux clients
    """

    score_intensite_bancaire = score_intensite_bancaire.select(
        "numerotiers",
        "score_equipement",
        "score_mycdm",
        "score_transaction",
        "score_flag_2000_m",
        "reco_concat",
        "niveau_activite",
        "partitiondate",
    )

    current_day_doesnt_exist = score_intensite_bancaire.filter(
        col("partitiondate") == lit(partition_date)
    ).isEmpty()

    current_day_only = (
        score_intensite_bancaire.select("numerotiers")
        .drop_duplicates()
        .withColumn("score_equipement", lit(None))
        .withColumn("score_mycdm", lit(None))
        .withColumn("score_transaction", lit(None))
        .withColumn("score_flag_2000_m", lit(None))
        .withColumn("reco_concat", lit(None))
        .withColumn("niveau_activite", lit(None))
        .withColumn("partitiondate", lit(partition_date))
    )

    score_intensite_bancaire_with_current_day = (
        score_intensite_bancaire.unionByName(current_day_only)
        if current_day_doesnt_exist
        else score_intensite_bancaire
    )

    return (
        score_intensite_bancaire_with_current_day.transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_equipement",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_equipement",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="score_equipement",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="score_equipement",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_mycdm",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_mycdm",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="score_mycdm",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="score_mycdm",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_transaction",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_transaction",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="score_transaction",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="score_transaction",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_flag_2000_m",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="score_flag_2000_M",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="score_flag_2000_M",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="score_flag_2000_M",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="reco_concat",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="reco_concat",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="reco_concat",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="reco_concat",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="niveau_activite",
            partition_date=partition_date,
            months=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="niveau_activite",
            partition_date=partition_date,
            months=2,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="niveau_activite",
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="niveau_activite",
            partition_date=partition_date,
            years=1,
            partition_columns=["numerotiers"],
            order_column="partitiondate",
        )
        .withColumnRenamed("numerotiers", "numero_tiers")
        .withColumnRenamed("last_month_score_flag_2000_m", "score_pallier_1_m_1")
        .withColumnRenamed("last_2_months_score_flag_2000_m", "score_pallier_1_m_2")
        .withColumnRenamed("last_quarter_score_flag_2000_m", "score_pallier_1_t_1")
        .withColumnRenamed("last_year_score_flag_2000_m", "score_pallier_1_y_1")
        .withColumnRenamed("last_month_score_transaction", "score_pallier_2_m_1")
        .withColumnRenamed("last_2_months_score_transaction", "score_pallier_2_m_2")
        .withColumnRenamed("last_quarter_score_transaction", "score_pallier_2_t_1")
        .withColumnRenamed("last_year_score_transaction", "score_pallier_2_y_1")
        .withColumnRenamed("last_month_score_mycdm", "score_pallier_3_m_1")
        .withColumnRenamed("last_2_months_score_mycdm", "score_pallier_3_m_2")
        .withColumnRenamed("last_quarter_score_mycdm", "score_pallier_3_t_1")
        .withColumnRenamed("last_year_score_mycdm", "score_pallier_3_y_1")
        .withColumnRenamed("last_month_score_equipement", "score_pallier_4_m_1")
        .withColumnRenamed("last_2_months_score_equipement", "score_pallier_4_m_2")
        .withColumnRenamed("last_quarter_score_equipement", "score_pallier_4_t_1")
        .withColumnRenamed("last_year_score_equipement", "score_pallier_4_y_1")
        .withColumnRenamed("last_month_reco_concat", "reco_concat_m_1")
        .withColumnRenamed("last_2_months_reco_concat", "reco_concat_m_2")
        .withColumnRenamed("last_quarter_reco_concat", "reco_concat_t_1")
        .withColumnRenamed("last_year_reco_concat", "reco_concat_y_1")
        .withColumnRenamed("last_month_niveau_activite", "niveau_activite_m_1")
        .withColumnRenamed("last_2_months_niveau_activite", "niveau_activite_m_2")
        .withColumnRenamed("last_quarter_niveau_activite", "niveau_activite_t_1")
        .withColumnRenamed("last_year_niveau_activite", "niveau_activite_y_1")
        .transform(filter_on_max_partition_date)
        .select(
            "numero_tiers",
            "score_pallier_1_m_1",
            "score_pallier_1_m_2",
            "score_pallier_1_t_1",
            "score_pallier_1_y_1",
            "score_pallier_2_m_1",
            "score_pallier_2_m_2",
            "score_pallier_2_t_1",
            "score_pallier_2_y_1",
            "score_pallier_3_m_1",
            "score_pallier_3_m_2",
            "score_pallier_3_t_1",
            "score_pallier_3_y_1",
            "score_pallier_4_m_1",
            "score_pallier_4_m_2",
            "score_pallier_4_t_1",
            "score_pallier_4_y_1",
            "niveau_activite_m_1",
            "niveau_activite_m_2",
            "niveau_activite_t_1",
            "niveau_activite_y_1",
            "reco_concat_m_1",
            "reco_concat_m_2",
            "reco_concat_t_1",
            "reco_concat_y_1",
        )
    )


def build_kpi_client(
    tier: DataFrame,
    compte: DataFrame,
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    activite_client: DataFrame,
    score_intensite_bancaire: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue consolidée des KPI clients.

    Cette fonction regroupe les indicateurs clés liés aux conquêtes, à l'activité,
    à l'inactivité et aux scores des clients, en fonction des données disponibles.

    Utilisation métier:
    - Suivi global des performances des clients
    - Analyse des segments actifs et inactifs
    - Rapport et tableaux de bord sur les indicateurs clés
    """

    tier = tier.withColumnRenamed("numerotiers", "numero_tiers").select(
        "numero_tiers", "date_ouverture_premier_compte"
    )

    kpis = [
        "is_conquete_s",
        "is_conquete_s_1",
        "is_conquete_m",
        "is_conquete_m_1",
        "is_conquete_m_2",
        "is_conquete_t",
        "is_conquete_t_1",
        "is_conquete_y",
        "is_conquete_y_1",
        "is_conquete_active_s",
        "is_conquete_active_s_1",
        "is_conquete_active_m",
        "is_conquete_active_m_1",
        "is_conquete_active_m_2",
        "is_conquete_active_t",
        "is_conquete_active_t_1",
        "is_conquete_active_y",
        "is_conquete_active_y_1",
        "is_conquete_encours_s",
        "is_conquete_encours_s_1",
        "is_conquete_encours_m",
        "is_conquete_encours_m_1",
        "is_conquete_encours_t",
        "is_conquete_encours_y",
        "is_conquete_encours_y_1",
        "is_conquete_inactive_s",
        "is_conquete_inactive_s_1",
        "is_conquete_inactive_m",
        "is_conquete_inactive_m_1",
        "is_conquete_inactive_t",
        "is_conquete_inactive_y",
        "is_conquete_inactive_y_1",
    ]

    active_conquest = get_active_conquest_from_history(
        mouvement_comptable, taux_change_bam, compte, partition_date
    )

    actif_client = get_actif_client_from_history(activite_client, partition_date)

    inactif_client = get_inactif_client_from_history(activite_client, partition_date)

    score_pallier = get_score_pallier_from_history(
        score_intensite_bancaire, partition_date
    )

    return (
        tier.transform(
            get_is_during_a_period_for_equipement,
            by="date_ouverture_premier_compte",
            name="conquete",
            partition_date=partition_date,
        )
        .withColumn(
            f"is_conquete_y_1",
            col("is_conquete_y_1")
            & (quarter(col("date_ouverture_premier_compte")) == 4),
        )
        .join(active_conquest, ["numero_tiers"], "left")
        .join(actif_client, ["numero_tiers"], "left")
        .join(inactif_client, ["numero_tiers"], "left")
        .join(score_pallier, ["numero_tiers"], "left")
        .na.fill(
            0,
            [
                "sum_s",
                "sum_s_1",
                "sum_m",
                "sum_m_1",
                "sum_m_2",
                "sum_t",
                "sum_t_1",
                "sum_y",
                "sum_y_1",
            ],
        )
        .na.fill(
            False,
            [
                "is_conquete_s",
                "is_conquete_s_1",
                "is_conquete_m",
                "is_conquete_m_1",
                "is_conquete_m_2",
                "is_conquete_t",
                "is_conquete_t_1",
                "is_conquete_y",
                "is_conquete_y_1",
            ],
        )
        .withColumn(
            "is_conquete_active_s", col("is_conquete_s") & (col("sum_s") >= 2000.0)
        )
        .withColumn(
            "is_conquete_active_s_1",
            col("is_conquete_s_1") & (col("sum_s_1") >= 2000.0),
        )
        .withColumn(
            "is_conquete_active_m", col("is_conquete_m") & (col("sum_m") >= 2000.0)
        )
        .withColumn(
            "is_conquete_active_m_1",
            col("is_conquete_m_1") & (col("sum_m_1") >= 2000.0),
        )
        .withColumn(
            "is_conquete_active_m_2",
            col("is_conquete_m_2") & (col("sum_m_2") >= 2000.0),
        )
        .withColumn(
            "is_conquete_active_t", col("is_conquete_t") & (col("sum_t") >= 2000.0)
        )
        .withColumn(
            "is_conquete_active_t_1",
            col("is_conquete_t_1") & (col("sum_t_1") >= 2000.0),
        )
        .withColumn(
            "is_conquete_active_y", col("is_conquete_y") & (col("sum_y") >= 2000.0)
        )
        .withColumn(
            "is_conquete_active_y_1",
            col("is_conquete_y_1") & (col("sum_y_1") >= 2000.0),
        )
        .withColumn(
            "is_conquete_encours_s", col("is_conquete_s") & (col("sum_s") < 2000.0)
        )
        .withColumn(
            "is_conquete_encours_s_1",
            col("is_conquete_s_1") & (col("sum_s_1") < 2000.0),
        )
        .withColumn(
            "is_conquete_encours_m", col("is_conquete_m") & (col("sum_m") < 2000.0)
        )
        .withColumn(
            "is_conquete_encours_m_1",
            col("is_conquete_m_1") & (col("sum_m_1") < 2000.0),
        )
        .withColumn(
            "is_conquete_encours_t", col("is_conquete_t") & (col("sum_t") < 2000.0)
        )
        .withColumn(
            "is_conquete_encours_y", col("is_conquete_y") & (col("sum_y") < 2000.0)
        )
        .withColumn(
            "is_conquete_encours_y_1",
            col("is_conquete_y_1") & (col("sum_y_1") < 2000.0),
        )
        .withColumn("somme_montant", round(col("sum_y"), 2))
        .withColumn(
            "is_conquete_inactive_s",
            col("is_conquete_active_s") & (col("is_inactif_s") == 1),
        )
        .withColumn(
            "is_conquete_inactive_s_1",
            col("is_conquete_active_s_1") & (col("is_inactif_s_1") == 1),
        )
        .withColumn(
            "is_conquete_inactive_m",
            col("is_conquete_active_m") & (col("is_inactif_s") == 1),
        )
        .withColumn(
            "is_conquete_inactive_m_1",
            col("is_conquete_active_m_1") & (col("is_inactif_m_1") == 1),
        )
        .withColumn(
            "is_conquete_inactive_t",
            col("is_conquete_active_t") & (col("is_inactif_s") == 1),
        )
        .withColumn(
            "is_conquete_inactive_y",
            col("is_conquete_active_y") & (col("is_inactif_s") == 1),
        )
        .withColumn(
            "is_conquete_inactive_y_1",
            col("is_conquete_active_y_1") & (col("is_inactif_s") == 1),
        )
        .na.fill(False, kpis)
        .select(
            "numero_tiers",
            "somme_montant",
            "is_actif_s",
            "is_actif_s_1",
            "is_actif_s_2",
            "is_actif_m_1",
            "is_actif_m_2",
            "is_actif_t_1",
            "is_actif_y_1",
            "is_inactif_s",
            "is_inactif_s_1",
            "is_inactif_s_2",
            "is_inactif_m_1",
            "is_inactif_m_2",
            "is_inactif_t_1",
            "is_inactif_y_1",
            "score_pallier_1_m_1",
            "score_pallier_1_m_2",
            "score_pallier_1_t_1",
            "score_pallier_1_y_1",
            "score_pallier_2_m_1",
            "score_pallier_2_m_2",
            "score_pallier_2_t_1",
            "score_pallier_2_y_1",
            "score_pallier_3_m_1",
            "score_pallier_3_m_2",
            "score_pallier_3_t_1",
            "score_pallier_3_y_1",
            "score_pallier_4_m_1",
            "score_pallier_4_m_2",
            "score_pallier_4_t_1",
            "score_pallier_4_y_1",
            "niveau_activite_m_1",
            "niveau_activite_m_2",
            "niveau_activite_t_1",
            "niveau_activite_y_1",
            "reco_concat_m_1",
            "reco_concat_m_2",
            "reco_concat_t_1",
            "reco_concat_y_1",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
    )
