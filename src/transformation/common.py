from pyspark.sql import Column, DataFrame, Window, SparkSession
from pyspark.sql.functions import (
    add_months,
    coalesce,
    col,
    concat_ws,
    date_add,
    date_format,
    date_trunc,
    datediff,
    dayofweek,
    expr,
    lag,
    last,
    last_day,
    lit,
    lower,
    max,
    md5,
    month,
    months_between,
    quarter,
    to_date,
    trim,
    weekofyear,
    when,
    year,
    expr,
    date_sub,
)


def map_account_balance(
    df: DataFrame,
    column_debit: str,
    column_credit: str,
) -> DataFrame:
    """
    Applique les règles de mapping comptable pour corriger les soldes débiteurs et créditeurs.

    Cette fonction:
    - Gère les différents sens comptables (AA, AP, PP) selon les règles métier
    - Réaffecte les montants entre débit et crédit selon le sens_mapping:
        * AA: Actif-Actif -> Différence entre débit et crédit
        * AP: Actif-Passif -> Montants positifs uniquement
        * PP: Passif-Passif -> Différence entre crédit et débit
    """

    return (
        df.withColumn(
            f"{column_debit}_",
            when(
                col("sens_mapping") == "AA",
                coalesce(col(column_debit), lit(0))
                - coalesce(col(column_credit), lit(0)),
            ).when(
                (col("sens_mapping") == "AP") & (col(column_debit) >= 0),
                col(column_debit),
            ),
        )
        .withColumn(
            f"{column_credit}_",
            when(
                col("sens_mapping") == "PP",
                coalesce(col(column_credit), lit(0))
                - coalesce(col(column_debit), lit(0)),
            ).when(
                (col("sens_mapping") == "AP") & (col(column_credit) >= 0),
                col(column_credit),
            ),
        )
        .withColumn(column_debit, col(f"{column_debit}_"))
        .withColumn(column_credit, col(f"{column_credit}_"))
        .drop(f"{column_debit}_", f"{column_credit}_")
    )


def get_last_recorded_sold_per_client_from(
    df: DataFrame,
    by: str,
    days: int,
    partition_columns: list = ["numerodecompte", "devise"],
    order_column: str = "dttrtm",
) -> DataFrame:
    """
    Récupère le dernier solde enregistré pour un client sur une période donnée en jours.

    Cette fonction:
    - Calcule le solde à J-n pour chaque compte/devise
    - Utilise une fenêtre glissante basée sur la date de traitement
    - Permet un historique paramétrable en nombre de jours

    Utilisation métier:
    - Suivi de l'évolution des soldes clients
    - Analyse des variations quotidiennes
    - Détection des mouvements significatifs
    """

    column_name = f"last_day_{by}" if days == 1 else f"last_{days}_days_{by}"
    window = Window.partitionBy(*partition_columns).orderBy(order_column)

    return df.withColumn(column_name, lag(col(by), offset=days).over(window))


def get_last_recorded_sold_per_client_from_last_month(
    df: DataFrame,
    by: str,
    partition_date: str,
    months: int = 1,
    partition_columns: list = ["numerodecompte", "devise"],
    order_column: str = "dttrtm",
) -> DataFrame:
    """
    Récupère les soldes historiques mensuels des clients.

    Cette fonction:
    - Identifie le dernier solde connu du mois M-n
    - Gère le changement de mois dans le calcul
    - Compare les soldes entre différents mois

    Utilisation métier:
    - Analyse des variations mensuelles
    - Reporting mensuel de l'évolution des comptes
    - Identification des tendances mensuelles
    - Support au calcul des moyennes mensuelles
    """

    column_name = f"last_month_{by}" if months == 1 else f"last_{months}_months_{by}"
    window = Window.partitionBy(*partition_columns).orderBy(order_column)
    return (
        df.withColumn("rank", date_format(order_column, "yyyyMM"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_solde", lag(by).over(window))
        .withColumn(
            "months_between",
            months_between(
                date_trunc("MM", to_date(lit(partition_date))),
                date_trunc("MM", to_date(col(order_column))),
            ),
        )
        .withColumn("prev_months_between", lag("months_between").over(window))
        .withColumn(
            column_name,
            last(
                when(
                    (col("rank") != col("rank_diff"))
                    & (col("prev_months_between") == months),
                    col("prev_solde"),
                ),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop(
            "rank",
            "prev_solde",
            "rank_diff",
            "months_between",
            "prev_months_between",
        )
    )


def get_last_recorded_sold_per_client_from_last_quarter(
    df: DataFrame,
    by: str,
    partition_columns: list = ["numerodecompte", "devise"],
    order_column: str = "dttrtm",
) -> DataFrame:
    """
    Calcule le dernier solde enregistré par client pour le trimestre précédent.

    Cette fonction permet de récupérer, pour chaque client et devise, le dernier solde disponible
    pour le trimestre qui précède celui en cours, en se basant sur les dates de transaction (colonne `dttrtm`).
    Le calcul repose sur le trimestre et l'année de la date de transaction pour identifier les soldes
    pertinents.

    Utilisation métier:
    - Calcul du solde de clôture pour le dernier trimestre afin de produire des rapports financiers trimestriels.
    - Suivi des évolutions de solde au niveau client par trimestre.
    - Aide dans la gestion des clôtures trimestrielles et la préparation des états financiers.
    """

    window = Window.partitionBy(*partition_columns).orderBy(order_column)
    return (
        df.withColumn(
            "rank",
            concat_ws(
                "",
                lit(date_format(order_column, "yyyy")),
                quarter(to_date(col(order_column))),
            ),
        )
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_solde", lag(by).over(window))
        .withColumn(
            f"last_quarter_{by}",
            last(
                when(col("rank") != col("rank_diff"), col("prev_solde")),
                ignorenulls=True,
            ).over(
                Window.partitionBy(*partition_columns)
                .orderBy(order_column)
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .drop("rank", "prev_solde", "rank_diff")
    )


def get_last_recorded_sold_per_client_from_last_year(
    df: DataFrame,
    by: str,
    partition_date: str,
    years: int = 1,
    partition_columns: list = ["numerodecompte", "devise"],
    order_column: str = "dttrtm",
) -> DataFrame:
    """
    Calcule le dernier solde enregistré par client pour l'année précédente (ou n années précédentes).

    Cette fonction permet de déterminer, pour chaque client et devise, le dernier solde enregistré
    pour l'année (ou un nombre d'années donné) qui précède celle en cours. Le calcul se fait en fonction
    des dates de partition spécifiées et du nombre d'années à considérer.

    Utilisation métier:
    - Calcul du solde de clôture annuel pour les arrêtés financiers
    - Suivi des soldes clients dans le cadre de rapports annuels
    - Analyse des tendances à long terme par rapport à des années précédentes
    """

    column_name = f"last_year_{by}" if years == 1 else f"last_{years}_years_{by}"
    window = Window.partitionBy(*partition_columns).orderBy(order_column)
    return (
        df.withColumn("rank", year(order_column))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_solde", lag(by).over(window))
        .withColumn(
            "years_between", year(to_date(lit(partition_date))) - year(order_column)
        )
        .withColumn("prev_years_between", lag("years_between").over(window))
        .withColumn(
            column_name,
            last(
                when(
                    (col("rank") != col("rank_diff"))
                    & (col("prev_years_between") == years),
                    col("prev_solde"),
                ),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop(
            "rank", "prev_solde", "rank_diff", "years_between", "prev_years_between"
        )
    )


def get_last_recorded_sold_per_client_from_last_week(
    df: DataFrame,
    by: str,
    partition_date: str,
    weeks: int = 1,
    partition_columns: list = ["numerodecompte", "devise"],
    order_column: str = "dttrtm",
) -> DataFrame:
    """
    Calcule la valeur enregistrée pour chaque client à la même période la semaine dernière (ou x semaines précédentes).

    Cette fonction permet de récupérer, pour chaque client et devise, la dernière valeur enregistrée
    pour une période correspondant à une ou plusieurs semaines précédentes par rapport à une date donnée.

    Utilisation métier:
    - Suivi hebdomadaire des soldes clients pour les rapports d'activité
    - Analyse des variations des soldes sur une base hebdomadaire
    - Identification des tendances à court terme sur les comptes clients
    """

    column_name = f"last_week_{by}" if weeks == 1 else f"last_{weeks}_weeks_{by}"
    window = Window.partitionBy(*partition_columns).orderBy(order_column)
    return (
        df.withColumn(
            "rank",
            concat_ws(
                "",
                date_format(order_column, "yyyy"),
                weekofyear(to_date(col(order_column))),
            ),
        )
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_solde", lag(by).over(window))
        .withColumn(
            "weeks_between",
            datediff(
                date_trunc("week", to_date(lit(partition_date))),
                date_trunc("week", to_date(col(order_column))),
            )
            / 7,
        )
        .withColumn("prev_weeks_between", lag("weeks_between").over(window))
        .withColumn(
            column_name,
            last(
                when(
                    (col("rank") != col("rank_diff"))
                    & (col("prev_weeks_between") == weeks),
                    col("prev_solde"),
                ),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop(
            "rank", "prev_solde", "rank_diff", "weeks_between", "prev_weeks_between"
        )
    )


def get_last_recorded_sold_per_client_from_same_day_last_year(
    df: DataFrame, by: str, partition_date: str
) -> DataFrame:
    """
    Calcule la valeur enregistrée pour chaque client à la même date l'année précédente.

    Cette fonction permet de récupérer, pour chaque client et devise, la dernière valeur enregistrée
    pour une date correspondant exactement à celle d'un an auparavant.

    Utilisation métier:
    - Comparaison annuelle des soldes clients pour des rapports financiers
    - Suivi des variations interannuelles pour des analyses stratégiques
    - Élaboration de projections basées sur les tendances historiques
    """

    column_name = f"same_day_last_year_{by}"
    window = Window.partitionBy("numerodecompte", "devise").orderBy("dttrtm")
    return df.withColumn(
        column_name,
        last(
            when(
                col("dttrtm") == add_months(to_date(lit(partition_date)), -12),
                col(by),
            ),
            ignorenulls=True,
        ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )


def is_three_months_ago(start_date: Column, older_date: Column) -> Column:
    """
    Détermine si une date se situe dans l'intervalle des 3 derniers mois.

    Cette fonction permet d'identifier si une date est comprise dans une période
    de 3 mois par rapport à une date de référence.
    """
    return (months_between(start_date, older_date) <= 3) & (
        months_between(start_date, older_date) >= 0
    )


def get_last_day_of_week(dt: Column) -> Column:
    """
    Calcule le dernier jour (dimanche) de la semaine pour une date donnée.

    Cette fonction permet d'obtenir la date de fin de semaine, utile pour:
    - Les arrêtés hebdomadaires
    - Les calculs de bornes de période
    - La synchronisation des reportings hebdomadaires
    - L'alignement des cycles de traitement

    Note: En cas de date d'entrée correspondant à un dimanche,
    la fonction retourne cette même date.
    """
    return when(dayofweek(dt) == 1, dt).otherwise(date_add(dt, 8 - dayofweek(dt)))


def get_first_day_of_week(dt: Column) -> Column:
    """
    Calcule le premier jour (lundi) de la semaine pour une date donnée.

    Cette fonction permet d'obtenir la date de début de semaine, essentielle pour:
    - L'initialisation des cycles hebdomadaires
    - La planification des traitements
    - Le démarrage des périodes de reporting
    - La standardisation des analyses hebdomadaires

    Note: La fonction gère automatiquement le décalage nécessaire
    pour obtenir le lundi, quel que soit le jour d'entrée.
    """
    return when(dayofweek(dt) == 1, date_add(dt, -6)).otherwise(
        date_add(dt, 2 - dayofweek(dt))
    )


def get_is_during_a_period_for_equipement(
    df: DataFrame, by: str, name: str, partition_date: str
) -> DataFrame:
    """
    Détermine si un équipement appartient à différentes périodes temporelles en se basant sur une date spécifique.

    Cette fonction:
    - Compare une date de référence (by) avec différentes périodes:
        * Hebdomadaire (s): Même semaine de la même année
        * Semaine -1 (s_1): Semaine précédente de la même année
        * Semaine -2 (s_2): Il y a deux semaines de la même année
        * Mensuel (m): Même mois de la même année
        * Mois -1 (m_1): Mois précédent de la même année
        * Mois -2 (m_2): Il y a deux mois de la même année
        * Trimestriel (t): Même trimestre de la même année
        * Trimestre -1 (t_1): Trimestre précédent de la même année
        * Annuel (y): Même année
        * Année -1 (y_1): Année précédente

    Paramètres:
        df: DataFrame contenant les données
        by: Nom de la colonne contenant la date à comparer
        name: Préfixe pour nommer les colonnes résultantes
        partition_date: Date de référence pour les comparaisons

    Retourne:
        DataFrame enrichi avec des indicateurs booléens pour chaque période
        Format des colonnes: is_{name}_[s|s_1|s_2|m|m_1|m_2|t|t_1|y|y_1]

    Utilisation métier:
    - Suivi de la présence des équipements dans le temps
    - Analyse des cohortes d'équipements
    - Production d'indicateurs de stock aux différentes périodes
    - Support au calcul des taux d'attrition
    """

    dt = to_date(lit(partition_date))
    a_1_week_ago = dt - expr("INTERVAL 1 WEEK")
    a_2_weeks_ago = dt - expr("INTERVAL 2 WEEK")
    a_1_month_ago = add_months(dt, -1)
    a_2_months_ago = add_months(dt, -2)
    a_quarter_ago = add_months(dt, -3)
    a_year_ago = add_months(dt, -12)

    def same_year_as(other_date):
        return year(col(by)) == year(other_date)

    return (
        df.withColumn(
            f"is_{name}_s",
            same_year_as(dt) & (weekofyear(col(by)) == weekofyear(dt)),
        )
        .withColumn(
            f"is_{name}_s_1",
            same_year_as(a_1_week_ago)
            & (weekofyear(col(by)) == weekofyear(a_1_week_ago)),
        )
        .withColumn(
            f"is_{name}_s_2",
            same_year_as(a_2_weeks_ago)
            & (weekofyear(col(by)) == weekofyear(a_2_weeks_ago)),
        )
        .withColumn(
            f"is_{name}_m",
            same_year_as(dt) & (month(col(by)) == month(dt)),
        )
        .withColumn(
            f"is_{name}_m_1",
            same_year_as(a_1_month_ago) & (month(col(by)) == month(a_1_month_ago)),
        )
        .withColumn(
            f"is_{name}_m_2",
            same_year_as(a_2_months_ago) & (month(col(by)) == month(a_2_months_ago)),
        )
        .withColumn(
            f"is_{name}_t",
            same_year_as(dt) & (quarter(col(by)) == quarter(dt)),
        )
        .withColumn(
            f"is_{name}_t_1",
            same_year_as(a_quarter_ago)
            & (quarter(col(by)) == quarter(a_quarter_ago)),
        )
        .withColumn(f"is_{name}_y", same_year_as(dt))
        .withColumn(f"is_{name}_y_1", same_year_as(a_year_ago))
    )


def get_status_relative_to_last_day_of_period(
    df: DataFrame, by: str, partition_date: str, name: str
) -> DataFrame:
    """
    Détermine le statut d'un élément par rapport aux derniers jours de différentes périodes.

    Cette fonction calcule des indicateurs booléens pour différentes périodes:
    - Hebdomadaire:
        * s: jusqu'à aujourd'hui
        * s_1: jusqu'au dernier jour de la semaine précédente
        * s_2: jusqu'au dernier jour d'il y a 2 semaines
    - Mensuel (m, m_1, m_2): dernier jour de chaque mois
    - Trimestriel (t, t_1): dernier jour du trimestre
    - Annuel (y, y_1): dernier jour de l'année

    Utilisation:
    - Suivi des statuts aux dates clés
    - Support aux calculs réglementaires
    - Historisation des états aux dates de référence
    """
    dt = to_date(lit(partition_date))
    last_day_1_week_ago = get_last_day_of_week(dt - expr("INTERVAL 1 WEEK"))
    last_day_2_weeks_ago = get_last_day_of_week(dt - expr("INTERVAL 2 WEEK"))
    last_day_1_month_ago = last_day(add_months(dt, -1))
    last_day_2_months_ago = last_day(add_months(dt, -2))
    last_day_quarter_ago = date_add(date_trunc("quarter", dt), -1)
    last_day_year_ago = date_add(date_trunc("year", dt), -1)
    return (
        df.withColumn(f"is_{name}_s", col(by) <= dt)
        .withColumn(f"is_{name}_s_1", col(by) <= last_day_1_week_ago)
        .withColumn(f"is_{name}_s_2", col(by) <= last_day_2_weeks_ago)
        .withColumn(f"is_{name}_m", col(by) <= dt)
        .withColumn(f"is_{name}_m_1", col(by) <= last_day_1_month_ago)
        .withColumn(f"is_{name}_m_2", col(by) <= last_day_2_months_ago)
        .withColumn(f"is_{name}_t", col(by) <= dt)
        .withColumn(f"is_{name}_t_1", col(by) <= last_day_quarter_ago)
        .withColumn(f"is_{name}_y", col(by) <= dt)
        .withColumn(f"is_{name}_y_1", col(by) <= last_day_year_ago)
    )


def create_primary_key(*columns: str) -> Column:
    """
    Génère une clé primaire unique à partir d'une combinaison de colonnes.

    Cette fonction:
    - Normalise les valeurs (minuscules, suppression des espaces)
    - Crée une concaténation unique des valeurs
    - Génère un hash MD5 pour garantir l'unicité

    Utilisation:
    - Identification unique des enregistrements
    - Détection des doublons
    - Traçabilité des données
    - Support à la réconciliation
    """
    lowercase_columns = [
        when(
            col(column).isNotNull() & (trim(col(column)) != ""),
            trim(lower(col(column).cast("string"))),
        )
        for column in columns
    ]

    primary_string = concat_ws("|#", *lowercase_columns)

    return when(primary_string != "", md5(primary_string))


def filter_on_max_partition_date(df: DataFrame) -> DataFrame:
    """
    Filtre les données pour ne conserver que la partition la plus récente.

    Cette fonction:
    - Identifie la date de partition maximum
    - Ne conserve que les données de cette partition

    Utilisation:
    - Garantie de travailler sur les données les plus récentes
    - Nettoyage automatique des données historiques
    - Optimisation des performances de traitement
    """

    max_partition_date = df.select(
        max(col("partitiondate")).alias("max_partitiondate")
    ).first()

    max_partition_column = (
        lit(max_partition_date["max_partitiondate"])
        if max_partition_date
        else lit(None)
    )

    return df.filter(col("partitiondate") == max_partition_column)


def build_day_holiday(
    df: DataFrame, joursferie: DataFrame, partitiondate: str, nm: int
) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    input_jour = [
        [
            1,
        ],
        [
            2,
        ],
        [
            3,
        ],
        [
            4,
        ],
        [
            5,
        ],
        [
            6,
        ],
        [
            7,
        ],
        [
            0,
        ],
    ]
    schema = [
        "number_jour",
    ]
    nb_date = spark.createDataFrame(input_jour, schema)

    df = df.withColumn("key", lit(1)).withColumn(
        partitiondate, to_date(col(partitiondate), "yyyy-MM-dd")
    )
    nb_date = nb_date.withColumn("key", lit(1)).withColumn(
        "number_jour", col("number_jour").cast("int")
    )

    df1 = df.join(nb_date, ["key"], "left").withColumn(
        partitiondate, date_sub(col(partitiondate), col("number_jour") + lit(nm))
    )

    clause_join_date = df1[partitiondate] == joursferie["jourferie"]

    df1 = df1.join(joursferie, clause_join_date, "left").filter(
        (col("jourferie").isNull()) & (~dayofweek(col(partitiondate)).isin("7", "1"))
    )

    list_of_columns = df.drop(col(partitiondate)).drop(col("key")).columns
    df1 = df1.drop(col("key"))
    df1 = df1.groupBy(list_of_columns).agg(
        max(col(partitiondate)).alias(partitiondate)
    )

    return df1
