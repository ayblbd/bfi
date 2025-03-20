from functools import reduce
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    add_months,
    col,
    date_add,
    date_trunc,
    expr,
    last_day,
    lit,
    max,
    month,
    quarter,
    to_date,
    weekofyear,
    year,
)
from pyspark.sql.utils import AnalysisException


def load_from_assurance(
    path: str,
    file_name: str,
    partition_date: str,
    shuffle_partitions: str = None,
    encoding: str = "utf-8",
):
    """
    Charge un fichier texte spécifique depuis un chemin donné et une date de partition.

    Cette fonction permet de lire des fichiers texte encodés provenant d'un système
    d'assurance pour une date et un format de fichier précis. Il est possible
    de répartir les données sur plusieurs partitions Spark.
    """
    spark = SparkSession.builder.getOrCreate()

    df = (
        spark.read.format("text")
        .option("encoding", encoding)
        .load(
            f"{path}/{partition_date}/{file_name}_{partition_date.replace('-', '')}_*.txt"
        )
    )

    if shuffle_partitions:
        return df.repartition(int(shuffle_partitions))
    return df


def load_table_from(
    path: str,
    filter_date: str,
    shuffle_partitions: str = None,
    partition_column: str = "partitiondate",
) -> DataFrame:
    """
    Charge une table Parquet depuis un chemin spécifique en appliquant un filtre sur une date.

    Cette fonction lit une table stockée au format Parquet et permet de filtrer les
    données sur une colonne représentant une date de partition. La fonction peut
    également répartir les données sur plusieurs partitions Spark.
    """

    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet(path).filter(col(partition_column) == lit(filter_date))
    if shuffle_partitions:
        return df.repartition(int(shuffle_partitions))
    return df


def get_dates_from_n_months_ago(
    df: DataFrame, partition_date: str, months: int = 1, full_period: bool = False
) -> DataFrame:
    """
    Sélectionne les dates correspondant à un mois donné avant une date de référence.

    Cette fonction filtre les dates d'une table pour obtenir celles qui appartiennent
    au même mois et année qu'une date calculée n mois avant la date de référence.
    Elle peut retourner soit toutes les dates du mois, soit uniquement la dernière.
    """
    previous_date = add_months(to_date(lit(partition_date)), -months)
    filtered_df = (
        df.filter(
            (col("partitiondate") <= to_date(lit(partition_date)))
            & (year(col("partitiondate")) == year(previous_date))
            & (month(col("partitiondate")) == month(previous_date))
        )
        .select("partitiondate")
        .drop_duplicates()
    )

    if full_period:
        return filtered_df

    return filtered_df.select(max(col("partitiondate")).alias("partitiondate"))


def get_n_days_ago(df: DataFrame, partition_date: str, days: int = 1) -> DataFrame:
    """
    Récupère les dates correspondant aux derniers jours avant une date donnée.

    Cette fonction retourne une liste des dates les plus récentes, précédant ou
    correspondant à une date de référence, pour un nombre donné de jours.
    """
    return (
        df.select(col("partitiondate"))
        .filter(col("partitiondate") <= to_date(lit(partition_date)))
        .drop_duplicates()
        .orderBy(col("partitiondate").desc())
        .limit(days + 1)
    )


def get_dates_from_n_weeks_ago(
    df: DataFrame, partition_date: str, weeks: int = 1, full_period: bool = False
) -> DataFrame:
    """
    Sélectionne les dates correspondant à une ou plusieurs semaines avant une date de référence.

    Cette fonction extrait les dates d'une table pour identifier celles situées
    dans la même semaine qu'une date calculée n semaines avant la date de référence.
    Elle peut retourner toutes les dates de la semaine ou seulement la dernière.
    """
    previous_date = to_date(lit(partition_date)) - expr(f"INTERVAL {weeks} WEEK")
    filtered_df = (
        df.filter(
            (col("partitiondate") <= to_date(lit(partition_date)))
            & (year(col("partitiondate")) == year(previous_date))
            & (weekofyear(col("partitiondate")) == weekofyear(previous_date))
        )
        .select("partitiondate")
        .drop_duplicates()
    )

    if full_period:
        return filtered_df

    return filtered_df.select(max(col("partitiondate")).alias("partitiondate"))


def get_dates_from_n_quarters_ago(
    df: DataFrame, partition_date: str, quarters: int = 1, full_period: bool = False
) -> DataFrame:
    """
    Sélectionne les dates correspondant à un ou plusieurs trimestres avant une date de référence.

    Cette fonction permet de filtrer les dates appartenant au même trimestre
    qu'une date calculée n trimestres avant la date de référence. Elle peut
    retourner toutes les dates du trimestre ou uniquement la dernière.
    """
    n_months_ago = 3 * quarters
    previous_date = add_months(to_date(lit(partition_date)), -n_months_ago)
    filtered_df = (
        df.filter(
            (col("partitiondate") <= to_date(lit(partition_date)))
            & (year(col("partitiondate")) == year(previous_date))
            & (quarter(col("partitiondate")) == quarter(previous_date))
        )
        .select("partitiondate")
        .drop_duplicates()
    )

    if full_period:
        return filtered_df

    return filtered_df.select(max(col("partitiondate")).alias("partitiondate"))


def get_dates_from_n_years_ago(
    df: DataFrame, partition_date: str, years: int = 1, full_period: bool = False
) -> DataFrame:
    """
    Sélectionne les dates correspondant à une ou plusieurs années avant une date de référence.

    Cette fonction identifie les dates d'une table appartenant à la même année
    qu'une date calculée n années avant la date de référence. Elle peut
    retourner toutes les dates de l'année ou seulement la dernière.
    """
    previous_date = to_date(lit(partition_date)) - expr(f"INTERVAL {years} YEAR")
    filtered_df = (
        df.filter(
            (col("partitiondate") <= to_date(lit(partition_date)))
            & (year(col("partitiondate")) == year(previous_date))
        )
        .select("partitiondate")
        .drop_duplicates()
    )

    if full_period:
        return filtered_df

    return filtered_df.select(max(col("partitiondate")).alias("partitiondate"))


def get_same_day_from_n_years_ago(
    df: DataFrame, partition_date: str, years: int = 1
) -> DataFrame:
    """
    Récupère la date exacte correspondant au même jour d'une année antérieure.

    Cette fonction retourne la date équivalente au même jour et mois d'une date donnée,
    mais située n années dans le passé.
    """
    n_months_ago = -12 * years
    return (
        df.filter(
            col("partitiondate")
            == add_months(to_date(lit(partition_date)), n_months_ago)
        )
        .select("partitiondate")
        .drop_duplicates()
    )


def load_table_history_from(
    path: str,
    partition_date: str,
    days: Optional[int] = None,
    weeks: Optional[int] = None,
    quarters: Optional[int] = None,
    months: Optional[int] = None,
    years: Optional[int] = None,
    year_to_year: Optional[int] = None,
    full_period: bool = False,
    shuffle_partitions: str = None,
) -> DataFrame:
    """
    Charge l'historique d'une table Parquet en filtrant par des périodes spécifiques.

    Paramètres :
    - path : Chemin complet du fichier ou du répertoire contenant les données Parquet.
    - partition_date : Date de référence à partir de laquelle les filtres temporels seront appliqués.
    - days, weeks, quarters, months, years, year_to_year : Durées à inclure dans le filtre.
    - full_period : Si activé, renvoie toutes les dates dans la période définie ; sinon, seulement la dernière.
    - shuffle_partitions : Nombre de partitions Spark après répartition.

    Utilisation :
    - Chargement et traitement des données de fichiers historiques.
    - Amélioration des performances en réduisant ou augmentant le nombre de partitions Spark.
    - Exemple :
        df = load_table_history_from(
            "/data/transactions",
            "2023-12-31",
            months=3,
            shuffle_partitions=200
        )
    """
    spark = SparkSession.builder.getOrCreate()
    if shuffle_partitions:
        df = spark.read.parquet(path).repartition(int(shuffle_partitions))
    else:
        df = spark.read.parquet(path)

    return get_history_from(
        df,
        partition_date,
        days,
        weeks,
        quarters,
        months,
        years,
        year_to_year,
        full_period,
    )


def load_iceberg_table_history_from(
    table_path: str,
    partition_date: str,
    days: Optional[int] = None,
    weeks: Optional[int] = None,
    quarters: Optional[int] = None,
    months: Optional[int] = None,
    years: Optional[int] = None,
    year_to_year: Optional[int] = None,
    full_period: bool = False,
    shuffle_partitions: str = None,
) -> DataFrame:
    """
    Charge l'historique d'une table Iceberg en filtrant par des périodes spécifiques.

    Paramètres :
    - table_path : Nom ou chemin de la table Iceberg dans la source définie (par ex., Hive, Spark Catalog).
    - partition_date : Date pivot pour les filtres temporels.
    - days, weeks, quarters, months, years, year_to_year : Filtres temporels selon différentes unités.
    - full_period : Si activé, inclut toutes les dates dans la période définie ; sinon, seulement la dernière.
    - shuffle_partitions : Nombre de partitions Spark après répartition.

    Utilisation :
    - Chargement des données Iceberg pour des analyses temporelles.
    - Gestion de grands volumes de données tout en profitant de l'optimisation d'Iceberg.
    - Exemple :
        df = load_iceberg_table_history_from(
            "iceberg_catalog.transactions",
            "2023-12-31",
            quarters=4,
            full_period=True
        )
    """
    spark = SparkSession.builder.getOrCreate()
    if shuffle_partitions:
        df = spark.table(table_path).repartition(int(shuffle_partitions))
    else:
        df = spark.table(table_path)

    return get_history_from(
        df,
        partition_date,
        days,
        weeks,
        quarters,
        months,
        years,
        year_to_year,
        full_period,
    )


def get_history_from(
    df: DataFrame,
    partition_date: str,
    days: Optional[int] = None,
    weeks: Optional[int] = None,
    quarters: Optional[int] = None,
    months: Optional[int] = None,
    years: Optional[int] = None,
    year_to_year: Optional[int] = None,
    full_period: bool = False,
) -> DataFrame:
    """
    Filtre un DataFrame Spark pour extraire les données historiques selon des périodes temporelles spécifiques.

    Paramètres :
    - df : DataFrame Spark contenant une colonne `partitiondate` utilisée pour le filtrage.
    - partition_date : Date pivot utilisée comme point de départ pour les calculs historiques.
    - days : Nombre de jours avant `partition_date` à inclure.
    - weeks : Nombre de semaines avant `partition_date` à inclure.
    - quarters : Nombre de trimestres avant `partition_date` à inclure.
    - months : Nombre de mois avant `partition_date` à inclure.
    - years : Nombre d'années avant `partition_date` à inclure.
    - year_to_year : Extrait la date correspondant au même jour d'une année précédente.
    - full_period : Si activé, inclut toutes les dates de chaque période ; sinon, uniquement la dernière date de chaque période.

    Utilisation :
    - Filtrage des données historiques pour des analyses spécifiques.

    Exemple d'utilisation :
        # Charger un historique sur les 30 derniers jours et les 3 derniers mois
        filtered_df = get_history_from(
            df=my_data,
            partition_date="2024-12-31",
            days=30,
            months=3,
            full_period=True
        )
    """
    dates: List[DataFrame] = []

    # Extraction des N derniers jours
    if days or (days == 0):
        n_last_dates = get_n_days_ago(df, partition_date, days)
        dates.append(n_last_dates)

    # Extraction des N dernières semaines
    if weeks or (weeks == 0):
        for idx in range(weeks + 1):
            max_date_week = get_dates_from_n_weeks_ago(
                df, partition_date, idx, full_period
            )
            dates.append(max_date_week)

    # Extraction des N derniers mois
    if months or (months == 0):
        for idx in range(months + 1):
            max_date_month = get_dates_from_n_months_ago(
                df, partition_date, idx, full_period
            )
            dates.append(max_date_month)

    # Extraction des N derniers trimestres
    if quarters or (quarters == 0):
        for idx in range(quarters + 1):
            max_date_quarter = get_dates_from_n_quarters_ago(
                df, partition_date, idx, full_period
            )
            dates.append(max_date_quarter)

    # Extraction des N dernières années
    if years or (years == 0):
        for idx in range(years + 1):
            max_date_year = get_dates_from_n_years_ago(
                df, partition_date, idx, full_period
            )
            dates.append(max_date_year)

    # Extraction de la date exacte sur plusieurs années (year-to-year)
    if year_to_year:
        same_day_year = get_same_day_from_n_years_ago(
            df, partition_date, year_to_year
        )
        dates.append(same_day_year)

    # Union de toutes les périodes calculées, suppression des doublons et collecte des dates uniques
    union_all = (
        reduce(lambda left, right: left.unionByName(right), dates)
        .na.drop()
        .drop_duplicates()
        .collect()
    )

    unique_dates = {row["partitiondate"] for row in union_all}

    # Filtrage du DataFrame d'origine avec les dates uniques calculées
    return df.filter(col("partitiondate").isin(unique_dates))


def load_previous_n_months(
    df: DataFrame, partition_date: str, months: int = 1
) -> DataFrame:
    """
    Charge les dates correspondant au dernier jour d'un ou plusieurs mois précédents.

    Paramètres :
    - df : DataFrame Spark contenant une colonne `partitiondate`.
    - partition_date : Date pivot utilisée pour calculer les mois précédents.
    - months : Nombre de mois à inclure dans le calcul.

    Exemple d'utilisation :
        # Charger les données du dernier mois
        filtered_df = load_previous_n_months(my_data, "2024-12-31", months=1)
    """
    last_day_previous_n_month = last_day(
        add_months(to_date(lit(partition_date)), -months)
    )
    first_day_previous_n_month = date_trunc("MM", last_day_previous_n_month)

    return (
        df.filter(
            (col("partitiondate") >= first_day_previous_n_month)
            & (col("partitiondate") <= last_day_previous_n_month)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )


def load_table_with_history_from(
    path: str, partition_date: str, shuffle_partitions: str = None, days: int = 5
) -> DataFrame:
    """
    Charge une table historique avec des périodes spécifiques (jours, mois, années).

    - Lit un fichier Parquet depuis un chemin donné et extrait les dates pertinentes
      selon les dimensions temporelles suivantes :
        - Derniers `n` jours
        - Derniers mois (jusqu'à 4 mois avant la date pivot)
        - Dernières années (1 ou 2 années précédentes)
        - Même jour l'année précédente
    - Combine toutes ces périodes dans une table unique, utilisée pour filtrer les
      données d'origine.

    Paramètres :
    - path : Chemin du fichier ou répertoire contenant les données Parquet.
    - partition_date : Date pivot pour le calcul des périodes historiques.
    - shuffle_partitions : Nombre de partitions Spark après répartition (facultatif).
    - days : Nombre de jours avant la date pivot à inclure.

    Exemple d'utilisation :
        # Charger les données des 5 derniers jours et des périodes historiques
        filtered_df = load_table_with_history_from(
            "/data/transactions",
            "2024-12-31",
            shuffle_partitions="200",
            days=5
        )
    """

    spark = SparkSession.builder.getOrCreate()
    if shuffle_partitions:
        df = spark.read.parquet(path).repartition(int(shuffle_partitions))
    else:
        df = spark.read.parquet(path)

    n_last_dates = (
        df.select(col("partitiondate"))
        .filter(col("partitiondate") <= to_date(lit(partition_date)))
        .drop_duplicates()
        .orderBy(col("partitiondate").desc())
        .limit(days)
    )

    max_date_last_month = load_previous_n_months(df, partition_date, 1)
    max_date_last_2_months = load_previous_n_months(df, partition_date, 2)
    max_date_last_3_months = load_previous_n_months(df, partition_date, 3)
    max_date_last_4_months = load_previous_n_months(df, partition_date, 4)

    max_dates_last_months = (
        max_date_last_month.unionByName(max_date_last_2_months)
        .unionByName(max_date_last_3_months)
        .unionByName(max_date_last_4_months)
    )

    last_day_previous_year = date_add(
        date_trunc("yyyy", to_date(lit(partition_date))), -1
    )
    first_day_previous_year = date_trunc("yyyy", last_day_previous_year)

    max_date_previous_year = (
        df.filter(
            (col("partitiondate") >= first_day_previous_year)
            & (col("partitiondate") <= last_day_previous_year)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )

    last_day_previous_2_years = date_add(
        date_trunc("yyyy", first_day_previous_year), -1
    )
    first_day_previous_2_years = date_trunc("yyyy", last_day_previous_2_years)

    max_date_previous_2_years = (
        df.filter(
            (col("partitiondate") >= first_day_previous_2_years)
            & (col("partitiondate") <= last_day_previous_2_years)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )

    max_dates_previous_years = max_date_previous_year.unionByName(
        max_date_previous_2_years
    )

    max_date_same_day_last_year = (
        df.filter(
            col("partitiondate") == add_months(to_date(lit(partition_date)), -12)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )

    union_all = (
        n_last_dates.unionByName(max_dates_last_months)
        .unionByName(max_dates_previous_years)
        .unionByName(max_date_same_day_last_year)
        .na.drop()
    )

    return df.join(union_all, "partitiondate")


def load_table_with_unique_history_from(
    path: str, partition_date: str, shuffle_partitions: str = None, days: int = 5
) -> DataFrame:
    """
    Charge une table avec un historique unique selon différentes périodes temporelles.
    Cette fonction combine plusieurs périodes temporelles uniques : aujourd'hui, la semaine passée,
    le mois précédent, le trimestre précédent et l'année précédente.

    Paramètres :
    - path : Chemin du fichier Parquet à lire.
    - partition_date : Date pivot pour calculer les périodes historiques.
    - shuffle_partitions : Nombre de partitions Spark à configurer après répartition.
    - days : Nombre de jours avant la date pivot à inclure dans le filtre.

    Exemple d'utilisation :
        # Charger les données avec des périodes historiques uniques
        df = load_table_with_unique_history_from(
            "/data/transactions",
            "2024-12-31",
            shuffle_partitions="200",
            days=5
        )
    """

    spark = SparkSession.builder.getOrCreate()
    if shuffle_partitions:
        df = spark.read.parquet(path).repartition(int(shuffle_partitions))
    else:
        df = spark.read.parquet(path)

    today_date = (
        df.select(col("partitiondate"))
        .filter(col("partitiondate") == to_date(lit(partition_date)))
        .drop_duplicates()
    )

    first_day_previous_week = date_trunc(
        "WEEK", to_date(lit(partition_date)) - expr("INTERVAL 1 WEEK")
    )
    last_day_previous_week = date_add(
        date_trunc("WEEK", to_date(lit(partition_date))), -1
    )

    max_date_last_week = (
        df.filter(
            (col("partitiondate") >= first_day_previous_week)
            & (col("partitiondate") <= last_day_previous_week)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )

    max_date_last_month = load_previous_n_months(df, partition_date, 1)

    last_day_previous_quarter = last_day(
        add_months(to_date(lit(partition_date)), -3)
    )
    first_day_previous_quarter = date_trunc("QUARTER", last_day_previous_quarter)

    max_date_last_quarter = (
        df.filter(
            (col("partitiondate") >= first_day_previous_quarter)
            & (col("partitiondate") <= last_day_previous_quarter)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )

    last_day_previous_year = date_add(
        date_trunc("yyyy", to_date(lit(partition_date))), -1
    )
    first_day_previous_year = date_trunc("yyyy", last_day_previous_year)

    max_date_previous_year = (
        df.filter(
            (col("partitiondate") >= first_day_previous_year)
            & (col("partitiondate") <= last_day_previous_year)
        )
        .select(max(col("partitiondate")).alias("partitiondate"))
        .distinct()
    )

    union_all = (
        today_date.unionByName(max_date_last_week)
        .unionByName(max_date_last_month)
        .unionByName(max_date_last_quarter)
        .unionByName(max_date_previous_year)
        .na.drop()
    )

    return df.join(union_all, "partitiondate")


# Load the whole table
def load_parquet_table(
    path: str,
    shuffle_partitions: str = None,
) -> DataFrame:
    """
    Charge une table entière Parquet avec une option de répartition.

    Paramètres :
    - path : Chemin du fichier Parquet à lire.
    - shuffle_partitions : Nombre de partitions Spark après répartition (optionnel).

    Exemple d'utilisation :
        df = load_parquet_table("/data/my_table", shuffle_partitions="100")
    """

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(path)
    if shuffle_partitions:
        return df.repartition(int(shuffle_partitions))
    return df


def load_iceberg_table_from(
    table_path: str,
    filter_date: str,
    partition_column: str = "partitiondate",
) -> DataFrame:
    """
    Charge une table Iceberg en filtrant les données sur une date précise.

    Description :
    - Utilise Spark pour lire une table Iceberg depuis un chemin ou un catalogue.
    - Applique un filtre sur une colonne de partition pour extraire les données.

    Paramètres :
    - table_path : Nom ou chemin de la table Iceberg.
    - filter_date : Date utilisée pour filtrer les données.
    - partition_column : Colonne utilisée pour le filtrage (par défaut : `partitiondate`).

    Exemple d'utilisation :
        df = load_iceberg_table_from("iceberg_catalog.transactions", "2024-12-31")
    """
    spark = SparkSession.builder.getOrCreate()
    return spark.table(table_path).filter(col(partition_column) == lit(filter_date))


def load_latest_date_from_iceberg_table(table_path: str) -> DataFrame:
    """
    Charge les données correspondant à la date la plus récente d'une table Parquet.
    Identifie la date la plus récente dans une table en fonction d'une colonne de partition.
    Filtre les données pour inclure uniquement cette date.

    Paramètres :
    - path : Chemin du fichier ou répertoire Parquet.
    - shuffle_partitions : Nombre de partitions Spark après répartition (optionnel).
    - partition_column : Colonne utilisée pour identifier la date la plus récente.

    Utilisation :
    - Extraction des données les plus récentes pour des mises à jour incrémentielles.
    - Préparation des données pour des analyses basées sur des snapshots récents.

    Exemple d'utilisation :
        df = load_latest_date_from_table("/data/my_table", shuffle_partitions="50")
    """
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.table(table_path)
    return spark.sql(
        """
            SELECT * 
            FROM {df}
            WHERE partitiondate = (
                SELECT MAX(partitiondate) 
                FROM {df}
            )
            """,
        df=df,
    )


def load_iceberg_table_from_all(table_path: str) -> DataFrame:
    """
    Charge une table Iceberg complète.

    Paramètres :
    - table_path : Nom ou chemin de la table Iceberg à charger.

    Exemple d'utilisation :
        df = load_iceberg_table_from_all("iceberg_catalog.my_table")
    """
    spark = SparkSession.builder.getOrCreate()
    return spark.table(table_path)


def load_latest_date_from_table(
    path: str,
    shuffle_partitions: str = None,
    partition_column: str = "partitiondate",
) -> DataFrame:
    """
    Charge les données correspondant à la date la plus récente d'une table Parquet.
    Identifie la date maximale dans une colonne de partition spécifique.
    Filtre les données pour inclure uniquement celles correspondant à cette date.
    Répartit les données sur plusieurs partitions Spark si spécifié.

    Détails des paramètres :
    - path : Chemin de la table Parquet à charger.
    - shuffle_partitions : Nombre de partitions Spark à configurer après répartition.
    - partition_column : Colonne utilisée pour identifier la date la plus récente (par défaut : `partitiondate`).

    Utilisation :
    - Extraction des snapshots récents pour des analyses incrémentielles.
    - Chargement des données les plus récentes pour des mises à jour ou des rapports.

    Exemple d'utilisation :
        df = load_latest_date_from_table("/data/my_table", shuffle_partitions="50")
    """
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet(path)

    max_date = df.select(max(col(partition_column)).alias(partition_column))

    if shuffle_partitions:
        return df.join(max_date, partition_column).repartition(
            int(shuffle_partitions)
        )

    return df.join(max_date, partition_column)


def load_latest_value_from_table(
    path: str,
    partition_column: str = "partitiondate",
) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(path)
    max_partition = df.select(max(col("partitiondate")).cast("string")).first()[0]
    df_filtered = df.filter(col(partition_column) == lit(max_partition))

    return df_filtered


def load_latest_date_before_partition_date_from_iceberg(
    table_path: str,
    filter_date: str,
    partition_column: str = "partitiondate",
) -> DataFrame:
    """
    Charge les données correspondant à la date la plus récente avant une date donnée dans une table Iceberg.
    Filtre les données pour inclure uniquement les partitions antérieures ou égales à une date de référence (`filter_date`).
    Identifie la date maximale parmi les partitions valides.
    Joint les données d'origine avec cette partition pour retourner les lignes correspondantes.

    Paramètres :
    - table_path : Nom ou chemin de la table Iceberg.
    - filter_date : Date pivot utilisée pour filtrer les partitions.
    - partition_column : Colonne utilisée pour identifier la date la plus récente (par défaut : `partitiondate`).

    Utilisation :
    - Analyse des données disponibles avant une certaine date.
    - Extraction des snapshots historiques les plus récents.

    Exemple d'utilisation :
        df = load_latest_date_before_partition_date_from_iceberg(
            "iceberg_catalog.my_table", "2024-12-31"
        )
    """

    spark = SparkSession.builder.getOrCreate()
    df = spark.table(table_path)
    max_date = df.filter(col(partition_column) <= filter_date).select(
        max(col(partition_column)).alias(partition_column)
    )
    return df.join(max_date, partition_column)


def load_current_year_from_table(
    path: str,
    partition_date: str,
    shuffle_partitions: str = None,
    partition_column: str = "partitiondate",
) -> DataFrame:
    """
    Charge les données correspondant à l'année en cours jusqu'à une date donnée.

    Détails des paramètres :
    - path : Chemin du fichier ou répertoire Parquet.
    - partition_date : Date pivot utilisée pour limiter les données.
    - shuffle_partitions : Nombre de partitions Spark après répartition (optionnel).
    - partition_column : Colonne utilisée pour le filtrage.

    Exemple d'utilisation :
        df = load_current_year_from_table(
            "/data/my_table", "2024-12-31", shuffle_partitions="100"
        )
    """
    spark = SparkSession.builder.getOrCreate()

    df = (
        spark.read.parquet(path)
        .filter(year(col(partition_column)) == year(to_date(lit(partition_date))))
        .filter(col(partition_column) <= to_date(lit(partition_date)))
        # .filter(month(col(partition_column)) == 7) # TODO
    )

    if shuffle_partitions:
        return df.repartition(int(shuffle_partitions))
    return df


def load_iceberg_table_from_history(
    table_path: str,
    filter_date: str,
    shuffle_partitions: str = None,
    n_limit_hist: int = 6,
    partition_column: str = "partitiondate",
) -> DataFrame:
    """
    Charge un historique limité d'une table Iceberg en filtrant sur des périodes clés.
    Cette fonction extrait les données correspondant aux dernières `n_limit_hist`
    dates avant une date pivot, ainsi que les dates maximales du mois précédent
    et de l'année précédente.
    Combine ces périodes pour retourner une vue historique consolidée.
    Supporte la répartition des partitions Spark pour optimiser les performances.

    Paramètres :
    - table_path : Chemin ou nom de la table Iceberg à lire.
    - filter_date : Date pivot pour calculer les périodes historiques.
    - shuffle_partitions : Nombre de partitions Spark (optionnel).
    - n_limit_hist : Nombre maximum de dates récentes à inclure dans l'historique.
    - partition_column : Colonne utilisée pour les calculs temporels (par défaut : `partitiondate`).

    Utilisation :
    - Extraction des snapshots récents avec des périodes clés (mois, année).
    - Génération de rapports ou d'analyses basés sur des périodes historiques définies.

    Exemple d'utilisation :
        df = load_iceberg_table_from_history(
            "iceberg_catalog.my_table",
            "2024-12-31",
            shuffle_partitions="50",
            n_limit_hist=10
        )
    """
    spark = SparkSession.builder.getOrCreate()

    if shuffle_partitions:
        df = spark.table(table_path).repartition(int(shuffle_partitions))

    n_last_dates = (
        df.select(col(partition_column))
        .drop_duplicates()
        .filter(col(partition_column) <= to_date(lit(filter_date)))
        .orderBy(col(partition_column).desc())
        .limit(n_limit_hist)
    )

    last_day_previous_month = last_day(add_months(to_date(lit(filter_date)), -1))

    max_date_last_month = (
        df.filter(col(partition_column) <= last_day_previous_month)
        .select(max(col(partition_column)).alias(partition_column))
        .distinct()
    )

    last_day_previous_year = date_add(
        date_trunc("yyyy", to_date(lit(filter_date))), -1
    )

    max_date_previous_year = (
        df.filter(col(partition_column) <= last_day_previous_year)
        .select(max(col(partition_column)).alias(partition_column))
        .distinct()
    )

    union_all = n_last_dates.unionByName(max_date_last_month).unionByName(
        max_date_previous_year
    )

    return df.join(union_all, partition_column)


def load_last_n_days(df: DataFrame, partition_date: str, days: int = 90):
    """
    Charge les données des `n` derniers jours avant une date donnée.
    Calcule une plage de dates allant de `partition_date - days` à `partition_date`.
    Filtre les données pour inclure uniquement les partitions situées dans cette plage.

    Détails des paramètres :
    - df : DataFrame contenant une colonne `partitiondate`.
    - partition_date : Date pivot pour calculer les `n` derniers jours.
    - days : Nombre de jours à inclure dans le filtre.

    Exemple d'utilisation :
        recent_data = load_last_n_days(my_data, "2024-12-31", days=30)
    """

    first_date = date_add(to_date(lit(partition_date)), -days)
    last_date = to_date(lit(partition_date))
    n_last_dates = (
        df.select(col("partitiondate"))
        .filter(col("partitiondate").between(first_date, last_date))
        .drop_duplicates()
    )
    return df.join(n_last_dates, "partitiondate")


def load_table_with_n_days(
    path: str, partition_date: str, shuffle_partitions: str = None, days: int = 90
) -> DataFrame:
    """
    Charge une table Parquet et extrait les données des `n` derniers jours.
    Lit une table Parquet depuis un chemin donné.
    Filtre les données pour inclure uniquement les partitions des `n` derniers jours
    avant une date pivot.
    Supporte la répartition des données sur plusieurs partitions Spark.

    Détails des paramètres :
    - path : Chemin de la table Parquet à lire.
    - partition_date : Date pivot pour calculer les `n` derniers jours.
    - shuffle_partitions : Nombre de partitions Spark à configurer après répartition (optionnel).
    - days : Nombre de jours à inclure dans l'historique.

    Exemple d'utilisation :
        df = load_table_with_n_days("/data/my_table", "2024-12-31", days=60)
    """

    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.read.parquet(path).repartition(int(shuffle_partitions))
        if shuffle_partitions
        else spark.read.parquet(path)
    )
    return load_last_n_days(df, partition_date, days)


def load_full_history(
    df: DataFrame,
    filter_date: str,
    partition_column: str,
):
    """
    Charge l'historique complet jusqu'à une date donnée.
    Filtre les données pour inclure toutes les partitions antérieures ou égales à une date pivot.

    Détails des paramètres :
    - df : DataFrame contenant les données historiques.
    - filter_date : Date pivot pour limiter les partitions.
    - partition_column : Colonne utilisée pour les calculs temporels.

    Exemple d'utilisation :
        historical_data = load_full_history(my_data, "2024-12-31", "partitiondate")
    """
    return df.filter(col(partition_column) <= to_date(lit(filter_date)))


def load_table_with_full_history(
    path: str,
    filter_date: str,
    shuffle_partitions: str = None,
    partition_column: str = "partitiondate",
):
    """
    Charge une table Parquet et extrait l'historique complet jusqu'à une date donnée.
    Filtre les données pour inclure toutes les partitions jusqu'à une date pivot.
    Supporte la répartition des données pour optimiser les performances.

    Détails des paramètres :
    - path : Chemin de la table Parquet à lire.
    - filter_date : Date pivot pour limiter les partitions.
    - shuffle_partitions : Nombre de partitions Spark à configurer après répartition (optionnel).
    - partition_column : Colonne utilisée pour les calculs temporels.

    Exemple d'utilisation :
        df = load_table_with_full_history(
            "/data/my_table", "2024-12-31", shuffle_partitions="50"
        )
    """
    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.read.parquet(path).repartition(int(shuffle_partitions))
        if shuffle_partitions
        else spark.read.parquet(path)
    )
    return load_full_history(df, filter_date, partition_column)


def does_table_exist(path: str) -> bool:
    """
    Vérifie si une table Parquet existe dans un chemin donné.
    Tente de lire une table Parquet à partir du chemin spécifié.
    Si une exception `AnalysisException` est levée, cela indique que la table n'existe pas.

    Paramètres :
    - path : Chemin du fichier ou répertoire contenant la table Parquet.

    Retour :
    - bool : Retourne `True` si la table existe, sinon `False`.

    Utilisation :
    - Validation de l'existence des données avant exécution d'autres traitements.
    - Prévention des erreurs liées à l'absence d'une table ou de données.

    Exemple d'utilisation :
        exists = does_table_exist("/data/my_table")
    """
    spark = SparkSession.builder.getOrCreate()
    try:
        spark.read.parquet(path)
        does_exist = True
    except AnalysisException:
        does_exist = False
    return does_exist


def does_iceberg_table_exist(table_path: str) -> bool:
    """
    Vérifie si une table Iceberg existe dans un chemin donné ou dans le catalogue.
    Tente de lire une table Iceberg en utilisant `spark.table`.
    Si une exception `AnalysisException` est levée, cela indique que la table n'existe pas.

    Détails des paramètres :
    - table_path : Chemin ou nom de la table Iceberg.

    Retour :
    - bool : Retourne `True` si la table existe, sinon `False`.

    Utilisation technique :
    - Validation de l'existence d'une table Iceberg avant des opérations de lecture.
    - Prévention des erreurs lors d'une tentative d'accès à une table inexistante.

    Exemple d'utilisation :
        exists = does_iceberg_table_exist("iceberg_catalog.my_table")
    """

    spark = SparkSession.builder.getOrCreate()
    try:
        spark.table(table_path)
        does_exist = True
    except AnalysisException:
        does_exist = False
    return does_exist


def load_delta_from(path: str, table_path: str, partition_date: str) -> DataFrame:
    """
    Charge les données incrémentales (delta) en fonction des partitions d'une table Iceberg.
    Vérifie si la table Iceberg existe et contient des données pour la date spécifiée.
    Si oui, identifie la partition maximale existante pour charger uniquement les données
    après cette partition jusqu'à la `partition_date` spécifiée.
    Si la table Iceberg n'existe pas ou est vide, charge un historique complet depuis une table Parquet.

    Détails des paramètres :
    - path : Chemin de la table Parquet utilisée comme source.
    - table_path : Chemin ou nom de la table Iceberg utilisée pour valider l'existence des partitions.
    - partition_date : Date pivot pour limiter les partitions à charger.

    Retour :
    - DataFrame : Données incrémentales ou historique complet.

    Utilisation technique :
    - Chargement des données delta pour des mises à jour incrémentielles.
    - Préparation des données pour des analyses basées sur des snapshots récents.

    Exemple d'utilisation :
        df = load_delta_from("/data/source_table", "iceberg_catalog.my_table", "2024-12-31")
    """
    spark = SparkSession.builder.getOrCreate()

    if does_iceberg_table_exist(table_path) and (
        not load_iceberg_table_history_from(
            table_path, partition_date, days=0
        ).isEmpty()
    ):
        max_available_date = (
            load_iceberg_table_history_from(table_path, partition_date, days=0)
            .select("partitiondate")
            .drop_duplicates()
            .collect()
            .pop(0)
        )

        max_avaibale_value_date = max_available_date["partitiondate"]

        return spark.read.parquet(path).filter(
            (col("partitiondate") <= to_date(lit(partition_date)))
            & (col("partitiondate") > max_avaibale_value_date)
        )

    return load_table_history_from(path, partition_date, years=99, full_period=True)
