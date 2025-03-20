from typing import Set

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, split, to_date

from src.common.utils import get_logger


def get_column_ddl(column_name: str, dtype: str):
    """
    Génère une définition de colonne (DDL) pour une table.
    Crée une chaîne de caractères formatée représentant une colonne et son type de données
    compatible avec les requêtes SQL Spark.

    Détails des paramètres :
    - column_name : Nom de la colonne.
    - dtype : Type de données de la colonne.

    Retour :
    - str : Définition de colonne au format SQL.

    Exemple d'utilisation :
        column_ddl = get_column_ddl("age", "int")
        # Retourne : "age int"
    """
    return f"{column_name} {dtype}"


def add_new_columns(df: DataFrame, table_name: str, new_columns: Set[str]) -> None:
    """
    Ajoute de nouvelles colonnes à une table existante.
    Identifie les types de données des colonnes à ajouter à partir du DataFrame.
    Formate les colonnes avec leur DDL et exécute une requête `ALTER TABLE` pour les ajouter
    à la table spécifiée.

    Détails des paramètres :
    - df : DataFrame contenant les colonnes à ajouter.
    - table_name : Nom de la table cible.
    - new_columns : Ensemble des noms des colonnes à ajouter.

    Utilisation technique :
    - Extension dynamique d'une table Iceberg ou Parquet en ajoutant des colonnes manquantes.

    Exemple d'utilisation :
        add_new_columns(my_df, "my_table", {"new_col1", "new_col2"})
    """

    logger = get_logger()
    spark = SparkSession.builder.getOrCreate()

    dtypes = dict(df.dtypes)

    columns_ddl = [
        get_column_ddl(column_name, dtypes[column_name])
        for column_name in new_columns
    ]

    logger.warn(f"Adding new columns to {table_name}: {', '.join(columns_ddl)}")

    spark.sql(
        f"""
        ALTER TABLE {table_name}
        ADD COLUMNS (
            {", ".join(columns_ddl)}
        )
        """
    )


def get_table_columns(table_name: str) -> Set[str]:
    """
    Récupère les colonnes existantes d'une table Spark.
    Lit les métadonnées d'une table Spark pour récupérer la liste des colonnes existantes.
    Les noms des colonnes sont convertis en minuscules pour assurer la cohérence.

    Détails des paramètres :
    - table_name : Nom de la table cible.

    Retour :
    - Set[str] : Ensemble des noms des colonnes existantes en minuscules.

    Utilisation technique :
    - Vérification de l'existence de colonnes avant modification d'une table.

    Exemple d'utilisation :
        columns = get_table_columns("my_table")
    """

    spark = SparkSession.builder.getOrCreate()
    logger = get_logger()
    try:
        columns = {column.lower() for column in spark.table(table_name).columns}
        return columns
    except Exception:
        logger.warn(f"No existing table was found: {table_name}")
        return set()


def alter_table_with_new_columns_if_exits(df, table_name) -> None:
    """
    Ajoute dynamiquement des colonnes manquantes à une table si elle existe.
    Compare les colonnes attendues (déjà présentes dans la table) avec les colonnes du DataFrame.
    Identifie les colonnes manquantes et utilise `add_new_columns` pour les ajouter.

    Paramètres :
    - df : DataFrame contenant les colonnes à comparer.
    - table_name : Nom de la table cible.

    Utilisation :
    - Synchronisation des schémas entre une table existante et un nouveau DataFrame.

    Exemple d'utilisation :
        alter_table_with_new_columns_if_exits(my_df, "my_table")
    """

    expected_columns = get_table_columns(table_name)
    actual_columns = {column.lower() for column in df.columns}

    diff_columns: Set[str] = actual_columns - expected_columns

    if expected_columns and diff_columns:
        add_new_columns(df, table_name, diff_columns)


def to_iceberg(
    df: DataFrame, couche: str, domaine: str, table: str, partition_date: str
):
    """
    Crée une table Iceberg et insère les données avec gestion dynamique du schéma.
    Vérifie si la table Iceberg existe et ajoute les colonnes manquantes.
    Crée la table si elle n'existe pas en utilisant la partition `partitiondate`.
    Insère les données en écrasant les partitions existantes.

    Détails des paramètres :
    - df : DataFrame contenant les données à écrire.
    - couche : Couche logique de données (par ex., `bronze`, `silver`, `gold`).
    - domaine : Domaine fonctionnel ou sujet lié aux données.
    - table : Nom de la table cible.
    - partition_date : Date de partition pour les données.

    Utilisation technique :
    - Chargement des données dans une table Iceberg tout en gérant les mises à jour dynamiques du schéma.
    - Création de tables avec des partitions gérées.

    Exemple d'utilisation :
        to_iceberg(
            df=my_data,
            couche="gold",
            domaine="sales",
            table="transactions",
            partition_date="2024-12-31"
        )
    """
    spark = SparkSession.builder.getOrCreate()
    logger = get_logger()

    table_name = f"iceberg_catalog.{couche}.{domaine}.{table}"
    df_with_partition_date = df.withColumn(
        "partitiondate", to_date(lit(partition_date))
    )

    alter_table_with_new_columns_if_exits(df, table_name)

    logger.info(f"Creating the {table_name} if not exists.")

    spark.sql(
        """CREATE TABLE IF NOT EXISTS """
        + table_name
        + """ USING ICEBERG
        PARTITIONED BY (partitiondate)
        AS SELECT *
        FROM {df}""",
        df=df_with_partition_date,
    )

    logger.info(f"Writing partition_date: {partition_date} into {table_name}.")

    df_with_partition_date.writeTo(table_name).overwritePartitions()


#####################################################################################
#                                                                                   #
#                        One Time use for history load                              #
#                                                                                   #
#####################################################################################


def write_to_iceberg_for_auditdto_sql(
    df: DataFrame, couche: str, domaine: str, table: str
):
    spark = SparkSession.builder.getOrCreate()
    logger = get_logger()
    table_name = f"iceberg_catalog.{couche}.{domaine}.{table}"
    df_with_partition_date = df.withColumn(
        "partitiondate", to_date(lit(split(col("DATE"), " ").getItem(0)))
    )

    df_sorted = df_with_partition_date.sortWithinPartitions("partitiondate")
    df_sorted.createOrReplaceTempView("temp_table")

    logger.info(f"Creating the {table_name} if not exists.")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING ICEBERG
        PARTITIONED BY (partitiondate)
        AS SELECT *
        FROM temp_table
        """
    )

    logger.info(f"Writing partition_dates into {table_name}.")

    df_with_partition_date.writeTo(table_name).overwritePartitions()


#####################################################################################
#                                                                                   #
#                        One Time use for history load                              #
#                                                                                   #
#####################################################################################


def to_parquet(df: DataFrame, path: str, table: str, partition_date: str) -> None:
    logger = get_logger()

    logger.info(f"Creating the {table} if not exists.")

    (
        df.write.partitionBy("partitiondate")
        .option("compression", "snappy")
        .parquet(f"{path}/{table}", mode="append")
    )

    logger.info(f"Writing partition_date: {partition_date} into {path}/{table}.")


def to_iceberg_append(
    df, couche: str, domaine: str, table: str, partition_date: str
):
    spark = SparkSession.builder.getOrCreate()
    logger = get_logger()

    table_name = f"iceberg_catalog.{couche}.{domaine}.{table}"
    df_with_partition_date = df.withColumn(
        "partitiondate", to_date(lit(partition_date))
    )

    alter_table_with_new_columns_if_exits(df, table_name)

    logger.info(f"Creating the {table_name} if not exists.")

    spark.sql(
        """CREATE TABLE IF NOT EXISTS """
        + table_name
        + """ USING ICEBERG
        PARTITIONED BY (partitiondate)
        AS SELECT *
        FROM {df}""",
        df=df_with_partition_date,
    )

    logger.info(f"Writing partition_date: {partition_date} into {table_name}.")

    df_with_partition_date.writeTo(table_name).append()
