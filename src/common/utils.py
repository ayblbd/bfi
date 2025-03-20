import configparser
import inspect
from datetime import datetime, timedelta

from pyspark import SparkConf
from pyspark.sql import Column, DataFrame, SparkSession


def create_spark_session(
    config: configparser.ConfigParser, env: str, job_name: str
) -> SparkSession:
    """
    Crée une session Spark configurée pour Iceberg et Parquet.
    Configure Spark en fonction des paramètres fournis dans un fichier de configuration.
    Configure Iceberg comme catalogue SQL et ajuste les paramètres pour la gestion des partitions
      et des modes de lecture/écriture de Parquet.

    Paramètres :
    - config : Instance `ConfigParser` contenant les paramètres de configuration.
    - env : Environnement cible (`dev`, `test`, `prod`).
    - job_name : Nom de l'application Spark.

    Retour :
    - SparkSession : Session Spark configurée.

    Exemple d'utilisation :
        config = get_config("config.ini")
        spark = create_spark_session(config, "dev", "my_spark_job")
    """

    warehouse = config.get(env, "warehouse")
    catalog_name = config.get(env, "catalog_name")
    jars = config.get(env, "jars")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    config_spark = [
        ("spark.app.name", job_name),
        ("spark.jars", jars),
        (
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        ),
        (f"spark.sql.catalog.{catalog_name}.type", "hadoop"),
        (f"spark.sql.catalog.{catalog_name}.warehouse", warehouse),
        (
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        ),
        ("spark.sql.legacy.timeParserPolicy", "LEGACY"),
        ("spark.sql.shuffle.partitions", shuffle_partitions),
        ("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"),
        ("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED"),
        ("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED"),
        ("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"),
    ]
    conf = SparkConf().setAll(config_spark)
    return SparkSession.builder.config(conf=conf).getOrCreate()


def get_config(filename: str = r"config.ini") -> configparser.ConfigParser:
    """
    Charge les configurations depuis un fichier INI.:
    Utilise `ConfigParser` pour lire un fichier de configuration INI.
    Permet l'utilisation d'interpolations étendues pour référencer d'autres valeurs dans le fichier.

    Paramètres :
    - filename : Nom du fichier de configuration (par défaut : `config.ini`).

    Retour :
    - ConfigParser : Objet contenant les configurations.

    Exemple d'utilisation :
        config = get_config("config.ini")
    """

    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    config.read_file(open(filename))
    return config


def get_current_date_minus_12() -> str:
    """
    Retourne la date actuelle moins 12 heures.
    Calcule la date de la veille (en soustrayant 12 heures à la date actuelle).
    Retourne la date formatée en `YYYY-MM-DD`.

    Retour :
    - str : Date au format `YYYY-MM-DD`.

    Exemple d'utilisation :
        partition_date = get_current_date_minus_12()
    """

    yesterday = datetime.today() - timedelta(hours=12, minutes=0)
    partition_date = yesterday.strftime("%Y-%m-%d")
    return partition_date


def get_partition_date(args) -> str:
    """
    Récupère la date de partition depuis les arguments ou calcule une valeur par défaut.
    Vérifie si la date est fournie dans les arguments CLI.
    Si aucun argument valide n'est trouvé, utilise la date actuelle moins 12 heures.

    Détails des paramètres :
    - args : Liste des arguments passés en ligne de commande.

    Retour :
    - str : Date de partition.

    Exemple d'utilisation :
        partition_date = get_partition_date(sys.argv)
    """

    if len(args) < 3 or args[2] == "":
        partition_date = get_current_date_minus_12()
        return partition_date
    else:
        partition_date = args[2]
        return partition_date


def get_env(args) -> str:
    """
    Récupère l'environnement cible à partir des arguments passés en ligne de commande.

    Paramètres :
    - args : Liste des arguments passés en ligne de commande, typiquement `sys.argv`.

    Retour :
    - str : Nom de l'environnement (`"dev"` par défaut si aucun argument valide n'est fourni).

    Utilisation :
    - Permet de choisir dynamiquement l'environnement de configuration (par ex., `dev`, `test`, `prod`).
    - Utilisé dans des scripts pour définir les paramètres et configurations spécifiques à l'environnement.

    Exemple d'utilisation :
        # Appel depuis la ligne de commande : python script.py test
        env = get_env(sys.argv)
        # Retourne : "test"

        # Appel sans argument pour l'environnement
        env = get_env(["script.py"])
        # Retourne : "dev"
    """

    if len(args) < 2 or args[1] == "":
        return "dev"
    else:
        return args[1]


def get_logger():
    """
    Crée un logger configuré pour Spark.
    Récupère ou crée un logger basé sur Log4j.
    Associe le logger au module appelant pour un suivi contextuel.

    Retour :
    - Logger : Instance de logger Log4j.

    Exemple d'utilisation :
        logger = get_logger()
        logger.info("Message de journalisation")
    """
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # Determine the calling module's name
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    module_name = module.__name__ if module else "__main__"

    # Get the logger
    log4jLogger = sc._jvm.org.apache.log4j  # type: ignore
    LOGGER = log4jLogger.LogManager.getLogger(module_name)
    return LOGGER


def assert_dates_equality(
    left: DataFrame, right: DataFrame, title: str = ""
) -> None:
    """
    Compare les dates de partition de deux DataFrames.
    Vérifie si les deux DataFrames contiennent les mêmes dates de partition.
    Log les différences trouvées, si elles existent.

    Détails des paramètres :
    - left : Premier DataFrame pour la comparaison.
    - right : Second DataFrame pour la comparaison.
    - title : Titre contextuel pour la journalisation.

    Exemple d'utilisation :
        assert_dates_equality(df1, df2, "Comparison Title")
    """

    logger = get_logger()

    left = left.select("partitiondate").distinct()

    right = right.select("partitiondate").distinct()

    diff = left.union(right).subtract(left.intersect(right))

    partition_dates_diff = {
        row["partitiondate"].strftime("%Y-%m-%d") for row in diff.collect()
    }

    if partition_dates_diff:
        logger.error(
            f"[{title}] The two dataframes don't contain the same dates: {sorted(partition_dates_diff)}"
        )
        # sys.exit(1)


def update_spark_log_level(log_level="WARN"):
    """
    Met à jour le niveau de journalisation de Spark.

    Paramètres :
    - log_level : Niveau de journalisation souhaité (par défaut : `WARN`).

    Exemple d'utilisation :
        update_spark_log_level("ERROR")
    """

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)


def debug(
    df: DataFrame,
    predicate: Column,
) -> DataFrame:
    """
    Filtre et affiche les données d'un DataFrame pour le débogage.
    Applique un filtre sur le DataFrame.
    Affiche les résultats du filtre dans la console pour inspection.

    Détails des paramètres :
    - df : DataFrame à analyser.
    - predicate : Condition à appliquer pour le filtrage.

    Retour :
    - DataFrame : DataFrame original (non modifié).

    Exemple d'utilisation :
        debug(my_df, col("age") > 30)
    """

    update_spark_log_level()

    df.filter(predicate).show(truncate=False)

    return df


def get_is_history(args) -> bool:
    """
    Détermine si une charge historique doit être effectuée.
    Vérifie la présence et la valeur d'un argument CLI pour indiquer si une charge historique est requise.

    Détails des paramètres :
    - args : Liste des arguments passés en ligne de commande.

    Retour :
    - bool : `True` si une charge historique est requise, sinon `False`.

    Exemple d'utilisation :
        is_history = get_is_history(sys.argv)
    """
    logger = get_logger()
    if len(args) < 4 or args[3] == "":
        logger.info("Is history: False")
        return False
    else:
        is_history = True if args[3].lower() == "true" else False
        logger.info(f"Is history: {is_history}")
        return is_history
