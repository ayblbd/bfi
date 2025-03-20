import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_history_from,
    load_table_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.csc import (
    build_csc,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    config.get(env, "ods_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    dataware_path = config.get(env, "dataware_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM CSC {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    compte = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE",
        partition_date=partition_date,
        years=1,
        full_period=True,
    )

    ressource_emploi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.BFI.RESSOURCE_EMPLOI",
        partition_date=partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
    )

    donnees_compte = load_table_history_from(
        f"{dataware_path}/DONNEESCOMPTE",
        partition_date=partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        full_period=False,
        shuffle_partitions=shuffle_partitions,
    )

    csc = build_csc(
        compte,
        donnees_compte,
        ressource_emploi,
        partition_date,
    )

    to_iceberg(
        csc,
        "SOCLE",
        "EQUIPEMENT",
        "CSC",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
