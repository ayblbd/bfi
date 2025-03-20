import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
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
from src.transformation.intensite_bancaire_palier1 import (
    build_monthly_totals,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    oracle_gl_path = config.get(env, "oracle_gl_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM INTENSITE_BANCAIRE {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    mouvement_comptable = load_table_history_from(
        f"{oracle_gl_path}/MOUVEMENTCOMPTABLE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        months=2,
        full_period=True,
    )

    compte = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )

    tiers = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.CLIENT.TIER", partition_date
    )

    taux_change_bam = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.BFI.TAUX_CHANGE_BAM",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        months=2,
        full_period=True,
    )

    monthly_totals = build_monthly_totals(
        mouvement_comptable,
        taux_change_bam,
        compte,
        tiers,
        partition_date,
    )

    to_iceberg(
        monthly_totals,
        "SOCLE",
        "INTENSITE_BANCAIRE",
        "MONTHLY_TOTALS",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
