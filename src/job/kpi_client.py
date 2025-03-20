import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_history_from,
    load_latest_date_from_table,
    load_table_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.bfis import build_taux_change_bam_history
from src.transformation.kpi_client import build_kpi_client


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    config.get(env, "oracle_gl_path")
    catalog_name = config.get(env, "catalog_name")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    from_files_path = config.get(env, "from_files_path")
    ods_path = config.get(env, "ods_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM KPI CLIENT {partition_date}"
    )

    tier = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.CLIENT.TIER", partition_date, days=0
    )

    compte = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date, days=0
    )

    mouvement_comptable = load_table_history_from(
        f"{oracle_gl_path}/MOUVEMENTCOMPTABLE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        years=0,
        full_period=True,
    )

    ref_devise = load_latest_date_from_table(
        f"{ods_path}/EXPLOIT_REF_DEVISE", shuffle_partitions
    )

    virement_bam = load_table_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/VIREMENT",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        years=0,
        full_period=True,
    )

    bbe_bam = load_table_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/COURSBBE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        years=0,
        full_period=True,
    )

    activite_client = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.ACTIVITE_CLIENT.ACTIVITE_CLIENT",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
    )

    score_intensite_bancaire = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.INTENSITE_BANCAIRE.SCORE_INTENSITE_BANCAIRE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        months=2,
        quarters=1,
        years=1,
    )

    taux_change_bam = build_taux_change_bam_history(
        ref_devise, virement_bam, bbe_bam
    )

    kpi_client = build_kpi_client(
        tier,
        compte,
        mouvement_comptable,
        taux_change_bam,
        activite_client,
        score_intensite_bancaire,
        partition_date,
    )

    to_iceberg(
        kpi_client,
        "SOCLE",
        "EQUIPEMENT",
        "KPI_CLIENT",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
