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
    get_logger,
    get_partition_date,
)
from src.transformation.activite_client import (
    build_activite_client,
)
from src.transformation.bfis import build_taux_change_bam_history


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    from_files_path = config.get(env, "from_files_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    catalog_name = config.get(env, "catalog_name")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    ods_path = config.get(env, "ods_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM ACTIVITE CLIENT {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    marche = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.CLIENT.MARCHE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
        full_period=True,
    )

    tiers = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.CLIENT.TIER",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
        full_period=True,
    )

    ressource_emploi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.BFI.RESSOURCE_EMPLOI",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
        full_period=True,
    )

    compte = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
        full_period=True,
    )

    mouvement_comptable = load_table_history_from(
        f"{oracle_gl_path}/MOUVEMENTCOMPTABLE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        years=2,
        full_period=True,
    )

    operation_initiative_client = load_latest_date_from_table(
        f"{from_files_path}/OPERATION_INITIATIVE_CLIENT",
        shuffle_partitions,
    )

    map_ressource_emploi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.MAP_RESSOURCE_EMPLOI",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
        full_period=True,
    )

    ref_devise = load_latest_date_from_table(
        f"{ods_path}/EXPLOIT_REF_DEVISE", shuffle_partitions
    )

    virement_bam = load_table_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/VIREMENT",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        years=2,
        full_period=True,
    )

    bbe_bam = load_table_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/COURSBBE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        years=2,
        full_period=True,
    )
    taux_change_bam = build_taux_change_bam_history(
        ref_devise, virement_bam, bbe_bam
    )
    activite = build_activite_client(
        tiers,
        compte,
        marche,
        ressource_emploi,
        mouvement_comptable,
        operation_initiative_client,
        map_ressource_emploi,
        taux_change_bam,
        partition_date,
    )

    to_iceberg(
        activite,
        "SOCLE",
        "ACTIVITE_CLIENT",
        "ACTIVITE_CLIENT",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
