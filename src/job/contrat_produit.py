import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
    load_latest_date_from_table,
    load_table_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.contrat_produit import (
    build_contrat_produit,
    build_etat_contrat,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    catalog_name = config.get(env, "catalog_name")
    ods_path = config.get(env, "ods_path")
    from_files_path = config.get(env, "from_files_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM CONTRAT_PRODUIT {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    exploit_fonctionneavec = load_table_from(
        f"{ods_path}/EXPLOIT_FONCTIONNEAVEC", partition_date, shuffle_partitions
    )

    exploit_elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )

    exploit_souscompte = load_table_from(
        f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date, shuffle_partitions
    )

    exploit_titulaire = load_table_from(
        f"{ods_path}/EXPLOIT_TITULAIRE", partition_date, shuffle_partitions
    )

    contrat = load_table_from(
        f"{ods_path}/CONTRAT", partition_date, shuffle_partitions
    )

    type_elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPEELTCONTRAT", partition_date, shuffle_partitions
    )

    type_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPECONTRAT", partition_date, shuffle_partitions
    )

    compte = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )

    etat_contrat = load_latest_date_from_table(f"{from_files_path}/ETAT_CONTRAT")

    contrat_produit = build_contrat_produit(
        exploit_souscompte,
        exploit_fonctionneavec,
        exploit_elt_contrat,
        contrat,
        exploit_titulaire,
        type_contrat,
        type_elt_contrat,
        compte,
    )

    to_iceberg(
        contrat_produit,
        "SOCLE",
        "EQUIPEMENT",
        "CONTRAT_PRODUIT",
        partition_date,
    )

    etat_contrat = build_etat_contrat(etat_contrat)

    to_iceberg(
        etat_contrat,
        "SOCLE",
        "EQUIPEMENT",
        "ETAT_CONTRAT",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
