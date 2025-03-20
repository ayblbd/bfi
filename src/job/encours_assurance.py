import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
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
from src.transformation.encours_assurance import build_encours_assurance


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM ENCOURS ASSURANCE {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    axa_assurance_history = load_table_history_from(
        f"{from_files_path}/AXA_ASSURANCE",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    rma_watanya_history = load_table_history_from(
        f"{from_files_path}/RMA_WATANYA",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    saham_assurance_history = load_table_history_from(
        f"{from_files_path}/SAHAM_ASSURANCE",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    wafa_assurance_history = load_table_history_from(
        f"{from_files_path}/WAFA_ASSURANCE",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    )

    etat_contrat = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.ETAT_CONTRAT", partition_date
    )

    compte = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )

    # compte = load_iceberg_table_history_from(
    #     f"{catalog_name}.SOCLE.COMPTE.COMPTE",
    #     partition_date,
    #     shuffle_partitions=shuffle_partitions,
    #     days=0,
    # )
    # contrat_produit = spark.read.format("iceberg").load(
    #     "/iceberg_ayblbd/warehouse/SOCLE/EQUIPEMENT/CONTRAT_PRODUIT"
    # )
    # etat_contrat = spark.read.format("iceberg").load(
    #     "/iceberg_ayblbd/warehouse/SOCLE/EQUIPEMENT/ETAT_CONTRAT"
    # )

    assurances = build_encours_assurance(
        compte,
        axa_assurance_history,
        rma_watanya_history,
        saham_assurance_history,
        wafa_assurance_history,
        contrat_produit,
        etat_contrat,
        partition_date,
    )

    to_iceberg(
        assurances, "SOCLE", "COMPTABILITE", "ENCOURS_ASSURANCE", partition_date
    )

    spark.stop()


if __name__ == "__main__":
    main()
