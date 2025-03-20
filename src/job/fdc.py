import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
    load_latest_date_from_table,
    load_table_from,
    load_table_with_unique_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.fdc import (
    build_fdc,
    build_objectifs_fdc,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    dataware_path = config.get(env, "dataware_path")
    from_files_path = config.get(env, "from_files_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    catalog_name = config.get(env, "catalog_name")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM FDC {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    objectif_tro = load_latest_date_from_table(
        f"{from_files_path}/OBJECTIFS_TRO", shuffle_partitions
    )

    saisonnalite = load_latest_date_from_table(
        f"{from_files_path}/SAISONNALITE", shuffle_partitions
    )

    donnee_client = load_table_with_unique_history_from(
        f"{dataware_path}/DONNEESCLIENT", partition_date, shuffle_partitions, days=1
    )

    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    )

    rel_package_contrat = load_table_from(
        f"{ods_path}/EXP_RELPACKAGECONTRAT", partition_date, shuffle_partitions
    )

    information_tier = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.CLIENT.INFORMATION_TIER", partition_date
    )

    information_compte_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_CLIENT", partition_date
    )

    niveau_regroupement = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.AGENCE.NIVEAU_REGROUPEMENT", partition_date
    )

    ressource_emploi = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTABILITE.RESSOURCE_EMPLOI", partition_date
    )

    objectifs_fdc = build_objectifs_fdc(objectif_tro, saisonnalite)

    to_iceberg(
        objectifs_fdc,
        "SOCLE",
        "EQUIPEMENT",
        "OBJECTIFS_FDC",
        partition_date,
    )

    fdc = build_fdc(
        objectifs_fdc,
        information_tier,
        information_compte_client,
        donnee_client,
        contrat_produit,
        niveau_regroupement,
        rel_package_contrat,
        ressource_emploi,
        partition_date,
    )

    to_iceberg(
        fdc,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "FDC",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
