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
from src.transformation.intensite_bancaire_palier4 import (
    build_base_client,
    build_base_ressource_emploi,
    build_open_account,
    build_score_assurance,
    build_score_consommation,
    build_score_epargne,
    build_score_immobilier,
    build_valid_contract,
    calculate_score_equipement,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    magiclear_path = config.get(env, "magiclear_path")
    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM INTENSITE_BANCAIRE {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    v_solde_clt = load_table_from(
        f"{magiclear_path}/V_SOLDE_CLT", partition_date, shuffle_partitions
    )
    axa_assurance = load_table_from(
        f"{from_files_path}/AXA_ASSURANCE", partition_date, shuffle_partitions
    )
    wafa_assurance = load_table_from(
        f"{from_files_path}/WAFA_ASSURANCE", partition_date, shuffle_partitions
    )
    rma_watanya = load_table_from(
        f"{from_files_path}/RMA_WATANYA", partition_date, shuffle_partitions
    )
    saham_assurance = load_table_from(
        f"{from_files_path}/SAHAM_ASSURANCE", partition_date, shuffle_partitions
    )
    gamme_assurance = load_latest_date_from_table(
        f"{from_files_path}/GAMME_ASSURANCE"
    )
    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    )
    etat_contrat = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.ETAT_CONTRAT", partition_date
    )
    ressource_emploi = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.BFI.RESSOURCE_EMPLOI", partition_date
    )
    map_ressource_emploi = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.MAP_RESSOURCE_EMPLOI", partition_date
    )
    compte_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )

    # compte_client = spark.read.format("iceberg").load(
    #      "/iceberg_khawla/warehouse/SOCLE/COMPTE/COMPTE"
    # )
    # ressource_emploi = spark.read.format("iceberg").load(
    #      "/iceberg_khawla/warehouse/SOCLE/BFI/RESSOURCE_EMPLOI"
    # )
    # map_ressource_emploi = spark.read.format("iceberg").load(
    #      "/iceberg_khawla/warehouse/SOCLE/COMPTE/MAP_RESSOURCE_EMPLOI"
    # )
    # contrat_produit = spark.read.format("iceberg").load(
    #      "/iceberg_ayblbd/warehouse/SOCLE/EQUIPEMENT/CONTRAT_PRODUIT"
    # )
    # etat_contrat = spark.read.format("iceberg").load(
    #      "/iceberg_ayblbd/warehouse/SOCLE/EQUIPEMENT/ETAT_CONTRAT"
    # )

    compte_client_ouvert = build_open_account(compte_client)
    base_client = build_base_client(compte_client_ouvert)
    valid_contrat_produit = build_valid_contract(contrat_produit, etat_contrat)

    # --- EQUIPEMENT ---
    base_ressource_emploi = build_base_ressource_emploi(
        ressource_emploi, map_ressource_emploi, base_client
    )

    score_conso = build_score_consommation(base_ressource_emploi)
    score_immo = build_score_immobilier(base_ressource_emploi)
    score_assurance = build_score_assurance(
        valid_contrat_produit, gamme_assurance, base_client
    )
    score_epargne = build_score_epargne(
        valid_contrat_produit,
        v_solde_clt,
        axa_assurance,
        wafa_assurance,
        rma_watanya,
        saham_assurance,
        base_ressource_emploi,
        compte_client_ouvert,
        base_client,
    )

    score_equipement = calculate_score_equipement(
        score_conso, score_immo, score_assurance, score_epargne
    )
    to_iceberg(
        score_equipement,
        "SOCLE",
        "INTENSITE_BANCAIRE",
        "EQUIPEMENT",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
