import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
    load_latest_date_from_table,
    load_table_from,
    load_table_with_n_days,
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
from src.transformation.equipement import (
    build_assurance,
    build_assurance_kpis,
    build_carte,
    build_carte_kpis,
    build_csc,
    build_csc_kpis,
    build_digital,
    build_digital_kpis,
    build_equipement_corporate,
    build_equipement_corporate_kpis,
    build_objectifs_equipement,
    build_package,
    build_package_kpis,
    get_contrat_history,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    monetique_path = config.get(env, "monetique_path")
    adriadb_path = config.get(env, "adriadb_path")
    adriacm_path = config.get(env, "adriacm_path")
    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM EQUIPEMENT {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    audit_log_event = load_table_with_n_days(
        f"{adriadb_path}/AUDIT_LOG_EVENT",
        partition_date,
        shuffle_partitions,
        days=90,
    )

    utilisateur = load_table_with_n_days(
        f"{adriadb_path}/UTILISATEUR",
        partition_date,
        shuffle_partitions,
        days=90,
    )

    authorization = load_table_with_n_days(
        f"{monetique_path}/AUTHORIZATION",
        partition_date,
        shuffle_partitions,
        days=90,
    )

    elt_stock = load_table_from(
        f"{ods_path}/EXP_ELTSTOCK", partition_date, shuffle_partitions
    )

    type_elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPEELTCONTRAT", partition_date, shuffle_partitions
    )

    type_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPECONTRAT", partition_date, shuffle_partitions
    )

    gamme_assurance = load_latest_date_from_table(
        f"{from_files_path}/GAMME_ASSURANCE", shuffle_partitions
    )

    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    ).persist()

    relpackagecontrat = load_table_from(
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

    sous_compte_history = load_table_with_unique_history_from(
        f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date, shuffle_partitions
    )

    contrat_history = load_table_with_unique_history_from(
        f"{ods_path}/CONTRAT", partition_date, shuffle_partitions
    )
    contrat_history = get_contrat_history(contrat_history)

    objectif_tro = load_latest_date_from_table(
        f"{from_files_path}/OBJECTIFS_TRO", shuffle_partitions
    )

    saisonnalite = load_latest_date_from_table(
        f"{from_files_path}/SAISONNALITE", shuffle_partitions
    )
    exploit_elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )
    relation_commercial = load_latest_date_from_table(
        f"{adriacm_path}/RELATION_COMMERCIALE", shuffle_partitions
    )

    ebanking_corporate = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "EBAY",
        "E-Banking Corporate",
        exploit_elt_contrat,
    )

    to_iceberg(
        ebanking_corporate,
        "SOCLE",
        "EQUIPEMENT",
        "EBANKING_CORPORATE",
        partition_date,
    )

    eswift = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "SNT1",
        "E-Swift",
        exploit_elt_contrat,
    )
    to_iceberg(
        eswift,
        "SOCLE",
        "EQUIPEMENT",
        "ESWIFT",
        partition_date,
    )
    etrade = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "MS01",
        "E-Trade",
        exploit_elt_contrat,
    )
    to_iceberg(
        etrade,
        "SOCLE",
        "EQUIPEMENT",
        "ETRADE",
        partition_date,
    )
    edocument = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "CD01",
        "E-Document",
        exploit_elt_contrat,
    )
    to_iceberg(
        edocument,
        "SOCLE",
        "EQUIPEMENT",
        "EDOCUMENT",
        partition_date,
    )
    etebac_ip = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "EB01",
        "ETEBAC IP",
        exploit_elt_contrat,
    )
    to_iceberg(
        etebac_ip,
        "SOCLE",
        "EQUIPEMENT",
        "ETEBAC_IP",
        partition_date,
    )
    cdm_cheque_express = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "EX01",
        "CDM Chèques Express",
        exploit_elt_contrat,
    )
    to_iceberg(
        cdm_cheque_express,
        "SOCLE",
        "EQUIPEMENT",
        "CDM_CHEQUE_EXPRESS",
        partition_date,
    )
    mt101 = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "SSMT",
        "MT101",
        exploit_elt_contrat,
    )
    to_iceberg(
        mt101,
        "SOCLE",
        "EQUIPEMENT",
        "MT101",
        partition_date,
    )
    mt940 = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "SWFT",
        "MT940",
        exploit_elt_contrat,
    )
    to_iceberg(
        mt940,
        "SOCLE",
        "EQUIPEMENT",
        "MT940",
        partition_date,
    )

    equip_corpo_kpis = build_equipement_corporate_kpis(
        ebanking_corporate,
        eswift,
        etrade,
        edocument,
        etebac_ip,
        cdm_cheque_express,
        mt101,
        mt940,
    )

    to_iceberg(
        equip_corpo_kpis,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "EQUIPEMENT_CORPO",
        partition_date,
    )

    objectifs_equipement = build_objectifs_equipement(objectif_tro, saisonnalite)

    to_iceberg(
        objectifs_equipement,
        "SOCLE",
        "EQUIPEMENT",
        "OBJECTIFS_EQUIPEMENT",
        partition_date,
    )

    assurance = build_assurance(
        contrat_produit,
        contrat_history,
        type_contrat,
        information_tier,
        gamme_assurance,
        niveau_regroupement,
    )

    to_iceberg(
        assurance,
        "SOCLE",
        "EQUIPEMENT",
        "ASSURANCE",
        partition_date,
    )

    assurance_kpis = build_assurance_kpis(assurance, objectifs_equipement)

    to_iceberg(
        assurance_kpis,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "ASSURANCE",
        partition_date,
    )

    carte = build_carte(
        contrat_produit,
        contrat_history,
        elt_stock,
        type_elt_contrat,
        information_tier,
        information_compte_client,
        niveau_regroupement,
        authorization,
        assurance,
    )

    to_iceberg(
        carte,
        "SOCLE",
        "EQUIPEMENT",
        "CARTE",
        partition_date,
    )

    carte_kpis = build_carte_kpis(carte, objectifs_equipement)

    to_iceberg(
        carte_kpis,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "CARTE",
        partition_date,
    )

    package = build_package(
        contrat_produit,
        contrat_history,
        relpackagecontrat,
        type_contrat,
        information_tier,
        niveau_regroupement,
    )

    to_iceberg(
        package,
        "SOCLE",
        "EQUIPEMENT",
        "PACKAGE",
        partition_date,
    )

    package_kpis = build_package_kpis(package)

    to_iceberg(
        package_kpis,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "PACKAGE",
        partition_date,
    )

    csc = build_csc(
        contrat_produit,
        sous_compte_history,
        information_tier,
        niveau_regroupement,
        package,
        carte,
    )

    to_iceberg(
        csc,
        "SOCLE",
        "EQUIPEMENT",
        "CSC",
        partition_date,
    )

    csc_kpis = build_csc_kpis(csc, objectifs_equipement)

    to_iceberg(
        csc_kpis,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "CSC",
        partition_date,
    )

    digital = build_digital(
        contrat_produit,
        contrat_history,
        information_tier,
        niveau_regroupement,
        audit_log_event,
        utilisateur,
    )

    to_iceberg(
        digital,
        "SOCLE",
        "EQUIPEMENT",
        "DIGITAL",
        partition_date,
    )

    digital_kpis = build_digital_kpis(digital, objectifs_equipement)

    to_iceberg(
        digital_kpis,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "DIGITAL",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
