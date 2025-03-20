import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
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
from src.transformation.intensite_bancaire import (
    build_equipement_score,
    build_intensite_bancaire_score,
    build_monthly_totals,
    build_mycdm_score,
    build_transaction_score,
)

from src.transformation.bfis import build_taux_change_bam_history


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    adriadb_path = config.get(env, "adriadb_path")
    audit_path = config.get(env, "audit_path")
    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM INTENSITE BANCAIRE {partition_date}"
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

    niveau_activite_client = load_latest_date_from_table(
        f"{from_files_path}/NIVEAU_ACTIVITE_CLIENT"
    )

    compte = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    tiers = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.CLIENT.TIER",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    contrat_produit = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    etat_contrat = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.ETAT_CONTRAT",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    map_ressource_emploi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.MAP_RESSOURCE_EMPLOI",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    ressource_emploi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.BFI.RESSOURCE_EMPLOI",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    gamme_assurance = load_latest_date_from_table(
        f"{from_files_path}/GAMME_ASSURANCE"
    )

    opcvm = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTABILITE.OPCVM",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    encours_assurance = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTABILITE.ENCOURS_ASSURANCE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    audit = load_table_history_from(
        f"{audit_path}/AUDITDTO", partition_date, months=0, full_period=True
    )

    audit_log_event = load_table_history_from(
        f"{adriadb_path}/AUDIT_LOG_EVENT", partition_date, months=0, full_period=True
    )

    utilisateur = load_table_history_from(
        f"{adriadb_path}/UTILISATEUR", partition_date, months=0, full_period=True
    )

    ref_operation = load_latest_date_from_table(
        f"{from_files_path}/REFERENTIEL_OPERATION_BANQUE", shuffle_partitions
    )

    transaction_monetique = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.MONETIQUE.TRANSACTION_MONETIQUE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        months=0,
        full_period=True,
    )

    ref_devise = load_table_history_from(
        f"{ods_path}/EXPLOIT_REF_DEVISE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    virement_bam = load_table_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/VIREMENT",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        months=2,
        full_period=True,
    )

    bbe_bam = load_table_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/COURSBBE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        months=2,
        full_period=True,
    )

    taux_change_bam = build_taux_change_bam_history(
        ref_devise, virement_bam, bbe_bam
    )

    # Palier 1
    monthly_totals = build_monthly_totals(
        mouvement_comptable,
        taux_change_bam,
        compte,
        tiers,
        partition_date,
    )

    # Palier 2
    transaction_score = build_transaction_score(
        compte,
        mouvement_comptable,
        ref_operation,
        transaction_monetique,
        partition_date,
    )

    # Palier 3
    mydcm_score = build_mycdm_score(
        audit_log_event,
        utilisateur,
        audit,
        mouvement_comptable,
        ref_operation,
        compte,
        partition_date,
    )

    # Palier 4
    equipement_score = build_equipement_score(
        contrat_produit,
        etat_contrat,
        gamme_assurance,
        opcvm,
        encours_assurance,
        ressource_emploi,
        map_ressource_emploi,
        compte,
    )

    intensite_bancaire_score = build_intensite_bancaire_score(
        monthly_totals,
        transaction_score,
        mydcm_score,
        equipement_score,
        niveau_activite_client,
    )

    to_iceberg(
        intensite_bancaire_score,
        "SOCLE",
        "INTENSITE_BANCAIRE",
        "SCORE_INTENSITE_BANCAIRE",
        partition_date,
    )
    spark.stop()


if __name__ == "__main__":
    main()
