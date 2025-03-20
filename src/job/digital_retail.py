import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
    load_iceberg_table_history_from,
    load_latest_date_from_table,
    load_table_from,
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
from src.transformation.digital_retail import build_digital


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    adriadb_path = config.get(env, "adriadb_path")
    audit_path = config.get(env, "audit_path")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    ods_path = config.get(env, "ods_path")
    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM DIGITAL RETAIL {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    contrat = load_table_from(
        f"{ods_path}/CONTRAT", partition_date, shuffle_partitions
    )
    elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )
    ref_operation = load_latest_date_from_table(
        f"{from_files_path}/REFERENTIEL_OPERATION_BANQUE", shuffle_partitions
    )
    audit = load_table_history_from(
        f"{audit_path}/AUDITDTO", partition_date, months=15, full_period=True
    )
    audit_log_event = load_table_history_from(
        f"{adriadb_path}/AUDIT_LOG_EVENT",
        partition_date,
        months=15,
        full_period=True,
    )
    utilisateur = load_table_history_from(
        f"{adriadb_path}/UTILISATEUR", partition_date, months=15, full_period=True
    )
    mouvementcomptable = load_table_history_from(
        f"{oracle_gl_path}/MOUVEMENTCOMPTABLE",
        partition_date,
        months=15,
        full_period=True,
    )
    contrat_produit = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
    )
    etat_contrat = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.ETAT_CONTRAT", partition_date
    )
    compte_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )
    # compte_client = spark.read.format("iceberg").load(
    #     "/iceberg_khawla/warehouse/SOCLE/COMPTE/COMPTE"
    # )
    # contrat_produit = spark.read.format("iceberg").load(
    #     "/iceberg_ayblbd/warehouse/SOCLE/EQUIPEMENT/CONTRAT_PRODUIT"
    # )
    # etat_contrat = spark.read.format("iceberg").load(
    #     "/iceberg_ayblbd/warehouse/SOCLE/EQUIPEMENT/ETAT_CONTRAT"
    # )
    digital = build_digital(
        contrat_produit,
        contrat,
        elt_contrat,
        etat_contrat,
        mouvementcomptable,
        audit_log_event,
        utilisateur,
        audit,
        compte_client,
        ref_operation,
        partition_date,
    )

    to_iceberg(
        digital,
        "SOCLE",
        "EQUIPEMENT",
        "DIGITAL",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
