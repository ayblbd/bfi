import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
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
from src.transformation.intensite_bancaire_palier3 import (
    build_base_client,
    build_mycdm,
    build_open_account,
    calculate_score_mycdm,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

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

    audit = load_table_history_from(
        f"{audit_path}/AUDITDTO", partition_date, months=0, full_period=True
    )
    audit_log_event = load_table_history_from(
        f"{adriadb_path}/AUDIT_LOG_EVENT", partition_date, months=0, full_period=True
    )
    utilisateur = load_table_history_from(
        f"{adriadb_path}/UTILISATEUR", partition_date, months=0, full_period=True
    )
    mouvementcomptable = load_table_history_from(
        f"{oracle_gl_path}/MOUVEMENTCOMPTABLE",
        partition_date,
        months=0,
        full_period=True,
    )
    ref_operation = load_latest_date_from_table(
        f"{from_files_path}/REFERENTIEL_OPERATION_BANQUE", shuffle_partitions
    )
    compte_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )
    # compte_client = spark.read.format("iceberg").load(
    #     "/iceberg_khawla/warehouse/SOCLE/COMPTE/COMPTE"
    # )
    compte_client_ouvert = build_open_account(compte_client)
    base_client = build_base_client(compte_client_ouvert)

    # --- MYCDM ---
    mydcm = build_mycdm(
        audit_log_event,
        utilisateur,
        audit,
        mouvementcomptable,
        ref_operation,
        compte_client_ouvert,
        base_client,
    )

    mycdm_score = calculate_score_mycdm(mydcm)

    to_iceberg(
        mycdm_score,
        "SOCLE",
        "INTENSITE_BANCAIRE",
        "MYCDM",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
