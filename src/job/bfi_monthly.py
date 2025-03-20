import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_current_year_from_table,
    load_latest_date_from_table,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    assert_dates_equality,
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.bfi import (
    build_bfi_monthly_legacy,
    build_solde_client_devise_history,
    build_solde_client_history,
    build_taux_change_bam_history,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    from_files_path = config.get(env, "from_files_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM BFI MONTHLY {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    dm_sldcli_history = load_current_year_from_table(
        f"{oracle_gl_path}/DM_SLDCLI", partition_date, shuffle_partitions
    )

    rp_gl_bct_history = load_current_year_from_table(
        f"{oracle_gl_path}/RP_GL_BCT", partition_date, shuffle_partitions
    )

    assert_dates_equality(
        dm_sldcli_history, rp_gl_bct_history, title="Compare DM_SLDCLI to RP_GL_BCT"
    )

    t601_history = load_current_year_from_table(
        f"{ods_path}/T601", partition_date, shuffle_partitions
    )

    assert_dates_equality(
        dm_sldcli_history, t601_history, title="Compare DM_SLDCLI to T601"
    )

    ref_devise = load_latest_date_from_table(
        f"{ods_path}/EXPLOIT_REF_DEVISE", shuffle_partitions
    )

    nouveau_mapping_ressource_emploi = load_latest_date_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING", shuffle_partitions
    )

    virement_bam_history = load_current_year_from_table(
        f"{from_files_path}/TAUX_CHANGES_BAM/VIREMENT",
        partition_date,
        shuffle_partitions,
    )

    assert_dates_equality(
        dm_sldcli_history,
        virement_bam_history,
        title="Compare DM_SLDCLI to VIREMENT",
    )

    bbe_bam_history = load_current_year_from_table(
        f"{from_files_path}/TAUX_CHANGES_BAM/COURSBBE",
        partition_date,
        shuffle_partitions,
    )

    assert_dates_equality(
        dm_sldcli_history, bbe_bam_history, title="Compare DM_SLDCLI to COURSBBE"
    )

    solde_client_devise_history = build_solde_client_devise_history(
        dm_sldcli_history, rp_gl_bct_history, t601_history
    )

    taux_change_bam_history = build_taux_change_bam_history(
        ref_devise, virement_bam_history, bbe_bam_history
    )

    solde_client_history = build_solde_client_history(
        solde_client_devise_history, taux_change_bam_history
    )

    bfi_monthly = build_bfi_monthly_legacy(
        solde_client_history, nouveau_mapping_ressource_emploi, partition_date
    )

    to_iceberg(
        bfi_monthly,
        "APPLICATION",
        "TDB_BFI",
        "ENCOURS_MENSUEL",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
