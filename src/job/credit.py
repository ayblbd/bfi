import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_table_from,
    load_latest_value_from_table,
    load_iceberg_table_from,
    load_iceberg_table_from_all,
)
from src.common.table_writer import to_iceberg, to_iceberg_append
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.credit import (
    credit_ls_transformation,
    credit_wafasalaf_transformation,
    produit_transformation,
    select_ressource_emploi,
    select_l2hd_debl,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    config.get(env, "oracle_gl_path")
    ls = config.get(env, "ls_path")
    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    exploit_path = config.get(env, "exploit_path")
    ods_path = config.get(env, "ods_path")
    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM ENGAGEMENT {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    l2do_dgen = load_table_from(f"{ls}/L2DO_DGEN", partition_date)

    l2do_dlanc = load_table_from(f"{ls}/L2DO_DLANC", partition_date)

    l2do_tfonc = load_table_from(f"{ls}/L2DO_TFONC", partition_date)

    l2do_dcomm = load_table_from(f"{ls}/L2DO_DCOMM", partition_date)

    l2do_dpal = load_table_from(f"{ls}/L2DO_DPAL", partition_date)

    l2ei_echeanche = load_table_from(f"{ls}/L2EI_ECHEANCE", partition_date)

    l2hd_debl = load_table_from(f"{ls}/L2HD_DEBL", partition_date)

    joursferie = load_latest_value_from_table(f"{ods_path}/EXP_JOURSFERIES")

    ressource_emploi = load_iceberg_table_from_all(
        f"{catalog_name}.SOCLE.BFI.RESSOURCE_EMPLOI"
    )

    compte = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE", partition_date
    )
    map_ressource_emploi = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.MAP_RESSOURCE_EMPLOI", partition_date
    )
    deblocage_oav = load_table_from(f"{exploit_path}/DEBLOCAGEOAV", partition_date)

    produit = load_latest_value_from_table(f"{from_files_path}/CATEGORIE_DE_PRET")
    produit_credit = load_latest_value_from_table(
        f"{from_files_path}/PRODUIT_CREDIT"
    )
    rss_emploi = select_ressource_emploi(
        ressource_emploi, compte, map_ressource_emploi
    )
    credit_ls = credit_ls_transformation(
        l2do_dgen,
        l2do_dlanc,
        l2do_tfonc,
        l2do_dcomm,
        l2hd_debl,
        l2ei_echeanche,
        rss_emploi,
        compte,
        produit,
        joursferie,
        partition_date,
    )

    credit_wafasalaf = credit_wafasalaf_transformation(
        deblocage_oav, compte, partition_date
    )

    produit_processed = produit_transformation(produit, produit_credit)

    to_iceberg(credit_ls, "SOCLE", "CREDIT", "PRODUCTION_CREDIT", partition_date)
    to_iceberg_append(
        credit_wafasalaf, "SOCLE", "CREDIT", "PRODUCTION_CREDIT", partition_date
    )
    to_iceberg(produit_processed, "SOCLE", "CREDIT", "PRODUIT", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
