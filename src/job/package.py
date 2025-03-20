import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_history_from,
    load_latest_date_from_iceberg_table,
    load_table_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.package import build_package


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    config.get(env, "oracle_gl_path")
    catalog_name = config.get(env, "catalog_name")
    ods_path = config.get(env, "ods_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM PACKAGE {partition_date}"
    )

    rel_package_contrat = load_table_history_from(
        f"{ods_path}/EXP_RELPACKAGECONTRAT", partition_date=partition_date, days=0
    )

    etat_contrat = load_latest_date_from_iceberg_table(
        f"{catalog_name}.SOCLE.EQUIPEMENT.ETAT_CONTRAT",
    )

    contrat_produit = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
    )

    assurance = build_package(
        contrat_produit,
        rel_package_contrat,
        etat_contrat,
        partition_date,
    )

    to_iceberg(
        assurance,
        "SOCLE",
        "EQUIPEMENT",
        "PACKAGE",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
