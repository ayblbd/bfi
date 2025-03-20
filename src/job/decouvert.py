import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_history_from,
    load_table_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.decouvert import build_decouvert


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    config.get(env, "oracle_gl_path")
    ods_path = config.get(env, "ods_path")
    catalog_name = config.get(env, "catalog_name")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM DECOUVERT {partition_date}"
    )

    autorisation = load_table_history_from(
        f"{ods_path}/EXPLOIT_AUTORISATION", partition_date=partition_date, days=0
    )
    elt_contrat = load_table_history_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date=partition_date, days=0
    )

    compte = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE",
        partition_date=partition_date,
        days=0,
    )

    map_ressource_emploi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.MAP_RESSOURCE_EMPLOI",
        partition_date=partition_date,
        days=0,
    )

    bfi = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.BFI.RESSOURCE_EMPLOI",
        partition_date=partition_date,
        years=1,
        full_period=True,
    )

    assurance = build_decouvert(
        autorisation,
        elt_contrat,
        compte,
        bfi,
        map_ressource_emploi,
        partition_date,
    )

    to_iceberg(
        assurance,
        "SOCLE",
        "EQUIPEMENT",
        "DECOUVERT",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
