import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_history_from,
    load_latest_date_from_iceberg_table,
    load_latest_date_from_table,
    load_table_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.assurance import build_assurance


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    config.get(env, "oracle_gl_path")
    catalog_name = config.get(env, "catalog_name")
    from_files_path = config.get(env, "from_files_path")
    ods_path = config.get(env, "ods_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM ASSURANCE {partition_date}"
    )

    gamme_assurance = load_latest_date_from_table(
        f"{from_files_path}/GAMME_ASSURANCE"
    )

    type_contrat = load_table_history_from(
        f"{ods_path}/EXPLOIT_TYPECONTRAT", partition_date=partition_date, days=0
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

    assurance = build_assurance(
        contrat_produit,
        gamme_assurance,
        type_contrat,
        etat_contrat,
        partition_date,
    )

    to_iceberg(
        assurance,
        "SOCLE",
        "EQUIPEMENT",
        "ASSURANCE",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
