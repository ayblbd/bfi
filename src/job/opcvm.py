import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
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
from src.transformation.opcvm import build_detail_opcvm, build_opcvm


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    config.get(env, "ods_path")
    catalog_name = config.get(env, "catalog_name")
    magiclear_path = config.get(env, "magiclear_path")
    from_files_path = config.get(env, "from_files_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM OPCVM {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    v_corres_val_history = load_table_history_from(
        f"{magiclear_path}/V_CORRES_VAL",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    v_cours_ref_history = load_table_history_from(
        f"{magiclear_path}/V_COURS_REF",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    v_solde_clt_history = load_table_history_from(
        f"{magiclear_path}/V_SOLDE_CLT",
        partition_date,
        weeks=2,
        months=2,
        quarters=1,
        years=1,
        shuffle_partitions=shuffle_partitions,
    )

    code_valeur_opcvm = load_latest_date_from_table(
        f"{from_files_path}/CODE_VALEUR_OPCVM", shuffle_partitions
    )

    compte = load_iceberg_table_history_from(
        f"{catalog_name}.SOCLE.COMPTE.COMPTE",
        partition_date,
        shuffle_partitions=shuffle_partitions,
        days=0,
    )

    detail_opcvm = build_detail_opcvm(
        v_solde_clt_history,
        v_corres_val_history,
        v_cours_ref_history,
        code_valeur_opcvm,
        partition_date,
    )

    to_iceberg(
        detail_opcvm,
        "SOCLE",
        "COMPTABILITE",
        "OPCVM_DETAIL",
        partition_date,
    )

    opcvm = build_opcvm(
        v_solde_clt_history,
        v_corres_val_history,
        v_cours_ref_history,
        code_valeur_opcvm,
        compte,
        partition_date,
    )

    to_iceberg(opcvm, "SOCLE", "COMPTABILITE", "OPCVM", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
