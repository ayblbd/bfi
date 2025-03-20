import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import load_from_assurance
from src.common.table_writer import to_parquet
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.ingestion_assurance import (
    build_axa_assurance,
    build_rma_watanya,
    build_saham_assurance,
    build_wafa_assurance,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    from_assurances_files_path = config.get(env, "from_assurances_files_path")
    from_files_path = config.get(env, "from_files_path")

    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM INGESTION ASSURANCES {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    rma_watanya = load_from_assurance(
        from_assurances_files_path,
        "dwh_contrat_cdm",
        partition_date,
        shuffle_partitions,
    )

    rma_watanya_df = build_rma_watanya(rma_watanya, partition_date)

    to_parquet(rma_watanya_df, from_files_path, "RMA_WATANYA", partition_date)

    axa = load_from_assurance(
        from_assurances_files_path, "epargne_axa", partition_date, shuffle_partitions
    )

    axa_df = build_axa_assurance(axa, partition_date)

    to_parquet(axa_df, from_files_path, "AXA_ASSURANCE", partition_date)

    saham = load_from_assurance(
        from_assurances_files_path,
        "epargne_liberis",
        partition_date,
        shuffle_partitions,
    )

    saham_df = build_saham_assurance(saham, partition_date)

    to_parquet(saham_df, from_files_path, "SAHAM_ASSURANCE", partition_date)

    wafa = load_from_assurance(
        from_assurances_files_path,
        "encours_epargne",
        partition_date,
        shuffle_partitions,
    )

    wafa_df = build_wafa_assurance(wafa, partition_date)

    to_parquet(wafa_df, from_files_path, "WAFA_ASSURANCE", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
