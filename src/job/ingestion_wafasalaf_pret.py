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
    get_partition_date,
)
from src.transformation.ingestion_wafasalaf_pret import (
    build_wafasalaf_pret,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    from_assurances_pret_files_path = config.get(
        env, "from_assurances_pret_files_path"
    )
    from_files_path = config.get(env, "from_files_path")

    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM INGESTION WAFASALAF PRET {partition_date}"
    )

    wafasalaf_pret = load_from_assurance(
        from_assurances_pret_files_path,
        "wafasalaf_pret",
        partition_date,
        shuffle_partitions,
    )

    wafasalaf_pret_df = build_wafasalaf_pret(wafasalaf_pret, partition_date)

    to_parquet(wafasalaf_pret_df, from_files_path, "WAFASALAF_PRET", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
