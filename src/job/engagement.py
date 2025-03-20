import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_table_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.engagement import build_credit


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    config.get(env, "oracle_gl_path")
    ls = config.get(env, "ls_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM ENGAGEMENT {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    l2do_dgen = load_table_from(
        f"{ls}/L2DO_DGEN", partition_date, shuffle_partitions
    )

    l2do_dlanc = load_table_from(
        f"{ls}/L2DO_DLANC", partition_date, shuffle_partitions
    )

    l2do_tfonc = load_table_from(
        f"{ls}/L2DO_TFONC", partition_date, shuffle_partitions
    )

    l2do_dcomm = load_table_from(
        f"{ls}/L2DO_DCOMM", partition_date, shuffle_partitions
    )

    l2do_dpal = load_table_from(
        f"{ls}/L2DO_DPAL", partition_date, shuffle_partitions
    )

    l2hd_rbt_nat = load_table_from(
        f"{ls}/L2HD_RBT_NAT", partition_date, shuffle_partitions
    )

    credit = build_credit(
        l2do_dgen, l2do_dlanc, l2do_tfonc, l2do_dcomm, l2do_dpal, l2hd_rbt_nat
    )

    to_iceberg(credit, "SOCLE", "ENGAGEMENT", "CREDIT", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
