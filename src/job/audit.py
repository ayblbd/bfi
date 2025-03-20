import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

import src.transformation.audit as audit
from src.common.table_loader import load_table_from
from src.common.table_writer import (
    to_iceberg,
)
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    shuffle_partitions = config.get(env, "shuffle_partitions")
    partition_date: str = get_partition_date(sys.argv)
    pisteauditdb = config.get(env, "audit_path")

    audit_path = f"{pisteauditdb}/AUDITDTO"

    spark: SparkSession = create_spark_session(config, env, "CDM AUDIT 0.1")

    # auditdto_df = audit.load_hist_table(audit_path, shuffle_partitions, "AUDITDTO")
    auditdto_df = load_table_from(audit_path, partition_date, shuffle_partitions)
    # auditdto_df.cache()

    connexions = audit.audit_connexion_build_daily(auditdto_df)

    # Load connexions to socle
    to_iceberg(connexions, "SOCLE", "DIGITAL", "CONNEXION_WEB", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
