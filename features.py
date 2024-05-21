import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.transformations import transform
from src.utils import clean_dataframe, create_spark_session, get_config, to_hive


def main():
    config = get_config()

    env = os.environ.get("ENV", "prd")
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    db = config.get(env, "db")
    piste_audit_table = config.get(env, "piste_audit_table")
    features_table = config.get(env, "features_table")
    mobile_device_table = config.get(env, "mobile_device_table")
    device_history_table = config.get(env, "device_history_table")
    queue_name = config.get(env, "queue_name")

    config_spark = [
        ("spark.app.name", "Fraude Features v0.7"),
        ("spark.sql.hive.convertMetastoreOrc", "False"),
        ("mapreduce.input.fileinputformat.input.dir.recursive", "True"),
        # ("spark.dynamicAllocation.enabled", "False"),
        # ("spark.sql.shuffle.partitions", "1000"),
        ("tez.queue.name", queue_name),
    ]
    spark: SparkSession = create_spark_session(config_spark)

    audit_ebk = clean_dataframe(
        spark.table(f"{db}.{piste_audit_table}").select(
            "id",
            "audit_message",
            "audit_fact_date",
            "event_type",
            "user_id",
            "device_id",
        )
    )

    mobile_device = spark.table(f"{db}.{mobile_device_table}")
    device_history = spark.table(f"{db}.{device_history_table}")

    transformed = transform(
        audit_ebk.filter((col("audit_fact_date") >= "2022-11-01")),
        mobile_device,
        device_history,
    )

    to_hive(
        transformed,
        db,
        features_table,
    )

    spark.stop()


if __name__ == "__main__":
    main()
