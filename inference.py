import os

from ai.h2o.sparkling import H2OContext
from ai.h2o.sparkling.ml import H2OMOJOModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import add_months, col, current_timestamp, date_format, lit

from src.transformations import transform
from src.utils import (
    clean_dataframe,
    create_spark_session,
    get_config,
    get_max_audit_date,
    select_last_with_history,
    to_hive,
)


def infer():

    config = get_config()

    env = os.environ.get("ENV", "prd")
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    db = config.get(env, "db")
    path_reset = config.get(env, "path_reset")
    path_device = config.get(env, "path_device")
    piste_audit_table = config.get(env, "piste_audit_table")
    features_table = config.get(env, "features_table")
    mobile_device_table = config.get(env, "mobile_device_table")
    device_history_table = config.get(env, "device_history_table")
    model_table = config.get(env, "model_table")
    queue_name = config.get(env, "queue_name")

    config_spark = [
        ("spark.app.name", "Fraude Inference v0.2"),
        ("spark.sql.hive.convertMetastoreOrc", "False"),
        ("mapreduce.input.fileinputformat.input.dir.recursive", "True"),
        ("spark.dynamicAllocation.enabled", "False"),
        ("spark.sql.shuffle.partitions", "1000"),
        ("tez.queue.name", queue_name),
    ]
    spark: SparkSession = create_spark_session(config_spark)

    H2OContext.getOrCreate()

    max_audit_fact_date: int = get_max_audit_date(spark.table(f"{db}.{model_table}"))

    last_ebk_with_history: DataFrame = (
        clean_dataframe(
            select_last_with_history(
                spark.table(f"{db}.{piste_audit_table}"), max_audit_fact_date
            ).drop(
                "id_objet",
                "nr_contrat",
                "canal_type",
                "user_ip_address",
                "date_technique",
                "time",
                "device_id",
            )
        )
    ).filter(col("audit_fact_date") >= add_months(current_timestamp(), -6))

    # check if dataframe is empty
    if last_ebk_with_history.count() == 0:
        spark.stop()
        return

    last_mobile_device: DataFrame = spark.table(f"{db}.{mobile_device_table}").drop(
        "time"
    )

    last_device_history: DataFrame = spark.table(f"{db}.{device_history_table}").drop(
        "time"
    )

    model_reset = H2OMOJOModel.createFromMojo(path_reset)
    model_device = H2OMOJOModel.createFromMojo(path_device)

    transformed = transform(
        last_ebk_with_history, last_mobile_device, last_device_history
    ).filter(col("audit_fact_date") > lit(max_audit_fact_date))

    transformed.persist()

    to_hive(
        transformed,
        db,
        features_table,
        mode="append",
    )

    df = transformed.filter(col("event_type").isin("2", "5", "B"))

    predictions: DataFrame = (
        model_reset.transform(df)
        .withColumn(
            "normalized_score_reset", col("detailed_prediction.normalizedScore")
        )
        .withColumnRenamed("prediction", "prediction_reset")
        .drop("detailed_prediction")
    )

    predictions: DataFrame = (
        model_device.transform(predictions)
        .withColumn(
            "normalized_score_device", col("detailed_prediction.normalizedScore")
        )
        .withColumnRenamed("prediction", "prediction_device")
        .withColumn(
            "audit_fact_date", date_format("audit_fact_date", "yyyy-MM-dd HH:mm:ss")
        )
        .drop("detailed_prediction")
    )

    to_hive(
        predictions,
        db,
        model_table,
        mode="append",
    )

    spark.stop()


if __name__ == "__main__":
    infer()
