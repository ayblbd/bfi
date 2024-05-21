import datetime
import os
from typing import List, Tuple, Union

from ai.h2o.sparkling import H2OContext
from ai.h2o.sparkling.ml import H2OExtendedIsolationForest, H2OIsolationForest
from ai.h2o.sparkling.ml.models import (
    H2OExtendedIsolationForestMOJOModel,
    H2OIsolationForestMOJOModel,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.utils import create_spark_session, get_config, to_hive


def get_path(name: str) -> str:
    return f"hdfs:///fraud/model_{name}_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"


def train_model(
    df: DataFrame,
    featuresCols: List[str],
    model_name: str,
    extended: bool = False,
    seed: int = 12345,
    ntrees: int = 300,
    depth: int = 16,
) -> Tuple[
    Union[H2OIsolationForestMOJOModel, H2OExtendedIsolationForestMOJOModel], DataFrame
]:
    if extended:
        model = H2OExtendedIsolationForest(
            ntrees=ntrees,
            featuresCols=featuresCols,
            seed=seed,
        )
    else:
        model = H2OIsolationForest(
            ntrees=ntrees,
            maxDepth=depth,
            featuresCols=featuresCols,
            seed=seed,
        )

    model = model.fit(df)

    predictions = (
        model.transform(df)
        .withColumn(
            f"normalized_score_{model_name}", col("detailed_prediction.normalizedScore")
        )
        .withColumnRenamed("prediction", f"prediction_{model_name}")
        .drop("detailed_prediction")
    )
    return model, predictions


def train():
    config = get_config()

    env = os.environ.get("ENV", "prd")
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    db = config.get(env, "db")
    features_table = config.get(env, "features_table")
    queue_name = config.get(env, "queue_name")
    model_table = config.get(env, "model_table")

    config_spark = [
        ("spark.app.name", "Training v0.3"),
        ("spark.sql.hive.convertMetastoreOrc", "False"),
        ("mapreduce.input.fileinputformat.input.dir.recursive", "True"),
        ("spark.dynamicAllocation.enabled", "False"),
        ("tez.queue.name", queue_name),
    ]
    spark: SparkSession = create_spark_session(config_spark)
    H2OContext.getOrCreate()

    df = spark.table(f"{db}.{features_table}").filter(
        col("event_type").isin("2", "5", "B")
    )


    feats_reset = [
        "amount_virement",
        # "amount_disposition",
        "is_reset_pattern_virement",
        "time_diff_to_reset",
        # "time_diff_to_reset_normalized",
        "criterion",
    ]
    model_reset, predictions = train_model(
        df.filter(col("event_type").isin("2", "5", "B")),
        feats_reset,
        model_name="reset",
    )
    model_reset.write().save(get_path("reset"))

    feats_device = [
        # "device_count",
        "is_attijari_secure_deactivated_pattern",
        "is_attijari_secure_activated_pattern",
        "time_diff_to_attijari_secure_normalized",
        "device_age_normalized",
        # "device_count_last_7_days",
        "cumsum_disposition_12_hours",
        "cumsum_virement_12_hours",
    ]

    feats_device = [
        "is_safe_facture_device",
        "is_awb_secure_activated_safe_device",
        "average_amount_scaled_12h",
        "time_diff_to_attijari_secure_normalized",
        "cumsum_disposition_12_hours",
        "cumsum_virement_12_hours",
        "cumsum_cashexpress_12_hours",
        "device_age_normalized",
        "is_awb_secure_deactivated_safe_device",
    ]

    model_device, predictions = train_model(
        predictions, feats_device, model_name="device"
    )
    model_device.write().save(get_path("device"))

    to_hive(predictions, db, model_table)

    spark.stop()


if __name__ == "__main__":
    train()
