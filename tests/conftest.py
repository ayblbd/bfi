from typing import List

import pandas as pd
import pytest
from pyspark.sql import DataFrame


@pytest.fixture(scope="session")
def spark():
    import findspark

    findspark.init()

    import os

    os.environ["PYSPARK_PYTHON"] = "python"

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("CDM BFI v0.1")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.1",
        )
        .config(
            "spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "./.warehouse")
        .getOrCreate()
    )

    return spark


def compare_dataframes(actual: DataFrame, expected: pd.DataFrame, sort_by: List[str]):
    return pd.testing.assert_frame_equal(
        actual.toPandas()
        .sort_index(axis=1)
        .sort_values(sort_by)
        .reset_index(drop=True),
        expected.sort_index(axis=1).sort_values(sort_by).reset_index(drop=True),
        check_dtype=False,
    )
