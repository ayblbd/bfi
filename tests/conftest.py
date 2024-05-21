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

    spark = SparkSession.builder.master("local[1]").appName("Fraude v0.1").getOrCreate()

    return spark


def compare_dataframes(actual: DataFrame, expected: pd.DataFrame):
    return pd.testing.assert_frame_equal(
        actual.toPandas()
        .sort_index(axis=1)
        .sort_values(by=["user_id", "audit_fact_date"])
        .reset_index(drop=True),
        expected.sort_index(axis=1)
        .sort_values(by=["user_id", "audit_fact_date"])
        .reset_index(drop=True),
        check_dtype=False,
    )
