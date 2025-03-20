import datetime
from typing import List

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    import findspark

    findspark.init()

    import os

    os.environ["PYSPARK_PYTHON"] = "python"

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("Local unit tests")
        .getOrCreate()
    )

    return spark


def convert_date_columns_to_string(df: pd.DataFrame):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, datetime.date)).all():
            df[col] = df[col].astype(str)
    return df


def compare_dataframes(
    actual: DataFrame, expected: pd.DataFrame, sort_by: List[str]
):
    return pd.testing.assert_frame_equal(
        convert_date_columns_to_string(actual.toPandas())
        .sort_index(axis=1)
        .sort_values(sort_by)
        .reset_index(drop=True),
        expected.sort_index(axis=1).sort_values(sort_by).reset_index(drop=True),
        check_dtype=False,
    )


def create_empty_dataframe(*columns) -> DataFrame:
    spark = SparkSession.Builder().getOrCreate()

    struct_fields = [StructField(column, StringType(), True) for column in columns]

    schema = StructType(struct_fields)

    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
