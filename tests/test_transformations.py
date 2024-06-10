import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    add_months,
    col,
    current_timestamp,
    date_add,
    date_format,
    dense_rank,
    expr,
    lag,
    last,
    last_day,
    lead,
    lit,
    max,
    month,
    rank,
    row_number,
    to_date,
    when,
    year,
)

from tests.conftest import compare_dataframes


def get_last_recorded_sold_from_last_month(df):
    result = (
        df.withColumn(
            "rank", dense_rank().over(Window.partitionBy("account_id").orderBy(year("solde_date"), month("solde_date")))
        )
        .withColumn("prev_solde", lag("solde", 1).over(Window.partitionBy("account_id").orderBy("solde_date")))
        .withColumn("rank_diff", lag("rank", 1).over(Window.partitionBy("account_id").orderBy("solde_date")))
        .withColumn(
            "last_month_solde",
            last(when(col("rank") != col("rank_diff"), col("prev_solde")), ignorenulls=True).over(
                Window.partitionBy("account_id")
                .orderBy("solde_date")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    result.show(truncate=False)
    return result.drop("rank", "prev_solde", "rank_diff")


def get_last_recorded_sold_from_last_monthV2(df):
    window = Window.partitionBy("account_id").orderBy("solde_date")
    return (
        df.withColumn("rank", date_format("solde_date", "yyyyMM"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_solde", lag("solde").over(window))
        .withColumn(
            "last_month_solde",
            last(when(col("rank") != col("rank_diff"), col("prev_solde")), ignorenulls=True).over(
                Window.partitionBy("account_id")
                .orderBy("solde_date")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .drop("rank", "prev_solde", "rank_diff")
    )


def test_get_last_recorded_sold_from_last_month(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-20", 101),
            ("ID1", "2022-01-21", 101),
            ("ID1", "2022-01-22", 101),
            ("ID1", "2022-01-23", 101),
            ("ID1", "2022-01-27", 101),
            ("ID1", "2022-01-29", 100),
            ("ID1", "2022-02-04", 103),
            ("ID1", "2022-02-05", 200),
            ("ID1", "2022-03-27", 101),
            ("ID2", "2022-03-31", 300),
            ("ID2", "2022-02-01", 400),
        ],
        ["account_id", "solde_date", "solde"],
    )

    actual = get_last_recorded_sold_from_last_monthV2(df)

    expected = pd.DataFrame(
        [
            ("ID1", "2022-01-20", 101, None),
            ("ID1", "2022-01-21", 101, None),
            ("ID1", "2022-01-22", 101, None),
            ("ID1", "2022-01-23", 101, None),
            ("ID1", "2022-01-27", 101, None),
            ("ID1", "2022-01-29", 100, None),
            ("ID1", "2022-02-04", 103, 100),
            ("ID1", "2022-02-05", 200, 100),
            ("ID1", "2022-03-27", 101, 200),
            ("ID2", "2022-02-01", 400, None),
            ("ID2", "2022-03-31", 300, 400),
        ],
        columns=[
            "account_id",
            "solde_date",
            "solde",
            "last_month_solde",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["account_id", "solde_date"])


def get_last_recorded_sold_from_last_day(df):
    return df.withColumn(
        "last_day_solde",
        lag(col("solde")).over(Window.partitionBy("account_id").orderBy("solde_date")),
    )


def test_get_last_recorded_sold_from_last_day(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-20", 120),
            ("ID1", "2022-01-21", 121),
            ("ID1", "2022-01-22", 122),
            ("ID1", "2022-01-23", 123),
            ("ID1", "2022-01-27", 127),
            ("ID1", "2022-01-29", 129),
            ("ID1", "2022-02-04", 204),
            ("ID1", "2022-02-05", 205),
            ("ID1", "2022-03-27", 327),
            ("ID2", "2022-02-01", 201),
            ("ID2", "2022-03-31", 331),
        ],
        ["account_id", "solde_date", "solde"],
    )

    actual = get_last_recorded_sold_from_last_day(df)

    expected = pd.DataFrame(
        [
            ("ID1", "2022-01-20", 120, None),
            ("ID1", "2022-01-21", 121, 120),
            ("ID1", "2022-01-22", 122, 121),
            ("ID1", "2022-01-23", 123, 122),
            ("ID1", "2022-01-27", 127, 123),
            ("ID1", "2022-01-29", 129, 127),
            ("ID1", "2022-02-04", 204, 129),
            ("ID1", "2022-02-05", 205, 204),
            ("ID1", "2022-03-27", 327, 205),
            ("ID2", "2022-02-01", 201, None),
            ("ID2", "2022-03-31", 331, 201),
        ],
        columns=[
            "account_id",
            "solde_date",
            "solde",
            "last_day_solde",
        ],
    )

    actual.show(truncate=False)

    compare_dataframes(actual, expected, sort_by=["account_id", "solde_date"])


def get_last_recorded_sold_from_last_year(df):
    window = Window.partitionBy("account_id").orderBy("solde_date")
    return (
        df.withColumn("rank", year("solde_date"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_solde", lag("solde").over(window))
        .withColumn(
            "last_year_solde",
            last(when(col("rank") != col("rank_diff"), col("prev_solde")), ignorenulls=True).over(
                Window.partitionBy("account_id")
                .orderBy("solde_date")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .drop("rank", "prev_solde", "rank_diff")
    )


def test_get_last_recorded_sold_from_last_year(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2021-01-20", 120),
            ("ID1", "2021-01-21", 121),
            ("ID1", "2022-01-22", 122),
            ("ID1", "2022-01-23", 123),
            ("ID1", "2022-01-27", 127),
            ("ID1", "2022-01-29", 129),
            ("ID1", "2023-02-04", 204),
            ("ID1", "2023-02-05", 205),
            ("ID1", "2023-03-27", 327),
            ("ID2", "2022-02-01", 201),
            ("ID2", "2023-03-31", 331),
        ],
        ["account_id", "solde_date", "solde"],
    )

    actual = get_last_recorded_sold_from_last_year(df)

    expected = pd.DataFrame(
        [
            ("ID1", "2021-01-20", 120, None),
            ("ID1", "2021-01-21", 121, None),
            ("ID1", "2022-01-22", 122, 121),
            ("ID1", "2022-01-23", 123, 121),
            ("ID1", "2022-01-27", 127, 121),
            ("ID1", "2022-01-29", 129, 121),
            ("ID1", "2023-02-04", 204, 129),
            ("ID1", "2023-02-05", 205, 129),
            ("ID1", "2023-03-27", 327, 129),
            ("ID2", "2022-02-01", 201, None),
            ("ID2", "2023-03-31", 331, 201),
        ],
        columns=[
            "account_id",
            "solde_date",
            "solde",
            "last_year_solde",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["account_id", "solde_date"])
