import pandas as pd

from src.common.table_loader import (
    get_dates_from_n_months_ago,
    get_dates_from_n_quarters_ago,
    get_dates_from_n_weeks_ago,
    get_dates_from_n_years_ago,
    get_history_from,
    get_n_days_ago,
    get_same_day_from_n_years_ago,
    load_full_history,
    load_last_n_days,
    load_previous_n_months,
)
from tests.conftest import compare_dataframes


def test_load_previous_n_months_with_1_month_ago(spark):
    df = spark.createDataFrame(
        [
            ("ID2", "2023-09-30", "000C", 331),
            ("ID2", "2024-08-01", "000C", 331),
            ("ID2", "2024-08-21", "000C", 331),
            ("ID2", "2024-09-09", "000C", 331),
            ("ID2", "2024-09-21", "000C", 331),
            ("ID2", "2024-09-30", "000C", 331),
            ("ID2", "2024-10-01", "000C", 331),
        ],
        ["numerodecompte", "partitiondate", "devise", "solde"],
    )

    actual = load_previous_n_months(df, partition_date="2024-10-01")

    expected = pd.DataFrame(
        [
            ("2024-09-30",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_load_previous_n_months_with_2_months_ago(spark):
    df = spark.createDataFrame(
        [
            ("ID2", "2023-08-01", "000C", 331),
            ("ID2", "2023-08-23", "000C", 331),
            ("ID2", "2024-08-01", "000C", 331),
            ("ID2", "2024-08-21", "000C", 331),
            ("ID2", "2024-09-09", "000C", 331),
            ("ID2", "2024-09-21", "000C", 331),
            ("ID2", "2024-09-30", "000C", 331),
            ("ID2", "2024-10-01", "000C", 331),
        ],
        ["numerodecompte", "partitiondate", "devise", "solde"],
    )

    actual = load_previous_n_months(df, partition_date="2024-10-01", months=2)

    expected = pd.DataFrame(
        [
            ("2024-08-21",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_table_with_n_days(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2023-08-01"),
            ("ID2", "2023-08-23"),
            ("ID1", "2024-05-01"),
            ("ID2", "2024-05-23"),
            ("ID1", "2024-08-01"),
            ("ID2", "2024-08-21"),
            ("ID2", "2024-07-23"),
            ("ID1", "2024-07-01"),
            ("ID1", "2024-08-21"),
            ("ID2", "2024-09-09"),
            ("ID2", "2024-09-21"),
            ("ID2", "2024-09-30"),
            ("ID2", "2024-10-01"),
            ("ID1", "2024-10-10"),
        ],
        ["tra_num_porteur", "partitiondate"],
    )

    actual = load_last_n_days(df, partition_date="2024-10-10", days=90)

    expected = pd.DataFrame(
        [
            ("ID2", "2024-07-23"),
            ("ID1", "2024-08-01"),
            ("ID2", "2024-08-21"),
            ("ID1", "2024-08-21"),
            ("ID2", "2024-09-09"),
            ("ID2", "2024-09-21"),
            ("ID2", "2024-09-30"),
            ("ID2", "2024-10-01"),
            ("ID1", "2024-10-10"),
        ],
        columns=["tra_num_porteur", "partitiondate"],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_load_table_with_full_history(spark):
    df = spark.createDataFrame(
        [
            ("ID2", "2023-08-01"),
            ("ID2", "2023-08-23"),
            ("ID2", "2024-08-01"),
            ("ID2", "2024-08-21"),
            ("ID2", "2024-09-09"),
            ("ID2", "2024-09-21"),
            ("ID2", "2024-09-30"),
            ("ID2", "2024-10-01"),
            ("ID2", "2024-10-02"),
            ("ID2", "2024-10-03"),
            ("ID2", "2024-10-04"),
            ("ID2", "2024-10-05"),
            ("ID2", "2024-10-06"),
            ("ID2", "2024-10-07"),
            ("ID2", "2024-10-08"),
        ],
        ["numerodecompte", "partitiondate"],
    )

    actual = load_full_history(df, "2024-10-08", "partitiondate")

    expected = pd.DataFrame(
        [
            ("ID2", "2023-08-01"),
            ("ID2", "2023-08-23"),
            ("ID2", "2024-08-01"),
            ("ID2", "2024-08-21"),
            ("ID2", "2024-09-09"),
            ("ID2", "2024-09-21"),
            ("ID2", "2024-09-30"),
            ("ID2", "2024-10-01"),
            ("ID2", "2024-10-02"),
            ("ID2", "2024-10-03"),
            ("ID2", "2024-10-04"),
            ("ID2", "2024-10-05"),
            ("ID2", "2024-10-06"),
            ("ID2", "2024-10-07"),
            ("ID2", "2024-10-08"),
        ],
        columns=[
            "numerodecompte",
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = load_full_history(df, "2024-10-05", "partitiondate")

    expected = pd.DataFrame(
        [
            ("ID2", "2023-08-01"),
            ("ID2", "2023-08-23"),
            ("ID2", "2024-08-01"),
            ("ID2", "2024-08-21"),
            ("ID2", "2024-09-09"),
            ("ID2", "2024-09-21"),
            ("ID2", "2024-09-30"),
            ("ID2", "2024-10-01"),
            ("ID2", "2024-10-02"),
            ("ID2", "2024-10-03"),
            ("ID2", "2024-10-04"),
            ("ID2", "2024-10-05"),
        ],
        columns=[
            "numerodecompte",
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = load_full_history(df, "2024-09-30", "partitiondate")

    expected = pd.DataFrame(
        [
            ("ID2", "2023-08-01"),
            ("ID2", "2023-08-23"),
            ("ID2", "2024-08-01"),
            ("ID2", "2024-08-21"),
            ("ID2", "2024-09-09"),
            ("ID2", "2024-09-21"),
            ("ID2", "2024-09-30"),
        ],
        columns=[
            "numerodecompte",
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_dates_from_n_months_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "ID5",
                "2023-08-30",
            ),
            (
                "ID5",
                "2023-09-30",
            ),
            (
                "ID5",
                None,
            ),
            (
                "ID5",
                "2024-08-01",
            ),
            (
                "ID5",
                "2024-08-21",
            ),
            (
                "ID5",
                "2024-09-09",
            ),
            (
                "ID5",
                "2024-09-09",
            ),
            (
                "ID5",
                None,
            ),
            (
                "ID5",
                "2024-09-21",
            ),
            (
                "ID5",
                "2024-09-29",
            ),
            (
                "ID5",
                "2024-10-01",
            ),
            (
                "ID5",
                "2024-10-01",
            ),
            (
                "ID5",
                "2024-10-12",
            ),
            (
                "ID5",
                None,
            ),
            (
                "ID5",
                "2024-10-16",
            ),
            (
                "ID5",
                "2024-11-16",
            ),
        ],
        ["id", "partitiondate"],
    )

    actual = get_dates_from_n_months_ago(df, partition_date="2024-10-01", months=1)

    expected = pd.DataFrame(
        [
            ("2024-09-29",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_months_ago(df, partition_date="2024-10-01", months=2)

    expected = pd.DataFrame(
        [
            ("2024-08-21",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_months_ago(df, partition_date="2024-10-01", months=0)

    expected = pd.DataFrame(
        [
            ("2024-10-01",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_months_ago(
        df, partition_date="2024-10-12", months=0, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-10-01",),
            ("2024-10-12",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_months_ago(
        df, partition_date="2024-10-01", months=1, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-09-09",),
            ("2024-09-21",),
            ("2024-09-29",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_n_days_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "ID",
                "2023-08-30",
            ),
            (
                "ID",
                "2023-09-30",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-12",
            ),
            (
                "ID",
                "2023-10-11",
            ),
            (
                "ID",
                "2023-10-13",
            ),
            (
                "ID",
                "2023-10-10",
            ),
            (
                "ID",
                "2023-10-14",
            ),
            (
                "ID",
                "2023-10-15",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-08-01",
            ),
            (
                "ID",
                "2024-08-21",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-09-21",
            ),
            (
                "ID",
                "2024-09-29",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-12",
            ),
            (
                "ID",
                "2024-10-11",
            ),
            (
                "ID",
                "2024-10-13",
            ),
            (
                "ID",
                "2024-10-10",
            ),
            (
                "ID",
                "2024-10-14",
            ),
            (
                "ID",
                "2024-10-15",
            ),
            (
                "ID",
                "2024-10-16",
            ),
            (
                "ID",
                "2024-11-16",
            ),
            (
                "ID",
                None,
            ),
        ],
        ["id", "partitiondate"],
    )

    actual = get_n_days_ago(df, partition_date="2024-10-15", days=0)

    expected = pd.DataFrame(
        [
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_n_days_ago(df, partition_date="2024-10-15", days=1)

    expected = pd.DataFrame(
        [
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_n_days_ago(df, partition_date="2024-10-15", days=2)

    expected = pd.DataFrame(
        [
            ("2024-10-13",),
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_n_days_ago(df, partition_date="2024-10-15", days=5)

    expected = pd.DataFrame(
        [
            ("2024-10-10",),
            ("2024-10-11",),
            ("2024-10-12",),
            ("2024-10-13",),
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_dates_from_n_weeks_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "ID",
                "2023-08-30",
            ),
            (
                "ID",
                "2023-09-30",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-12",
            ),
            (
                "ID",
                "2023-10-11",
            ),
            (
                "ID",
                "2023-10-13",
            ),
            (
                "ID",
                "2023-10-10",
            ),
            (
                "ID",
                "2023-10-14",
            ),
            (
                "ID",
                "2023-10-15",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-08-01",
            ),
            (
                "ID",
                "2024-08-21",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-09-21",
            ),
            (
                "ID",
                "2024-09-28",
            ),
            (
                "ID",
                "2024-09-29",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-12",
            ),
            (
                "ID",
                "2024-10-11",
            ),
            (
                "ID",
                "2024-10-13",
            ),
            (
                "ID",
                "2024-10-10",
            ),
            (
                "ID",
                "2024-10-14",
            ),
            (
                "ID",
                "2024-10-15",
            ),
            (
                "ID",
                "2024-10-16",
            ),
            (
                "ID",
                "2024-11-16",
            ),
            (
                "ID",
                None,
            ),
        ],
        ["id", "partitiondate"],
    )

    actual = get_dates_from_n_weeks_ago(df, partition_date="2024-10-15", weeks=0)

    expected = pd.DataFrame(
        [
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_weeks_ago(
        df, partition_date="2024-10-15", weeks=1, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-10-10",),
            ("2024-10-11",),
            ("2024-10-12",),
            ("2024-10-13",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_weeks_ago(
        df, partition_date="2024-10-15", weeks=0, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_weeks_ago(df, partition_date="2024-10-15", weeks=3)

    expected = pd.DataFrame(
        [
            ("2024-09-29",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_dates_from_n_quarters_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "ID",
                "2023-08-30",
            ),
            (
                "ID",
                "2023-09-30",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-12",
            ),
            (
                "ID",
                "2023-10-11",
            ),
            (
                "ID",
                "2023-10-13",
            ),
            (
                "ID",
                "2023-10-10",
            ),
            (
                "ID",
                "2023-10-14",
            ),
            (
                "ID",
                "2023-10-15",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-08-01",
            ),
            (
                "ID",
                "2024-08-21",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-09-21",
            ),
            (
                "ID",
                "2024-09-28",
            ),
            (
                "ID",
                "2024-09-29",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-12",
            ),
            (
                "ID",
                "2024-10-11",
            ),
            (
                "ID",
                "2024-10-13",
            ),
            (
                "ID",
                "2024-10-10",
            ),
            (
                "ID",
                "2024-10-14",
            ),
            (
                "ID",
                "2024-10-15",
            ),
            (
                "ID",
                "2024-10-16",
            ),
            (
                "ID",
                "2024-11-16",
            ),
            (
                "ID",
                None,
            ),
        ],
        ["id", "partitiondate"],
    )

    actual = get_dates_from_n_quarters_ago(
        df, partition_date="2024-10-15", quarters=0, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-10-01",),
            ("2024-10-10",),
            ("2024-10-11",),
            ("2024-10-12",),
            ("2024-10-13",),
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_quarters_ago(
        df, partition_date="2024-10-15", quarters=0
    )

    expected = pd.DataFrame(
        [
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_quarters_ago(
        df, partition_date="2024-10-15", quarters=1
    )

    expected = pd.DataFrame(
        [
            ("2024-09-29",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_quarters_ago(
        df, partition_date="2024-10-15", quarters=1, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-08-01",),
            ("2024-08-21",),
            ("2024-09-09",),
            ("2024-09-21",),
            ("2024-09-28",),
            ("2024-09-29",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_dates_from_n_years_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "ID",
                "2023-08-30",
            ),
            (
                "ID",
                "2023-09-30",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-12",
            ),
            (
                "ID",
                "2023-10-11",
            ),
            (
                "ID",
                "2023-10-13",
            ),
            (
                "ID",
                "2023-10-10",
            ),
            (
                "ID",
                "2023-10-14",
            ),
            (
                "ID",
                "2023-10-16",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-08-01",
            ),
            (
                "ID",
                "2024-08-21",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-09-21",
            ),
            (
                "ID",
                "2024-09-28",
            ),
            (
                "ID",
                "2024-09-29",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-12",
            ),
            (
                "ID",
                "2024-10-11",
            ),
            (
                "ID",
                "2024-10-13",
            ),
            (
                "ID",
                "2024-10-10",
            ),
            (
                "ID",
                "2024-10-14",
            ),
            (
                "ID",
                "2024-10-15",
            ),
            (
                "ID",
                "2024-10-16",
            ),
            (
                "ID",
                "2024-11-16",
            ),
            (
                "ID",
                None,
            ),
        ],
        ["id", "partitiondate"],
    )

    actual = get_dates_from_n_years_ago(df, partition_date="2024-10-15", years=0)

    expected = pd.DataFrame(
        [
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_years_ago(df, partition_date="2024-10-15", years=1)

    expected = pd.DataFrame(
        [
            ("2023-10-16",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_dates_from_n_years_ago(
        df, partition_date="2024-10-15", years=0, full_period=True
    )

    expected = pd.DataFrame(
        [
            ("2024-08-01",),
            ("2024-08-21",),
            ("2024-09-09",),
            ("2024-09-21",),
            ("2024-09-28",),
            ("2024-09-29",),
            ("2024-10-01",),
            ("2024-10-12",),
            ("2024-10-11",),
            ("2024-10-13",),
            ("2024-10-10",),
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_same_day_from_n_years_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "ID",
                "2022-01-14",
            ),
            (
                "ID",
                "2022-10-14",
            ),
            (
                "ID",
                "2022-10-15",
            ),
            (
                "ID",
                "2023-08-30",
            ),
            (
                "ID",
                "2023-09-30",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-01",
            ),
            (
                "ID",
                "2023-10-12",
            ),
            (
                "ID",
                "2023-10-11",
            ),
            (
                "ID",
                "2023-10-13",
            ),
            (
                "ID",
                "2023-10-10",
            ),
            (
                "ID",
                "2023-10-14",
            ),
            (
                "ID",
                "2023-10-15",
            ),
            (
                "ID",
                "2023-10-16",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-08-01",
            ),
            (
                "ID",
                "2024-08-21",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                "2024-09-09",
            ),
            (
                "ID",
                None,
            ),
            (
                "ID",
                "2024-09-21",
            ),
            (
                "ID",
                "2024-09-28",
            ),
            (
                "ID",
                "2024-09-29",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-01",
            ),
            (
                "ID",
                "2024-10-12",
            ),
            (
                "ID",
                "2024-10-11",
            ),
            (
                "ID",
                "2024-10-13",
            ),
            (
                "ID",
                "2024-10-10",
            ),
            (
                "ID",
                "2024-10-14",
            ),
            (
                "ID",
                "2024-10-15",
            ),
            (
                "ID",
                "2024-10-16",
            ),
            (
                "ID",
                "2024-11-16",
            ),
            (
                "ID",
                None,
            ),
        ],
        ["id", "partitiondate"],
    )

    actual = get_same_day_from_n_years_ago(df, partition_date="2024-10-15", years=0)

    expected = pd.DataFrame(
        [
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_same_day_from_n_years_ago(df, partition_date="2024-10-15", years=1)

    expected = pd.DataFrame(
        [
            ("2023-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_same_day_from_n_years_ago(df, partition_date="2024-10-15", years=2)

    expected = pd.DataFrame(
        [
            ("2022-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_get_history_from(spark):
    df = spark.createDataFrame(
        [
            ("2022-01-14",),
            ("2022-10-14",),
            ("2022-10-15",),
            ("2023-08-30",),
            ("2023-09-30",),
            ("2023-10-01",),
            ("2023-10-01",),
            ("2023-10-12",),
            ("2023-10-11",),
            ("2023-10-13",),
            ("2023-10-10",),
            ("2023-10-14",),
            ("2023-10-15",),
            ("2023-10-16",),
            (None,),
            ("2024-08-01",),
            ("2024-08-21",),
            ("2024-09-09",),
            ("2024-09-09",),
            (None,),
            ("2024-09-21",),
            ("2024-09-28",),
            ("2024-09-29",),
            ("2024-10-01",),
            ("2024-10-01",),
            ("2024-10-12",),
            ("2024-10-11",),
            ("2024-10-13",),
            ("2024-10-10",),
            ("2024-10-14",),
            ("2024-10-15",),
            (None,),
            ("2024-10-16",),
        ],
        ["partitiondate"],
    )

    actual = get_history_from(
        df,
        partition_date="2024-10-15",
        days=2,
        weeks=1,
        quarters=1,
        months=1,
        years=2,
        year_to_year=1,
        full_period=False,
    )

    expected = pd.DataFrame(
        [
            ("2022-10-15",),
            ("2023-10-15",),
            ("2023-10-16",),
            ("2024-09-29",),
            ("2024-10-13",),
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    actual = get_history_from(
        df,
        partition_date="2024-10-15",
        days=2,
        weeks=1,
        quarters=1,
        months=1,
        years=1,
        year_to_year=1,
        full_period=True,
    )

    expected = pd.DataFrame(
        [
            ("2023-08-30",),
            ("2023-09-30",),
            ("2023-10-01",),
            ("2023-10-01",),
            ("2023-10-10",),
            ("2023-10-11",),
            ("2023-10-12",),
            ("2023-10-13",),
            ("2023-10-14",),
            ("2023-10-15",),
            ("2023-10-16",),
            ("2024-08-01",),
            ("2024-08-21",),
            ("2024-09-09",),
            ("2024-09-09",),
            ("2024-09-21",),
            ("2024-09-28",),
            ("2024-09-29",),
            ("2024-10-01",),
            ("2024-10-01",),
            ("2024-10-10",),
            ("2024-10-11",),
            ("2024-10-12",),
            ("2024-10-13",),
            ("2024-10-14",),
            ("2024-10-15",),
        ],
        columns=[
            "partitiondate",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])
