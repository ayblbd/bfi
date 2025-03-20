import pandas as pd
from pyspark.sql.functions import col, lit

from src.transformation.common import (
    filter_on_max_partition_date,
    get_first_day_of_week,
    get_is_during_a_period_for_equipement,
    get_last_day_of_week,
    get_last_recorded_sold_per_client_from,
    get_last_recorded_sold_per_client_from_last_month,
    is_three_months_ago,
    map_account_balance,
    build_day_holiday,
)
from tests.conftest import compare_dataframes


def test_get_last_recorded_sold_per_client_from(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2021-01-20", "000C", 120),
            ("ID1", "2021-01-21", "000C", 121),
            ("ID1", "2022-01-22", "000C", 122),
            ("ID1", "2022-01-23", "000C", 123),
            ("ID1", "2022-01-27", "000C", 127),
            ("ID2", "2022-01-29", "000C", 129),
            ("ID2", "2023-02-04", "000C", 204),
            ("ID2", "2023-02-05", "000C", 205),
            ("ID2", "2023-03-27", "000C", 327),
            ("ID2", "2021-04-22", "001C", 422),
            ("ID2", "2022-04-27", "001C", 427),
            ("ID3", "2022-05-27", "001C", 527),
            ("ID3", "2022-02-01", "000C", 201),
            ("ID3", "2023-03-31", "000C", 331),
        ],
        ["numerodecompte", "dttrtm", "devise", "solde"],
    )

    actual = get_last_recorded_sold_per_client_from(df, by="solde", days=1)

    expected = pd.DataFrame(
        [
            ("ID1", "2021-01-20", "000C", 120, None),
            ("ID1", "2021-01-21", "000C", 121, 120),
            ("ID1", "2022-01-22", "000C", 122, 121),
            ("ID1", "2022-01-23", "000C", 123, 122),
            ("ID1", "2022-01-27", "000C", 127, 123),
            ("ID2", "2022-01-29", "000C", 129, None),
            ("ID2", "2023-02-04", "000C", 204, 129),
            ("ID2", "2023-02-05", "000C", 205, 204),
            ("ID2", "2023-03-27", "000C", 327, 205),
            ("ID2", "2021-04-22", "001C", 422, None),
            ("ID2", "2022-04-27", "001C", 427, 422),
            ("ID3", "2022-05-27", "001C", 527, None),
            ("ID3", "2022-02-01", "000C", 201, None),
            ("ID3", "2023-03-31", "000C", 331, 201),
        ],
        columns=[
            "numerodecompte",
            "dttrtm",
            "devise",
            "solde",
            "last_day_solde",
        ],
    )

    compare_dataframes(
        actual,
        expected,
        sort_by=[
            "numerodecompte",
            "dttrtm",
            "devise",
        ],
    )


def test_get_last_recorded_sold_per_client_from_last_month(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2024-01-22", "001C", 122),
            ("ID1", "2024-02-27", "001C", 227),
            ("ID1", "2024-03-31", "001C", 331),
            ("ID2", "2024-02-01", "000C", 201),
            ("ID2", "2024-03-31", "000C", 331),
            ("ID3", "2022-01-29", "000C", 129),
            ("ID3", "2023-02-04", "000C", 204),
            ("ID3", "2024-02-05", "000C", 205),
            ("ID3", "2024-03-31", "000C", 331),
        ],
        ["numerodecompte", "dttrtm", "devise", "solde"],
    )

    actual = get_last_recorded_sold_per_client_from_last_month(
        df, by="solde", partition_date="2024-03-31"
    )

    expected = pd.DataFrame(
        [
            ("ID1", "2024-01-22", "001C", 122, None),
            ("ID1", "2024-02-27", "001C", 227, None),
            ("ID1", "2024-03-31", "001C", 331, 227),
            ("ID2", "2024-02-01", "000C", 201, None),
            ("ID2", "2024-03-31", "000C", 331, 201),
            ("ID3", "2022-01-29", "000C", 129, None),
            ("ID3", "2023-02-04", "000C", 204, None),
            ("ID3", "2024-02-05", "000C", 205, None),
            ("ID3", "2024-03-31", "000C", 331, 205),
        ],
        columns=[
            "numerodecompte",
            "dttrtm",
            "devise",
            "solde",
            "last_month_solde",
        ],
    )

    compare_dataframes(
        actual,
        expected,
        sort_by=[
            "numerodecompte",
            "dttrtm",
            "devise",
        ],
    )


def test_get_last_recorded_sold_per_client_from_last_month_with_2_months(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2023-01-22", "001C", 122),
            ("ID1", "2024-01-22", "001C", 122),
            ("ID1", "2024-02-27", "001C", 227),
            ("ID1", "2024-03-31", "001C", 331),
            ("ID2", "2024-01-01", "000C", 101),
            ("ID2", "2024-01-02", "000C", 102),
            ("ID2", "2024-02-01", "000C", 201),
            ("ID2", "2024-03-31", "000C", 331),
            ("ID3", "2022-01-29", "000C", 129),
            ("ID3", "2024-01-04", "000C", 104),
            ("ID3", "2024-02-05", "000C", 205),
            ("ID3", "2024-03-31", "000C", 327),
        ],
        ["numerodecompte", "dttrtm", "devise", "solde"],
    )

    actual = get_last_recorded_sold_per_client_from_last_month(
        df, by="solde", partition_date="2024-03-31", months=2
    )

    expected = pd.DataFrame(
        [
            ("ID1", "2023-01-22", "001C", 122, None),
            ("ID1", "2024-01-22", "001C", 122, None),
            ("ID1", "2024-02-27", "001C", 227, 122),
            ("ID1", "2024-03-31", "001C", 331, 122),
            ("ID2", "2024-01-01", "000C", 101, None),
            ("ID2", "2024-01-02", "000C", 102, None),
            ("ID2", "2024-02-01", "000C", 201, 102),
            ("ID2", "2024-03-31", "000C", 331, 102),
            ("ID3", "2022-01-29", "000C", 129, None),
            ("ID3", "2024-01-04", "000C", 104, None),
            ("ID3", "2024-02-05", "000C", 205, 104),
            ("ID3", "2024-03-31", "000C", 327, 104),
        ],
        columns=[
            "numerodecompte",
            "dttrtm",
            "devise",
            "solde",
            "last_2_months_solde",
        ],
    )

    compare_dataframes(
        actual,
        expected,
        sort_by=[
            "numerodecompte",
            "dttrtm",
            "devise",
        ],
    )


def test_map_account_balance(spark):
    df = spark.createDataFrame(
        [
            (5, 3, "AA", 1),
            (None, 3, "AA", 2),
            (5, None, "AA", 3),
            (None, None, "AA", 4),
            (7, 0, "AA", 5),
            (None, 3, "AP", 6),
            (5, None, "AP", 7),
            (None, None, "AP", 8),
            (0, None, "AP", 9),
            (5, 3, "PP", 10),
            (None, 3, "PP", 11),
            (5, None, "PP", 12),
            (None, None, "PP", 13),
        ],
        ["debit", "credit", "sens_mapping", "id"],
    )

    actual = df.transform(
        map_account_balance, column_debit="debit", column_credit="credit"
    )

    expected = pd.DataFrame(
        [
            (2, None, "AA", 1),
            (-3, None, "AA", 2),
            (5, None, "AA", 3),
            (0, None, "AA", 4),
            (7, None, "AA", 5),
            (None, 3, "AP", 6),
            (5, None, "AP", 7),
            (None, None, "AP", 8),
            (0, None, "AP", 9),
            (None, -2, "PP", 10),
            (None, 3, "PP", 11),
            (None, -5, "PP", 12),
            (None, 0, "PP", 13),
        ],
        columns=["debit", "credit", "sens_mapping", "id"],
    )

    compare_dataframes(
        actual, expected, sort_by=["id", "debit", "credit", "sens_mapping"]
    )


def test_get_is_during_a_period_for_equipement(spark):
    df = spark.createDataFrame(
        [
            ("id1", "2024-11-25", "2024-11-26"),
            ("id2", "2024-11-20", "2024-11-26"),
            ("id3", "2024-11-15", "2024-11-26"),
            ("id4", "2024-10-28", "2024-11-26"),
            ("id5", "2024-09-12", "2024-11-26"),
            ("id6", "2024-01-02", "2024-11-26"),
            ("id7", "2023-08-02", "2024-11-26"),
        ],
        ["id", "date_souscription", "partitiondate"],
    )

    actual = get_is_during_a_period_for_equipement(
        df,
        by="date_souscription",
        name="souscrit",
        partition_date="2024-11-26",
    )

    expected = pd.DataFrame(
        [
            (
                "id1",
                "2024-11-25",
                "2024-11-26",
                True,
                False,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
            ),
            (
                "id2",
                "2024-11-20",
                "2024-11-26",
                False,
                True,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
            ),
            (
                "id3",
                "2024-11-15",
                "2024-11-26",
                False,
                False,
                True,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
            ),
            (
                "id4",
                "2024-10-28",
                "2024-11-26",
                False,
                False,
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ),
            (
                "id5",
                "2024-09-12",
                "2024-11-26",
                False,
                False,
                False,
                False,
                False,
                True,
                False,
                True,
                True,
                False,
            ),
            (
                "id6",
                "2024-01-02",
                "2024-11-26",
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
                False,
            ),
            (
                "id7",
                "2023-08-02",
                "2024-11-26",
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
            ),
        ],
        columns=[
            "id",
            "date_souscription",
            "partitiondate",
            "is_souscrit_s",
            "is_souscrit_s_1",
            "is_souscrit_s_2",
            "is_souscrit_m",
            "is_souscrit_m_1",
            "is_souscrit_m_2",
            "is_souscrit_t",
            "is_souscrit_t_1",
            "is_souscrit_y",
            "is_souscrit_y_1",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["id"])


def test_get_last_day_of_week(spark):
    df = spark.createDataFrame(
        [
            ("2024-12-01",),
            ("2024-12-02",),
            ("2024-12-03",),
            ("2024-12-04",),
            ("2024-12-05",),
            ("2024-12-06",),
            ("2024-12-07",),
            ("2024-12-08",),
            ("2024-12-09",),
        ],
        ["date_souscription"],
    )

    actual = df.withColumn(
        "last_day_of_last_week", get_last_day_of_week(col("date_souscription"))
    )

    expected = pd.DataFrame(
        [
            ("2024-12-01", "2024-12-01"),
            ("2024-12-02", "2024-12-08"),
            ("2024-12-03", "2024-12-08"),
            ("2024-12-04", "2024-12-08"),
            ("2024-12-05", "2024-12-08"),
            ("2024-12-06", "2024-12-08"),
            ("2024-12-07", "2024-12-08"),
            ("2024-12-08", "2024-12-08"),
            ("2024-12-09", "2024-12-15"),
        ],
        columns=["date_souscription", "last_day_of_last_week"],
    )

    compare_dataframes(actual, expected, sort_by=["date_souscription"])


def test_get_first_day_of_week(spark):
    df = spark.createDataFrame(
        [
            ("2024-12-01",),
            ("2024-12-02",),
            ("2024-12-03",),
            ("2024-12-04",),
            ("2024-12-05",),
            ("2024-12-06",),
            ("2024-12-07",),
            ("2024-12-08",),
            ("2024-12-09",),
        ],
        ["date_souscription"],
    )

    actual = df.withColumn(
        "first_day_of_last_week", get_first_day_of_week(col("date_souscription"))
    )

    expected = pd.DataFrame(
        [
            ("2024-12-01", "2024-11-25"),
            ("2024-12-02", "2024-12-02"),
            ("2024-12-03", "2024-12-02"),
            ("2024-12-04", "2024-12-02"),
            ("2024-12-05", "2024-12-02"),
            ("2024-12-06", "2024-12-02"),
            ("2024-12-07", "2024-12-02"),
            ("2024-12-08", "2024-12-02"),
            ("2024-12-09", "2024-12-09"),
        ],
        columns=["date_souscription", "first_day_of_last_week"],
    )

    compare_dataframes(actual, expected, sort_by=["date_souscription"])


def test_is_three_months_ago(spark):
    df = spark.createDataFrame(
        [
            (
                "2024-01-01",
                "2024-01-02",
            ),
            (
                "2024-02-02",
                "2024-01-02",
            ),
            (
                "2024-03-03",
                "2024-01-02",
            ),
            (
                "2024-04-04",
                "2024-01-04",
            ),
            (
                "2024-05-05",
                None,
            ),
            (
                "2024-06-06",
                "2023-06-06",
            ),
            (
                "2024-07-07",
                "2025-07-13",
            ),
            (
                None,
                None,
            ),
        ],
        ["start_date", "older_date"],
    )

    actual = df.withColumn(
        "is_three_months_ago",
        is_three_months_ago(col("start_date"), col("older_date")),
    )

    expected = pd.DataFrame(
        [
            ("2024-01-01", "2024-01-02", False),
            ("2024-02-02", "2024-01-02", True),
            ("2024-03-03", "2024-01-02", True),
            ("2024-04-04", "2024-01-04", True),
            ("2024-05-05", None, None),
            ("2024-06-06", "2023-06-06", False),
            ("2024-07-07", "2025-07-13", False),
            (None, None, None),
        ],
        columns=["start_date", "older_date", "is_three_months_ago"],
    )

    compare_dataframes(actual, expected, sort_by=["start_date"])


def test_filter_on_max_partition_date(spark):
    df = spark.createDataFrame(
        [
            (None,),
            ("2025-01-01",),
            ("2025-01-02",),
            ("2024-10-01",),
        ],
        ["partitiondate"],
    )

    actual = df.transform(filter_on_max_partition_date)

    expected = pd.DataFrame(
        [
            ("2025-01-02",),
        ],
        columns=["partitiondate"],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])

    df = spark.createDataFrame(
        [
            ("",),
        ],
        ["partitiondate"],
    ).withColumn("partitiondate", lit(None).cast("string"))

    actual = df.transform(filter_on_max_partition_date)

    expected = pd.DataFrame(
        [],
        columns=["partitiondate"],
    )

    compare_dataframes(actual, expected, sort_by=["partitiondate"])


def test_build_day_holiday(spark):
    df = spark.createDataFrame(
        [
            ("2025-01-16", "1"),
            ("2025-01-18", "2"),
            ("2025-01-17", "3"),
            ("2025-01-20", "4"),
            ("2025-01-31", "5"),
        ],
        ["partitiondate", "id"],
    )

    joursferie = spark.createDataFrame(
        [
            ("2025-01-17",),
            ("2025-01-20",),
        ],
        ["jourferie"],
    )
    partitiondate = "partitiondate"

    actual = build_day_holiday(df, joursferie, partitiondate, 1)

    expected = pd.DataFrame(
        [
            ("2025-01-15", "1"),
            ("2025-01-16", "2"),
            ("2025-01-16", "3"),
            ("2025-01-16", "4"),
            ("2025-01-30", "5"),
        ],
        columns=["partitiondate", "id"],
    )
    compare_dataframes(actual, expected, sort_by=["id"])