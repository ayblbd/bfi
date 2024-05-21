import configparser
from datetime import datetime
from unittest.mock import mock_open, patch

import pandas as pd
import pytest
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

from src.utils import (
    avg_daily_cases,
    avg_frauded_users_per_day,
    clean_dataframe,
    create_spark_session,
    extract_field_from_message,
    extract_field_with_brackets_from_message,
    get_config,
    get_max_audit_date,
    get_optimal_thr,
    is_ip,
    select_full_with_history,
    select_last,
    select_last_with_history,
    select_live,
    to_csv,
    to_hive,
    to_orc,
    to_parquet,
    to_unix,
)
from tests.conftest import compare_dataframes


def test_create_spark_session():
    config = [
        ("spark.app.name", "Fraud Detection"),
        ("spark.sql.hive.convertMetastoreOrc", "False"),
        ("spark.sql.shuffle.partitions", "1000"),
        ("tez.queue.name", "analytics"),
    ]
    spark = create_spark_session(config)

    assert spark.conf.get("spark.app.name") == "Fraud Detection"
    assert spark.conf.get("spark.sql.hive.convertMetastoreOrc") == "False"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "1000"
    assert spark.conf.get("tez.queue.name") == "analytics"


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="[DEFAULT]\ntest = 1\nurl = hdfs://path/to/file.zip",
)
def test_get_config(mocker):
    actual = get_config()
    expected = configparser.ConfigParser()
    expected.read_dict({"DEFAULT": {"test": "1", "url": "hdfs://path/to/file.zip"}})
    assert actual == expected
    mocker.assert_called_once_with("config.ini")


def test_clean_dataframe(spark):
    df = spark.createDataFrame(
        [
            ("0", "1", "2022-01-01 00:00:00", "message", "WRONG EVENT TYPE"),
            ("1", "1", "2022-01-01 00:00:00", "message", ""),  # Duplicate id
            ("1", "1", "2022-01-01 00:00:01", "message", "1"),  # Duplicate id
            ("2", "2", "2022-01-01 00:00:02", "message", "EFFECTUER_VIREMENT"),
            ("3", "3", "2022-01-01 00:00:03", "message", "MAJ_INTERVENANT"),
            ("4", "4", "2022-01-01 00:00:04", "message", "SUPPRIME_BENEFICIAIRE"),
            ("5", "5", "2022-01-01 00:00:05", "message", "MISE_A_DISPOSITION"),
            ("6", "6", "2022-01-01 00:00:06", "message", "ACTIVATION_ATTIJARI_SECURITE"),
            ("7", "7", "2022-01-01 00:00:07", "message", "DESACTIVATION_ATTIJARI_SECURITE"),
            ("8", "8", "2022-01-01 00:00:08", "message", "LOGIN"),
            ("9", "9", "2022-01-01 00:00:09", "message", "RESET_PASSWORD_TOKEN_SENT_SUCCESS"),
            ("10", "10", "2022-01-01 00:00:10", "facture", "EFFECTUER_PAIEMENT"),
            ("11", "11", "2022-01-01 00:00:10", "Detail de la reference wafaCash", "EFFECTUER_CASH_EXPRESS"),
            ("12", "12", "2022-01-01 00:00:11", "CashExpress benef", "EFFECTUER_CASH_EXPRESS"),
            ("13", "13", None, "message", "A"),
            ("14", "14", "2022-01-01 00:00:01", "message", "AJOUTER_BENEFICIAIRE"),
        ],
        ["id", "user_id", "audit_fact_date", "audit_message", "event_type"],
    )

    actual = clean_dataframe(df)

    expected = pd.DataFrame(
        [
            (2, "2", "2022-01-01 00:00:02", "message", "2"),
            (3, "3", "2022-01-01 00:00:03", "message", "3"),
            (5, "5", "2022-01-01 00:00:05", "message", "5"),
            (6, "6", "2022-01-01 00:00:06", "message", "6"),
            (7, "7", "2022-01-01 00:00:07", "message", "7"),
            (8, "8", "2022-01-01 00:00:08", "message", "8"),
            (9, "9", "2022-01-01 00:00:09", "message", "9"),
            (10, "10", "2022-01-01 00:00:10", "facture", "A"),
            (12, "12", "2022-01-01 00:00:11", "CashExpress benef", "B"),
            (14, "14", "2022-01-01 00:00:01", "message", "1"),

        ],
        columns=["id", "user_id", "audit_fact_date", "audit_message", "event_type"],
    )

    compare_dataframes(actual, expected)


def test_get_max_audit_date(spark):
    df = spark.createDataFrame(
        [
            ("0", "2022-01-01 00:00:00"),
            ("1", "2022-01-01 00:00:00"),
            ("2", "2022-01-01 00:00:02"),
            ("3", "2022-01-01 00:00:03"),
            ("4", "2022-01-01 00:00:04"),
        ],
        ["user_id", "audit_fact_date"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = get_max_audit_date(df)

    expected = int(datetime(2022, 1, 1, 0, 0, 4).timestamp())

    assert actual == expected


def test_avg_daily_cases_normal(spark):
    df = spark.createDataFrame(
        [
            (1660647600, 5),
            (1660567602, 2),
            (1660567605, 1),
            (1660487609, 3),
            (1660487603, 4),
            (1660487602, 3),
            (1660407600, 4),
            (1660327600, 5),
            (1660327602, 8),
            (1660327609, 1),
            (1660499602, 6),
            (1660585602, 1),
            (1660591602, 5),
            (1660757602, 7),
            (1660757605, 2),
            (1660929602, 1),
            (1661131602, 1),
            (1661187602, 3),
        ],
        ["audit_fact_date", "prediction_device"],
    )

    actual = avg_daily_cases(df, 3, "device")

    expected = 1.25

    assert actual == expected


def test_avg_daily_cases_empty_df(spark):
    field = [
        StructField("audit_fact_date", LongType(), True),
        StructField("prediction_device", IntegerType(), True),
    ]
    schema = StructType(field)

    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    actual = avg_daily_cases(df, 3.5, "device")

    expected = 0.0

    assert actual == expected


def test_avg_daily_cases_thr_negative(spark):
    df = spark.createDataFrame(
        [
            (1660647600, 5),
            (1660567602, 2),
            (1660567605, 1),
        ],
        ["audit_fact_date", "prediction_device"],
    )

    with pytest.raises(Exception) as e:
        avg_daily_cases(df, -1, "device")  # Should raise an exception

    assert str(e.value) == "Threshold must be greater than 0"
    assert e.type == ValueError


def test_avg_daily_cases_incorrect_fraud_type(spark):
    df = spark.createDataFrame(
        [
            (1660647600, 5),
            (1660567602, 2),
            (1660567605, 1),
        ],
        ["audit_fact_date", "prediction_device"],
    )

    with pytest.raises(Exception) as e:
        avg_daily_cases(df, 1, "other")  # Should raise an exception

    assert str(e.value) == "Fraud type must be one of ['device', 'reset']"
    assert e.type == ValueError


def test_get_optimal_thr(spark):
    df = spark.createDataFrame(
        [
            (1660647601, 5),
            (1660567603, 2),
            (1660567606, 1),
            (1660487610, 3),
            (1660487604, 4),
            (1660487603, 3),
            (1660407601, 4),
            (1660327601, 5),
            (1660327603, 8),
            (1660327610, 1),
            (1660499603, 6),
            (1660585603, 1),
            (1660591603, 5),
            (1660757603, 7),
            (1660757606, 2),
            (1660929603, 1),
            (1661131603, 1),
            (1661187603, 3),
        ],
        ["audit_fact_date", "prediction_device"],
    )

    actual = get_optimal_thr(df, "device", target_avg=1.25, error_margin=0.1)

    assert 3 < actual < 4


def test_get_optimal_thr_incorrect_fraud_type(spark):
    df = spark.createDataFrame(
        [
            (1660647601, 3),
            (1660567603, 3),
            (1660567606, 3),
            (1660487610, 3),
        ],
        ["audit_fact_date", "prediction_device"],
    )

    with pytest.raises(Exception) as e:
        get_optimal_thr(
            df, "other", target_avg=1.25, error_margin=0.1
        )  # Should raise an exception
    assert str(e.value) == "Fraud type must be one of ['device', 'reset']"
    assert e.type == ValueError


def test_get_optimal_thr_same_split(spark):
    df = spark.createDataFrame(
        [
            (1660647601, 3),
            (1660567603, 3),
            (1660567606, 3),
            (1660487610, 3),
        ],
        ["audit_fact_date", "prediction_device"],
    )

    actual = get_optimal_thr(df, "device", target_avg=1.25, error_margin=0.1)

    assert actual == 3


def test_avg_frauded_users_per_day(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 0.3),
            ("ID2", 1, 0.8),
            ("ID3", 1, 0.9),
            ("ID4", 1, 0.2),
            ("ID1", 86402, 0.9),
            ("ID2", 86402, 0.2),
            ("ID3", 86402, 0.2),
            ("ID4", 86402, 0.2),
            ("ID1", 259206, 0.9),
            ("ID2", 259206, 0.9),
            ("ID3", 259206, 0.9),
            ("ID4", 259206, 0.2),
        ],
        ["user_id", "audit_fact_date", "prediction_device"],
    )

    actual = avg_frauded_users_per_day(df, 0.3, "device")

    assert actual == 2


def test_to_unix(spark):
    df = spark.createDataFrame(
        [
            ("1", "2022-01-01 00:00:00"),
            ("1", "2022-01-01 00:00:01"),
            ("1", "2022-01-30 05:10:50"),
            ("1", "2022-02-11 23:14:40"),
            ("1", "2022-06-06 12:00:09"),
        ],
        ["user_id", "audit_fact_date"],
    )

    actual = to_unix(df, "audit_fact_date")

    expected = pd.DataFrame(
        [
            ("1", 1640991600),
            ("1", 1640991601),
            ("1", 1643515850),
            ("1", 1644617680),
            ("1", 1654513209),
        ],
        columns=["user_id", "audit_fact_date"],
    )

    compare_dataframes(actual, expected)


def test_is_ip(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "192.168.1.1"),
            ("ID1", 1, "10.18.1.3"),
            ("ID1", 1, "120.14.1.6"),
            ("ID1", 1, "119.168.1.7"),
            ("ID1", 1, "169.168.255.255"),
            ("ID1", 1, "123.123.1"),
            ("ID1", 1, "123.123.1.2.3"),
            ("ID1", 1, "777.1.1.1"),
        ],
        ["user_id", "audit_fact_date", "user_ip_address"],
    )

    actual = df.withColumn("is_ip", is_ip())

    expected = pd.DataFrame(
        [
            ("ID1", 1, "192.168.1.1", False),
            ("ID1", 1, "10.18.1.3", True),
            ("ID1", 1, "120.14.1.6", True),
            ("ID1", 1, "119.168.1.7", True),
            ("ID1", 1, "169.168.255.255", True),
            ("ID1", 1, "123.123.1", False),
            ("ID1", 1, "123.123.1.2.3", False),
            ("ID1", 1, "777.1.1.1", False),
        ],
        columns=["user_id", "audit_fact_date", "user_ip_address", "is_ip"],
    )

    compare_dataframes(actual, expected)


def test_extract_field_from_message(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "motif 1"),
            ("ID1", 1, "motif A2"),
            ("ID1", 1, "motif B 3"),
            ("ID1", 1, "motif AB CD 4"),
            ("ID1", 1, "motif -5 DH"),
            ("ID1", 1, "motif 6/4"),
            ("ID1", 1, "motif 7.3"),
            ("ID1", 1, "motif ABCDE"),
            ("ID1", 1, "motif "),
        ],
        ["user_id", "audit_fact_date", "audit_message"],
    )

    actual = df.withColumn("motif", extract_field_from_message("motif"))

    expected = pd.DataFrame(
        [
            ("ID1", 1, "motif 1", "1"),
            ("ID1", 1, "motif A2", "A2"),
            ("ID1", 1, "motif B 3", "B 3"),
            ("ID1", 1, "motif AB CD 4", "AB CD 4"),
            ("ID1", 1, "motif -5 DH", "-5 DH"),
            ("ID1", 1, "motif 6/4", "6/4"),
            ("ID1", 1, "motif 7.3", "7.3"),
            ("ID1", 1, "motif ABCDE", "ABCDE"),
            ("ID1", 1, "motif ", ""),
        ],
        columns=["user_id", "audit_fact_date", "audit_message", "motif"],
    )

    compare_dataframes(actual, expected)


def test_extract_field_with_brackets_from_message(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "motif [1]"),
            ("ID1", 1, "motif [A2]"),
            ("ID1", 1, "motif [B 3]"),
            ("ID1", 1, "motif [AB CD 4]"),
            ("ID1", 1, "motif [-5 DH]"),
            ("ID1", 1, "motif [6/4]"),
            ("ID1", 1, "motif [7.3]"),
            ("ID1", 1, "motif [Mon message] ABC [123]"),
            ("ID1", 1, "motif [Mon m]essage] ABC [123]"),
            ("ID1", 1, "motif []"),
            ("ID1", 1, "motif [    ]"),
        ],
        ["user_id", "audit_fact_date", "audit_message"],
    )

    actual = df.withColumn("motif", extract_field_with_brackets_from_message("motif"))

    expected = pd.DataFrame(
        [
            ("ID1", 1, "motif [1]", "1"),
            ("ID1", 1, "motif [A2]", "A2"),
            ("ID1", 1, "motif [B 3]", "B 3"),
            ("ID1", 1, "motif [AB CD 4]", "AB CD 4"),
            ("ID1", 1, "motif [-5 DH]", "-5 DH"),
            ("ID1", 1, "motif [6/4]", "6/4"),
            ("ID1", 1, "motif [7.3]", "7.3"),
            ("ID1", 1, "motif [Mon message] ABC [123]", "Mon message"),
            ("ID1", 1, "motif [Mon m]essage] ABC [123]", "Mon m]essage"),
            ("ID1", 1, "motif []", ""),
            ("ID1", 1, "motif [    ]", "    "),
        ],
        columns=["user_id", "audit_fact_date", "audit_message", "motif"],
    )

    compare_dataframes(actual, expected)


def test_select_live(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 1, "INFO11"),
            ("ID1", 2, 2, "INFO12"),
            ("ID2", 1, 1, "INFO21"),
            ("ID2", 2, 2, "INFO22"),
            ("ID2", 3, 3, "INFO23"),
        ],
        ["user_id", "audit_fact_date", "time", "message"],
    )

    actual = select_live(df, ["user_id", "message", "audit_fact_date"], ["user_id"])
    expected = pd.DataFrame(
        [
            ("ID1", 2, 2, "INFO12"),
            ("ID2", 3, 3, "INFO23"),
        ],
        columns=["user_id", "audit_fact_date", "time", "message"],
    )
    compare_dataframes(actual, expected)


def test_select_last(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 1, "INFO11"),
            ("ID1", 2, 2, "INFO12"),
            ("ID2", 1, 1, "INFO21"),
            ("ID2", 2, 2, "INFO22"),
            ("ID2", 3, 3, "INFO23"),
            ("ID3", 3, 3, "INFO33"),
        ],
        ["user_id", "audit_fact_date", "time", "message"],
    )

    actual = select_last(df, ["user_id", "message", "audit_fact_date"])
    expected = pd.DataFrame(
        [
            ("ID2", 3, "INFO23"),
            ("ID3", 3, "INFO33"),
        ],
        columns=["user_id", "audit_fact_date", "message"],
    )
    compare_dataframes(actual, expected)


def test_select_last_with_history(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00", "INFO11"),
            ("ID1", "2022-01-02 00:00:00", "INFO12"),
            ("ID1", "2022-01-05 00:00:00", "INFO15"),
            ("ID1", "2022-01-10 00:00:00", "INFO110"),
            ("ID2", "2022-01-01 00:00:00", "INFO20"),
            ("ID2", "2022-01-02 00:00:00", "INFO21"),
            ("ID2", "2022-01-03 00:00:00", "INFO22"),
            ("ID3", "2022-01-05 00:00:00", "INFO35"),
            ("ID3", "2022-01-06 00:00:00", "INFO36"),
        ],
        ["user_id", "audit_fact_date", "audit_message"],
    )

    actual = select_last_with_history(df, unix_timestamp(lit("2022-01-04 00:00:00")))

    expected = pd.DataFrame(
        [
            ("ID1", "2022-01-01 00:00:00", "INFO11"),
            ("ID1", "2022-01-02 00:00:00", "INFO12"),
            ("ID1", "2022-01-05 00:00:00", "INFO15"),
            ("ID1", "2022-01-10 00:00:00", "INFO110"),
            ("ID3", "2022-01-05 00:00:00", "INFO35"),
            ("ID3", "2022-01-06 00:00:00", "INFO36"),
        ],
        columns=["user_id", "audit_fact_date", "audit_message"],
    )

    compare_dataframes(actual, expected)


def test_select_full_with_history(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "INFO11"),
            ("ID1", "2022-05-25 15:00:58.9", "INFO12"),
            ("ID1", "2022-05-25 15:00:59.0", "INFO15"),
            ("ID1", "2022-05-25 15:00:59.1", "INFO15"),
            ("ID1", "2022-05-25 16:00:00.0", "INFO110"),
            ("ID1", "2022-06-01 00:00:00.0", "INFO20"),
            ("ID2", "2022-01-02 00:00:00.0", "INFO21"),
            ("ID2", "2022-01-03 00:00:00.0", "INFO22"),
            ("ID3", "2022-05-25 15:00:00.0", "INFO35"),
            ("ID3", "2022-05-26 15:00:59.0", "INFO36"),
        ],
        ["user_id", "audit_fact_date", "audit_message"],
    )

    actual = select_full_with_history(
        df, ["user_id", "audit_fact_date", "audit_message"]
    )

    expected = pd.DataFrame(
        [
            ("ID1", "2022-05-25 15:00:59.0", "INFO15"),
            ("ID1", "2022-05-25 15:00:59.1", "INFO15"),
            ("ID1", "2022-05-25 16:00:00.0", "INFO110"),
            ("ID1", "2022-06-01 00:00:00.0", "INFO20"),
            ("ID3", "2022-05-26 15:00:59.0", "INFO36"),
        ],
        columns=["user_id", "audit_fact_date", "audit_message"],
    )

    compare_dataframes(actual, expected)


@patch("pyspark.sql.DataFrameWriter.csv")
def test_to_csv(mocker, spark):
    df = spark.createDataFrame(
        [
            ("ID2", 3, 3, "INFO23"),
        ],
        ["user_id", "audit_fact_date", "time", "message"],
    )

    to_csv(df, "test_path")

    mocker.assert_called_once_with(
        path="/tmp/test_path",
        mode="overwrite",
        header="true",
        sep="\u0002",
        nullValue="",
        emptyValue="",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        escape="\u0000",
    )


@patch("pyspark.sql.DataFrameWriter.parquet")
def test_to_parquet(mocker, spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 1, "INFO11"),
        ],
        ["user_id", "audit_fact_date", "time", "message"],
    )

    to_parquet(df, "test_path")

    mocker.assert_called_once_with(
        path="/tmp/test_path",
        mode="overwrite",
    )


@patch("pyspark.sql.DataFrameWriter.orc")
def test_to_orc(mocker, spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 1, "INFO11"),
        ],
        ["user_id", "audit_fact_date", "time", "message"],
    )

    to_orc(df, "test_path")

    mocker.assert_called_once_with(
        path="/tmp/test_path",
        mode="overwrite",
    )


@patch("pyspark.sql.DataFrameWriter.saveAsTable")
def test_to_hive(mocker, spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 2, "INFO12"),
        ],
        ["user_id", "audit_fact_date", "time", "message"],
    )

    to_hive(df, "awb_rec", "test_orc_audit")

    mocker.assert_called_once_with(
        name="awb_rec.test_orc_audit", mode="overwrite", format="orc"
    )
