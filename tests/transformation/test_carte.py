import pandas as pd

from src.transformation.carte import (
    get_card_activity_from_history,
    get_card_status_from_history,
    get_contract_status_from_history,
)
from tests.conftest import compare_dataframes


def test_get_contract_status_from_history(spark):
    df = spark.createDataFrame(
        [
            ("0004170831004000745", "00100030012664", "RESILIE", "2024-02-27"),
            ("0004170831004000745", "00100030012664", "VALIDE", "2023-10-25"),
            ("0004170831004000745", "00100030012664", "VALIDE", "2024-09-27"),
            ("0004170831004000745", "00100030012664", "RESILIE", "2024-10-01"),
            ("0004170831004000745", "00100030012664", "VALIDE", "2024-11-07"),
            ("0004170831004000745", "00100030012664", "RESILIE", "2024-11-12"),
            ("0004170831004000745", "00100030012664", "VALIDE", "2024-11-15"),
            ("0004170831004000745", "00100030012664", "VALIDE", "2024-11-22"),
            ("0004170831004000745", "00100030012664", "VALIDE", "2024-11-25"),
        ],
        ["numero_carte", "numero_contrat_interne", "etat_produit", "partitiondate"],
    )

    actual = get_contract_status_from_history(df, partition_date="2024-11-25")

    expected = pd.DataFrame(
        [
            (
                "0004170831004000745",
                "00100030012664",
                True,
                True,
                True,
                True,
                False,
                True,
                True,
                True,
                True,
                True,
            ),
        ],
        columns=[
            "numero_carte",
            "numero_contrat_interne",
            "is_valid_s",
            "is_valid_s_1",
            "is_valid_s_2",
            "is_valid_m",
            "is_valid_m_1",
            "is_valid_m_2",
            "is_valid_t",
            "is_valid_t_1",
            "is_valid_y",
            "is_valid_y_1",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numero_carte"])


def test_get_card_status_from_history_with_valid_status(spark):
    df = spark.createDataFrame(
        [
            ("0004170831004000745", "2", "2012-06-20 00:00:00.0", "8", "2015-02-27"),
            ("0004170831004000745", "2", "2014-06-16 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-18 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-23 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-23 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-04-24 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-25 00:00:00.0", "8", "2015-02-27"),
            ("0004170831004000745", "2", "2016-06-13 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2018-06-12 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2020-06-19 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2022-06-14 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2020-06-12 00:00:00.0", "5", "2015-02-27"),
            ("0004170831004000745", "2", "2024-06-12 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "1", "2024-06-12 00:00:00.0", "9", "2015-02-27"),
        ],
        ["idtexterne", "typeeltstock", "dtretrait", "etateltstock", "partitiondate"],
    )

    actual = get_card_status_from_history(
        df, etat="2", name="valid", partition_date="2015-02-27"
    )

    expected = pd.DataFrame(
        [
            (
                "0004170831004000745",
                True,
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
        ],
        columns=[
            "numero_carte",
            "is_valid_s",
            "is_valid_s_1",
            "is_valid_s_2",
            "is_valid_m",
            "is_valid_m_1",
            "is_valid_m_2",
            "is_valid_t",
            "is_valid_t_1",
            "is_valid_y",
            "is_valid_y_1",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numero_carte"])


def test_get_card_status_from_history_with_withdrawn_status(spark):
    df = spark.createDataFrame(
        [
            ("0004170831004000745", "2", "2014-06-20 00:00:00.0", "8", "2015-02-27"),
            ("0004170831004000745", "2", "2014-06-16 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-18 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-23 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-23 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-04-24 00:00:00.0", "2", "2015-02-27"),
            ("0004170831004000745", "2", "2015-02-25 00:00:00.0", "8", "2015-02-27"),
            ("0004170831004000745", "2", "2016-06-13 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2018-06-12 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2020-06-19 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2022-06-14 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "2", "2020-06-12 00:00:00.0", "5", "2015-02-27"),
            ("0004170831004000745", "2", "2024-06-12 00:00:00.0", "9", "2015-02-27"),
            ("0004170831004000745", "1", "2024-06-12 00:00:00.0", "9", "2015-02-27"),
        ],
        ["idtexterne", "typeeltstock", "dtretrait", "etateltstock", "partitiondate"],
    )

    actual = get_card_status_from_history(
        df, etat="8", name="withdrawn", partition_date="2015-02-27"
    )

    expected = pd.DataFrame(
        [
            (
                "0004170831004000745",
                True,
                False,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                True,
            ),
        ],
        columns=[
            "numero_carte",
            "is_withdrawn_s",
            "is_withdrawn_s_1",
            "is_withdrawn_s_2",
            "is_withdrawn_m",
            "is_withdrawn_m_1",
            "is_withdrawn_m_2",
            "is_withdrawn_t",
            "is_withdrawn_t_1",
            "is_withdrawn_y",
            "is_withdrawn_y_1",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numero_carte"])


def test_get_card_activity_from_history(spark):
    df = spark.createDataFrame(
        [
            ("N", "1", "0004170831004000745", "2024-10-10", "000"),
            ("N", "1", "0004170831004000745", "2024-08-30", "000"),
            ("F", "3", "0004170831004000745", "2024-10-17", "000"),
            ("N", "3", "0004170831004000745", "2024-10-01", "404"),
            ("N", "2", "0004170831004000745", "2023-10-10", "000"),
            ("N", "3", "0004170831004000745", "2023-06-30", "000"),
            ("N", "3", "0004170831004000745", "2024-06-30", "000"),
            ("N", "3", "0004170831004000745", "2024-10-09", "000"),
            ("W", "3", "0004170831004000745", "2024-10-17", "000"),
        ],
        [
            "aut_reve_stat",
            "aut_bin_type",
            "aut_prim_acct_numb_f002",
            "aut_requ_syst_time",
            "aut_resp_code_f039",
        ],
    )

    actual = get_card_activity_from_history(
        df, partition_date="2024-10-17", name="active"
    )

    expected = pd.DataFrame(
        [
            (
                "0004170831004000745",
                True,
                True,
                False,
                True,
                True,
                True,
                True,
                True,
                True,
                False,
            ),
        ],
        columns=[
            "numero_carte",
            "is_active_s",
            "is_active_s_1",
            "is_active_s_2",
            "is_active_m",
            "is_active_m_1",
            "is_active_m_2",
            "is_active_t",
            "is_active_t_1",
            "is_active_y",
            "is_active_y_1",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numero_carte"])
