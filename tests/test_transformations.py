import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp, unix_timestamp

from src.transformations import (
    accounts_count,
    amount_cashexpress,
    amount_disposition,
    amount_scaled,
    amount_virement,
    average_amount_scaled_12h,
    count_device_per_uuid,
    criterion,
    cumsum_disposition_12_hours,
    cumsum_virement_12_hours,
    device_age,
    device_age_normalized,
    device_change,
    device_count,
    device_count_last_7_days,
    device_count_last_14_days,
    device_count_last_30_days,
    device_pattern_features,
    ffill,
    fill_all,
    ip_features,
    is_attijari_secure_activated_pattern,
    is_attijari_secure_deactivated_pattern,
    is_awb_secure_activated_safe_device,
    is_awb_secure_deactivated_safe_device,
    is_facture_reset_pattern_shutdown,
    is_low_virement,
    is_new_beneficiary,
    is_reset,
    is_reset_pattern_disposition,
    is_reset_pattern_virement,
    is_safe_facture_device,
    join_tables,
    rank,
    ratio_amount_cumsum_virement,
    reset_pattern_features,
    time_diff,
    time_diff_inv,
    time_diff_to_attijari_secure,
    time_diff_to_attijari_secure_normalized,
    time_diff_to_last_maxed_transaction,
    time_diff_to_last_money_operation_by_ip,
    time_diff_to_last_significant_transaction,
    time_diff_to_reset,
    time_diff_to_reset_caped,
    time_diff_to_reset_exp,
    time_diff_to_reset_inv,
    time_diff_to_reset_normalized,
    time_diff_to_reset_normalized_sigmoid,
    time_diff_to_reset_sigmoid,
)
from tests.conftest import compare_dataframes


def test_fill_all(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, None, None, None, None, None, None, None),
            ("ID1", 2, 1, 1, 1, 1, 1, 1, 0),
            ("ID2", 1, None, None, None, None, None, None, None),
            ("ID2", 2, 1, 1, 1, 1, 1, 1, 0),
        ],
        [
            "user_id",
            "audit_fact_date",
            "device_change",
            "device_count",
            "cumsum_disposition_12_hours",
            "cumsum_virement_12_hours",
            "average_amount_scaled_12h",
            "time_diff_to_attijari_secure",
            "device_age",
        ],
    )

    actual = fill_all(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, 0, 0, 0, 0, 0, -1, -1, 1e-7, 1e-7),
            ("ID1", 2, 1, 1, 1, 1, 1, 1, 0, 0.5, 1),
            ("ID2", 1, 0, 0, 0, 0, 0, -1, -1, 1e-7, 1e-7),
            ("ID2", 2, 1, 1, 1, 1, 1, 1, 0, 0.5, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "device_change",
            "device_count",
            "cumsum_disposition_12_hours",
            "cumsum_virement_12_hours",
            "average_amount_scaled_12h",
            "time_diff_to_attijari_secure",
            "device_age",
            "time_diff_to_attijari_secure_normalized",
            "device_age_normalized",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_pattern_features(spark):
    mobile_device = spark.createDataFrame(
        [
            (
                1,
                "ID1",
                "iPhone 13",
                "iOS16/iPhone11/appver5.2",
                "UUID-APPLE",
                "2022-01-01 00:00:00",
            ),
            (
                2,
                "ID1",
                "Samsung S22",
                "android12/samsungSM-22/appver7.3",
                "UUID-SAMSUNG",
                "2022-02-20 00:00:00",
            ),
            (
                3,
                "ID1",
                "HTC 10",
                "android10/htc10/appver6.3",
                "UUID-HTC",
                "2022-09-01 00:00:00",
            ),
        ],
        ["id", "id_user", "name", "os", "uuid", "creation_date"],
    )

    device_history = spark.createDataFrame(
        [
            (1, "2022-01-01 00:00:00"),
            (1, "2022-02-18 00:00:00"),
            (2, "2022-02-20 00:00:00"),
            (2, "2022-09-01 00:00:00"),
            (3, "2022-09-03 00:00:00"),
            (1, "2022-09-10 00:00:00"),
            (1, "2022-09-11 00:00:00"),
        ],
        ["mobile_device_id", "connection_date"],
    )

    audit_ebk = (
        spark.createDataFrame(
            [
                ("2022-01-01 00:00:00", "ID1", "8", 0, 0, 0, None),
                ("2022-01-02 00:00:00", "ID1", "2", 1000, 0, 0, None),  # Virement (Filtred)
                ("2022-01-03 00:00:00", "ID1", "5", 0, 2000, 0, None),  # Disposition (Filtred)
                ("2022-02-18 00:00:00", "ID1", "8", 0, 0, 0, "   "),
                ("2022-02-18 00:00:00", "ID1", "3", 0, 0, 0, "", ),  # New device
                ("2022-02-19 00:00:00", "ID1", "8", 0, 0, 0, "   "),
                ("2022-02-19 00:00:01", "ID1", "2", 2500, 0, 0, None),  # Virement
                ("2022-02-20 00:00:00", "ID1", "8", 0, 0, 0, "   "),
                ("2022-02-20 00:00:01", "ID1", "5", 0, 1500, 0, None),  # Disposition(New dev)
                ("2022-09-01 00:00:00", "ID1", "8", 0, 0, 0, "   "),
                ("2022-09-01 00:00:01", "ID1", "7", 0, 0, 0, None),  # Descativ Attijari Secure
                ("2022-09-01 00:00:04", "ID1", "6", 0, 0, 0, None),  # Activ Attijari Secure
                ("2022-09-01 00:00:05", "ID1", "1", 0, 0, 0, None),  # Ajouter benef
                ("2022-09-03 00:00:00", "ID1", "8", 0, 0, 0, "   "),
                ("2022-09-03 00:00:02", "ID1", "2", 10000, 0, 0, ""),  # Virement
                ("2022-09-10 00:00:00", "ID1", "8", 0, 0, 0, "S21"),
                ("2022-09-10 00:00:01", "ID1", "A", 0, 0, 0, "S21"),
                ("2022-09-10 00:00:02", "ID1", "5", 0, 2000, 0, "S21"),  # Disposition
                ("2022-09-11 00:00:00", "ID1", "B", 0, 0, 1000, "I10"),  # Cashexpress
            ],
            [
                "audit_fact_date",
                "user_id",
                "event_type",
                "amount_virement",
                "amount_disposition",
                "amount_cashexpress",
                "device_id",
            ],
        )
        .withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))
        .withColumn("audit_message", lit("Message"))
    )

    audit_ebk = amount_scaled(audit_ebk)

    actual = device_pattern_features(
        audit_ebk,
        mobile_device,
        device_history,
        unix_timestamp(to_timestamp(lit("2022-10-05"))),
    )

    expected = pd.DataFrame(
        [
            {
                "audit_fact_date": 1645138800,
                "user_id": "ID1",
                "event_type": "8",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "   ",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 1,
                "device_count": 1,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1645138800,
                "user_id": "ID1",
                "event_type": "3",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 0,
                "device_count": 1,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1645225200,
                "user_id": "ID1",
                "event_type": "8",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "   ",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 0,
                "device_count": 1,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1645225201,
                "user_id": "ID1",
                "event_type": "2",
                "amount_virement": 2500,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": None,
                "audit_message": "Message",
                "amount_scaled": 0.05,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 0,
                "device_count": 1,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": 23932800,
                "device_age_normalized": 4.178365917136068e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 2500,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0.05,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1645311600,
                "user_id": "ID1",
                "event_type": "8",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "   ",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 2,
                "name": "Samsung S22",
                "os": "android12/samsungSM-22/appver7.3",
                "uuid": "UUID-SAMSUNG",
                "creation_date": 1645311600,
                "phone": "samsungSM-22",
                "device_change": 1,
                "device_count": 2,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1645311601,
                "user_id": "ID1",
                "event_type": "5",
                "amount_virement": 0,
                "amount_disposition": 1500,
                "amount_cashexpress": 0,
                "device_id": None,
                "audit_message": "Message",
                "amount_scaled": 0.75,
                "mobile_device_id": 2,
                "name": "Samsung S22",
                "os": "android12/samsungSM-22/appver7.3",
                "uuid": "UUID-SAMSUNG",
                "creation_date": 1645311600,
                "phone": "samsungSM-22",
                "device_change": 0,
                "device_count": 2,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": 19612800,
                "device_age_normalized": 5.09871078587908e-08,
                "cumsum_disposition_12_hours": 1500,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0.75,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1661986800,
                "user_id": "ID1",
                "event_type": "8",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "   ",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 2,
                "name": "Samsung S22",
                "os": "android12/samsungSM-22/appver7.3",
                "uuid": "UUID-SAMSUNG",
                "creation_date": 1645311600,
                "phone": "samsungSM-22",
                "device_change": 0,
                "device_count": 2,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1661986801,
                "user_id": "ID1",
                "event_type": "7",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": None,
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 2,
                "name": "Samsung S22",
                "os": "android12/samsungSM-22/appver7.3",
                "uuid": "UUID-SAMSUNG",
                "creation_date": 1645311600,
                "phone": "samsungSM-22",
                "device_change": 0,
                "device_count": 2,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1661986804,
                "user_id": "ID1",
                "event_type": "6",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": None,
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 2,
                "name": "Samsung S22",
                "os": "android12/samsungSM-22/appver7.3",
                "uuid": "UUID-SAMSUNG",
                "creation_date": 1645311600,
                "phone": "samsungSM-22",
                "device_change": 0,
                "device_count": 2,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1661986805,
                "user_id": "ID1",
                "event_type": "1",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": None,
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 2,
                "name": "Samsung S22",
                "os": "android12/samsungSM-22/appver7.3",
                "uuid": "UUID-SAMSUNG",
                "creation_date": 1645311600,
                "phone": "samsungSM-22",
                "device_change": 0,
                "device_count": 2,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1662159600,
                "user_id": "ID1",
                "event_type": "8",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "   ",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 3,
                "name": "HTC 10",
                "os": "android10/htc10/appver6.3",
                "uuid": "UUID-HTC",
                "creation_date": 1661986800,
                "phone": "htc10",
                "device_change": 1,
                "device_count": 3,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1662159602,
                "user_id": "ID1",
                "event_type": "2",
                "amount_virement": 10000,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "",
                "audit_message": "Message",
                "amount_scaled": 0.2,
                "mobile_device_id": 3,
                "name": "HTC 10",
                "os": "android10/htc10/appver6.3",
                "uuid": "UUID-HTC",
                "creation_date": 1661986800,
                "phone": "htc10",
                "device_change": 0,
                "device_count": 3,
                "is_attijari_secure_deactivated_pattern": 1,
                "is_attijari_secure_activated_pattern": 1,
                "time_diff_to_attijari_secure": 172798,
                "time_diff_to_attijari_secure_normalized": 5.787070527028513e-06,
                "device_age": 2937600,
                "device_age_normalized": 3.4041382747350643e-07,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 10000,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0.2,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 1,
                "is_awb_secure_activated_safe_device": 1,
            },
            {
                "audit_fact_date": 1662764400,
                "user_id": "ID1",
                "event_type": "8",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "S21",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 1,
                "device_count": 4,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1662764401,
                "user_id": "ID1",
                "event_type": "A",
                "amount_virement": 0,
                "amount_disposition": 0,
                "amount_cashexpress": 0,
                "device_id": "S21",
                "audit_message": "Message",
                "amount_scaled": 0,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 0,
                "device_count": 4,
                "is_attijari_secure_deactivated_pattern": 0,
                "is_attijari_secure_activated_pattern": 0,
                "time_diff_to_attijari_secure": -1,
                "time_diff_to_attijari_secure_normalized": 9.9999990000001e-08,
                "device_age": -1,
                "device_age_normalized": 9.9999990000001e-08,
                "cumsum_disposition_12_hours": 0,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 0,
                "is_safe_facture_device": 0,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                "audit_fact_date": 1662764402,
                "user_id": "ID1",
                "event_type": "5",
                "amount_virement": 0,
                "amount_disposition": 2000,
                "amount_cashexpress": 0,
                "device_id": "S21",
                "audit_message": "Message",
                "amount_scaled": 1,
                "mobile_device_id": 1,
                "name": "iPhone 13",
                "os": "iOS16/iPhone11/appver5.2",
                "uuid": "UUID-APPLE",
                "creation_date": 1640991600,
                "phone": "iPhone11",
                "device_change": 0,
                "device_count": 4,
                "is_attijari_secure_deactivated_pattern": 1,
                "is_attijari_secure_activated_pattern": 1,
                "time_diff_to_attijari_secure": 777598,
                "time_diff_to_attijari_secure_normalized": 1.2860098842719705e-06,
                "device_age": 23932800,
                "device_age_normalized": 4.178365917136068e-08,
                "cumsum_disposition_12_hours": 2000,
                "cumsum_virement_12_hours": 0,
                "cumsum_cashexpress_12_hours": 0,
                "average_amount_scaled_12h": 1,
                "is_safe_facture_device": 1,
                "is_awb_secure_deactivated_safe_device": 0,
                "is_awb_secure_activated_safe_device": 0,
            },
            {
                'audit_fact_date': 1662850800,
                'user_id': 'ID1',
                'event_type': 'B',
                'amount_virement': 0,
                'amount_disposition': 0,
                'amount_cashexpress': 1000,
                'device_id': 'I10',
                'audit_message': 'Message',
                'amount_scaled': 0.0125,
                'mobile_device_id': 1,
                'name': 'iPhone 13',
                'os': 'iOS16/iPhone11/appver5.2',
                'uuid': 'UUID-APPLE',
                'creation_date': 1640991600,
                'phone': 'iPhone11',
                'device_change': 0,
                'device_count': 4,
                'is_attijari_secure_deactivated_pattern': 0,
                'is_attijari_secure_activated_pattern': 0,
                'time_diff_to_attijari_secure': -1,
                'time_diff_to_attijari_secure_normalized': 9.9999990000001e-08,
                'device_age': 23932800,
                'device_age_normalized': 4.178365917136068e-08,
                'cumsum_disposition_12_hours': 0,
                'cumsum_virement_12_hours': 0,
                "cumsum_cashexpress_12_hours": 1000,
                'average_amount_scaled_12h': 0.0125,
                'is_safe_facture_device': 1,
                'is_awb_secure_deactivated_safe_device': 0,
                'is_awb_secure_activated_safe_device': 0
            }
        ]
    )
    compare_dataframes(actual, expected)


def test_reset_pattern_features(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "action_1", 0, 0),
            ("ID1", 2, "1", 0, 0),
            ("ID1", 3, "8", 0, 0),
            ("ID1", 4, "9", 0, 0),  # Reset
            ("ID1", 5, "5", 0, 1000),  # Dispo
            ("ID1", 6, "1", 0, 0),
            ("ID1", 86407, "2", 30000, 0),  # Virement
            ("ID1", 86408, "3", 0, 0),
            ("ID1", 86409, "1", 0, 0),
            ("ID1", 86410, "A", 0, 0),
            ("ID1", 86411, "2", 50000, 0),  # Reset Pattern (931)
            ("ID2", 1, "1", 0, 0),
            ("ID2", 86500, "9", 0, 0),  # Reset
            ("ID2", 86510, "3", 0, 0),
            ("ID2", 173030, "5", 0, 2000),  # Reset Pattern (93)
            ("ID2", 173031, "9", 0, 0),  # Reset
            ("ID2", 173032, "5", 0, 1500),  # Dispo
            ("ID2", 173033, "3", 0, 0),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "amount_disposition",
        ],
    )

    actual = reset_pattern_features(df)

    expected = pd.DataFrame(
        [
            {
                "user_id": "ID1",
                "audit_fact_date": 1,
                "event_type": "action_1",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": -1,
                "time_diff_inv": 0.36787944117144233,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 2,
                "event_type": "1",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 4,
                "event_type": "9",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 2,
                "time_diff_inv": 1.6487212707001282,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 5,
                "event_type": "5",
                "amount_virement": 0,
                "amount_disposition": 1000,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 6,
                "event_type": "1",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 86407,
                "event_type": "2",
                "amount_virement": 30000,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 86401,
                "time_diff_inv": 1.0000115740070947,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 86408,
                "event_type": "3",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 86409,
                "event_type": "1",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID1",
                "audit_fact_date": 86411,
                "event_type": "2",
                "amount_virement": 50000,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 1,
                "is_reset": 1,
                "time_diff": 2,
                "time_diff_inv": 1.6487212707001282,
                "time_diff_to_reset": 86407,
                "time_diff_to_reset_normalized": 0.00001157300249976854,
                "criterion": 0.00001157300249976854,
                "time_diff_to_reset_normalized_sigmoid": 0.7310745075,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 1,
                "event_type": "1",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": -1,
                "time_diff_inv": 0.36787944117144233,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 86500,
                "event_type": "9",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 86499,
                "time_diff_inv": 1.0000115608941194,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 86510,
                "event_type": "3",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 10,
                "time_diff_inv": 1.1051709180756477,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 173030,
                "event_type": "5",
                "amount_virement": 0,
                "amount_disposition": 2000,
                "is_reset_pattern_disposition": 1,
                "is_reset_pattern_virement": 0,
                "is_reset": 1,
                "time_diff": 86520,
                "time_diff_inv": 1.0000115580880609,
                "time_diff_to_reset": 86530,
                "time_diff_to_reset_normalized": 0.000011556551987149113,
                "criterion": 0.000011556551987149113,
                "time_diff_to_reset_normalized_sigmoid": 0.7313543039,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 173031,
                "event_type": "9",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 173032,
                "event_type": "5",
                "amount_virement": 0,
                "amount_disposition": 1500,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
            {
                "user_id": "ID2",
                "audit_fact_date": 173033,
                "event_type": "3",
                "amount_virement": 0,
                "amount_disposition": 0,
                "is_reset_pattern_disposition": 0,
                "is_reset_pattern_virement": 0,
                "is_reset": 0,
                "time_diff": 1,
                "time_diff_inv": 2.718281828459045,
                "time_diff_to_reset": -1,
                "time_diff_to_reset_normalized": 0.000000099999990000001,
                "criterion": 0,
                "time_diff_to_reset_normalized_sigmoid": 1,
            },
        ]
    )

    compare_dataframes(actual, expected)


def test_rank(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "T1", "11"),
            ("ID1", "T2", "12"),
            ("ID1", "T3", "5"),
            ("ID1", "T4", "9"),
            ("ID1", "T5", "7"),
            ("ID1", "T6", "8"),
            ("ID1", "T7", "9"),
            ("ID1", "T8", "1"),
            ("ID1", "T9", "2"),
        ],
        ["user_id", "audit_fact_date", "event_type"],
    )

    actual = rank(df, "rank", "9")

    expected = pd.DataFrame(
        [
            ("ID1", "T1", "11", 0),
            ("ID1", "T2", "12", 0),
            ("ID1", "T3", "5", 0),
            ("ID1", "T4", "9", 1),
            ("ID1", "T5", "7", 1),
            ("ID1", "T6", "8", 1),
            ("ID1", "T7", "9", 2),
            ("ID1", "T8", "1", 2),
            ("ID1", "T9", "2", 2),
        ],
        columns=["user_id", "audit_fact_date", "event_type", "rank"],
    )

    compare_dataframes(actual, expected)


def test_join_join_tables(spark):
    mobile_device = spark.createDataFrame(
        [
            (1, "U1", "S1", "os11", "UU1", 1),
            (2, "U2", "S2", "os12", "UU2", 2),
            (3, "U2", "S22", "os120", "UU22", 21),
        ],
        ["id", "id_user", "name", "os", "uuid", "creation_date"],
    )

    device_history = spark.createDataFrame(
        [
            (1, 1),
            (1, 11),
            (2, 2),
            (3, 22),
        ],
        ["mobile_device_id", "connection_date"],
    )

    audit_ebk = spark.createDataFrame(
        [
            (3, "8", "U1", None),  # S1
            (4, "8", "U2", None),  # S2
            (10, "8", "U2", None),  # None cause not close to any device used by user U2
            (14, "8", "U1", None),  # S1
            (23, "8", "U2", None),  # S22
            (24, "8", "U2", "       "),  # S22
            (25, "8", "U2", "EVO1_U2"),
            (26, "8", "U1", "EVO1_U1"),
            (27, "8", "U3", "EVO1_U3"),
            (28, "8", "U1", "EVO2_U1"),
        ],
        ["audit_fact_date", "event_type", "user_id", "device_id"],
    )

    actual = join_tables(audit_ebk, mobile_device, device_history)

    expected = pd.DataFrame(
        [
            (3, "8", "U1", None, 1, "S1", "os11", "UU1", 1),
            (4, "8", "U2", None, 2, "S2", "os12", "UU2", 2),
            (10, "8", "U2", None, None, None, None, None, None),
            (14, "8", "U1", None, 1, "S1", "os11", "UU1", 1),
            (23, "8", "U2", None, 3, "S22", "os120", "UU22", 21),
            (24, "8", "U2", "       ", 3, "S22", "os120", "UU22", 21),
            (25, "8", "U2", "EVO1_U2", 3, "S22", "os120", "UU22", 21),
            (26, "8", "U1", "EVO1_U1", None, None, None, None, None),
            (27, "8", "U3", "EVO1_U3", None, None, None, None, None),
            (28, "8", "U1", "EVO2_U1", None, None, None, None, None),
        ],
        columns=[
            "audit_fact_date",
            "event_type",
            "user_id",
            "device_id",
            "mobile_device_id",
            "name",
            "os",
            "uuid",
            "creation_date",
        ],
    )

    compare_dataframes(actual, expected)


def test_amount_scaled(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 10, 0, 0),
            ("ID1", 1, 500, 0, 0),
            ("ID1", 1, 5000, 0, 0),
            ("ID1", 1, 50000, 0, 0),
            ("ID1", 1, 0, 10, 0),
            ("ID1", 1, 0, 1000, 0),
            ("ID1", 1, 0, 2000, 0),
            ("ID1", 1, 0, 0, 8000),
            ("ID1", 1, 0, 0, 10000),
            ("ID1", 1, 0, 0, 80000),
        ],
        [
            "user_id",
            "audit_fact_date",
            "amount_virement",
            "amount_disposition",
            "amount_cashexpress",
        ],
    )

    actual = amount_scaled(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, 10, 0, 0, 0.0002),
            ("ID1", 1, 500, 0, 0, 0.01),
            ("ID1", 1, 5000, 0, 0, 0.1),
            ("ID1", 1, 50000, 0, 0, 1),
            ("ID1", 1, 0, 10, 0, 0.005),
            ("ID1", 1, 0, 1000, 0, 0.5),
            ("ID1", 1, 0, 2000, 0, 1),
            ("ID1", 1, 0, 0, 8000, 0.1),
            ("ID1", 1, 0, 0, 10000, 0.125),
            ("ID1", 1, 0, 0, 80000, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "amount_virement",
            "amount_disposition",
            "amount_cashexpress",
            "amount_scaled",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1"),
            ("ID1", "2022-01-02 00:00:00.0", "action_2"),
            ("ID1", "2022-01-03 00:00:00.0", "action_3"),
            ("ID1", "2022-01-04 00:00:00.0", "RESET_PASSWORD_TOKEN_SENT_SUCCESS"),
            ("ID1", "2022-01-05 00:00:00.0", "AJOUTER_BENEFICIAIRE"),
            ("ID1", "2022-01-06 00:00:00.0", "MAJ_INTERVENANT"),
            ("ID1", "2022-01-07 00:00:00.0", "EFFECTUER_VIREMENT"),
            ("ID1", "2022-01-08 00:01:00.0", "RESET_PASSWORD_TOKEN_SENT_SUCCESS"),
            ("ID1", "2022-01-09 00:00:00.0", "EFFECTUER_VIREMENT"),
        ],
        ["user_id", "audit_fact_date", "event_type"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = time_diff(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", -1),
            ("ID1", 1641078000, "action_2", 86400),
            ("ID1", 1641164400, "action_3", 86400),
            ("ID1", 1641250800, "RESET_PASSWORD_TOKEN_SENT_SUCCESS", 86400),
            ("ID1", 1641337200, "AJOUTER_BENEFICIAIRE", 86400),
            ("ID1", 1641423600, "MAJ_INTERVENANT", 86400),
            ("ID1", 1641510000, "EFFECTUER_VIREMENT", 86400),
            ("ID1", 1641596400, "RESET_PASSWORD_TOKEN_SENT_SUCCESS", 86460),
            ("ID1", 1641682800, "EFFECTUER_VIREMENT", 86340),
        ],
        columns=["user_id", "audit_fact_date", "event_type", "time_diff"],
    )

    compare_dataframes(actual, expected)


def test_time_diff_inv(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "A1", None),
            ("ID1", 10, "A2", 9),
            ("ID1", 50, "A3", 40),
            ("ID1", 86400, "A4", 86350),
            ("ID1", 172800, "A5", 86400),
            ("ID1", 172800, "A6", 0),
        ],
        ["user_id", "audit_fact_date", "event_type", "time_diff"],
    )

    actual = time_diff_inv(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "A1", None, 1),
            ("ID1", 10, "A2", 9, 1.1175190687418637),
            ("ID1", 50, "A3", 40, 1.0253151205244289),
            ("ID1", 86400, "A4", 86350, 1.0000115808429695),
            ("ID1", 172800, "A5", 86400, 1.000011574141054),
            ("ID1", 172800, "A6", 0, 2.718281828459045),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff",
            "time_diff_inv",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0, False, False),
            ("ID1", "2022-01-02 00:00:00.0", "action_2", 0, False, False),
            ("ID1", "2022-01-03 00:00:00.0", "action_3", 0, False, False),
            ("ID1", "2022-01-04 00:00:00.0", "9", 1, False, False),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1, False, False),
            ("ID1", "2022-01-06 00:00:00.0", "3", 1, False, False),
            ("ID1", "2022-01-07 00:00:00.0", "2", 1, True, False),
            ("ID1", "2022-01-08 00:00:00.0", "9", 2, False, False),
            ("ID1", "2022-01-09 00:00:00.0", "2", 2, False, False),
            ("ID2", "2022-01-16 00:00:00.0", "action_3", 0, False, False),
            ("ID2", "2022-01-10 00:00:00.0", "9", 1, False, False),
            ("ID2", "2022-01-11 00:00:00.0", "1", 1, False, False),
            ("ID2", "2022-01-12 00:00:00.0", "3", 1, False, False),
            ("ID2", "2022-01-13 00:00:00.0", "2", 1, True, False),
            ("ID2", "2022-01-14 00:00:00.0", "9", 2, False, False),
            ("ID2", "2022-01-24 00:00:00.0", "2", 2, False, False),
            ("ID5", "2022-01-01 00:00:00.0", "9", 1, False, False),
            ("ID5", "2022-01-02 00:00:00.0", "3", 1, False, False),
            ("ID5", "2022-01-03 00:00:00.0", "5", 1, False, True),
            ("ID5", "2022-01-04 00:00:00.0", "9", 2, False, False),
            ("ID5", "2022-01-05 00:00:00.0", "5", 2, False, False),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
        ],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = time_diff_to_reset(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, False, False, -1),
            ("ID1", 1641078000, "action_2", 0, False, False, -1),
            ("ID1", 1641164400, "action_3", 0, False, False, -1),
            ("ID1", 1641250800, "9", 1, False, False, -1),
            ("ID1", 1641337200, "1", 1, False, False, -1),
            ("ID1", 1641423600, "3", 1, False, False, -1),
            ("ID1", 1641510000, "2", 1, True, False, 259200),
            ("ID1", 1641596400, "9", 2, False, False, -1),
            ("ID1", 1641682800, "2", 2, False, False, -1),
            ("ID2", 1641769200, "9", 1, False, False, -1),
            ("ID2", 1641855600, "1", 1, False, False, -1),
            ("ID2", 1641942000, "3", 1, False, False, -1),
            ("ID2", 1642028400, "2", 1, True, False, 259200),
            ("ID2", 1642114800, "9", 2, False, False, -1),
            ("ID2", 1642287600, "action_3", 0, False, False, -1),
            ("ID2", 1642978800, "2", 2, False, False, -1),
            ("ID5", 1640991600, "9", 1, False, False, -1),
            ("ID5", 1641078000, "3", 1, False, False, -1),
            ("ID5", 1641164400, "5", 1, False, True, 172800),
            ("ID5", 1641250800, "9", 2, False, False, -1),
            ("ID5", 1641337200, "5", 2, False, False, -1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
            "time_diff_to_reset",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset_inv(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1642287600, "action_3", -1),
            ("ID1", 1642978800, "2", -1),
            ("ID1", 1640991600, "9", -1),
            ("ID1", 1641164400, "5", 172800),
            ("ID1", 1641250800, "9", -1),
            ("ID1", 1641250801, "5", 1),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
        ],
    )

    actual = time_diff_to_reset_inv(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1642287600, "action_3", -1, 1),
            ("ID1", 1642978800, "2", -1, 1),
            ("ID1", 1640991600, "9", -1, 1),
            ("ID1", 1641164400, "5", 172800, 0.000005787037037037037),
            ("ID1", 1641250800, "9", -1, 1),
            ("ID1", 1641250801, "5", 1, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
            "time_diff_to_reset_inv",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset_exp(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "a1", -1),
            ("ID1", 2, "a2", -1),
            ("ID1", 49, "a3", -1),
            ("ID1", 50, "9", -1),
            ("ID1", 51, "1", -1),
            ("ID1", 52, "3", -1),
            ("ID1", 60, "2", 10),
            ("ID1", 70, "9", -1),
            ("ID1", 86471, "2", 86401),
            ("ID2", 86471, "9", -1),
            ("ID2", 86472, "1", -1),
            ("ID2", 86473, "3", -1),
            ("ID2", 86474, "2", 3),
            ("ID2", 86475, "9", -1),
            ("ID2", 86476, "a3", -1),
            ("ID2", 172875, "2", 86400),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
        ],
    )

    actual = time_diff_to_reset_exp(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "a1", -1, 2.718281828459045),
            ("ID1", 2, "a2", -1, 2.718281828459045),
            ("ID1", 49, "a3", -1, 2.718281828459045),
            ("ID1", 50, "9", -1, 2.718281828459045),
            ("ID1", 51, "1", -1, 2.718281828459045),
            ("ID1", 52, "3", -1, 2.718281828459045),
            ("ID1", 60, "2", 10, 1.1051709180756477),
            ("ID1", 70, "9", -1, 2.718281828459045),
            ("ID1", 86471, "2", 86401, 1.0000115740070947),
            ("ID2", 86471, "9", -1, 2.718281828459045),
            ("ID2", 86472, "1", -1, 2.718281828459045),
            ("ID2", 86473, "3", -1, 2.718281828459045),
            ("ID2", 86474, "2", 3, 1.3956124250860895),
            ("ID2", 86475, "9", -1, 2.718281828459045),
            ("ID2", 86476, "a3", -1, 2.718281828459045),
            ("ID2", 172875, "2", 86400, 1.000011574141054),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
            "time_diff_to_reset_exp",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset_normalized(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, -1),
            ("ID1", 1641078000, "action_2", 0, -1),
            ("ID1", 1641164400, "action_3", 0, -1),
            ("ID1", 1641250800, "9", 1, -1),
            ("ID1", 1641337200, "1", 1, -1),
            ("ID1", 1641423600, "3", 1, -1),
            ("ID1", 1641510000, "2", 1, 259200),
            ("ID1", 1641596400, "9", 2, -1),
            ("ID1", 1641682800, "2", 2, -1),
            ("ID2", 1641769200, "9", 1, -1),
            ("ID2", 1641855600, "1", 1, -1),
            ("ID2", 1641942000, "3", 1, -1),
            ("ID2", 1642028400, "2", 1, 259200),
            ("ID2", 1642114800, "9", 2, -1),
            ("ID2", 1642287600, "action_3", 0, -1),
            ("ID2", 1642978800, "2", 2, -1),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "time_diff_to_reset",
        ],
    )

    actual = time_diff_to_reset_normalized(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, -1, 0.0000001000),
            ("ID1", 1641078000, "action_2", 0, -1, 0.0000001000),
            ("ID1", 1641164400, "action_3", 0, -1, 0.0000001000),
            ("ID1", 1641250800, "9", 1, -1, 0.0000001000),
            ("ID1", 1641337200, "1", 1, -1, 0.0000001000),
            ("ID1", 1641423600, "3", 1, -1, 0.0000001000),
            ("ID1", 1641510000, "2", 1, 259200, 0.0000038580),
            ("ID1", 1641596400, "9", 2, -1, 0.0000001000),
            ("ID1", 1641682800, "2", 2, -1, 0.0000001000),
            ("ID2", 1641769200, "9", 1, -1, 0.0000001000),
            ("ID2", 1641855600, "1", 1, -1, 0.0000001000),
            ("ID2", 1641942000, "3", 1, -1, 0.0000001000),
            ("ID2", 1642028400, "2", 1, 259200, 0.0000038580),
            ("ID2", 1642114800, "9", 2, -1, 0.0000001000),
            ("ID2", 1642287600, "action_3", 0, -1, 0.0000001000),
            ("ID2", 1642978800, "2", 2, -1, 0.0000001000),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "time_diff_to_reset",
            "time_diff_to_reset_normalized",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset_normalized_sigmoid(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0, False, False, -1),
            ("ID1", "2022-01-02 00:00:00.0", "action_2", 0, False, False, -1),
            ("ID1", "2022-01-03 00:00:00.0", "action_3", 0, False, False, -1),
            ("ID1", "2022-01-04 00:00:00.0", "9", 1, False, False, -1),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1, False, False, -1),
            ("ID1", "2022-01-06 00:00:00.0", "3", 1, False, False, -1),
            ("ID1", "2022-01-07 00:00:00.0", "2", 1, True, False, 259200),
            ("ID1", "2022-01-08 00:00:00.0", "9", 2, False, False, -1),
            ("ID1", "2022-01-09 00:00:00.0", "2", 2, False, False, -1),
            ("ID2", "2022-01-16 00:00:00.0", "action_3", 0, False, False, -1),
            ("ID2", "2022-01-10 00:00:00.0", "9", 1, False, False, -1),
            ("ID2", "2022-01-11 00:00:00.0", "1", 1, False, False, -1),
            ("ID2", "2022-01-12 00:00:00.0", "3", 1, False, False, 259200),
            ("ID2", "2022-01-13 00:00:00.0", "2", 1, True, False, -1),
            ("ID2", "2022-01-14 00:00:00.0", "9", 2, False, False, -1),
            ("ID2", "2022-01-24 00:00:00.0", "2", 2, False, False, -1),
            ("ID5", "2022-01-01 00:00:00.0", "9", 1, False, False, -1),
            ("ID5", "2022-01-02 00:00:00.0", "3", 1, False, False, -1),
            ("ID5", "2022-01-03 00:00:00.0", "5", 1, False, True, 172800),
            ("ID5", "2022-01-04 00:00:00.0", "9", 2, False, False, -1),
            ("ID5", "2022-01-05 00:00:00.0", "5", 2, False, False, -1),
            ("ID7", "2022-01-05 00:00:00.0", "9", 1, False, False, -1),
            ("ID7", "2022-01-05 00:00:01.0", "5", 1, False, False, 1000),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
            "time_diff_to_reset",
        ],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = time_diff_to_reset_normalized_sigmoid(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, False, False, -1, 1.0),
            ("ID1", 1641078000, "action_2", 0, False, False, -1, 1.0),
            ("ID1", 1641164400, "action_3", 0, False, False, -1, 1.0),
            ("ID1", 1641250800, "9", 1, False, False, -1, 1.0),
            ("ID1", 1641337200, "1", 1, False, False, -1, 1.0),
            ("ID1", 1641423600, "3", 1, False, False, -1, 1.0),
            ("ID1", 1641510000, "2", 1, True, False, 259200, 0.9525741268224334),
            ("ID1", 1641596400, "9", 2, False, False, -1, 1.0),
            ("ID1", 1641682800, "2", 2, False, False, -1, 1.0),
            ("ID2", 1641769200, "9", 1, False, False, -1, 1.0),
            ("ID2", 1641855600, "1", 1, False, False, -1, 1.0),
            ("ID2", 1641942000, "3", 1, False, False, 259200, 0.9525741268224334),
            ("ID2", 1642028400, "2", 1, True, False, -1, 1.0),
            ("ID2", 1642114800, "9", 2, False, False, -1, 1.0),
            ("ID2", 1642287600, "action_3", 0, False, False, -1, 1.0),
            ("ID2", 1642978800, "2", 2, False, False, -1, 1.0),
            ("ID5", 1640991600, "9", 1, False, False, -1, 1.0),
            ("ID5", 1641078000, "3", 1, False, False, -1, 1.0),
            ("ID5", 1641164400, "5", 1, False, True, 172800, 0.8807970779778823),
            ("ID5", 1641250800, "9", 2, False, False, -1, 1.0),
            ("ID5", 1641337200, "5", 2, False, False, -1, 1.0),
            ("ID7", 1641337200, "9", 1, False, False, -1, 1.0),
            ("ID7", 1641338200, "5", 1, False, False, 1000, 0.5028934862178347),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
            "time_diff_to_reset",
            "time_diff_to_reset_normalized_sigmoid",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset_caped(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "a1", -1),
            ("ID1", 2, "a2", -1),
            ("ID1", 49, "a3", -1),
            ("ID1", 50, "9", -1),
            ("ID1", 51, "1", -1),
            ("ID1", 52, "3", -1),
            ("ID1", 60, "2", 10),
            ("ID1", 70, "9", -1),
            ("ID1", 86471, "2", 86401),
            ("ID2", 86471, "9", -1),
            ("ID2", 86472, "1", -1),
            ("ID2", 86473, "3", -1),
            ("ID2", 86474, "2", 3),
            ("ID2", 86475, "9", -1),
            ("ID2", 86476, "a3", -1),
            ("ID2", 172875, "2", 86400),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
        ],
    )

    actual = time_diff_to_reset_caped(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "a1", -1, -1),
            ("ID1", 2, "a2", -1, -1),
            ("ID1", 49, "a3", -1, -1),
            ("ID1", 50, "9", -1, -1),
            ("ID1", 51, "1", -1, -1),
            ("ID1", 52, "3", -1, -1),
            ("ID1", 60, "2", 10, 10),
            ("ID1", 70, "9", -1, -1),
            ("ID1", 86471, "2", 86401, 0),
            ("ID2", 86471, "9", -1, -1),
            ("ID2", 86472, "1", -1, -1),
            ("ID2", 86473, "3", -1, -1),
            ("ID2", 86474, "2", 3, 3),
            ("ID2", 86475, "9", -1, -1),
            ("ID2", 86476, "a3", -1, -1),
            ("ID2", 172875, "2", 86400, 86400),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
            "time_diff_to_reset_caped",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_reset_sigmoid(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "a1", -1),
            ("ID1", 2, "a2", -1),
            ("ID1", 49, "a3", -1),
            ("ID1", 50, "9", -1),
            ("ID1", 51, "1", -1),
            ("ID1", 52, "3", -1),
            ("ID1", 60, "2", 10),
            ("ID1", 70, "9", -1),
            ("ID1", 86471, "2", 86401),
            ("ID2", 86471, "9", -1),
            ("ID2", 86472, "1", -1),
            ("ID2", 86473, "3", -1),
            ("ID2", 86474, "2", 3),
            ("ID2", 86475, "9", -1),
            ("ID2", 86476, "a3", -1),
            ("ID2", 172875, "2", 86400),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
        ],
    )

    actual = time_diff_to_reset_sigmoid(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "a1", -1, 0.2689414213699951),
            ("ID1", 2, "a2", -1, 0.2689414213699951),
            ("ID1", 49, "a3", -1, 0.2689414213699951),
            ("ID1", 50, "9", -1, 0.2689414213699951),
            ("ID1", 51, "1", -1, 0.2689414213699951),
            ("ID1", 52, "3", -1, 0.2689414213699951),
            ("ID1", 60, "2", 10, 0.9999546021312976),
            ("ID1", 70, "9", -1, 0.2689414213699951),
            ("ID1", 86471, "2", 86401, 1.0),
            ("ID2", 86471, "9", -1, 0.2689414213699951),
            ("ID2", 86472, "1", -1, 0.2689414213699951),
            ("ID2", 86473, "3", -1, 0.2689414213699951),
            ("ID2", 86474, "2", 3, 0.9525741268224334),
            ("ID2", 86475, "9", -1, 0.2689414213699951),
            ("ID2", 86476, "a3", -1, 0.2689414213699951),
            ("ID2", 172875, "2", 86400, 1.0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "time_diff_to_reset",
            "time_diff_to_reset_sigmoid",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_reset_pattern_virement(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "T1", "action_1", 0),
            ("ID1", "T2", "action_2", 0),
            ("ID1", "T3", "action_3", 0),
            ("ID1", "T4", "9", 1),
            ("ID1", "T5", "3", 1),
            ("ID1", "T6", "1", 1),
            ("ID1", "T7", "2", 1),
            ("ID1", "T8", "9", 2),
            ("ID1", "T9", "2", 2),
        ],
        ["user_id", "audit_fact_date", "event_type", "rank"],
    )

    actual = is_reset_pattern_virement(df)

    expected = pd.DataFrame(
        [
            ("ID1", "T8", "9", 2, False),
            ("ID1", "T9", "2", 2, False),
            ("ID1", "T1", "action_1", 0, False),
            ("ID1", "T2", "action_2", 0, False),
            ("ID1", "T3", "action_3", 0, False),
            ("ID1", "T4", "9", 1, False),
            ("ID1", "T5", "3", 1, False),
            ("ID1", "T6", "1", 1, False),
            ("ID1", "T7", "2", 1, True),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_reset_pattern_disposition(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "action_1", 0),
            ("ID1", 2, "action_2", 0),
            ("ID1", 3, "action_3", 0),
            ("ID1", 4, "9", 1),
            ("ID1", 5, "3", 1),
            ("ID1", 6, "5", 1),
            ("ID1", 7, "9", 2),
            ("ID1", 8, "1", 2),
            ("ID1", 9, "3", 2),
            ("ID1", 10, "2", 2),
            ("ID1", 11, "5", 2),
            ("ID2", 12, "9", 1),
            ("ID2", 13, "3", 1),
            ("ID2", 14, "9", 2),
            ("ID2", 15, "5", 2),
        ],
        ["user_id", "audit_fact_date", "event_type", "rank"],
    )

    actual = is_reset_pattern_disposition(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "action_1", 0, 0),
            ("ID1", 2, "action_2", 0, 0),
            ("ID1", 3, "action_3", 0, 0),
            ("ID1", 4, "9", 1, 0),
            ("ID1", 5, "3", 1, 0),
            ("ID1", 6, "5", 1, 1),
            ("ID1", 7, "9", 2, 0),
            ("ID1", 8, "1", 2, 0),
            ("ID1", 9, "3", 2, 0),
            ("ID1", 10, "2", 2, 0),
            ("ID1", 11, "5", 2, 1),
            ("ID2", 12, "9", 1, 0),
            ("ID2", 13, "3", 1, 0),
            ("ID2", 14, "9", 2, 0),
            ("ID2", 15, "5", 2, 0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_disposition",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_reset(spark):
    df = spark.createDataFrame(
        [
            (1, "ID1", 0, 0),
            (84601, "ID2", 0, 1),
            (169201, "ID3", 1, 0),
        ],
        [
            "audit_fact_date",
            "user_id",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
        ],
    )

    actual = is_reset(df)

    expected = pd.DataFrame(
        [
            (1, "ID1", 0, 0, 0),
            (84601, "ID2", 0, 1, 1),
            (169201, "ID3", 1, 0, 1),
        ],
        columns=[
            "audit_fact_date",
            "user_id",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
            "is_reset",
        ],
    )

    compare_dataframes(actual, expected)


def test_criterion(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "T1", "action_1", 0, 0, 0, 0.000000210),
            ("ID1", "T2", "action_2", 0, 0, 0, 0.0000001000),
            ("ID1", "T3", "action_3", 0, 0, 0, 0.0000001000),
            ("ID1", "T4", "9", 1, 1, 0, 0.0000001000),
            ("ID1", "T5", "3", 1, 0, 0, 0.0000005239),
            ("ID1", "T6", "1", 1, 0, 0, 0.0000001000),
            ("ID1", "T7", "5", 1, 0, 1, 0.000000239),
            ("ID1", "T8", "9", 2, 1, 0, 0.0000002300),
            ("ID1", "T9", "2", 2, 0, 0, 0.0000001000),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
            "time_diff_to_reset_normalized",
        ],
    )

    actual = criterion(df)

    expected = pd.DataFrame(
        [
            ("ID1", "T1", "action_1", 0, 0, 0, 0.000000210, 0.0),
            ("ID1", "T2", "action_2", 0, 0, 0, 0.0000001000, 0.0),
            ("ID1", "T3", "action_3", 0, 0, 0, 0.0000001000, 0.0),
            ("ID1", "T4", "9", 1, 1, 0, 0.0000001000, 0.0000001000),
            ("ID1", "T5", "3", 1, 0, 0, 0.0000005239, 0.0),
            ("ID1", "T6", "1", 1, 0, 0, 0.0000001000, 0.0),
            ("ID1", "T7", "5", 1, 0, 1, 0.000000239, 0.000000239),
            ("ID1", "T8", "9", 2, 1, 0, 0.0000002300, 0.0000002300),
            ("ID1", "T9", "2", 2, 0, 0, 0.0000001000, 0.0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_reset_pattern_virement",
            "is_reset_pattern_disposition",
            "time_diff_to_reset_normalized",
            "criterion",
        ],
    )

    compare_dataframes(actual, expected)


def test_accounts_count(spark):
    df = spark.createDataFrame(
        [
            (1, "ID1", "192.168.1.1", "2022-01-01 00:00:00.0", "action"),
            (2, "ID1", " ", "2022-01-01 00:00:01.0", "action"),
            (3, "ID1", "193.168.1", "2022-01-01 00:00:02.0", "action"),
            (4, "ID1", "193.168.1.1", "2022-01-01 00:00:03.0", "action_1"),
            (5, "ID1", "193.168.1.1", "2022-01-02 00:00:00.0", "2"),
            (6, "ID2", "193.168.1.1", "2022-01-03 00:00:00.0", "action_3"),
            (
                7,
                "ID2",
                "193.168.1.1",
                "2022-01-04 00:00:00.0",
                "9",
            ),
            (8, "ID2", "193.168.1.1", "2022-01-05 00:00:00.0", "5"),
            (9, "ID3", "193.168.1.2", "2022-01-06 00:00:00.0", "3"),
            (10, "ID3", "193.168.1.2", "2022-01-07 00:00:00.0", "2"),
            (11, "ID4", "193.168.1.2", "2022-01-08 00:01:00.0", "2"),
            (12, "ID5", "193.168.1.3", "2022-01-09 00:00:00.0", "2"),
        ],
        ["id", "user_id", "user_ip_address", "audit_fact_date", "event_type"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = accounts_count(df)

    expected = pd.DataFrame(
        [
            (1, "ID1", "192.168.1.1", 1640991600, "action", None),
            (2, "ID1", " ", 1640991601, "action", None),
            (3, "ID1", "193.168.1", 1640991602, "action", None),
            (4, "ID1", "193.168.1.1", 1640991603, "action_1", None),
            (5, "ID1", "193.168.1.1", 1641078000, "2", 1),
            (6, "ID2", "193.168.1.1", 1641164400, "action_3", None),
            (
                7,
                "ID2",
                "193.168.1.1",
                1641250800,
                "9",
                None,
            ),
            (8, "ID2", "193.168.1.1", 1641337200, "5", 2),
            (9, "ID3", "193.168.1.2", 1641423600, "3", None),
            (10, "ID3", "193.168.1.2", 1641510000, "2", 1),
            (11, "ID4", "193.168.1.2", 1641596460, "2", 2),
            (12, "ID5", "193.168.1.3", 1641682800, "2", 1),
        ],
        columns=[
            "id",
            "user_id",
            "user_ip_address",
            "audit_fact_date",
            "event_type",
            "accounts_count",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_last_money_operation_by_ip(spark):
    df = spark.createDataFrame(
        [
            (1, "ID1", "10.168.1.1", "2022-01-01 00:00:00.0", "action_1"),
            (2, "ID1", "10.168.1.1", "2022-01-02 00:00:00.0", "2"),
            (3, "ID2", "10.168.1.1", "2022-01-03 00:00:00.0", "action_3"),
            (
                4,
                "ID2",
                "192.168.1.1",
                "2022-01-04 00:00:00.0",
                "9",
            ),
            (5, "ID2", "10.168.1.1", "2022-01-05 00:00:00.0", "5"),
            (6, "ID3", "10.168.1.2", "2022-01-06 00:00:00.0", "3"),
            (7, "ID3", "10.168.1.2", "2022-01-07 00:00:00.0", "2"),
            (8, "ID4", "10.168.1.2", "2022-01-08 00:01:00.0", "2"),
            (9, "ID5", "10.168.1.3", "2022-01-09 00:00:00.0", "2"),
            (10, "ID6", "", "2022-01-10 00:00:00.0", "action"),
            (11, "ID6", " ", "2022-01-11 00:00:00.0", "action"),
            (12, "ID6", "10.168.1", "2022-01-12 00:00:00.0", "action"),
            (13, "ID7", "192.168.18.10", "2022-01-12 00:00:00.0", "action"),
        ],
        ["id", "user_id", "user_ip_address", "audit_fact_date", "event_type"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = time_diff_to_last_money_operation_by_ip(df)

    expected = pd.DataFrame(
        [
            (1, "ID1", "10.168.1.1", 1640991600, "action_1", None),
            (2, "ID1", "10.168.1.1", 1641078000, "2", None),
            (3, "ID2", "10.168.1.1", 1641164400, "action_3", None),
            (
                4,
                "ID2",
                "192.168.1.1",
                1641250800,
                "9",
                None,
            ),
            (5, "ID2", "10.168.1.1", 1641337200, "5", 259200),
            (6, "ID3", "10.168.1.2", 1641423600, "3", None),
            (7, "ID3", "10.168.1.2", 1641510000, "2", None),
            (8, "ID4", "10.168.1.2", 1641596460, "2", 86460),
            (9, "ID5", "10.168.1.3", 1641682800, "2", None),
            (10, "ID6", "", 1641769200, "action", None),
            (11, "ID6", " ", 1641855600, "action", None),
            (12, "ID6", "10.168.1", 1641942000, "action", None),
            (13, "ID7", "192.168.18.10", 1641942000, "action", None),
        ],
        columns=[
            "id",
            "user_id",
            "user_ip_address",
            "audit_fact_date",
            "event_type",
            "time_diff_to_last_money_operation_by_ip",
        ],
    )

    compare_dataframes(actual, expected)


def test_ip_features(spark):
    df = spark.createDataFrame(
        [
            (1, "ID1", "192.168.1.1", "2022-01-01 00:00:00.0", "action"),
            (2, "ID1", " ", "2022-01-01 00:00:01.0", "action"),
            (3, "ID1", "193.168.1", "2022-01-01 00:00:02.0", "action"),
            (4, "ID1", "193.168.1.1", "2022-01-01 00:00:03.0", "2"),
            (5, "ID1", "193.168.1.1", "2022-01-02 00:00:00.0", "2"),
            (6, "ID2", "193.168.1.1", "2022-01-03 00:00:00.0", "action_3"),
            (7, "ID2", "193.168.1.1", "2022-01-04 00:00:00.0", "9"),
            (8, "ID2", "193.168.1.1", "2022-01-05 00:00:00.0", "5"),
            (9, "ID3", "193.168.1.2", "2022-01-06 00:00:00.0", "2"),
            (10, "ID3", "193.168.1.2", "2022-01-07 00:00:00.0", "2"),
            (11, "ID4", "193.168.1.2", "2022-01-08 00:01:00.0", "2"),
            (12, "ID5", "193.168.1.3", "2022-01-09 00:00:00.0", "2"),
        ],
        ["id", "user_id", "user_ip_address", "audit_fact_date", "event_type"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = ip_features(df)

    expected = pd.DataFrame(
        [
            (1, "ID1", "192.168.1.1", 1640991600, "action", None, None),
            (2, "ID1", " ", 1640991601, "action", None, None),
            (3, "ID1", "193.168.1", 1640991602, "action", None, None),
            (4, "ID1", "193.168.1.1", 1640991603, "2", 1, None),
            (5, "ID1", "193.168.1.1", 1641078000, "2", 1, 86397),
            (6, "ID2", "193.168.1.1", 1641164400, "action_3", None, None),
            (7, "ID2", "193.168.1.1", 1641250800, "9", None, None),
            (8, "ID2", "193.168.1.1", 1641337200, "5", 2, 259200),
            (9, "ID3", "193.168.1.2", 1641423600, "2", 1, None),
            (10, "ID3", "193.168.1.2", 1641510000, "2", 1, 86400),
            (11, "ID4", "193.168.1.2", 1641596460, "2", 2, 86460),
            (12, "ID5", "193.168.1.3", 1641682800, "2", 1, None),
        ],
        columns=[
            "id",
            "user_id",
            "user_ip_address",
            "audit_fact_date",
            "event_type",
            "accounts_count",
            "time_diff_to_last_money_operation_by_ip",
        ],
    )

    compare_dataframes(actual, expected)


def test_ffill(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", None),
            ("ID1", "2022-01-02 00:00:00.0", "action_2", "0"),
            ("ID1", "2022-01-03 00:00:00.0", "action_3", None),
            ("ID1", "2022-01-04 00:00:00.0", "9", "1"),
            ("ID1", "2022-01-05 00:00:00.0", "1", None),
            ("ID1", "2022-01-06 00:00:00.0", "3", None),
            ("ID1", "2022-01-07 00:00:00.0", "2", None),
            ("ID1", "2022-01-08 00:01:00.0", "9", "2"),
            ("ID1", "2022-01-09 00:00:00.0", "2", None),
            ("ID2", "2022-01-10 00:00:00.0", "2", "1"),
            ("ID2", "2022-01-11 00:00:00.0", "2", None),
            ("ID2", "2022-01-12 00:00:00.0", "2", "2"),
        ],
        ["user_id", "audit_fact_date", "event_type", "uuid"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = ffill(df, "uuid")

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", None),
            ("ID1", 1641078000, "action_2", "0"),
            ("ID1", 1641164400, "action_3", "0"),
            ("ID1", 1641250800, "9", "1"),
            ("ID1", 1641337200, "1", "1"),
            ("ID1", 1641423600, "3", "1"),
            ("ID1", 1641510000, "2", "1"),
            ("ID1", 1641596400, "9", "2"),
            ("ID1", 1641682800, "2", "2"),
            ("ID2", 1641769200, "2", "1"),
            ("ID2", 1641855600, "2", "1"),
            ("ID2", 1641942000, "2", "2"),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "uuid",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_count(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "", "", "action_1", None),
            ("ID1", "2022-01-02 00:00:00.0", "", "", "action_2", "3"),
            (
                "ID1",
                "2022-01-03 00:00:00.0",
                "Ayoub",
                "android11/samsungSM-G986B/appver5.15.1",
                "action_3",
                "0",
            ),
            (
                "ID1",
                "2022-01-04 00:00:00.0",
                "Ayoub",
                "android11/samsungSM-G986B/appver5.15.2",
                "9",
                "1",
            ),
            ("ID1", "2022-01-05 00:00:00.0", "", "", "1", "1"),
            ("ID1", "2022-01-06 00:00:00.0", "", "", "3", "1"),
            (
                "ID1",
                "2022-01-07 00:00:00.0",
                "Ayoub",
                "android11/samsungSM-G986B/appver5.15.1",
                "2",
                "1",
            ),
            (
                "ID1",
                "2022-01-08 00:01:00.0",
                "Ayoub2",
                "android11/samsungSM-G986C/appver5.15.1",
                "9",
                "2",
            ),
            ("ID1", "2022-01-09 00:00:00.0", "", "", "2", "2"),
            (
                "ID2",
                "2022-01-10 00:00:00.0",
                "Ayoub4",
                "android11/XiaomiM2102J20SG/appver5.18",
                "2",
                "1",
            ),
            (
                "ID2",
                "2022-01-11 00:00:00.0",
                "Ayoub5",
                "android14/XiaomiM2102J20SG/appver5.18",
                "2",
                "2",
            ),
        ],
        ["user_id", "audit_fact_date", "name", "os", "event_type", "uuid"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = device_count(device_change(df))

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "", "", "action_1", None, 0, None, 0),
            ("ID1", 1641078000, "", "", "action_2", "3", 1, None, 1),
            (
                "ID1",
                1641164400,
                "Ayoub",
                "android11/samsungSM-G986B/appver5.15.1",
                "action_3",
                "0",
                1,
                "samsungSM-G986B",
                0,
            ),
            (
                "ID1",
                1641250800,
                "Ayoub",
                "android11/samsungSM-G986B/appver5.15.2",
                "9",
                "1",
                1,
                "samsungSM-G986B",
                0,
            ),
            ("ID1", 1641337200, "", "", "1", "1", 1, None, 0),
            ("ID1", 1641423600, "", "", "3", "1", 1, None, 0),
            (
                "ID1",
                1641510000,
                "Ayoub",
                "android11/samsungSM-G986B/appver5.15.1",
                "2",
                "1",
                1,
                "samsungSM-G986B",
                0,
            ),
            (
                "ID1",
                1641596400,
                "Ayoub2",
                "android11/samsungSM-G986C/appver5.15.1",
                "9",
                "2",
                2,
                "samsungSM-G986C",
                1,
            ),
            ("ID1", 1641682800, "", "", "2", "2", 2, None, 0),
            (
                "ID2",
                1641769200,
                "Ayoub4",
                "android11/XiaomiM2102J20SG/appver5.18",
                "2",
                "1",
                1,
                "XiaomiM2102J20SG",
                1,
            ),
            (
                "ID2",
                1641855600,
                "Ayoub5",
                "android14/XiaomiM2102J20SG/appver5.18",
                "2",
                "2",
                1,
                "XiaomiM2102J20SG",
                0,
            ),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "name",
            "os",
            "event_type",
            "uuid",
            "device_count",
            "phone",
            "device_change",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_attijari_secure_deactivated_pattern(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0),
            ("ID1", "2022-01-02 00:00:00.0", "7", 0),
            ("ID1", "2022-01-03 00:00:00.0", "3", 0),
            ("ID1", "2022-01-04 00:00:00.0", "6", 1),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1),
            ("ID1", "2022-01-06 00:00:00.0", "3", 1),
            ("ID1", "2022-01-07 00:00:00.0", "2", 1),
            ("ID1", "2022-01-08 00:01:00.0", "5", 2),
            ("ID1", "2022-01-09 00:00:00.0", "2", 2),
            ("ID2", "2022-01-10 00:00:00.0", "2", 0),
            ("ID2", "2022-01-11 00:00:00.0", "2", 1),
        ],
        ["user_id", "audit_fact_date", "event_type", "device_count"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = is_attijari_secure_deactivated_pattern(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, False),
            ("ID1", 1641078000, "7", 0, False),
            ("ID1", 1641164400, "3", 0, False),
            ("ID1", 1641250800, "6", 1, False),
            ("ID1", 1641337200, "1", 1, False),
            ("ID1", 1641423600, "3", 1, False),
            ("ID1", 1641510000, "2", 1, True),
            ("ID1", 1641596400, "5", 2, True),
            ("ID1", 1641682800, "2", 2, True),
            ("ID2", 1641769200, "2", 0, False),
            ("ID2", 1641855600, "2", 1, False),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "device_count",
            "is_attijari_secure_deactivated_pattern",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_attijari_secure_activated_pattern(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0),
            ("ID1", "2022-01-02 00:00:00.0", "7", 0),
            ("ID1", "2022-01-03 00:00:00.0", "3", 0),
            ("ID1", "2022-01-04 00:00:00.0", "6", 1),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1),
            ("ID1", "2022-01-06 00:00:00.0", "3", 1),
            ("ID1", "2022-01-07 00:00:00.0", "2", 1),
            ("ID1", "2022-01-08 00:01:00.0", "5", 2),
            ("ID1", "2022-01-09 00:00:00.0", "2", 2),
            ("ID2", "2022-01-10 00:00:00.0", "2", 0),
            ("ID2", "2022-01-11 00:00:00.0", "2", 1),
        ],
        ["user_id", "audit_fact_date", "event_type", "device_count"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = is_attijari_secure_activated_pattern(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, False),
            ("ID1", 1641078000, "7", 0, False),
            ("ID1", 1641164400, "3", 0, False),
            ("ID1", 1641250800, "6", 1, False),
            ("ID1", 1641337200, "1", 1, False),
            ("ID1", 1641423600, "3", 1, False),
            ("ID1", 1641510000, "2", 1, True),
            ("ID1", 1641596400, "5", 2, True),
            ("ID1", 1641682800, "2", 2, True),
            ("ID2", 1641769200, "2", 0, False),
            ("ID2", 1641855600, "2", 1, False),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "device_count",
            "is_attijari_secure_activated_pattern",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_attijari_secure(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", False, False),
            (
                "ID1",
                "2022-01-02 00:00:00.0",
                "7",
                False,
                False,
            ),
            ("ID1", "2022-01-03 00:00:00.0", "3", False, False),
            (
                "ID1",
                "2022-01-04 00:00:00.0",
                "6",
                False,
                False,
            ),
            ("ID1", "2022-01-05 00:00:00.0", "1", False, False),
            ("ID1", "2022-01-06 00:00:00.0", "3", False, False),
            ("ID1", "2022-01-07 00:00:00.0", "2", False, True),
            ("ID1", "2022-01-08 00:01:00.0", "5", True, False),
            ("ID1", "2022-01-09 00:00:00.0", "2", True, True),
            ("ID2", "2022-01-10 00:00:00.0", "2", False, False),
            ("ID2", "2022-01-11 00:00:00.0", "2", False, False),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "is_attijari_secure_activated_pattern",
            "is_attijari_secure_deactivated_pattern",
        ],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = time_diff_to_attijari_secure(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", False, False, -1),
            ("ID1", 1641078000, "7", False, False, -1),
            ("ID1", 1641164400, "3", False, False, -1),
            ("ID1", 1641250800, "6", False, False, -1),
            ("ID1", 1641337200, "1", False, False, -1),
            ("ID1", 1641423600, "3", False, False, -1),
            ("ID1", 1641510000, "2", False, True, 259200),
            ("ID1", 1641596460, "5", True, False, 345660),
            ("ID1", 1641682800, "2", True, True, 432000),
            ("ID2", 1641769200, "2", False, False, -1),
            ("ID2", 1641855600, "2", False, False, -1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "is_attijari_secure_activated_pattern",
            "is_attijari_secure_deactivated_pattern",
            "time_diff_to_attijari_secure",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_attijari_secure_normalized(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1640991600, "action_1", False, False, -1),
            ("ID1", 1641078000, "7", False, False, -1),
            ("ID1", 1641164400, "3", False, False, -1),
            ("ID1", 1641250800, "6", False, False, -1),
            ("ID1", 1641337200, "1", False, False, -1),
            ("ID1", 1641423600, "3", False, False, -1),
            ("ID1", 1641510000, "2", False, True, 259200),
            ("ID1", 1641596460, "5", True, False, 345660),
            ("ID1", 1641682800, "2", True, True, 432000),
            ("ID2", 1641769200, "2", False, False, -1),
            ("ID2", 1641855600, "2", False, False, -1),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "is_attijari_secure_activated_pattern",
            "is_attijari_secure_deactivated_pattern",
            "time_diff_to_attijari_secure",
        ],
    )

    actual = time_diff_to_attijari_secure_normalized(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", False, False, -1, 0.0000001000),
            ("ID1", 1641078000, "7", False, False, -1, 0.0000001000),
            ("ID1", 1641164400, "3", False, False, -1, 0.0000001000),
            ("ID1", 1641250800, "6", False, False, -1, 0.0000001000),
            ("ID1", 1641337200, "1", False, False, -1, 0.0000001000),
            ("ID1", 1641423600, "3", False, False, -1, 0.0000001000),
            ("ID1", 1641510000, "2", False, True, 259200, 0.0000038580),
            ("ID1", 1641596460, "5", True, False, 345660, 0.0000028930),
            ("ID1", 1641682800, "2", True, True, 432000, 0.0000023148),
            ("ID2", 1641769200, "2", False, False, -1, 0.0000001000),
            ("ID2", 1641855600, "2", False, False, -1, 0.0000001000),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "is_attijari_secure_activated_pattern",
            "is_attijari_secure_deactivated_pattern",
            "time_diff_to_attijari_secure",
            "time_diff_to_attijari_secure_normalized",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_age(spark):
    df = (
        spark.createDataFrame(
            [
                ("ID1", "2022-01-01 00:00:00.0", "8", "2021-01-01 00:00:00.0"),
                (
                    "ID1",
                    "2022-01-02 00:00:00.0",
                    "7",
                    "2021-01-01 00:00:00.0",
                ),
                (
                    "ID1",
                    "2022-01-03 00:00:00.0",
                    "3",
                    "2021-01-01 00:00:00.0",
                ),
                (
                    "ID1",
                    "2022-01-04 00:00:00.0",
                    "6",
                    "2021-01-01 00:00:00.0",
                ),
                (
                    "ID1",
                    "2022-01-05 00:00:00.0",
                    "1",
                    "2021-01-01 00:00:00.0",
                ),
                (
                    "ID1",
                    "2022-01-06 00:00:00.0",
                    "3",
                    "2021-01-01 00:00:00.0",
                ),
                (
                    "ID1",
                    "2022-01-07 00:00:00.0",
                    "2",
                    "2021-01-01 00:00:00.0",
                ),
                ("ID1", "2022-01-08 00:01:00.0", "8", "2021-01-02 00:00:00.0"),
                (
                    "ID1",
                    "2022-01-09 00:00:00.0",
                    "2",
                    "2021-01-02 00:00:00.0",
                ),
                ("ID2", "2022-01-10 00:00:00.0", "8", "2021-01-03 00:00:00.0"),
                (
                    "ID2",
                    "2022-01-11 00:00:00.0",
                    "2",
                    "2021-01-03 00:00:00.0",
                ),
            ],
            ["user_id", "audit_fact_date", "event_type", "creation_date"],
        )
        .withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))
        .withColumn("creation_date", unix_timestamp("creation_date"))
    )

    actual = device_age(df, unix_timestamp(to_timestamp(lit("2023-01-01"))))

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "8", 1609455600, -1),
            ("ID1", 1641078000, "7", 1609455600, -1),
            ("ID1", 1641164400, "3", 1609455600, -1),
            ("ID1", 1641250800, "6", 1609455600, -1),
            ("ID1", 1641337200, "1", 1609455600, -1),
            ("ID1", 1641423600, "3", 1609455600, -1),
            ("ID1", 1641510000, "2", 1609455600, 63072000),
            ("ID1", 1641596460, "8", 1609542000, -1),
            ("ID1", 1641682800, "2", 1609542000, 62985600),
            ("ID2", 1641769200, "8", 1609628400, -1),
            ("ID2", 1641855600, "2", 1609628400, 62899200),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "creation_date",
            "device_age",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_age_normalized(spark):
    df = spark.createDataFrame(
        [
            ("ID1", 1640991600, "8", 1609455600, -1),
            ("ID1", 1641078000, "7", 1609455600, -1),
            ("ID1", 1641164400, "3", 1609455600, -1),
            ("ID1", 1641250800, "6", 1609455600, -1),
            ("ID1", 1641337200, "1", 1609455600, -1),
            ("ID1", 1641423600, "3", 1609455600, -1),
            ("ID1", 1641510000, "2", 1609455600, 63072000),
            ("ID1", 1641596460, "8", 1609542000, -1),
            ("ID1", 1641682800, "2", 1609542000, 62985600),
            ("ID2", 1641769200, "8", 1609628400, -1),
            ("ID2", 1641855600, "2", 1609628400, 62899200),
        ],
        ["user_id", "audit_fact_date", "event_type", "creation_date", "device_age"],
    )

    actual = device_age_normalized(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "8", 1609455600, -1, 0.0000001000),
            ("ID1", 1641078000, "7", 1609455600, -1, 0.0000001000),
            ("ID1", 1641164400, "3", 1609455600, -1, 0.0000001000),
            ("ID1", 1641250800, "6", 1609455600, -1, 0.0000001000),
            ("ID1", 1641337200, "1", 1609455600, -1, 0.0000001000),
            ("ID1", 1641423600, "3", 1609455600, -1, 0.0000001000),
            ("ID1", 1641510000, "2", 1609455600, 63072000, 0.0000000159),
            ("ID1", 1641596460, "8", 1609542000, -1, 0.0000001000),
            ("ID1", 1641682800, "2", 1609542000, 62985600, 0.0000000159),
            ("ID2", 1641769200, "8", 1609628400, -1, 0.0000001000),
            ("ID2", 1641855600, "2", 1609628400, 62899200, 0.0000000159),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "creation_date",
            "device_age",
            "device_age_normalized",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_count_last_7_days(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0),
            ("ID1", "2022-01-02 00:00:00.0", "action_2", 0),
            ("ID1", "2022-01-03 00:00:00.0", "action_3", 1),
            (
                "ID1",
                "2022-01-04 00:00:00.0",
                "9",
                1,
            ),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1),
            ("ID1", "2022-01-06 00:00:00.0", "3", 0),
            ("ID1", "2022-01-07 00:00:00.0", "2", 0),
            (
                "ID1",
                "2022-01-08 00:00:00.0",
                "9",
                1,
            ),
            ("ID1", "2022-01-09 00:00:00.0", "2", 0),
            (
                "ID2",
                "2022-01-10 00:00:00.0",
                "9",
                0,
            ),
            ("ID2", "2022-01-11 00:00:00.0", "1", 0),
            ("ID2", "2022-01-12 00:00:00.0", "3", 0),
            ("ID2", "2022-01-13 00:00:00.0", "2", 0),
            (
                "ID2",
                "2022-01-14 00:00:00.0",
                "9",
                0,
            ),
            ("ID2", "2022-01-20 00:00:00.0", "action_3", 1),
            ("ID2", "2022-01-24 00:00:00.0", "2", 0),
        ],
        ["user_id", "audit_fact_date", "event_type", "device_change"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = device_count_last_7_days(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, 0),
            ("ID1", 1641078000, "action_2", 0, 0),
            ("ID1", 1641164400, "action_3", 1, 0),
            (
                "ID1",
                1641250800,
                "9",
                1,
                0,
            ),
            ("ID1", 1641337200, "1", 1, 0),
            ("ID1", 1641423600, "3", 0, 0),
            ("ID1", 1641510000, "2", 0, 3),
            (
                "ID1",
                1641596400,
                "9",
                1,
                0,
            ),
            ("ID1", 1641682800, "2", 0, 4),
            (
                "ID2",
                1641769200,
                "9",
                0,
                0,
            ),
            ("ID2", 1641855600, "1", 0, 0),
            ("ID2", 1641942000, "3", 0, 0),
            ("ID2", 1642028400, "2", 0, 0),
            (
                "ID2",
                1642114800,
                "9",
                0,
                0,
            ),
            ("ID2", 1642633200, "action_3", 1, 0),
            ("ID2", 1642978800, "2", 0, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "device_change",
            "device_count_last_7_days",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_count_last_14_days(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0),
            ("ID1", "2022-01-02 00:00:00.0", "action_2", 0),
            ("ID1", "2022-01-03 00:00:00.0", "action_3", 1),
            (
                "ID1",
                "2022-01-04 00:00:00.0",
                "9",
                1,
            ),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1),
            ("ID1", "2022-01-06 00:00:00.0", "3", 0),
            ("ID1", "2022-01-07 00:00:00.0", "2", 0),
            (
                "ID1",
                "2022-01-08 00:00:00.0",
                "9",
                1,
            ),
            ("ID1", "2022-01-09 00:00:00.0", "2", 0),
            (
                "ID2",
                "2022-01-10 00:00:00.0",
                "9",
                0,
            ),
            ("ID2", "2022-01-11 00:00:00.0", "1", 0),
            ("ID2", "2022-01-12 00:00:00.0", "3", 0),
            ("ID2", "2022-01-13 00:00:00.0", "2", 0),
            (
                "ID2",
                "2022-01-14 00:00:00.0",
                "9",
                0,
            ),
            ("ID2", "2022-01-16 00:00:00.0", "action_3", 1),
            ("ID2", "2022-01-24 00:00:00.0", "2", 0),
        ],
        ["user_id", "audit_fact_date", "event_type", "device_change"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = device_count_last_14_days(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, 0),
            ("ID1", 1641078000, "action_2", 0, 0),
            ("ID1", 1641164400, "action_3", 1, 0),
            (
                "ID1",
                1641250800,
                "9",
                1,
                0,
            ),
            ("ID1", 1641337200, "1", 1, 0),
            ("ID1", 1641423600, "3", 0, 0),
            ("ID1", 1641510000, "2", 0, 3),
            (
                "ID1",
                1641596400,
                "9",
                1,
                0,
            ),
            ("ID1", 1641682800, "2", 0, 4),
            (
                "ID2",
                1641769200,
                "9",
                0,
                0,
            ),
            ("ID2", 1641855600, "1", 0, 0),
            ("ID2", 1641942000, "3", 0, 0),
            ("ID2", 1642028400, "2", 0, 0),
            (
                "ID2",
                1642114800,
                "9",
                0,
                0,
            ),
            ("ID2", 1642287600, "action_3", 1, 0),
            ("ID2", 1642978800, "2", 0, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "device_change",
            "device_count_last_14_days",
        ],
    )

    compare_dataframes(actual, expected)


def test_device_count_last_30_days(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-01 00:00:00.0", "action_1", 0),
            ("ID1", "2022-01-02 00:00:00.0", "action_2", 0),
            ("ID1", "2022-01-03 00:00:00.0", "action_3", 1),
            (
                "ID1",
                "2022-01-04 00:00:00.0",
                "9",
                1,
            ),
            ("ID1", "2022-01-05 00:00:00.0", "1", 1),
            ("ID1", "2022-01-06 00:00:00.0", "3", 0),
            ("ID1", "2022-01-07 00:00:00.0", "2", 0),
            (
                "ID1",
                "2022-01-08 00:00:00.0",
                "9",
                1,
            ),
            ("ID1", "2022-01-09 00:00:00.0", "2", 0),
            (
                "ID2",
                "2022-01-10 00:00:00.0",
                "9",
                0,
            ),
            ("ID2", "2022-01-11 00:00:00.0", "1", 0),
            ("ID2", "2022-01-12 00:00:00.0", "3", 0),
            ("ID2", "2022-01-13 00:00:00.0", "2", 0),
            (
                "ID2",
                "2022-01-14 00:00:00.0",
                "9",
                0,
            ),
            ("ID2", "2022-01-16 00:00:00.0", "action_3", 1),
            ("ID2", "2022-02-15 00:00:00.0", "2", 0),
        ],
        ["user_id", "audit_fact_date", "event_type", "device_change"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = device_count_last_30_days(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1640991600, "action_1", 0, 0),
            ("ID1", 1641078000, "action_2", 0, 0),
            ("ID1", 1641164400, "action_3", 1, 0),
            (
                "ID1",
                1641250800,
                "9",
                1,
                0,
            ),
            ("ID1", 1641337200, "1", 1, 0),
            ("ID1", 1641423600, "3", 0, 0),
            ("ID1", 1641510000, "2", 0, 3),
            (
                "ID1",
                1641596400,
                "9",
                1,
                0,
            ),
            ("ID1", 1641682800, "2", 0, 4),
            (
                "ID2",
                1641769200,
                "9",
                0,
                0,
            ),
            ("ID2", 1641855600, "1", 0, 0),
            ("ID2", 1641942000, "3", 0, 0),
            ("ID2", 1642028400, "2", 0, 0),
            (
                "ID2",
                1642114800,
                "9",
                0,
                0,
            ),
            ("ID2", 1642287600, "action_3", 1, 0),
            ("ID2", 1644879600, "2", 0, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "device_change",
            "device_count_last_30_days",
        ],
    )

    compare_dataframes(actual, expected)


def test_amount_disposition(spark):
    df = spark.createDataFrame(
        [
            (
                "ID1",
                "2022-01-07 00:00:00.0",
                "5",
                "Mise à disposition depuis la carte XXXXXXXXXXXX4495  sans notification par SMS,  montant=500,  "
                "motif=Aa",
            ),
        ],
        ["user_id", "audit_fact_date", "event_type", "audit_message"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = amount_disposition(df)

    expected = pd.DataFrame(
        [
            (
                "ID1",
                1641510000,
                "5",
                "Mise à disposition depuis la carte XXXXXXXXXXXX4495  sans notification par SMS,  montant=500,  "
                "motif=Aa",
                500.0,
            ),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "audit_message",
            "amount_disposition",
        ],
    )

    compare_dataframes(actual, expected)


def test_amount_virement(spark):
    df = spark.createDataFrame(
        [
            (
                "ID1",
                "2022-01-07 00:00:00.0",
                "2",
                "Compte bénéficiaire [007780000238400030981292] Compte émetteur [000769S000300167] Montant [600] Date"
                " [14/10/2019] Motif [téléphone ] Initiateur [Vy000266810]",
            ),
            (
                "ID2",
                "2022-01-07 00:00:00",
                "B",
                "CashExpress beneficiaire[ ABCABC DAF] Compte emetteur [00049] Montant [330.00] Date [29/12/2022] Motif [Autres] Initiateur [AA111301161]",
            ),
            ("ID2", "2022-01-07 00:00:00", "B", "Detail de la reference wafaCash"),
            ("ID2", "2022-01-07 00:00:00", "1", "Ajouter beneficiare 123"),
        ],
        ["user_id", "audit_fact_date", "event_type", "audit_message"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = amount_virement(df)

    expected = pd.DataFrame(
        [
            (
                "ID1",
                1641510000,
                "2",
                "Compte bénéficiaire [007780000238400030981292] Compte émetteur [000769S000300167] Montant [600] Date "
                "[14/10/2019] Motif [téléphone ] Initiateur [Vy000266810]",
                600.0,
            ),
            (
                "ID2",
                1641510000,
                "B",
                "CashExpress beneficiaire[ ABCABC DAF] Compte emetteur [00049] Montant [330.00] Date [29/12/2022] Motif [Autres] Initiateur [AA111301161]",
                0.0,
            ),
            ("ID2", 1641510000, "B", "Detail de la reference wafaCash", 0.0),
            ("ID2", 1641510000, "1", "Ajouter beneficiare 123", 0.0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "audit_message",
            "amount_virement",
        ],
    )

    compare_dataframes(actual, expected)

def test_amount_cashexpress(spark):
    df = spark.createDataFrame(
        [
            (
                "ID1",
                "2022-01-07 00:00:00.0",
                "2",
                "Compte bénéficiaire [007780000238400030981292] Compte émetteur [000769S000300167] Montant [600] Date"
                " [14/10/2019] Motif [téléphone ] Initiateur [Vy000266810]",
            ),
            (
                "ID2",
                "2022-01-07 00:00:00",
                "B",
                "CashExpress beneficiaire[ ABCABC DAF] Compte emetteur [00049] Montant [330.00] Date [29/12/2022] Motif [Autres] Initiateur [AA111301161]",
            ),
            ("ID2", "2022-01-07 00:00:00", "B", "Detail de la reference wafaCash"),
            ("ID2", "2022-01-07 00:00:00", "1", "Ajouter beneficiare 123"),
        ],
        ["user_id", "audit_fact_date", "event_type", "audit_message"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = amount_cashexpress(df)

    expected = pd.DataFrame(
        [
            (
                "ID1",
                1641510000,
                "2",
                "Compte bénéficiaire [007780000238400030981292] Compte émetteur [000769S000300167] Montant [600] Date "
                "[14/10/2019] Motif [téléphone ] Initiateur [Vy000266810]",
                0.0,
            ),
            (
                "ID2",
                1641510000,
                "B",
                "CashExpress beneficiaire[ ABCABC DAF] Compte emetteur [00049] Montant [330.00] Date [29/12/2022] Motif [Autres] Initiateur [AA111301161]",
                330.0,
            ),
            ("ID2", 1641510000, "B", "Detail de la reference wafaCash", 0.0),
            ("ID2", 1641510000, "1", "Ajouter beneficiare 123", 0.0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "audit_message",
            "amount_cashexpress",
        ],
    )

    compare_dataframes(actual, expected)


def test_cumsum_disposition_12_hours(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-05 00:00:00.0", "5", 500),
            ("ID1", "2022-01-06 00:00:00.0", "2", 0),
            ("ID1", "2022-01-07 00:00:00.0", "5", 500),
            ("ID1", "2022-01-07 05:59:00.0", "5", 600),
        ],
        ["user_id", "audit_fact_date", "event_type", "amount_disposition"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = cumsum_disposition_12_hours(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1641337200, "5", 500, 500),
            ("ID1", 1641423600, "2", 0, 0),
            ("ID1", 1641510000, "5", 500, 500),
            ("ID1", 1641531540, "5", 600, 1100),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_disposition",
            "cumsum_disposition_12_hours",
        ],
    )

    compare_dataframes(actual, expected)


def test_cumsum_virement_12_hours(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2022-01-05 00:00:00.0", "2", 500),
            ("ID1", "2022-01-06 00:00:00.0", "5", 0),
            ("ID1", "2022-01-07 00:00:00.0", "2", 500),
            ("ID1", "2022-01-07 05:59:00.0", "2", 600),
        ],
        ["user_id", "audit_fact_date", "event_type", "amount_virement"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = cumsum_virement_12_hours(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1641337200, "2", 500, 500),
            ("ID1", 1641423600, "5", 0, 0),
            ("ID1", 1641510000, "2", 500, 500),
            ("ID1", 1641531540, "2", 600, 1100),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "cumsum_virement_12_hours",
        ],
    )

    compare_dataframes(actual, expected)


def test_count_device_per_uuid(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "2021-01-01 00:00:00.0", "8", "uuid1"),
            ("ID2", "2021-01-02 00:00:00.0", "3", "uuid1"),
            ("ID3", "2021-01-03 00:00:00.0", "1", "uuid1"),
            ("ID1", "2021-01-04 00:00:00.0", "4", "uuid1"),
            ("ID1", "2021-01-05 00:00:00.0", "5", "uuid1"),
            ("ID1", "2022-01-06 00:00:00.0", "8", "uuid1"),
            ("ID1", "2022-01-07 00:00:00.0", "9", "uuid1"),
            ("ID2", "2022-01-07 05:59:00.0", "2", "uuid1"),
            ("ID1", "2022-01-07 05:59:00.0", "5", "uuid1"),
        ],
        ["user_id", "audit_fact_date", "event_type", "uuid"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = count_device_per_uuid(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1609455600, "8", "uuid1", 0),
            ("ID2", 1609542000, "3", "uuid1", 0),
            ("ID3", 1609628400, "1", "uuid1", 0),
            ("ID1", 1609714800, "4", "uuid1", 0),
            ("ID1", 1609801200, "5", "uuid1", 3),
            ("ID1", 1641423600, "8", "uuid1", 0),
            ("ID1", 1641510000, "9", "uuid1", 0),
            ("ID2", 1641531540, "2", "uuid1", 2),
            ("ID1", 1641531540, "5", "uuid1", 2),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "uuid",
            "count_device_per_uuid",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_last_significant_transaction(spark):
    df = spark.createDataFrame(
        [
            ("U1", 10, "2", 25000, 0),
            ("U1", 20, "3", 0, 0),
            ("U1", 30, "5", 0, 1800),
            ("U1", 40, "5", 0, 1000),
            ("U1", 50, "2", 10000, 0),
            ("U2", 60, "5", 0, 2000),
            ("U2", 70, "4", 0, 0),
            ("U2", 80, "5", 0, 1300),
            ("U2", 91, "2", 40000, 0),
            ("U2", 92, "1", 0, 0),
            ("U2", 100, "2", 12000, 0),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "amount_disposition",
        ],
    )

    actual = time_diff_to_last_significant_transaction(df)

    expected = pd.DataFrame(
        [
            ("U1", 10, "2", 25000, 0, -1),
            ("U1", 20, "3", 0, 0, -1),
            ("U1", 30, "5", 0, 1800, -1),
            ("U1", 40, "5", 0, 1000, 10),
            ("U1", 50, "2", 10000, 0, 40),
            ("U2", 60, "5", 0, 2000, -1),
            ("U2", 70, "4", 0, 0, -1),
            ("U2", 80, "5", 0, 1300, 20),
            ("U2", 91, "2", 40000, 0, -1),
            ("U2", 92, "1", 0, 0, -1),
            ("U2", 100, "2", 12000, 0, 9),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "amount_disposition",
            "time_diff_to_last_significant_transaction",
        ],
    )

    compare_dataframes(actual, expected)


def test_time_diff_to_last_maxed_transaction(spark):
    df = spark.createDataFrame(
        [
            ("U1", 10, "2", 50000, 0),
            ("U1", 20, "3", 0, 0),
            ("U1", 39, "5", 0, 2000),
            ("U1", 40, "5", 0, 1000),
            ("U1", 50, "2", 10000, 0),
            ("U2", 60, "5", 0, 2000),
            ("U2", 70, "4", 0, 0),
            ("U2", 80, "5", 0, 1300),
            ("U2", 91, "2", 50000, 0),
            ("U2", 92, "1", 0, 0),
            ("U2", 100, "2", 12000, 0),
        ],
        [
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "amount_disposition",
        ],
    )

    actual = time_diff_to_last_maxed_transaction(df)

    expected = pd.DataFrame(
        [
            ("U1", 10, "2", 50000, 0, -1),
            ("U1", 20, "3", 0, 0, -1),
            ("U1", 39, "5", 0, 2000, -1),
            ("U1", 40, "5", 0, 1000, 1),
            ("U1", 50, "2", 10000, 0, 40),
            ("U2", 60, "5", 0, 2000, -1),
            ("U2", 70, "4", 0, 0, -1),
            ("U2", 80, "5", 0, 1300, 20),
            ("U2", 91, "2", 50000, 0, -1),
            ("U2", 92, "1", 0, 0, -1),
            ("U2", 100, "2", 12000, 0, 9),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "amount_disposition",
            "time_diff_to_last_maxed_transaction",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_facture_reset_pattern_shutdown(spark):
    df = spark.createDataFrame(
        [
            ("ID1", "T1", "action_1", 0),
            ("ID1", "T2", "9", 1),
            ("ID1", "T3", "3", 1),
            ("ID1", "T4", "9", 2),
            ("ID1", "T5", "3", 2),
            ("ID1", "T6", "A", 2),  # Shutdown
            ("ID1", "T7", "1", 2),
            ("ID1", "T8", "2", 2),  # virement
            ("ID2", "T1", "action_2", 0),
            ("ID2", "T2", "9", 1),
            ("ID2", "T3", "A", 1),  # Shutdown
            ("ID2", "T4", "3", 1),
            ("ID2", "T5", "2", 1),  # virement
            ("ID2", "T6", "1", 1),
            ("ID2", "T7", "2", 1),  # virement
            ("ID3", "T1", "action_1", 0),
            ("ID3", "T2", "9", 1),
            ("ID3", "T3", "1", 1),
            ("ID3", "T4", "A", 1),
            ("ID3", "T5", "3", 1),
            ("ID3", "T6", "A", 1),  # Shutdown
            ("ID3", "T7", "A", 1),  # Shutdown
            ("ID3", "T8", "1", 1),
            ("ID3", "T9", "5", 1),
            ("ID4", "T1", "action_1", 0),
            ("ID4", "T2", "9", 1),
            ("ID4", "T3", "1", 1),
            ("ID4", "T4", "5", 1),  # dispo
            ("ID4", "T5", "3", 1),
            ("ID4", "T6", "1", 1),
            ("ID4", "T7", "A", 1),
            ("ID4", "T8", "5", 1),  # dispo
            ("ID5", "T8", "9", 1),
            ("ID5", "T8", "3", 1),
            ("ID5", "T8", "1", 1),
            ("ID5", "T8", "A", 1),  # Shutdown
            ("ID5", "T8", "1", 1),
            ("ID5", "T8", "2", 1),  # virement
            ("ID6", "T1", "9", 1),
            ("ID6", "T2", "3", 1),
            ("ID6", "T3", "5", 1),
            ("ID6", "T4", "A", 1),  # shutdown
        ],
        ["user_id", "audit_fact_date", "event_type", "rank"],
    )

    actual = is_facture_reset_pattern_shutdown(df)

    expected = pd.DataFrame(
        [
            ("ID1", "T1", "action_1", 0, 0),
            ("ID1", "T2", "9", 1, 0),
            ("ID1", "T3", "3", 1, 0),
            ("ID1", "T4", "9", 2, 0),
            ("ID1", "T5", "3", 2, 0),
            ("ID1", "T6", "A", 2, 0),  # Shutdown
            ("ID1", "T7", "1", 2, 0),
            ("ID1", "T8", "2", 2, 1),  # virement
            ("ID2", "T1", "action_2", 0, 0),
            ("ID2", "T2", "9", 1, 0),
            ("ID2", "T3", "A", 1, 0),  # Shutdown
            ("ID2", "T4", "3", 1, 0),
            ("ID2", "T5", "2", 1, 0),  # virement
            ("ID2", "T6", "1", 1, 0),
            ("ID2", "T7", "2", 1, 1),  # virement
            ("ID3", "T1", "action_1", 0, 0),
            ("ID3", "T2", "9", 1, 0),
            ("ID3", "T3", "1", 1, 0),
            ("ID3", "T4", "A", 1, 0),
            ("ID3", "T5", "3", 1, 0),
            ("ID3", "T6", "A", 1, 0),  # Shutdown
            ("ID3", "T7", "A", 1, 0),  # Shutdown
            ("ID3", "T8", "1", 1, 0),
            ("ID3", "T9", "5", 1, 1),  # dispo
            ("ID4", "T1", "action_1", 0, 0),
            ("ID4", "T2", "9", 1, 0),
            ("ID4", "T3", "1", 1, 0),
            ("ID4", "T4", "5", 1, 0),  # dispo
            ("ID4", "T5", "3", 1, 0),
            ("ID4", "T6", "1", 1, 0),
            ("ID4", "T7", "A", 1, 0),
            ("ID4", "T8", "5", 1, 1),  # dispo
            ("ID5", "T8", "9", 1, 0),
            ("ID5", "T8", "3", 1, 0),
            ("ID5", "T8", "1", 1, 0),
            ("ID5", "T8", "A", 1, 0),  # Shutdown
            ("ID5", "T8", "1", 1, 0),
            ("ID5", "T8", "2", 1, 1),  # virement
            ("ID6", "T1", "9", 1, 0),
            ("ID6", "T2", "3", 1, 0),
            ("ID6", "T3", "5", 1, 0),
            ("ID6", "T4", "A", 1, 0),  # shutdown
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "rank",
            "is_facture_reset_pattern_shutdown",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_safe_facture_device(spark):
    df = spark.createDataFrame(
        [
            ("3", "U1", 1641034800, "S21"),
            ("5", "U1", 1641034801, "S21"),  # Dispo (not safe)
            ("A", "U1", 1641119400, "S22"),  # Bill
            ("3", "U1", 1641204000, "S10"),
            ("5", "U1", 1641288600, "S22"),  # Dispo (safe)
            ("A", "U1", 1641373200, "S10"),  # Bill
            ("3", "U1", 1641457800, "S22"),
            ("4", "U1", 1641542400, "S22"),
            ("3", "U1", 1641627000, "S22"),
            ("2", "U1", 1641711600, "S21"),  # Virement (Not safe new device % bill)
            ("A", "U1", 1641796200, "A11"),  # Bill
            ("2", "U1", 1641880800, "A11"),  # Virement (Safe)
        ],
        ["event_type", "user_id", "audit_fact_date", "mobile_device_id"],
    )

    actual = is_safe_facture_device(df)

    expected = pd.DataFrame(
        [
            ("3", "U1", 1641034800, "S21", 0),
            ("5", "U1", 1641034801, "S21", 0),  # Dispo (not safe)
            ("A", "U1", 1641119400, "S22", 0),  # Bill
            ("3", "U1", 1641204000, "S10", 0),
            ("5", "U1", 1641288600, "S22", 1),  # Dispo (safe)
            ("A", "U1", 1641373200, "S10", 0),  # Bill
            ("3", "U1", 1641457800, "S22", 0),
            ("4", "U1", 1641542400, "S22", 0),
            ("3", "U1", 1641627000, "S22", 0),
            ("2", "U1", 1641711600, "S21", 0),  # Virement (Not safe new device % bill)
            ("A", "U1", 1641796200, "A11", 0),  # Bill
            ("2", "U1", 1641880800, "A11", 1),  # Virement (Safe)
        ],
        columns=[
            "event_type",
            "user_id",
            "audit_fact_date",
            "mobile_device_id",
            "is_safe_facture_device",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_new_beneficiary(spark):
    df = spark.createDataFrame(
        [
            ("1", "RIB ajouté: 123", "2022-01-01 00:00:00", "1", "user1"),
            ("2", "RIB ajouté: 456", "2022-01-01 00:00:00", "1", "user2"),
            ("3", "RIB ajouté: 789", "2022-01-01 00:00:00", "1", "user3"),
            (
                "4",
                "Compte bénéficiaire [123] Compte émetteur [A]",
                "2022-01-01 00:00:00",
                "2",
                "user1",
            ),
            (
                "5",
                "Compte bénéficiaire [456] Compte émetteur [B]",
                "2022-01-05 00:00:00",
                "2",
                "user2",
            ),
            (
                "6",
                "Compte bénéficiaire [789] Compte émetteur [C]",
                "2022-03-01 00:00:00",
                "2",
                "user3",
            ),
        ],
        ["id", "audit_message", "audit_fact_date", "event_type", "user_id"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = is_new_beneficiary(df)

    expected = pd.DataFrame(
        [
            ("1", "RIB ajouté: 123", 1640991600, "1", "user1", 0),
            ("2", "RIB ajouté: 456", 1640991600, "1", "user2", 0),
            ("3", "RIB ajouté: 789", 1640991600, "1", "user3", 0),
            (
                "4",
                "Compte bénéficiaire [123] Compte émetteur [A]",
                1640991600,
                "2",
                "user1",
                0,
            ),
            (
                "5",
                "Compte bénéficiaire [456] Compte émetteur [B]",
                1641337200,
                "2",
                "user2",
                0,
            ),
            (
                "6",
                "Compte bénéficiaire [789] Compte émetteur [C]",
                1646089200,
                "2",
                "user3",
                0,
            ),
        ],
        columns=[
            "id",
            "audit_message",
            "audit_fact_date",
            "event_type",
            "user_id",
            "is_new_beneficiary",
        ],
    )

    compare_dataframes(actual, expected)


def test_average_amount_scaled_12h(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("1", "2022-01-01 00:00:00", "2", 0.5),
            ("1", "2022-01-01 00:00:01", "5", 0.7),
            ("1", "2022-01-01 11:59:59", "2", 1.0),
            ("1", "2022-01-01 12:00:00", "5", 1.0),
            ("2", "2022-01-01 00:00:00", "2", 1.0),
            ("2", "2022-01-01 00:00:01", "2", 1.0),
            ("2", "2022-01-01 12:11:11", "B", 0.4),
        ],
        ["user_id", "audit_fact_date", "event_type", "amount_scaled"],
    ).withColumn("audit_fact_date", unix_timestamp("audit_fact_date"))

    actual = average_amount_scaled_12h(df)

    expected = pd.DataFrame(
        [
            ("1", 1640991600, "2", 0.5, 0.5),
            ("1", 1640991601, "5", 0.7, 0.6),
            ("1", 1641034799, "2", 1, 0.733333),
            ("1", 1641034800, "5", 1, 0.8),
            ("2", 1640991600, "2", 1, 1.0),
            ("2", 1640991601, "2", 1, 1.0),
            ("2", 1641035471, "B", 0.4, 0.4),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_scaled",
            "average_amount_scaled_12h",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_low_virement(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "2", 1000.0),
            ("ID1", 2, "2", 10.0),
            ("ID1", 3, "1", 0.0),
            ("ID1", 4, "2", 500.0),
            ("ID1", 5, "2", 499.99),
        ],
        schema=["user_id", "audit_fact_date", "event_type", "amount_virement"],
    )

    actual = is_low_virement(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "2", 1000.0, 0),
            ("ID1", 2, "2", 10.0, 1),
            ("ID1", 3, "1", 0.0, 0),
            ("ID1", 4, "2", 500.0, 0),
            ("ID1", 5, "2", 499.99, 1),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "is_low_virement",
        ],
    )

    compare_dataframes(actual, expected)


def test_ratio_amount_cumsum_virement(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("ID1", 1, "2", 1000.0, 0.0),
            ("ID1", 2, "2", 10.0, 1010.0),
            ("ID1", 3, "1", 0.0, 0.0),
            ("ID1", 4, "2", 500.0, 1510.0),
            ("ID1", 5, "2", 499.99, 2009.99),
        ],
        schema=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "cumsum_virement_12_hours",
        ],
    )

    actual = ratio_amount_cumsum_virement(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, "2", 1000.0, 0, None),
            ("ID1", 2, "2", 10.0, 1010, 0.009900990099009901),
            ("ID1", 3, "1", 0.0, 0, 0.0),
            ("ID1", 4, "2", 500.0, 1510, 0.33112582781456956),
            ("ID1", 5, "2", 499.99, 2009.99, 0.24875248135562864),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "event_type",
            "amount_virement",
            "cumsum_virement_12_hours",
            "ratio_amount_cumsum_virement",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_awb_secure_deactivated_safe_device(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 1, 0),
            ("ID1", 2, 1, 1),
            ("ID1", 3, 0, 0),
            ("ID1", 4, 0, 1),
        ],
        schema=[
            "user_id",
            "audit_fact_date",
            "is_attijari_secure_deactivated_pattern",
            "is_safe_facture_device",
        ],
    )

    actual = is_awb_secure_deactivated_safe_device(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, 1, 0, 1),
            ("ID1", 2, 1, 1, 0),
            ("ID1", 3, 0, 0, 0),
            ("ID1", 4, 0, 1, 0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "is_attijari_secure_deactivated_pattern",
            "is_safe_facture_device",
            "is_awb_secure_deactivated_safe_device",
        ],
    )

    compare_dataframes(actual, expected)


def test_is_awb_secure_activated_safe_device(spark: SparkSession):
    df = spark.createDataFrame(
        [
            ("ID1", 1, 1, 0),
            ("ID1", 2, 1, 1),
            ("ID1", 3, 0, 0),
            ("ID1", 4, 0, 1),
        ],
        schema=[
            "user_id",
            "audit_fact_date",
            "is_attijari_secure_activated_pattern",
            "is_safe_facture_device",
        ],
    )

    actual = is_awb_secure_activated_safe_device(df)

    expected = pd.DataFrame(
        [
            ("ID1", 1, 1, 0, 1),
            ("ID1", 2, 1, 1, 0),
            ("ID1", 3, 0, 0, 0),
            ("ID1", 4, 0, 1, 0),
        ],
        columns=[
            "user_id",
            "audit_fact_date",
            "is_attijari_secure_activated_pattern",
            "is_safe_facture_device",
            "is_awb_secure_activated_safe_device",
        ],
    )

    compare_dataframes(actual, expected)
