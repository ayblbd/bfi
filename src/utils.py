import configparser
from typing import List, Tuple

import numpy as np
from pyspark import SparkConf
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    from_unixtime,
    last,
    lit,
    max,
    min,
    regexp_extract,
    sum,
    to_date,
    trim,
    unix_timestamp,
    when,
)
from pyspark.sql.types import TimestampType

from src.constants import events_mapping


def create_spark_session(config_list: List[Tuple[str, str]]) -> SparkSession:
    config = SparkConf().setAll(config_list)
    return SparkSession.builder.enableHiveSupport().config(conf=config).getOrCreate()


def get_config(filename: str = r"config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read_file(open(filename))
    return config


def extract_field_from_message(field: str) -> Column:
    return regexp_extract(col("audit_message"), f"{field}\\s*(.*)", 1)


def extract_field_with_brackets_from_message(field: str) -> Column:
    return regexp_extract(col("audit_message"), f"{field}\\s*\\[([^\\[]*)\\].*", 1)


def to_csv(df: DataFrame, path: str, partitions: int = 1) -> None:
    df.coalesce(partitions).write.csv(
        path=f"/tmp/{path}",
        mode="overwrite",
        header="true",
        sep="\u0002",
        nullValue="",
        emptyValue="",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        escape="\u0000",
    )


def is_ip(col_name: str = "user_ip_address") -> Column:
    return (
        regexp_extract(
            trim(col(col_name)),
            r"(^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$)",
            0,
        )
        != lit("")
    ) & (~trim(col(col_name)).startswith("192.168"))


def to_unix(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, unix_timestamp(col_name))


def ffill(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(
        col_name,
        last(col_name, ignorenulls=True).over(
            Window.partitionBy("user_id")
            .orderBy("audit_fact_date")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )


def to_parquet(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.parquet(
        path=f"/tmp/{path}",
        mode="overwrite",
    )


def to_orc(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.orc(
        path=f"/tmp/{path}",
        mode="overwrite",
    )


def to_hive(df: DataFrame, db: str, table: str, mode: str = "overwrite") -> None:
    df.write.saveAsTable(name=f"{db}.{table}", format="orc", mode=mode)


def select_last(
    df: DataFrame,
    columns: List[str],
    max_time: DataFrame = None,
) -> DataFrame:
    if not max_time:
        max_time = df.select(max("time").alias("time"))
    return df.join(max_time, "time").select(columns)


def select_live(df: DataFrame, columns: List[str], by: List[str]) -> DataFrame:
    full_by: str = ", ".join(by)
    max_date: DataFrame = (
        df.groupBy(full_by).agg(max("time").alias("time")).select("time", full_by)
    )
    return df.select(*set(columns + ["time"])).join(max_date, by + ["time"])


def select_last_with_history(df: DataFrame, max_audit_fact_date: int) -> DataFrame:

    last_users = (
        df.filter(col("audit_fact_date") > from_unixtime(lit(max_audit_fact_date)))
        .select("user_id")
        .drop_duplicates()
    )

    return df.join(last_users, ["user_id"])


def select_full_with_history(
    df: DataFrame,
    columns: List[str],
) -> DataFrame:
    last_users: DataFrame = (
        df.filter(col("audit_fact_date") >= lit("2022-05-25 15:00:59.0"))
        .select(columns)
        .drop_duplicates()
    )

    return df.join(last_users, columns)


def clean_dataframe(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("id", col("id").cast("long"))
        .drop_duplicates(["id"])
        .filter(col("audit_fact_date").isNotNull())
        .filter(col("event_type").isin(set(events_mapping.keys())))
        .replace(events_mapping, subset=["event_type"])
        .filter(
            ~(
                (col("audit_message") == lit("Detail de la reference wafaCash"))
                & (col("event_type") == lit("B"))
            )
        )
    )


def get_max_audit_date(df: DataFrame):
    return df.select(max(col("audit_fact_date")).alias("MAX")).limit(1).collect()[0].MAX


def avg_daily_cases(df: DataFrame, thr: float, fraud_type: str) -> float:
    if thr < 0:
        raise ValueError("Threshold must be greater than 0")
    FRAUD_TYPES = ["device", "reset"]
    fraud_type = fraud_type.lower()
    if fraud_type not in FRAUD_TYPES:
        raise ValueError(f"Fraud type must be one of {FRAUD_TYPES}")

    result = (
        df.withColumn(
            "date",
            to_date(
                col("audit_fact_date").cast(dataType=TimestampType()),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn(
            "fraud", when(col(f"prediction_{fraud_type}") <= lit(thr), 1).otherwise(0)
        )
        .groupBy("date")
        .agg(sum("fraud").alias("sum_fraud"))
        .agg(avg("sum_fraud").alias("AVG"))
        .collect()[0]
        .AVG
    )
    return result if result else 0.0


def avg_frauded_users_per_day(df: DataFrame, thr: float, fraud_type: str):
    return (
        df.withColumn(
            "date",
            to_date(
                col("audit_fact_date").cast(dataType=TimestampType()),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn(
            "fraud", when(col(f"prediction_{fraud_type}") <= lit(thr), 1).otherwise(0)
        )
        .groupBy("date", "user_id")
        .agg(sum("fraud"))
        .orderBy("date")
        .withColumn("fraud", when(col("sum(fraud)") > 0, 1).otherwise(0))
        .groupBy("date")
        .agg(sum("fraud").alias("sum_fraud"))
        .agg(avg("sum_fraud").alias("AVG"))
        .collect()[0]
        .AVG
    )


def get_optimal_thr(
    df: DataFrame, fraud_type: str, target_avg: float = 30, error_margin: float = 5
) -> float:
    FRAUD_TYPES = ["device", "reset"]
    fraud_type = fraud_type.lower()
    if fraud_type not in FRAUD_TYPES:
        raise ValueError(f"Fraud type must be one of {FRAUD_TYPES}")
    min_split = df.agg(min(f"prediction_{fraud_type}")).collect()[0][0]
    max_split = df.agg(max(f"prediction_{fraud_type}")).collect()[0][0]
    if min_split == max_split:  # Same value for all rows
        return max_split
    thr_list = np.arange(min_split, max_split, 0.1).tolist()
    optimal_thr = binary_search_threshold(
        df, thr_list, fraud_type, target_avg, error_margin
    )
    if isinstance(optimal_thr, float):  # if exact threshold is found
        return optimal_thr
    # if only boundaries are found
    left_thr, right_thr = optimal_thr
    left_avg = avg_daily_cases(df, left_thr, fraud_type)
    right_avg = avg_daily_cases(df, right_thr, fraud_type)
    return left_thr if left_avg > right_avg else right_thr


def binary_search_threshold(
    df: DataFrame,
    thr_list: List[float],
    fraud_type: str,
    target_avg: float,
    error_margin: float,
):
    left = 0
    right = len(thr_list) - 1
    left_avg = avg_daily_cases(df, thr_list[left], fraud_type)
    if (
        target_avg <= left_avg
    ):  # if target_avg is lower than the lowest possible average, return the lowest threshold
        return thr_list[left]
    right_avg = avg_daily_cases(df, thr_list[right], fraud_type)
    if (
        right_avg <= target_avg
    ):  # if target_avg is higher than the highest possible average, return the max threshold
        return thr_list[right]
    while left <= right:
        mid = left + (right - left) // 2
        mid_avg = avg_daily_cases(df, thr_list[mid], fraud_type)
        if (
            abs(mid_avg - target_avg) <= error_margin
        ):  # if calculated average is within error margin of target average, return the threshold
            return thr_list[mid]
        elif mid_avg < target_avg:
            left = mid + 1
        else:
            right = mid - 1
    # If exact target not found then return the bounding values
    return thr_list[right], thr_list[left]
