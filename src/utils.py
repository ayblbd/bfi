import configparser
from typing import Callable, List, Tuple

from pyspark import SparkConf
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, to_date


def create_spark_session(config_list: List[Tuple[str, str]]) -> SparkSession:
    config = SparkConf().setAll(config_list)
    return SparkSession.builder.config(conf=config).getOrCreate()


def get_config(filename: str = r"config.ini") -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read_file(open(filename))
    return config


def load_parquet_from_yesterday(
    spark: SparkSession, path: str, partition_column: str = "partition_date"
) -> DataFrame:
    return (
        spark.read.format("parquet")
        .load(path)
        .filter(col(partition_column) == get_current_date_minus_12())
    )


def get_current_date_minus_12(
    timestamp: Callable[[], Column] = current_timestamp
) -> Column:
    return to_date(timestamp() - expr("INTERVAL 12 HOURS"))


def to_iceberg(spark: SparkSession, df, couche: str, domaine: str, table: str):
    table_name = f"iceberg_catalog.{couche}.{domaine}.{table}"

    df.createOrReplaceTempView("temp_table")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING ICEBERG
        PARTITIONED BY (partition_date)
        AS SELECT *
        FROM temp_table
        """
    )

    df.writeTo(table_name).overwritePartitions()
