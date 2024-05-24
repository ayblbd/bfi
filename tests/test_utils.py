from src.utils import create_spark_session


def test_create_spark_session():
    config = [
        ("spark.app.name", "CDM BFI v0.1"),
        ("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.iceberg_catalog.type", "hadoop"),
        ("spark.sql.catalog.iceberg_catalog.warehouse", "./.warehouse"),
    ]
    spark = create_spark_session(config)

    assert spark.conf.get("spark.app.name") == "CDM BFI v0.1"
    assert (
        spark.conf.get("spark.sql.catalog.iceberg_catalog")
        == "org.apache.iceberg.spark.SparkCatalog"
    )
    assert spark.conf.get("spark.sql.catalog.iceberg_catalog.type") == "hadoop"
    assert (
        spark.conf.get("spark.sql.catalog.iceberg_catalog.warehouse") == "./.warehouse"
    )
