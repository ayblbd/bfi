from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, Column, current_timestamp, to_date, current_date
import subprocess

# Ensure a SparkSession is created and available as `spark`
spark = SparkSession.builder.appName("TEST AGENCE").getOrCreate()

def get_current_date_minus_12(timestamp: Column = current_timestamp()) -> Column:
    return to_date(timestamp - expr("INTERVAL 12 HOURS"))

# Load your data from a Parquet file
ods_exp_t608_2 = spark.read.format("parquet").load("/nifi/DLKPROD/T608_test").filter(col("partition_date") == get_current_date_minus_12())

# Apply the SQL query
result_df = ods_exp_t608_2.selectExpr(
    "ENTITE as CODEAGENCE",
    "NOMENTIT as NOMAGENCE",
    "GRREG as CODEREGION",
    "DIRZONE as CODEBANQUE",
    "SUCCURSALE AS CODESUCCURSALE",
    "TELAGE AS TELEPHONE",
    "LOCALI",
    "CVILLE",
    "SECTGEO",
    "OPERATIONNELLE"
).filter(
    (col("Entite").isNotNull()) &
    (col("GRREG").isNotNull()) &
    (col("DFVAL") > current_date()) &
    (col("SUCCURSALE").isNotNull())
)

# Definition of the domain and table for saving the result
COUCHE = "SOCLE"
DOMAINE = "AGENCE"
TABLE = "ENTITE_NIVEAU_0"

# Searching for .parquet files in the temporary folder

result_df.write.partitionBy("partition_date").writeTo(
    f"iceberg_catalog.{COUCHE}.{DOMAINE}.{TABLE}"
).overwritePartitions()


spark.stop()
