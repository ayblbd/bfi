from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, substring, udf, when, col, current_date
import pyspark.sql.functions as F
from pyspark.sql.types import *
import subprocess

# Ensure a SparkSession is created and available as `spark`
spark = SparkSession.builder.appName("TEST AGENCE").getOrCreate()

# Load your data from a Parquet file
ods_exp_t608_2 = spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/ODS/EXP_T608_2")

conditions = (
    col("GRREG").isNull() &
    (col("DFVAL") > current_date()) &
    col("DIRZONE").isNull()
)

# Apply the conditions, select the required columns, and order by ENTITE
result_df = ods_exp_t608_2.filter(conditions).select(
        col("ENTITE").alias("CODEBANQUE"),
        col("NOMENTIT").alias("NOMBANQUE"),
        col("LOCALI"),
        col("CVILLE"),
        col("SECTGEO"),
        col("OPERATIONNELLE")
    )

# Definition of the domain and table for saving the result
COUCHE = "SOCLE"
DOMAINE = "AGENCE"
TABLE = "ENTITE_NIVEAU_3"

# Saving the resulting table
result_df.coalesce(1).write.parquet(
    f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP", mode="overwrite")

# Searching for .parquet files in the temporary folder
parquet_files = subprocess.run(["hdfs", "dfs", "-find", 
                f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP/*.parquet"],
                capture_output=True, text=True)

# Handling the existence of .parquet files
if parquet_files.returncode == 0 and parquet_files.stdout:
    # Remove the _SUCCESS file generated by Spark
    subprocess.run(["hdfs", "dfs", "-rm", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP/_SUCCESS"])

    # Rename the part-XXXXX.parquet files to the target table name
    for filename in parquet_files.stdout.splitlines():
        subprocess.run(["hdfs", "dfs", "-mv", filename, 
            f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}"])
    
    # Remove the temporary folder after renaming the files
    subprocess.run(["hdfs", "dfs", "-rm", "-r", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP"])

# Stop the Spark session if no further operations are needed
spark.stop()
