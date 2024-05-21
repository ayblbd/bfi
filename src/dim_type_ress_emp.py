from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, substring, udf, when, col, current_date, lit
import pyspark.sql.functions as F
from pyspark.sql.types import *
import subprocess

# Ensure a SparkSession is created and available as `spark`
spark = SparkSession.builder.appName("TEST RESS").getOrCreate()
df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/ODS/CAPJOUR_PARAM2_OE")

# First part of the UNION - Working with LIGNE_CREDIT
part1 = df.filter(col("LIGNE_CREDIT").isNotNull()).select(
    col("PCI"),
    col("NATURE"),
    col("LIGNE_CREDIT").alias("CODE_LIGNE"),
    lit("RESS").alias("CATEGORIE"),
    when(substring(col("LIGNE_CREDIT"), 1, 2) == '50', 'R.BAM/TRES/CCP')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '51', 'DET/EC')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '52', 'DET/SF')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '61', 'RAV')
    .when(substring(col("LIGNE_CREDIT"), 1, 3).isin(['624', '627']), 'REP')
    .when(substring(col("LIGNE_CREDIT"), 1, 3).isin(['625', '626']), 'RAT')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '65', 'DEP.DIV')
    .when(substring(col("LIGNE_CREDIT"), 1, 2).isin(['67', '68']), 'DEP.DIV')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '73', 'CD')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '10', 'E.BAM/TRES/CCP')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '11', 'CAV/EC')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '12', 'CAV/EF')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '21', 'CAV')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '22', 'TRESO')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '23', 'EQUIPI')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '24', 'CONS')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '25', 'IMMO')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '27', 'DEPOTS DIV')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '28', 'CRE.DIV')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '29', 'CRE.SOUF')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '31', 'TITRES')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '91', 'HB.ENG/EC')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '92', 'HB.ENG/CLI')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '95', 'HB.AVAL/EC')
    .when(substring(col("LIGNE_CREDIT"), 1, 2) == '96', 'HB.CAUT.AVAL')
    .otherwise('AUTRES').alias("Type_RESS_EMPO")
)


# Second part of the UNION - Working with LIGNE_Debit
part2 = df.filter(col("LIGNE_Debit").isNotNull()).select(
    col("PCI"),
    col("NATURE"),
    col("LIGNE_Debit").alias("CODE_LIGNE"),
    lit("EMP").alias("CATEGORIE"),
    when(substring(col("LIGNE_Debit"), 1, 2) == '50', 'R.BAM/TRES/CCP')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '51', 'DET/EC')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '52', 'DET/SF')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '61', 'RAV')
    .when(substring(col("LIGNE_Debit"), 1, 3).isin(['624', '627']), 'REP')
    .when(substring(col("LIGNE_Debit"), 1, 3).isin(['625', '626']), 'RAT')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '65', 'DEP.DIV')
    .when(substring(col("LIGNE_Debit"), 1, 2).isin(['67', '68']), 'DEP.DIV')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '73', 'CD')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '10', 'E.BAM/TRES/CCP')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '11', 'CAV/EC')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '12', 'CAV/EF')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '21', 'CAV')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '22', 'TRESO')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '23', 'EQUIPI')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '24', 'CONS')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '25', 'IMMO')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '27', 'DEPOTS DIV')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '28', 'CRE.DIV')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '29', 'CRE.SOUF')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '31', 'TITRES')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '91', 'HB.ENG/EC')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '92', 'HB.ENG/CLI')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '95', 'HB.AVAL/EC')
    .when(substring(col("LIGNE_Debit"), 1, 2) == '96', 'HB.CAUT.AVAL')
    .otherwise('AUTRES').alias("Type_RESS_EMPO")
)

# Union the two parts
union_df = part1.union(part2)
df_result = union_df.distinct()

# Definition of the domain and table for saving the result
COUCHE = "SOCLE"
DOMAINE = "COMPTABILITE"
TABLE = "TYPE_RESS_EMP"

# Saving the resulting table
df_result.coalesce(1).write.parquet(
    f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP", mode="overwrite")

# Searching for .parquet files in the temporary folder
parquet_files = subprocess.run(["hdfs", "dfs", "-find", 
                f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP/*.parquet"],
                capture_output=True, text=True)

# Handling the existence of .parquet files
if parquet_files.returncode == 0 and parquet_files.stdout:
    # Remove the _SUCCESS file generated by Spark
    subprocess.run(["hdfs", "dfs", "-rm", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP/_SUCCESS"])
# Handling the existence of .parquet files
   # subprocess.run(["hdfs", "dfs", "-rm", "-r", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}])

    # Rename the part-XXXXX.parquet files to the target table name
    for filename in parquet_files.stdout.splitlines():
        subprocess.run(["hdfs", "dfs", "-mv", filename, 
            f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}"])
    
    # Remove the temporary folder after renaming the files
    subprocess.run(["hdfs", "dfs", "-rm", "-r", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP"])

# Stop the Spark session if no further operations are needed
spark.stop()
