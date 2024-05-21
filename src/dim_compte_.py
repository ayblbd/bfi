from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, substring, udf, when, col, current_date
import pyspark.sql.functions as F
from pyspark.sql.types import *
import subprocess

# Ensure a SparkSession is created and available as `spark`
spark = SparkSession.builder.appName("TEST DEVIS").getOrCreate()

# Load your data from a Parquet file
compte_commercial_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/COMPTECOMMERCIAL")
exp_ret_documents_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/RETDOCUMENTS")
client_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/CLIENT")
titulaire_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/TITULAIRE")
sous_compte_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/SOUSCOMPTE")
elt_contrat_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/ELT_CONTRAT")
contrat_df= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/CONTRAT")
fdctier= spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/FDCCOMPTE/FDCCOMPTE")
T705_df = spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/T705")

######################################################################
# Perform the join operations
base_query = compte_commercial_df.alias("q") \
    .join(client_df.alias("EXPLOIT_CLIENT"), col("EXPLOIT_CLIENT.NUMEROTIERS") == col("EXPLOIT_CLIENT.NUMEROTIERS")) \
    .join(titulaire_df.alias("EXPLOIT_TITULAIRE"), col("EXPLOIT_CLIENT.NUMEROTIERS") == col("EXPLOIT_TITULAIRE.NUMEROTIERS")) \
    .join(sous_compte_df.alias("EXPLOIT_SOUSCOMPTE"), (col("q.NUMERODECOMPTE") == col("EXPLOIT_SOUSCOMPTE.NUMERODECOMPTE")) ) \
    .join(elt_contrat_df.alias("EXPLOIT_ELT_CONTRAT"), ((col("EXPLOIT_TITULAIRE.NUMEROCONTRATINTERNE") == col("EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE"))& 
                                                     (col("EXPLOIT_SOUSCOMPTE.NUMEROELEMENTINTERNE") == col("EXPLOIT_ELT_CONTRAT.NUMEROELEMENTINTERNE")))) \
    .join(contrat_df.alias("EXPLOIT_CONTRAT"), col("EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE") == col("EXPLOIT_CONTRAT.NUMEROCONTRATINTERNE")) \
    .where((col("EXPLOIT_TITULAIRE.TITULAIREPRINCIPAL") == 1) & 
           (col("EXPLOIT_ELT_CONTRAT.TYPEDEPRODUIT") == "CA03")) \
    .select(
        col("EXPLOIT_CLIENT.NUMEROTIERS").alias("NUMEROTIERS"),
        col("q.NUMERODECOMPTE").alias("NUMERODECOMPTE"),
        col("EXPLOIT_CLIENT.DEVISE").alias("DEVISE"),
        col("q.INTITULE").alias("INTITULE"),
        when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == "0", "OUVERT")
        .when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == "1", "OUVERT")
        .when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == "3", "CLOTURE")
        .when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == "5", "ACLORE")
        .otherwise("DEMCLOT").alias("ETAT_COMPTE"),
        col("EXPLOIT_ELT_CONTRAT.DATEEFFET").alias("DATEEFFET"),
        col("EXPLOIT_ELT_CONTRAT.DATEECHEANCE").alias("DATEECHEANCE"),
        col("q.NATURECOMPTE").alias("NATURECOMPTE")
    )

# Adding FDCTIERS
core_with_fdctiers = base_query.join(
    fdctier.alias("a"),
    ((base_query["NUMERODECOMPTE"] == col("a.NUMERODECOMPTE")) & (col("a.TYPEFDCCOMPTE") == 1))
    ).select(
    base_query["NUMEROTIERS"],
    base_query["NUMERODECOMPTE"], 
    base_query["DEVISE"], 
    base_query["INTITULE"], 
    base_query["ETAT_COMPTE"], 
    base_query["DATEEFFET"], 
    base_query["DATEECHEANCE"], 
    base_query["NATURECOMPTE"], 
    col("a.LIEUFDCCOMPTE").alias("LIEUFDCCOMPTE"),
    col("a.CODEFONDSDECOMMERCE").alias("CODEFONDSDECOMMERCE"))


# First LEFT JOIN with T705
core2 = core_with_fdctiers.join(
    T705_df.alias("b"),
    (col("b.RATTACHE") == core_with_fdctiers["LIEUFDCCOMPTE"]) & 
    (col("b.CODEFONDSDECOMMERCE") == core_with_fdctiers["CODEFONDSDECOMMERCE"]) & 
    (col("b.DFVAL") > current_date())
).select(
    "NUMEROTIERS", "NUMERODECOMPTE", "NATURECOMPTE", "INTITULE", "ETAT_COMPTE", "DEVISE", col("b.RATTACHE").alias("AGENCEGEST"), 
    col("b.CDGEST").alias("CD_FDC_GEST"),
    col("b.GESTIONNAIRE").alias("GESTIONNAIRE_GEST"))

# Second FDCTIERS join and renaming for clarity
core3 = core2.alias("core2").join(
    fdctier.alias("a"),
    (col("core2.NUMERODECOMPTE") == col("a.NUMERODECOMPTE")) & (col("a.TYPEFDCCOMPTE") == 2)).select(
    col("a.NUMERODECOMPTE").alias("NUMERODECOMPTE"), "NUMEROTIERS", "NATURECOMPTE", "INTITULE", "ETAT_COMPTE", "DEVISE", "AGENCEGEST" ,"CD_FDC_GEST",
    "GESTIONNAIRE_GEST", col("a.LIEUFDCCOMPTE").alias("LIEUFDCCOMPTE"),
    col("a.CODEFONDSDECOMMERCE").alias("CODEFONDSDECOMMERCE") )

# Second LEFT JOIN with T705 for core4 and final selection
core4 = core3.join(
    T705_df.alias("b"),
    (core3["LIEUFDCCOMPTE"] == col("b.RATTACHE")) &
    (core3["CODEFONDSDECOMMERCE"] == col("b.CODEFONDSDECOMMERCE")) &
    (col("b.DFVAL") > current_date())
).select(
    core3["NUMEROTIERS"],
    core3["NUMERODECOMPTE"],
    core3["NATURECOMPTE"],
    core3["INTITULE"],
    core3["ETAT_COMPTE"],
    core3["DEVISE"],
    core3["AGENCEGEST"],
    core3["CD_FDC_GEST"],
    core3["GESTIONNAIRE_GEST"],
    col("b.RATTACHE").alias("AGENCEOP"),
    col("b.CDGEST").alias("CD_FDC_OP"),
    col("b.GESTIONNAIRE").alias("GESTIONNAIRE_OP"))

# Since the SQL query specifies DISTINCT, let's apply a distinct operation to the final DataFrame.
distinct_core4 = core4.distinct()


# Definition of the domain and table for saving the result
COUCHE = "SOCLE"
DOMAINE = "COMPTE"
TABLE = "COMPTE_CLIENT_ENRICHED_DAILY"

# Saving the resulting table
distinct_core4.coalesce(1).write.parquet(
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
    for filename in parquet_files.stdout.splitlines():
        subprocess.run(["hdfs", "dfs", "-mv", filename, 
            f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}"])
    
    # Remove the temporary folder after renaming the files
    subprocess.run(["hdfs", "dfs", "-rm", "-r", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP"])

# Stop the Spark session if no further operations are needed
spark.stop()