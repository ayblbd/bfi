from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr, substring, udf, when, col, current_date
import pyspark.sql.functions as F
from pyspark.sql.types import *
import subprocess

spark = SparkSession.builder.appName("COMPTE TENU PAR CLIENTS").getOrCreate()

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
CPTETENUPARCLIENT_DF = spark.read.format("parquet").load("hdfs://10.9.125.134:8020/nifi/DLKPROD/CPTETENUPARCLIENT/CPTETENUPARCLIENT")

compte_commercial_df.createOrReplaceTempView("COMPTECOMMERCIAL")
exp_ret_documents_df.createOrReplaceTempView("RETDOCUMENTS")
client_df.createOrReplaceTempView("CLIENT")
titulaire_df.createOrReplaceTempView("TITULAIRE")
sous_compte_df.createOrReplaceTempView("SOUSCOMPTE")
elt_contrat_df.createOrReplaceTempView("ELT_CONTRAT")
contrat_df.createOrReplaceTempView("CONTRAT")
fdctier.createOrReplaceTempView("FDCCOMPTE")
T705_df.createOrReplaceTempView("T705")
CPTETENUPARCLIENT_DF.createOrReplaceTempView("CPTETENUPARCLIENT")


result_df = spark.sql(
    """
    select distinct NUMEROTIERS, NUMERODECOMPTE, NUMEROCOMPTEORIGINE, DEVISE, NATURECOMPTE, INTITULE, REPRESENTANTETRANGER, ETAT_COMPTE, DATEEFFET, DATEECHEANCE,  AgenceGEST, CDFDC_GEST, GESTIONNAIRE_GEST, NOMGEST_GEST, QUALITEPTF, AgenceOP, CDFDC_OP, GESTIONNAIRE_OP, NOMGEST_OP FROM (select core2.*, b.RATTACHE as AgenceOP,

    b.CDGEST as CDFDC_OP,

    b.GESTIONNAIRE as GESTIONNAIRE_OP ,

    b.NOMGEST as NOMGEST_OP from (select core.*,

    b.RATTACHE as AgenceGEST,

    b.CDGEST as CDFDC_GEST,

    b.GESTIONNAIRE as GESTIONNAIRE_GEST ,

    b.NOMGEST as NOMGEST_GEST,

    b.QUALITEPTF from (SELECT

    EXPLOIT_CLIENT.NUMEROTIERS,

    q.NUMERODECOMPTE,
    
    q.NUMEROCOMPTEORIGINE,

    EXPLOIT_CLIENT.DEVISE,

    q.NATURECOMPTE,

    q.INTITULE,

    q.REPRESENTANTETRANGER,

    CASE EXPLOIT_SOUSCOMPTE.ETATCOMPTE

        WHEN '0' THEN 'OUVERT'

        WHEN '1' THEN 'OUVERT'

        WHEN '3' THEN 'CLOTURE'

        WHEN '5' THEN 'ACLORE'

        ELSE 'DEMCLOT'

    END AS ETAT_COMPTE,

    EXPLOIT_ELT_CONTRAT.DATEEFFET,

    EXPLOIT_ELT_CONTRAT.DATEECHEANCE,

    q.NATURECOMPTE

FROM

    (SELECT
       CPTTENUPARCLIENT.NUMERODECOMPTE,
       CPTTENUPARCLIENT.NUMEROCOMPTEORIGINE,
       EXPLOIT_COMPTECOMMERCIAL.NATURECOMPTE,
       EXPLOIT_COMPTECOMMERCIAL.INTITULE,
       EXPLOIT_COMPTECOMMERCIAL.REPRESENTANTETRANGER
       
    FROM
        CPTETENUPARCLIENT  AS CPTTENUPARCLIENT LEFT JOIN  
COMPTECOMMERCIAL AS EXPLOIT_COMPTECOMMERCIAL
    ON CPTTENUPARCLIENT.NUMEROCOMPTEORIGINE= EXPLOIT_COMPTECOMMERCIAL.NUMERODECOMPTE) 
q

    LEFT JOIN SOUSCOMPTE AS EXPLOIT_SOUSCOMPTE ON  EXPLOIT_SOUSCOMPTE.numerodecompte = q.NUMEROCOMPTEORIGINE,

    CLIENT AS EXPLOIT_CLIENT,

    TITULAIRE AS EXPLOIT_TITULAIRE,

    ELT_CONTRAT AS EXPLOIT_ELT_CONTRAT,

    CONTRAT AS EXPLOIT_CONTRAT

WHERE

    EXPLOIT_CLIENT.numerotiers = EXPLOIT_TITULAIRE.numerotiers

    AND

    EXPLOIT_SOUSCOMPTE.numeroelementinterne = EXPLOIT_ELT_CONTRAT.numeroelementinterne

    AND

    EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE = EXPLOIT_TITULAIRE.NUMEROCONTRATINTERNE

    AND

    EXPLOIT_TITULAIRE.TITULAIREPRINCIPAL = 1

    AND

    EXPLOIT_ELT_CONTRAT.TYPEDEPRODUIT = 'CA03'

    AND

    EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE = EXPLOIT_CONTRAT.NUMEROCONTRATINTERNE) core , FDCCOMPTE a,

    T705 b

where

    b.RATTACHE = a.LIEUFDCCOMPTE

    AND b.CODEFONDSDECOMMERCE = a.CODEFONDSDECOMMERCE

    AND b.DFVAL > CURRENT_DATE

    AND a.TYPEFDCCOMPTE=1 and core.NUMEROCOMPTEORIGINE=a.NUMERODECOMPTE) core2 , FDCCOMPTE a,

    T705 b

where

    b.RATTACHE = a.LIEUFDCCOMPTE

    AND b.CODEFONDSDECOMMERCE = a.CODEFONDSDECOMMERCE

    AND b.DFVAL > CURRENT_DATE

    AND a.TYPEFDCCOMPTE=2 and core2.NUMEROCOMPTEORIGINE=a.NUMERODECOMPTE);
"""
)


COUCHE = "SOCLE"
DOMAINE = "COMPTE"
TABLE = "INFOS_COMPTE_TENU_CLIENT"

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
# Handling the existence of .parquet files
    for filename in parquet_files.stdout.splitlines():
        subprocess.run(["hdfs", "dfs", "-mv", filename, 
            f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}"])
    
    # Remove the temporary folder after renaming the files
    subprocess.run(["hdfs", "dfs", "-rm", "-r", f"hdfs://10.9.125.134:8020/{COUCHE}/{DOMAINE}/{TABLE}_TEMP"])

# Stop the Spark session if no further operations are needed
spark.stop()
