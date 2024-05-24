import os

from pyspark.sql import DataFrame, SparkSession

from src.transformations import (
    build_compte_tenu,
    build_dim_agence,
    build_dim_compte,
    build_dim_level0,
    build_dim_level1,
    build_dim_level3,
    build_dim_type_ress_emp,
    build_emplois_daily_hist,
    build_ress_daily_hist,
    build_sldcli_nature_daily_devises,
    build_taux_change_bam_hist,
)
from src.utils import (
    create_spark_session,
    get_config,
    load_parquet_from_yesterday,
    to_iceberg,
)


def main():
    config = get_config()

    env = os.environ.get("ENV", "prd")
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    warehouse = config.get(env, "warehouse")
    raw_path = config.get(env, "raw_path")

    config_spark = [
        ("spark.app.name", "CDM BFI v0.1"),
        ("spark.jars", "/opt/spark/jars/iceberg-spark-runtime-3.3_2.12-1.4.1.jar"),
        ("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.iceberg_catalog.type", "hadoop"),
        ("spark.sql.catalog.iceberg_catalog.warehouse", warehouse),
        (
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        ),
    ]

    spark: SparkSession = create_spark_session(config_spark)

    compte_commercial = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/COMPTECOMMERCIAL"
    )

    ret_documents = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/RETDOCUMENTS"
    )

    client = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/CLIENT")

    titulaire = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/TITULAIRE")

    sous_compte = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/SOUSCOMPTE")

    elt_contrat = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/ELT_CONTRAT")

    contrat = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/CONTRAT")

    fdc_compte = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/FDCCOMPTE/FDCCOMPTE"
    )

    t705 = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/T705")

    cpte_tenu_par_client = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/CPTETENUPARCLIENT/CPTETENUPARCLIENT"
    )

    exp_t608_2 = load_parquet_from_yesterday(spark, f"{raw_path}/ODS/EXP_T608_2")

    capjour_param2_oe = load_parquet_from_yesterday(
        spark, f"{raw_path}/ODS/CAPJOUR_PARAM2_OE"
    )

    mapping_type_emploi = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/MAPPING_TYPE_EMPLOI/MAPPING_TYPE_EMPLOI"
    )

    dm_sldcli = load_parquet_from_yesterday(spark, f"{raw_path}/DLKPROD/DM_SLDCLI")

    rp_gl_bct = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/RP_GL_BCT/RP_GL_BCT"
    )

    reference_nature_compte_pci = load_parquet_from_yesterday(
        spark,
        f"{raw_path}/DLKPROD/REFERENCE_NATURE_COMPTE_PCI/REFERENCE_NATURE_COMPTE_PCI",
    )

    mapping_type_ressource = load_parquet_from_yesterday(
        spark, f"{raw_path}/DLKPROD/MAPPING_TYPE_RESSOURCE/MAPPING_TYPE_RESSOURCE"
    )

    exploit_ref_devise = load_parquet_from_yesterday(
        spark, f"{raw_path}/ODS/EXPLOIT_REF_DEVISE/EXPLOIT_REF_DEVISE"
    )

    taux_change_billet_hist = (
        spark.read.option("header", "true")
        .option("sep", ",")
        .csv("/CHARGEMENT/TAUX_CHANGE_BILLET_HIST.csv")
    )
    taux_change_virement_hist = (
        spark.read.option("header", "true")
        .option("sep", ",")
        .csv("/CHARGEMENT/TAUX_CHANGE_VIREMENT_HIST.csv")
    )

    compte_tenu_client: DataFrame = build_compte_tenu(
        spark,
        compte_commercial,
        ret_documents,
        client,
        titulaire,
        sous_compte,
        elt_contrat,
        contrat,
        fdc_compte,
        t705,
        cpte_tenu_par_client,
    )

    to_iceberg(spark, compte_tenu_client, "SOCLE", "COMPTE", "COMPTE_TENU_CLIENT")

    compte_client = build_dim_compte(
        compte_commercial,
        client,
        titulaire,
        sous_compte,
        elt_contrat,
        contrat,
        fdc_compte,
        t705,
    )

    to_iceberg(spark, compte_client, "SOCLE", "COMPTE", "COMPTE_CLIENT")

    dim_level0 = build_dim_level0(exp_t608_2)
    dim_level1 = build_dim_level1(exp_t608_2)
    dim_level3 = build_dim_level3(exp_t608_2)

    niveau_regroup = build_dim_agence(
        spark,
        exp_t608_2,
        dim_level1,
        dim_level0,
        dim_level3,
    )

    to_iceberg(spark, niveau_regroup, "SOCLE", "AGENCE", "NIVEAU_REGROUP")

    type_ress_emp = build_dim_type_ress_emp(capjour_param2_oe)

    to_iceberg(spark, type_ress_emp, "SOCLE", "COMPTABILITE", "TYPE_RESS_EMP")

    encours_emplois = build_emplois_daily_hist(
        capjour_param2_oe, type_ress_emp, mapping_type_emploi
    )

    to_iceberg(spark, encours_emplois, "SOCLE", "COMPTABILITE", "ENCOURS_EMPLOIS")

    taux_change_bam_hist = build_taux_change_bam_hist(
        spark, taux_change_billet_hist, taux_change_virement_hist
    )

    to_iceberg(
        spark, taux_change_bam_hist, "nifi", "DLKPROD", "TAUX_CHANGE_BAM_HIST"
    )  # WOULD WORK WITH DIFFERENT catalog

    solde_client = build_sldcli_nature_daily_devises(
        spark,
        dm_sldcli,
        rp_gl_bct,
        reference_nature_compte_pci,
        taux_change_bam_hist,
        exploit_ref_devise,
    )

    to_iceberg(
        spark,
        solde_client,
        "SOCLE",
        "COMPTABILITE",
        "SOLDE_CLIENT",
    )

    encours_ressource = build_ress_daily_hist(
        solde_client, mapping_type_ressource, type_ress_emp
    )

    to_iceberg(spark, encours_ressource, "SOCLE", "COMPTABILITE", "ENCOURS_RESSOURCE")

    spark.stop()


if __name__ == "__main__":
    main()
