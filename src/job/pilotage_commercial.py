import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

from src.common.table_loader import (
    load_iceberg_table_from,
    load_iceberg_table_from_history,
    load_table_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.pilotage_commercial import (
    build_encours_assurance_axa,
    build_encours_assurance_rma,
    build_encours_assurance_saham,
    build_encours_assurance_wafa,
    build_encours_assurances,
    build_fact_table_ressource_emplois,
    build_kpi,
    build_leasing,
    build_opcvm,
    build_pourcent_quarter_saisonnality,
    build_tro,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    config.get(env, "ods_path")
    magiclear_path = config.get(env, "magiclear_path")
    cassiopea_path = config.get(env, "cassiopea_path")
    from_files_path = config.get(env, "from_files_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM PILOTAGE COMMERCIAL {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    n_limit_hist: int = 6

    information_tier = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.CLIENT.INFORMATION_TIER", partition_date
    )
    compte_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_CLIENT", partition_date
    )
    compte_tenu_par_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_TENU_PAR_CLIENT",
        partition_date,
    )
    agence_infos = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.AGENCE.NIVEAU_REGROUPEMENT", partition_date
    )

    saisonnalite = load_table_from(
        f"{from_files_path}/SAISONNALITE", partition_date, shuffle_partitions
    )
    saisonnalite_with_trim = build_pourcent_quarter_saisonnality(saisonnalite)

    objectifs_tro = load_table_from(
        f"{from_files_path}/OBJECTIFS_TRO", partition_date, shuffle_partitions
    )

    ressource_emplois = load_iceberg_table_from_history(
        f"{catalog_name}.SOCLE.COMPTABILITE.RESSOURCE_EMPLOI",
        partition_date,
        shuffle_partitions,
        n_limit_hist,
    )

    kpi_ressource_emplois = (
        ressource_emplois.transform(
            build_kpi,
            partition_date=partition_date,
            by="soldejourcr_devise",
            has_delta=True,
        )
        .transform(
            build_kpi,
            partition_date=partition_date,
            by="soldejourdb_devise",
            has_delta=True,
        )
        .filter(col("partitiondate") == to_date(lit(partition_date)))
    )

    fact_table = build_fact_table_ressource_emplois(
        kpi_ressource_emplois, information_tier
    )

    to_iceberg(
        fact_table,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "RESSOURCE_EMPLOI",
        partition_date,
    )

    tro_ressources_emplois = build_tro(
        df_kpi_final=fact_table,
        objectifs_tro=objectifs_tro,
        saisonnalite=saisonnalite_with_trim,
        categorie="RESSOURCE_EMPLOI",
    )

    to_iceberg(
        tro_ressources_emplois,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "TRO_RESSOURCE_EMPLOI",
        partition_date,
    )

    # OPCVM: Construction de la table enrichie
    v_corres_val = load_table_from(
        f"{magiclear_path}/V_CORRES_VAL", partition_date, shuffle_partitions
    )

    v_cours_ref = load_table_from(
        f"{magiclear_path}/V_COURS_REF", partition_date, shuffle_partitions
    )

    v_solde_clt = load_table_from(
        f"{magiclear_path}/V_SOLDE_CLT", partition_date, shuffle_partitions
    )

    load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_CLIENT", partition_date
    )
    load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_TENU_PAR_CLIENT",
        partition_date,
    )
    agence_infos = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.AGENCE.NIVEAU_REGROUPEMENT", partition_date
    )

    opcvm = build_opcvm(
        v_corres_val,
        v_cours_ref,
        v_solde_clt,
        information_tier,
        compte_client,
        compte_tenu_par_client,
        agence_infos,
        partition_date,
    )

    to_iceberg(opcvm, "SOCLE", "COMPTABILITE", "ENCOURS_OPCVM", partition_date)

    opcvm_hist = load_iceberg_table_from_history(
        f"{catalog_name}.SOCLE.COMPTABILITE.ENCOURS_OPCVM",
        partition_date,
        shuffle_partitions,
        n_limit_hist,
    )

    kpi_opcvm = opcvm_hist.transform(
        build_kpi,
        partition_date=partition_date,
        by="ENCOURS",
        partition_columns=[
            "NUMERO_COMPTE",
            "CODE_VALEUR",
            "CATEGORIE_LIBELLE",
            "TYPE_LIBELLE",
        ],
        order_column="DATE_TRAITEMENT",
        has_delta=True,
    ).filter(
        (col("partitiondate") == to_date(lit(partition_date)))
        & (col("DATE_REALISATION") == to_date(lit(partition_date)))
    )

    to_iceberg(
        kpi_opcvm, "APPLICATION", "TDB_PILOTAGE_COMMERCIAL", "OPCVM", partition_date
    )
    kpi_opcvm = load_iceberg_table_from(
        table_path=f"{catalog_name}.APPLICATION.TDB_PILOTAGE_COMMERCIAL.OPCVM",
        partition_column="partitiondate",
        filter_date=partition_date,
    )

    tro_opcvm = build_tro(
        df_kpi_final=kpi_opcvm,
        objectifs_tro=objectifs_tro,
        saisonnalite=saisonnalite_with_trim,
        categorie="OPCVM",
    )

    to_iceberg(
        tro_opcvm,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "TRO_OPCVM",
        partition_date,
    )

    # ASSURANCES
    epargne_axa = load_table_from(
        f"{from_files_path}/AXA_ASSURANCE", partition_date, shuffle_partitions
    )

    epargne_rma = load_table_from(
        f"{from_files_path}/RMA_WATANYA", partition_date, shuffle_partitions
    )

    epargne_liberis = load_table_from(
        f"{from_files_path}/SAHAM_ASSURANCE", partition_date, shuffle_partitions
    )

    encours_epargne = load_table_from(
        f"{from_files_path}/WAFA_ASSURANCE", partition_date, shuffle_partitions
    )

    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    )

    # Construction de la table enrichie ASSURANCES_AXA
    encours_axa = build_encours_assurance_axa(
        epargne_axa,
        contrat_produit,
        compte_client,
        compte_tenu_par_client,
        information_tier,
        agence_infos,
    )

    # Construction de la table enrichie ASSURANCES_RMA
    encours_rma = build_encours_assurance_rma(
        epargne_rma,
        contrat_produit,
        compte_client,
        compte_tenu_par_client,
        information_tier,
        agence_infos,
    )

    # Construction de la table enrichie ASSURANCES_SAHAM
    encours_saham = build_encours_assurance_saham(
        epargne_liberis,
        contrat_produit,
        compte_client,
        compte_tenu_par_client,
        information_tier,
        agence_infos,
    )

    # Construction de la table enrichie ASSURANCES_WAFA
    encours_wafa = build_encours_assurance_wafa(
        encours_epargne,
        contrat_produit,
        compte_client,
        compte_tenu_par_client,
        information_tier,
        agence_infos,
    )

    # Construction de la table enrichie ASSURANCES regroupant ces differentes tables enrichies
    encours_assurances = build_encours_assurances(
        encours_axa, encours_rma, encours_saham, encours_wafa
    )

    to_iceberg(
        encours_assurances,
        "SOCLE",
        "COMPTABILITE",
        "ENCOURS_ASSURANCES",
        partition_date,
    )

    encours_assurances_hist = load_iceberg_table_from_history(
        f"{catalog_name}.SOCLE.COMPTABILITE.ENCOURS_ASSURANCES",
        partition_date,
        shuffle_partitions,
        n_limit_hist,
    )

    kpi_assurances = encours_assurances_hist.transform(
        build_kpi,
        partition_date=partition_date,
        by="ENCOURS",
        has_delta=False,
        partition_columns=["NUMERO_COMPTE", "ENTREPRISE", "PRODUIT", "IDENTIFIANT"],
        order_column="DATE_TRAITEMENT",
    ).filter(col("partitiondate") == to_date(lit(partition_date)))
    to_iceberg(
        kpi_assurances,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "ASSURANCES",
        partition_date,
    )

    tro_assurances = build_tro(
        df_kpi_final=kpi_assurances,
        objectifs_tro=objectifs_tro,
        saisonnalite=saisonnalite_with_trim,
        categorie="ASSURANCES",
    )

    to_iceberg(
        tro_assurances,
        "APPLICATION",
        "TDB_PILOTAGE_COMMERCIAL",
        "TRO_ASSURANCES",
        partition_date,
    )

    # LEASING
    rib = load_table_from(
        f"{cassiopea_path}/RIB", partition_date, shuffle_partitions
    )
    actrib = load_table_from(
        f"{cassiopea_path}/ACTRIB", partition_date, shuffle_partitions
    )
    acteur = load_table_from(
        f"{cassiopea_path}/ACTEUR", partition_date, shuffle_partitions
    )
    dosrubrique = load_table_from(
        f"{cassiopea_path}/DOSRUBRIQUE", partition_date, shuffle_partitions
    )
    dossier = load_table_from(
        f"{cassiopea_path}/DOSSIER", partition_date, shuffle_partitions
    )
    dosrubecheancier = load_table_from(
        f"{cassiopea_path}/DOSRUBECHEANCIER", partition_date, shuffle_partitions
    )
    facture = load_table_from(
        f"{cassiopea_path}/FACTURE", partition_date, shuffle_partitions
    )

    leasing_df = build_leasing(
        rib, actrib, acteur, dosrubrique, dossier, dosrubecheancier, facture
    )
    to_iceberg(
        leasing_df, "SOCLE", "COMPTABILITE", "ENCOURS_LEASING_V2", partition_date
    )

    spark.stop()


if __name__ == "__main__":
    main()
