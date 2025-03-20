import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
    load_latest_date_from_table,
    load_table_from,
    load_table_with_history_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.bfis import (
    build_contentieux,
    build_encours_emplois,
    build_encours_ressource,
    build_fact_table,
    build_information_tier,
    build_ressource_emplois,
    build_solde_client,
    build_solde_client_devise,
    build_solde_client_devise_history,
    build_solde_client_history,
    build_solde_client_history_with_propagation,
    build_taux_change_bam,
    build_taux_change_bam_history,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    from_files_path = config.get(env, "from_files_path")
    dataware_path = config.get(env, "dataware_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM BFI {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    dm_sldcli = load_table_from(
        f"{oracle_gl_path}/DM_SLDCLI", partition_date, shuffle_partitions
    )

    dm_sldcli_history = load_table_with_history_from(
        f"{oracle_gl_path}/DM_SLDCLI", partition_date, shuffle_partitions
    )

    rp_gl_bct = load_table_from(
        f"{oracle_gl_path}/RP_GL_BCT", partition_date, shuffle_partitions
    )

    rp_gl_bct_history = load_table_with_history_from(
        f"{oracle_gl_path}/RP_GL_BCT", partition_date, shuffle_partitions
    )

    t601 = load_table_from(f"{ods_path}/T601", partition_date, shuffle_partitions)

    t601_history = load_table_with_history_from(
        f"{ods_path}/T601", partition_date, shuffle_partitions
    )

    load_latest_date_from_table(
        f"{from_files_path}/TYPE_RESSOURCE", shuffle_partitions
    )

    nouveau_mapping_ressource_emploi = load_latest_date_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING", shuffle_partitions
    )

    ref_devise = load_latest_date_from_table(
        f"{ods_path}/EXPLOIT_REF_DEVISE", shuffle_partitions
    )

    virement_bam = load_table_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/VIREMENT",
        partition_date,
        shuffle_partitions,
    )

    virement_bam_history = load_table_with_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/VIREMENT",
        partition_date,
        shuffle_partitions,
    )

    bbe_bam = load_table_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/COURSBBE",
        partition_date,
        shuffle_partitions,
    )

    bbe_bam_history = load_table_with_history_from(
        f"{from_files_path}/TAUX_CHANGES_BAM/COURSBBE",
        partition_date,
        shuffle_partitions,
    )

    load_table_from(
        f"{ods_path}/EXPLOIT_COMPTECOMMERCIAL", partition_date, shuffle_partitions
    )

    load_table_from(
        f"{ods_path}/EXP_RETDOCUMENTS", partition_date, shuffle_partitions
    )

    client = load_table_from(
        f"{ods_path}/EXPLOIT_CLIENT", partition_date, shuffle_partitions
    )

    load_table_from(
        f"{ods_path}/EXPLOIT_TITULAIRE", partition_date, shuffle_partitions
    )

    load_table_from(
        f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date, shuffle_partitions
    )

    load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )

    load_table_from(f"{ods_path}/CONTRAT", partition_date, shuffle_partitions)

    load_table_from(
        f"{ods_path}/EXPLOIT_FDCCOMPTE", partition_date, shuffle_partitions
    )

    t705 = load_table_from(f"{ods_path}/T705", partition_date, shuffle_partitions)

    fdcg_compte_ctx = load_table_from(
        f"{ods_path}/EXPLOIT_FDCGCOMPTECTX", partition_date, shuffle_partitions
    )

    load_table_from(
        f"{ods_path}/EXP_CPTETENUPARCLIENT", partition_date, shuffle_partitions
    )

    load_table_from(f"{ods_path}/T608", partition_date, shuffle_partitions)

    mapping_marche_ge_pme = load_latest_date_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING_MARCHE_GE_PME", shuffle_partitions
    )

    mapping_pro_retail = load_latest_date_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING_PRO_RETAIL", shuffle_partitions
    )

    fdc_tier = load_table_from(
        f"{ods_path}/EXP_FDCTIERS", partition_date, shuffle_partitions
    )

    utilisateur = load_latest_date_from_table(
        f"{ods_path}/EXP_UTILISATEUR", shuffle_partitions
    )

    t706 = load_latest_date_from_table(f"{ods_path}/T706", shuffle_partitions)

    donnee_client = load_table_from(
        f"{dataware_path}/DONNEESCLIENT", partition_date, shuffle_partitions
    )

    segmentation = load_table_from(
        f"{ods_path}/EXPLOIT_SEGMENTATION", partition_date, shuffle_partitions
    )

    ref_sous_segment = load_table_from(
        f"{ods_path}/EXP_REF_SOUS_SEGMENT", partition_date, shuffle_partitions
    )

    information_compte_tenu_par_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_TENU_PAR_CLIENT",
        partition_date,
    )

    information_compte_client = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_CLIENT", partition_date
    )

    niveau_regroupement = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.AGENCE.NIVEAU_REGROUPEMENT", partition_date
    )

    taux_change_bam = build_taux_change_bam(ref_devise, virement_bam, bbe_bam)

    to_iceberg(
        taux_change_bam,
        "SOCLE",
        "BFI",
        "TAUX_CHANGE_BAM",
        partition_date,
    )

    solde_client_devise = build_solde_client_devise(dm_sldcli, rp_gl_bct, t601)

    solde_client = build_solde_client(
        solde_client_devise,
        taux_change_bam,
    )

    encours_ressource = build_encours_ressource(
        solde_client, nouveau_mapping_ressource_emploi
    )

    encours_emplois = build_encours_emplois(
        solde_client, nouveau_mapping_ressource_emploi
    )

    contentieux = build_contentieux(
        t705, fdcg_compte_ctx, niveau_regroupement, partition_date
    )

    information_tier = build_information_tier(
        client,
        fdc_tier,
        t705,
        t706,
        mapping_marche_ge_pme,
        mapping_pro_retail,
        donnee_client,
        segmentation,
        ref_sous_segment,
        partition_date,
    )

    ressource_emplois = build_ressource_emplois(
        encours_ressource,
        encours_emplois,
        information_compte_tenu_par_client,
        information_compte_client,
        niveau_regroupement,
        contentieux,
        information_tier,
        utilisateur,
    )

    solde_client_devise_history = build_solde_client_devise_history(
        dm_sldcli_history, rp_gl_bct_history, t601_history
    )

    taux_change_bam_history = build_taux_change_bam_history(
        ref_devise, virement_bam_history, bbe_bam_history
    )

    solde_client_history = build_solde_client_history(
        solde_client_devise_history, taux_change_bam_history
    )

    solde_client_history_with_propagation = (
        build_solde_client_history_with_propagation(
            solde_client_history, partition_date
        )
    )

    fact_table = build_fact_table(
        ressource_emplois,
        solde_client_history_with_propagation,
        nouveau_mapping_ressource_emploi,
    )
    to_iceberg(
        fact_table,
        "SOCLE",
        "BFI",
        "RESSOURCE_EMPLOI",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
