import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.common.table_loader import load_table_from, load_latest_value_from_table
from src.common.table_writer import to_iceberg, to_iceberg_append
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.compte import (
    compte_transformation,
    cpttenu_transformation,
    devise_transformation,
    etat_compte_transformation,
    ressource_emploi_transformation,
    compte_sld_transformation,
    compte_origine_transformation,
)


def main():
    config = get_config()
    env = get_env(sys.argv)
    partitiondate = get_partition_date(sys.argv)
    ods_path = config.get(env, "ods_path")
    dataware_path = config.get(env, "dataware_path")
    oracle_gl_path = config.get(env, "oracle_gl_path")
    from_files_path = config.get(env, "from_files_path")
    spark = create_spark_session(config, env, "CDM.COMPTE.1.0")

    ##### Read data from Raw Layer
    compte = load_table_from(f"{ods_path}/EXPLOIT_SOUSCOMPTE", partitiondate)
    elt_contrat = load_table_from(f"{ods_path}/EXPLOIT_ELT_CONTRAT", partitiondate)
    titulaire = load_table_from(f"{ods_path}/EXPLOIT_TITULAIRE", partitiondate)
    dm_sldcli = load_table_from(f"{oracle_gl_path}/DM_SLDCLI", partitiondate)
    compte_commercial = load_table_from(
        f"{ods_path}/EXPLOIT_COMPTECOMMERCIAL", partitiondate
    )
    fdctiers = load_table_from(f"{ods_path}/EXP_FDCTIERS", partitiondate)
    fdccompte = load_table_from(f"{ods_path}/EXPLOIT_FDCCOMPTE", partitiondate)
    fdcgcompte_ctx = load_table_from(
        f"{ods_path}/EXPLOIT_FDCGCOMPTECTX", partitiondate
    )
    donnee_compte = load_table_from(f"{dataware_path}/DONNEESCOMPTE", partitiondate)
    cpt_tenu = load_table_from(f"{ods_path}/EXP_CPTETENUPARCLIENT", partitiondate)
    t605 = load_latest_value_from_table(f"{ods_path}/T605")
    map_ressource_emploi = load_latest_value_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING"
    )

    #### Process Data
    etatcompte_processed = etat_compte_transformation()
    compte_processed = compte_transformation(
        compte,
        elt_contrat,
        titulaire,
        compte_commercial,
        fdccompte,
        fdcgcompte_ctx,
        t605,
        donnee_compte,
        fdctiers,
        partitiondate,
    )
    cpt_tenu_processed = cpttenu_transformation(
        cpt_tenu,
        t605,
        compte,
        elt_contrat,
        titulaire,
        fdccompte,
        fdcgcompte_ctx,
        fdctiers,
        partitiondate,
    )

    map_ressource_emploi_processed = ressource_emploi_transformation(
        map_ressource_emploi
    )

    devise_processed = devise_transformation(t605)

    compte_sld_processed = compte_sld_transformation(
        dm_sldcli, compte_processed, cpt_tenu_processed
    )
    compte_origine_processed = compte_origine_transformation(compte_processed)
    #### Load Data On Socle Layer
    to_iceberg(compte_processed, "SOCLE", "COMPTE", "COMPTE", partitiondate)
    to_iceberg_append(cpt_tenu_processed, "SOCLE", "COMPTE", "COMPTE", partitiondate)
    to_iceberg_append(
        compte_sld_processed, "SOCLE", "COMPTE", "COMPTE", partitiondate
    )
    to_iceberg(etatcompte_processed, "SOCLE", "COMPTE", "ETATCOMPTE", partitiondate)
    to_iceberg(
        map_ressource_emploi_processed,
        "SOCLE",
        "COMPTE",
        "MAP_RESSOURCE_EMPLOI",
        partitiondate,
    )
    to_iceberg(devise_processed, "SOCLE", "COMPTE", "DEVISE", partitiondate)
    to_iceberg(
        compte_origine_processed,
        "APPLICATION",
        "COMPTE",
        "COMPTE_ORIGINE",
        partitiondate,
    )

    spark.stop()


if __name__ == "__main__":
    main()
