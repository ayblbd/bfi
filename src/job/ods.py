import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_latest_date_from_table,
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
from src.transformation.ods import (
    build_information_compte_client,
    build_information_compte_tenu_par_client,
    build_niveau_regroupement,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM ODS {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    compte_commercial = load_table_from(
        f"{ods_path}/EXPLOIT_COMPTECOMMERCIAL", partition_date, shuffle_partitions
    )

    ret_documents = load_table_from(
        f"{ods_path}/EXP_RETDOCUMENTS", partition_date, shuffle_partitions
    )

    client = load_table_from(
        f"{ods_path}/EXPLOIT_CLIENT", partition_date, shuffle_partitions
    )

    titulaire = load_table_from(
        f"{ods_path}/EXPLOIT_TITULAIRE", partition_date, shuffle_partitions
    )

    sous_compte = load_table_from(
        f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date, shuffle_partitions
    )

    elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )

    contrat = load_table_from(
        f"{ods_path}/CONTRAT", partition_date, shuffle_partitions
    )

    fdc_compte = load_table_from(
        f"{ods_path}/EXPLOIT_FDCCOMPTE", partition_date, shuffle_partitions
    )

    t705 = load_table_from(f"{ods_path}/T705", partition_date, shuffle_partitions)

    cpte_tenu_par_client = load_table_from(
        f"{ods_path}/EXP_CPTETENUPARCLIENT", partition_date, shuffle_partitions
    )

    t608 = load_table_from(f"{ods_path}/T608", partition_date, shuffle_partitions)

    utilisateur = load_table_from(
        f"{ods_path}/EXP_UTILISATEUR", partition_date, shuffle_partitions
    )

    ref_devise = load_latest_date_from_table(
        f"{ods_path}/EXPLOIT_REF_DEVISE", shuffle_partitions
    )

    exploit_fonctionneavec = load_table_from(
        f"{ods_path}/EXPLOIT_FONCTIONNEAVEC", partition_date, shuffle_partitions
    )

    exploit_elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )

    exploit_souscompte = load_table_from(
        f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date, shuffle_partitions
    )

    exploit_titulaire = load_table_from(
        f"{ods_path}/EXPLOIT_TITULAIRE", partition_date, shuffle_partitions
    )

    contrat = load_table_from(
        f"{ods_path}/CONTRAT", partition_date, shuffle_partitions
    )

    type_elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPEELTCONTRAT", partition_date, shuffle_partitions
    )

    type_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPECONTRAT", partition_date, shuffle_partitions
    )

    information_compte_tenu_par_client = build_information_compte_tenu_par_client(
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
        ref_devise,
    )

    to_iceberg(
        information_compte_tenu_par_client,
        "SOCLE",
        "COMPTE",
        "INFORMATION_COMPTE_TENU_PAR_CLIENT",
        partition_date,
    )

    information_compte_client = build_information_compte_client(
        compte_commercial,
        client,
        titulaire,
        sous_compte,
        elt_contrat,
        contrat,
        fdc_compte,
        t705,
        ref_devise,
        partition_date,
    )

    to_iceberg(
        information_compte_client,
        "SOCLE",
        "COMPTE",
        "INFORMATION_COMPTE_CLIENT",
        partition_date,
    )

    niveau_regroupement = build_niveau_regroupement(
        t608,
        t705,
        utilisateur,
    )

    to_iceberg(
        niveau_regroupement, "SOCLE", "AGENCE", "NIVEAU_REGROUPEMENT", partition_date
    )

    spark.stop()


if __name__ == "__main__":
    main()
