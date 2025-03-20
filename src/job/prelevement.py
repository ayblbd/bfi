import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
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
from src.transformation.prelevement import (
    build_prelevement_contrat,
    build_prelevement_contrat_approvisionne,
    build_stock_contrat_banque,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")
    raw_path = config.get(env, "raw_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM PRELEVEMENT {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    type_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPECONTRAT", partition_date, shuffle_partitions
    )

    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    ).persist()

    produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.PRODUIT.PRODUIT", partition_date
    )

    exp_contratprerenouv_banque = load_table_from(
        f"{ods_path}/EXP_CONTRATPRERENOUV_BANQUE", partition_date, shuffle_partitions
    )

    typecontrat = load_table_from(
        f"{ods_path}/EXPLOIT_TYPECONTRAT", partition_date, shuffle_partitions
    )

    contrat = load_table_from(
        f"{ods_path}/CONTRAT", partition_date, shuffle_partitions
    )

    mouvement_position = load_table_from(
        f"{raw_path}/EXPLOIT/MOUVEMENTPOSITION", partition_date, shuffle_partitions
    )

    sous_compte = load_table_from(
        f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date, shuffle_partitions
    )

    sage_decodage = load_table_from(
        f"{ods_path}/EXPLOIT_SAGEDECODAGE", partition_date, shuffle_partitions
    )

    titulaire = load_table_from(
        f"{ods_path}/EXPLOIT_TITULAIRE", partition_date, shuffle_partitions
    )
    renouv_produit = load_table_from(
        f"{ods_path}/EXPLOIT_RENOUV_PRODUIT", partition_date, shuffle_partitions
    )
    categorie_assurance = load_table_from(
        f"{ods_path}/EXP_CATEGORIE_ASSURANCE", partition_date, shuffle_partitions
    )

    prelevement_contrat = build_prelevement_contrat(
        contrat,
        mouvement_position,
        typecontrat,
        sous_compte,
        sage_decodage,
        titulaire,
        renouv_produit,
        categorie_assurance,
    )

    to_iceberg(
        prelevement_contrat,
        "SOCLE",
        "EQUIPEMENT",
        "PRELEVEMENT_CONTRAT",
        partition_date,
    )

    stock_contrat_banque = build_stock_contrat_banque(
        contrat_produit,
        produit,
        contrat,
    )

    to_iceberg(
        stock_contrat_banque,
        "SOCLE",
        "EQUIPEMENT",
        "STOCK_CONTRAT_BANQUE",
        partition_date,
    )

    prelevement_contrat_approvisionne = build_prelevement_contrat_approvisionne(
        mouvement_position,
        exp_contratprerenouv_banque,
        type_contrat,
        contrat,
        sous_compte,
        sage_decodage,
    )

    to_iceberg(
        prelevement_contrat_approvisionne,
        "SOCLE",
        "EQUIPEMENT",
        "PRELEVEMENT_APPROVISIONNE",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
