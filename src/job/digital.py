import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_iceberg_table_from,
    load_table_from,
)
from src.common.table_writer import (
    to_iceberg,
)
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.digital import (
    build_digital_e_corpo,
    build_digital_my_cdm_web,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    adriacm_path = config.get(env, "adriacm_path")
    mongo_path = config.get(env, "mongo_path")
    ods_path = config.get(env, "ods_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM DIGITAL {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    information_tier = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.CLIENT.INFORMATION_TIER", partition_date
    )

    niveau_regroupement = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.AGENCE.NIVEAU_REGROUPEMENT", partition_date
    )
    contrat_produit = load_iceberg_table_from(
        f"{catalog_name}.SOCLE.EQUIPEMENT.CONTRAT_PRODUIT", partition_date
    )
    elt_contrat = load_table_from(
        f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date, shuffle_partitions
    )
    relation_commerciale = load_table_from(
        f"{adriacm_path}/RELATION_COMMERCIALE", partition_date, shuffle_partitions
    )
    audit = load_table_from(
        f"{mongo_path}/AUDIT", partition_date, shuffle_partitions
    )
    audit_log_event = load_table_from(
        f"{adriacm_path}/AUDIT_LOG_EVENT", partition_date, shuffle_partitions
    )
    contrat_abonnement = load_table_from(
        f"{adriacm_path}/CONTRAT_ABONNEMENT", partition_date, shuffle_partitions
    )
    rattachement_abonne_contrat = load_table_from(
        f"{adriacm_path}/RATTACHEMENT_ABONNE_CONTRAT",
        partition_date,
        shuffle_partitions,
    )
    utilisateur = load_table_from(
        f"{adriacm_path}/UTILISATEUR", partition_date, shuffle_partitions
    )

    e_corpo = build_digital_e_corpo(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        elt_contrat,
        audit_log_event,
        relation_commerciale,
        contrat_abonnement,
        rattachement_abonne_contrat,
        utilisateur,
    )
    to_iceberg(
        e_corpo,
        "SOCLE",
        "DIGITAL",
        "E_CORPO",
        partition_date,
    )

    my_cdm_web = build_digital_my_cdm_web(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        elt_contrat,
        audit,
    )
    to_iceberg(
        my_cdm_web,
        "SOCLE",
        "DIGITAL",
        "CDM_WEB",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
