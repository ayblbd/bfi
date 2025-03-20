import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_latest_value_from_table,
    load_table_from,
)
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.tiers import (
    marche_transformation,
    marche_transformation_alim,
    segmentation_transformation,
    tiers_client_joins,
    tiers_transformation,
)


def main() -> None:
    config = get_config()
    env = get_env(sys.argv)
    partition_date: str = get_partition_date(sys.argv)

    assert config.has_section(
        env
    ), f"Environment {env} not found in config.ini file"  ## replace with ValueError
    from_files_path = config.get(env, "from_files_path")
    ods_path = config.get(env, "ods_path")
    dataware_path = config.get(env, "dataware_path")
    spark: SparkSession = create_spark_session(config, env, "CDM.TIERS.1.0")

    ##### Read data from Raw Layer
    tiers = load_table_from(f"{ods_path}/EXPLOIT_TIERS", partition_date)
    client = load_table_from(f"{ods_path}/EXPLOIT_CLIENT", partition_date)
    fdc_tiers = load_table_from(f"{ods_path}/EXP_FDCTIERS", partition_date)
    donnee_client = load_table_from(f"{dataware_path}/DONNEESCLIENT", partition_date)
    compte = load_table_from(f"{ods_path}/EXPLOIT_SOUSCOMPTE", partition_date)
    elt_contrat = load_table_from(f"{ods_path}/EXPLOIT_ELT_CONTRAT", partition_date)
    titulaire = load_table_from(f"{ods_path}/EXPLOIT_TITULAIRE", partition_date)
    personne_physique = load_table_from(
        f"{ods_path}/EXPLOIT_PERSONNE_PHYSIQUE", partition_date
    )
    t702 = load_latest_value_from_table(f"{ods_path}/T702")
    segmentation = load_table_from(
        f"{ods_path}/EXPLOIT_SEGMENTATION", partition_date
    )
    sous_segmentation = load_table_from(
        f"{ods_path}/EXP_REF_SOUS_SEGMENT", partition_date
    )
    marche_mapping_pme_ge = load_latest_value_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING_MARCHE_GE_PME"
    )
    marche_mapping_pro_retail = load_latest_value_from_table(
        f"{from_files_path}/NOUVEAU_MAPPING_PRO_RETAIL"
    )
    #### Process Data
    tier_client_join = tiers_client_joins(tiers, client)
    marche_processed_intermediaire = marche_transformation(
        tier_client_join, marche_mapping_pme_ge, marche_mapping_pro_retail, fdc_tiers
    )
    segmentation_processed = segmentation_transformation(
        marche_processed_intermediaire,
        segmentation,
        sous_segmentation,
        partition_date,
    )
    tiers_processed = tiers_transformation(
        tier_client_join,
        segmentation_processed,
        marche_processed_intermediaire,
        fdc_tiers,
        donnee_client,
        compte,
        elt_contrat,
        titulaire,
        personne_physique,
        t702,
        partition_date,
    )
    marche_processed = marche_transformation_alim(marche_processed_intermediaire)

    #### Load Data On Socle Layer
    to_iceberg(
        segmentation_processed, "SOCLE", "CLIENT", "SEGMENTATION", partition_date
    )
    to_iceberg(tiers_processed, "SOCLE", "CLIENT", "TIER", partition_date)
    to_iceberg(marche_processed, "SOCLE", "CLIENT", "MARCHE", partition_date)

    spark.stop()


if __name__ == "__main__":
    main()
