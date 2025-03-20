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
from src.transformation.fdcs import (
    build_niveau_regroupement,
    fdc_transformation,
    objectif_transformation,
    utilisateur_transformation,
    niveau_regroupement_transformation,
)


def main() -> None:
    config = get_config()
    env = get_env(sys.argv)
    partition_date: str = get_partition_date(sys.argv)

    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    ods_path = config.get(env, "ods_path")
    from_files_path = config.get(env, "from_files_path")
    hr_access_path = config.get(env, "hr_access_path")
    spark: SparkSession = create_spark_session(config, env, "CDM.FDC.1.0")

    ##### Read data from Raw Layer
    fdc = load_table_from(f"{ods_path}/T705", partition_date)
    t608 = load_table_from(f"{ods_path}/T608", partition_date)
    Utilisateur = load_table_from(f"{ods_path}/EXPLOIT_UTILISATEUR", partition_date)
    objectif = load_latest_value_from_table(f"{from_files_path}/OBJECTIF_BFI_CORPO")
    saisonalite = load_latest_value_from_table(f"{from_files_path}/SAISONNALITE")
    habilitation = load_latest_value_from_table(
        f"{from_files_path}/HABILITATION_RETAIL"
    )
    iam_personnes_poste = load_table_from(
        f"{hr_access_path}/IAM_PERSONNES_POSTE", partition_date
    )

    #### Process Data
    niveau_regroupement_processed = build_niveau_regroupement(t608, fdc, Utilisateur)
    fdc_processed = fdc_transformation(fdc, partition_date)
    objectif_processed = objectif_transformation(objectif, saisonalite)
    utilisateur_processed = utilisateur_transformation(
        iam_personnes_poste, habilitation
    )
    niveau_regroupement_level_processed = niveau_regroupement_transformation(t608)

    #### Load Data On Socle Layer
    to_iceberg(fdc_processed, "SOCLE", "FDC", "FDC", partition_date)
    to_iceberg(fdc_processed, "SOCLE", "FDC", "FDC_CONTENTIEUX", partition_date)
    to_iceberg(
        niveau_regroupement_processed,
        "SOCLE",
        "FDC",
        "NIVEAU_REGROUPEMENT",
        partition_date,
    )
    to_iceberg(objectif_processed, "SOCLE", "FDC", "OBJECTIF", partition_date)
    to_iceberg(utilisateur_processed, "SOCLE", "FDC", "UTILISATEUR", partition_date)
    to_iceberg(
        niveau_regroupement_level_processed,
        "SOCLE",
        "FDC",
        "NIVEAU_REGROUPEMENT_LEVEL",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
