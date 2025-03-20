import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession
from src.common.table_loader import load_latest_value_from_table
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_logger,
    get_partition_date,
)
from src.transformation.objectif_fdc import (
    build_dim_objectif_fdc,
    build_objectif_fdc,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"
    from_files_path = config.get(env, "from_files_path")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM OBJECTIF FDC {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    objectif = load_latest_value_from_table(f"{from_files_path}/OBJECTIFS_TRO")
    saisonalite = load_latest_value_from_table(f"{from_files_path}/SAISONNALITE")
    semaine_mos_trimestre = load_latest_value_from_table(
        f"{from_files_path}/MOIS_TRIMESTRE_SEMAINE"
    )

    columns_to_drop = [
        "code_fdc",
        "code_region",
        "region",
        "code_groupe",
        "libelle_agence",
        "code_agence",
        "groupe",
        "code_emploi",
        "partitiondate",
    ]
    dim_code_produit_fdc = build_dim_objectif_fdc(
        objectif, saisonalite, semaine_mos_trimestre, columns_to_drop
    )
    objectif_fdc = build_objectif_fdc(
        objectif, saisonalite, semaine_mos_trimestre, columns_to_drop
    )

    to_iceberg(objectif_fdc, "SOCLE", "OBJECTIF", "OBJECTIF_FDC", partition_date)

    to_iceberg(
        dim_code_produit_fdc,
        "SOCLE",
        "OBJECTIF",
        "CODE_PRODUIT_FDC",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
