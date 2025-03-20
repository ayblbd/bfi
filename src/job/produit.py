import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.common.table_loader import load_latest_value_from_table
from src.common.table_writer import to_iceberg
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_partition_date,
)
from src.transformation.produit import build_type_produit


def main():
    config = get_config()
    env = get_env(sys.argv)
    partitiondate = get_partition_date(sys.argv)
    ods_path = config.get(env, "ods_path")
    exploit_path = config.get(env, "exploit_path")
    spark = create_spark_session(config, env, "CDM.COMPTE.1.0")

    ##### Read data from Raw Layer
    contratelt = load_latest_value_from_table(f"{exploit_path}/RELCONTRATELT")
    type_eltcontrat = load_latest_value_from_table(
        f"{ods_path}/EXPLOIT_TYPEELTCONTRAT"
    )
    type_contrat = load_latest_value_from_table(f"{ods_path}/EXPLOIT_TYPECONTRAT")
    #### Process Data
    type_produit_processed = build_type_produit(
        contratelt, type_eltcontrat, type_contrat, partitiondate
    )
    #### Load Data On Socle Layer
    to_iceberg(type_produit_processed, "SOCLE", "PRODUIT", "PRODUIT", partitiondate)

    spark.stop()


if __name__ == "__main__":
    main()
