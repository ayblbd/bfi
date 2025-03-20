import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import SparkSession

from src.common.table_loader import (
    load_latest_date_before_partition_date_from_iceberg,
    load_table_from,
    load_table_history_from,
    load_table_with_full_history,
)
from src.common.table_writer import (
    to_iceberg,
)
from src.common.utils import (
    create_spark_session,
    get_config,
    get_env,
    get_is_history,
    get_logger,
    get_partition_date,
)
from src.transformation.monetique import (
    achat_ecom_tpe,
    achat_gab_cdm_cdm_iam_meditel,
    achat_gab_cdm_cdm_lydec,
    achat_gab_cdm_confrere_iam_meditel,
    achat_gab_cdm_confrere_lydec,
    build_transaction_monetique,
    retrait_gab_cdm_cdm,
    retrait_gab_cdm_confrere,
    retrait_gab_cdm_etranger,
    retrait_gab_confrere_cdm,
    retrait_gab_etranger_cdm,
)


def main() -> None:
    config = get_config()

    env = get_env(sys.argv)
    assert config.has_section(env), f"Environment {env} not found in config.ini file"

    monetique_path = config.get(env, "monetique_path")
    catalog_name = config.get(env, "catalog_name")
    shuffle_partitions = config.get(env, "shuffle_partitions")

    partition_date: str = get_partition_date(sys.argv)

    spark: SparkSession = create_spark_session(
        config, env, f"CDM MONETIQUE {partition_date}"
    )

    logger = get_logger()

    logger.info(f"Partition date: {partition_date}")

    is_history: bool = get_is_history(sys.argv)

    authorization = (
        load_table_with_full_history(
            f"{monetique_path}/AUTHORIZATION", partition_date, shuffle_partitions
        )
        if is_history
        else load_table_from(
            f"{monetique_path}/AUTHORIZATION", partition_date, shuffle_partitions
        )
    )

    card = load_table_history_from(f"{monetique_path}/CARD", partition_date, days=0)

    acquier_bank_cmi = load_table_history_from(
        f"{monetique_path}/ACQUIER_BANK_CMI", partition_date, days=0
    )

    interfaces = load_table_history_from(
        f"{monetique_path}/INTERFACES", partition_date, days=0
    )

    country = load_table_history_from(
        f"{monetique_path}/COUNTRY", partition_date, days=0
    )

    merchant_activity = load_table_history_from(
        f"{monetique_path}/MERCHANT_ACTIVITY", partition_date, days=0
    )

    currency = load_table_history_from(
        f"{monetique_path}/CURRENCY", partition_date, days=0
    )

    transaction_code = load_table_history_from(
        f"{monetique_path}/TRANSACTION_CODE", partition_date, days=0
    )

    authorization_response_f039 = load_table_history_from(
        f"{monetique_path}/AUTHORIZATION_RESPONSE_F039",
        partition_date,
        days=0,
    )

    response_code_action = load_table_history_from(
        f"{monetique_path}/RESPONSE_CODE_ACTION", partition_date, days=0
    )

    card_program = load_table_history_from(
        f"{monetique_path}/CARD_PROGRAM", partition_date, days=0
    )

    cdm_operator_reco_detail = load_table_history_from(
        f"{monetique_path}/CDM_OPERATOR_RECO_DETAIL",
        partition_date,
        days=0,
    )

    cdm_operator_reconciliation = load_table_history_from(
        f"{monetique_path}/CDM_OPERATOR_RECONCILIATION",
        partition_date,
        days=0,
    )

    atm_utility_operator = load_table_history_from(
        f"{monetique_path}/ATM_UTILITY_OPERATOR", partition_date, days=0
    )

    lydec_transaction_arrete = load_table_history_from(
        f"{monetique_path}/LYDEC_TRANSACTION_ARRETE",
        partition_date,
        days=0,
    )

    information_compte_client = load_latest_date_before_partition_date_from_iceberg(
        f"{catalog_name}.SOCLE.COMPTE.INFORMATION_COMPTE_CLIENT", partition_date
    )

    # RETRAIT CARTE CDM
    retrait_gab_cdm_cdm_df = retrait_gab_cdm_cdm(
        authorization,
        interfaces,
        country,
        merchant_activity,
        card,
        card_program,
        currency,
        transaction_code,
        authorization_response_f039,
        response_code_action,
        information_compte_client,
    )

    retrait_gab_confrere_cdm_df = retrait_gab_confrere_cdm(
        authorization,
        country,
        merchant_activity,
        card,
        card_program,
        currency,
        acquier_bank_cmi,
        transaction_code,
        authorization_response_f039,
        response_code_action,
        information_compte_client,
    )

    retrait_gab_etranger_cdm_df = retrait_gab_etranger_cdm(
        authorization,
        country,
        merchant_activity,
        card,
        card_program,
        currency,
        transaction_code,
        authorization_response_f039,
        response_code_action,
        information_compte_client,
    )

    # RETRAIT CARTE CONFRERE
    retrait_gab_cdm_confrere_df = retrait_gab_cdm_confrere(
        authorization,
        interfaces,
        country,
        merchant_activity,
        currency,
        acquier_bank_cmi,
        transaction_code,
        authorization_response_f039,
        response_code_action,
    )

    # RETRAIT CARTE INTERNATIONAL
    retrait_gab_cdm_etranger_df = retrait_gab_cdm_etranger(
        authorization,
        interfaces,
        country,
        merchant_activity,
        currency,
        transaction_code,
        authorization_response_f039,
        response_code_action,
    )

    # PAIEMENT CARTE CDM
    achat_gab_cdm_cdm_lydec_df = achat_gab_cdm_cdm_lydec(
        lydec_transaction_arrete,
        authorization,
        interfaces,
        merchant_activity,
        card,
        card_program,
        currency,
        transaction_code,
        authorization_response_f039,
        response_code_action,
        information_compte_client,
    )

    achat_gab_cdm_cdm_iam_meditel_df = achat_gab_cdm_cdm_iam_meditel(
        cdm_operator_reco_detail,
        authorization,
        interfaces,
        merchant_activity,
        card,
        card_program,
        currency,
        transaction_code,
        cdm_operator_reconciliation,
        atm_utility_operator,
        authorization_response_f039,
        response_code_action,
        information_compte_client,
    )

    # PAIEMENT CARTE CONFRERE
    achat_gab_cdm_confrere_lydec_df = achat_gab_cdm_confrere_lydec(
        lydec_transaction_arrete,
        authorization,
        interfaces,
        merchant_activity,
        currency,
        transaction_code,
        authorization_response_f039,
        response_code_action,
        acquier_bank_cmi,
    )

    achat_gab_cdm_confrere_iam_meditel_df = achat_gab_cdm_confrere_iam_meditel(
        cdm_operator_reco_detail,
        authorization,
        interfaces,
        merchant_activity,
        currency,
        transaction_code,
        cdm_operator_reconciliation,
        atm_utility_operator,
        authorization_response_f039,
        response_code_action,
        acquier_bank_cmi,
    )

    # PAIEMENT E-COM
    achat_ecom_tpe_df = achat_ecom_tpe(
        authorization,
        country,
        merchant_activity,
        card,
        card_program,
        currency,
        transaction_code,
        information_compte_client,
    )

    transaction_monetique = build_transaction_monetique(
        retrait_gab_cdm_cdm_df,
        retrait_gab_confrere_cdm_df,
        retrait_gab_etranger_cdm_df,
        achat_gab_cdm_cdm_lydec_df,
        achat_gab_cdm_cdm_iam_meditel_df,
        achat_ecom_tpe_df,
        retrait_gab_cdm_confrere_df,
        achat_gab_cdm_confrere_iam_meditel_df,
        achat_gab_cdm_confrere_lydec_df,
        retrait_gab_cdm_etranger_df,
    )

    to_iceberg(
        transaction_monetique,
        "SOCLE",
        "MONETIQUE",
        "TRANSACTION_MONETIQUE",
        partition_date,
    )

    spark.stop()


if __name__ == "__main__":
    main()
