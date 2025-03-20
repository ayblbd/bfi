from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    lit,
    min,
    substring,
    to_date,
    upper,
    when,
)
from pyspark.sql.types import FloatType


def select_authorization(authorization: DataFrame) -> DataFrame:
    return (
        authorization.withColumnRenamed("aut_bin_iden", "tra_bin_iden")
        .withColumn("numerodecompte", substring(col("aut_acco_id1_f102"), 11, 11))
        .withColumnRenamed("aut_acqr_inst_coun_code_f019", "cou_iden")
        .withColumnRenamed("aut_merc_type_f018", "mac_iden")
        .withColumnRenamed("aut_prim_acct_numb_f002", "tra_num_porteur")
        .withColumnRenamed("aut_tran_curr_f049", "cur_ide")
        .withColumnRenamed("aut_tran_code", "tco_code")
        .withColumn("rco_code", substring(col("aut_resp_code_f039"), 1, 1))
        .withColumn("are_code", substring(col("aut_resp_code_f039"), 2, 2))
        .withColumnRenamed("aut_acqr_inst_id_code_f032", "abc_acq_code")
        .withColumnRenamed("aut_forw_inst_id_code_f033", "abc_fwd_code")
        .withColumnRenamed("aut_code", "tra_num_transaction")
        .withColumnRenamed("aut_card_accp_id_code_f042", "tra_num_terminal")
        .withColumn(
            "tra_mois_transaction",
            date_format(to_date(col("aut_requ_syst_time")), "MM"),
        )
        .withColumn(
            "tra_annee_transaction",
            date_format(to_date(col("aut_requ_syst_time")), "yyyy"),
        )
        .withColumnRenamed("aut_requ_syst_time", "tra_date_transaction")
        .withColumnRenamed("partitiondate", "tra_date_traitement")
        .withColumnRenamed("aut_resp_code_f039", "tra_code_reponse")
        .select(
            "aut_bin_type",
            "aut_reve_stat",
            "tco_code",
            "aut_sour_inte_code",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_num_transaction",
            "tra_num_porteur",
            "numerodecompte",
            "tra_num_terminal",
            "cou_iden",
            "abc_acq_code",
            "abc_fwd_code",
            "aut_card_data_inpt_mode_f22_09",
            "mac_iden",
            "aut_merc_dom",
            "aut_cert_numb_f048_11",
            "aut_addi_data_f048",
            "cur_ide",
            "aut_tran_amou_f004",
            "rco_code",
            "are_code",
            "tra_code_reponse",
            "aut_bill_amou_f006",
            "tra_bin_iden",
            "tra_date_traitement",
        )
    )


def select_interfaces(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("int_code", "aut_sour_inte_code").select(
        "aut_sour_inte_code", "int_ity_code"
    )


def select_country(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tra_pays_terminal", upper(col("cou_name")))
        .withColumn("tra_pays_porteur", upper(col("cou_name")))
        .select("cou_iden", "tra_pays_porteur", "tra_pays_terminal")
    )


def select_transaction_code(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("tco_labe", "tra_lib_transaction").select(
        "tco_code", "tra_lib_transaction"
    )


def select_card_program(df: DataFrame) -> DataFrame:
    return df.withColumn("tra_type_carte", upper(col("cpr_labe"))).select(
        "cpr_code", "tra_type_carte"
    )


def select_currency(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("cur_alph_code", "tra_code_devise").select(
        "cur_ide", "tra_code_devise"
    )


def select_authorization_response_f039(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("are_labe", "tra_reponse_autorisation").select(
        "are_code", "tra_reponse_autorisation"
    )


def select_response_code_action(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("rco_iden", "tra_reponse_action").select(
        "rco_code", "tra_reponse_action"
    )


def select_merchant_activity(df: DataFrame) -> DataFrame:
    return df.withColumn("tra_activite_commercant", upper(col("mac_labe"))).select(
        "mac_iden", "tra_activite_commercant"
    )


def select_card(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "car_date_expiration",
            when(
                col("car_rene_new_expi_date").isNotNull(),
                col("car_rene_new_expi_date"),
            ).otherwise(col("car_expi_date")),
        )
        .withColumnRenamed("car_firs_crea_date", "car_date_souscription")
        .withColumnRenamed("car_rene_date", "car_date_renouvellement")
        .withColumnRenamed("car_cpr_code", "cpr_code")
        .withColumnRenamed("car_numb", "tra_num_porteur")
        .select(
            "tra_num_porteur",
            "cpr_code",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
        )
    )


def select_acquier_bank_cmi(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("tra_bq_terminal", col("abc_labe"))
        .withColumn("tra_bq_porteur", col("abc_labe"))
        .select("abc_acq_code", "abc_fwd_code", "tra_bq_terminal", "tra_bq_porteur")
    )


def select_cdm_operator_reconciliation(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("cor_oper_code", "auo_code")
        .withColumnRenamed("cor_code", "cor_cor_code")
        .select("cor_cor_code", "auo_code")
    )


def select_cdm_operator_reco_detail(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("cor_aut_code", "tra_num_transaction").select(
        "tra_num_transaction",
        "cor_ate_num",
        "cor_tco_code",
        "cor_cor_code",
        "cor_card_numb",
        "cor_amount",
    )


def select_lydec_transaction_arrete(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("lygb_atr_code", "tra_num_transaction").select(
        "tra_num_transaction", "lygb_montant"
    )


def select_atm_utility_operator(df: DataFrame) -> DataFrame:
    return df.select("auo_code", "auo_corp_name")


def select_information_compte(df: DataFrame) -> DataFrame:
    return df.select("numerotiers", "numerodecompte")


def retrait_gab_cdm_cdm(
    authorization: DataFrame,
    interfaces: DataFrame,
    country: DataFrame,
    merchant_activity: DataFrame,
    card: DataFrame,
    card_program: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    information_compte: DataFrame,
) -> DataFrame:
    is_retrait_gab = col("aut_tran_code") == "07000"
    is_cdm_bin = col("aut_bin_type") == "3"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_cdm_interface = col("int_ity_code") == "14"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(
        is_retrait_gab & is_cdm_bin & is_non_cancelled
    )
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    country = select_country(country)
    merchant_activity = select_merchant_activity(merchant_activity)
    card = select_card(card)
    card_program = select_card_program(card_program)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    information_compte = select_information_compte(information_compte)

    return (
        authorization.join(information_compte, ["numerodecompte"], "left")
        .join(interfaces, ["aut_sour_inte_code"])
        .join(country, ["cou_iden"])
        .join(merchant_activity, ["mac_iden"])
        .join(card, ["tra_num_porteur"])
        .join(card_program, ["cpr_code"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn("tra_categorie_transaction", lit("RETRAIT GAB CDM - CARTE CDM"))
        .withColumn("tra_type_transaction", lit("RETRAIT NATIONAL"))
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_bq_porteur", lit("CDM"))
        .withColumnRenamed("aut_tran_amou_f004", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def retrait_gab_confrere_cdm(
    authorization: DataFrame,
    country: DataFrame,
    merchant_activity: DataFrame,
    card: DataFrame,
    card_program: DataFrame,
    currency: DataFrame,
    acquier_bank_cmi: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    information_compte: DataFrame,
) -> DataFrame:
    is_retrait_gab = col("aut_tran_code") == "07000"
    is_cdm_bin = col("aut_bin_type") == "3"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_source_confrere = col("aut_sour_inte_code") == "75"
    is_national_network = col("aut_merc_dom") == "N"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(
        is_retrait_gab
        & is_cdm_bin
        & is_non_cancelled
        & is_source_confrere
        & is_national_network
    )

    country = select_country(country)
    merchant_activity = select_merchant_activity(merchant_activity)
    card = select_card(card)
    card_program = select_card_program(card_program)
    currency = select_currency(currency)
    acquier_bank_cmi = select_acquier_bank_cmi(acquier_bank_cmi)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    information_compte = select_information_compte(information_compte)

    return (
        authorization.join(information_compte, ["numerodecompte"], "left")
        .join(country, ["cou_iden"])
        .join(merchant_activity, ["mac_iden"])
        .join(card, ["tra_num_porteur"])
        .join(card_program, ["cpr_code"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .join(acquier_bank_cmi, ["abc_acq_code", "abc_fwd_code"], "left")
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn(
            "tra_categorie_transaction", lit("RETRAIT GAB CONFRERE - CARTE CDM")
        )
        .withColumn("tra_type_transaction", lit("RETRAIT NATIONAL"))
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_porteur", lit("CDM"))
        .withColumnRenamed("aut_tran_amou_f004", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def retrait_gab_cdm_confrere(
    authorization: DataFrame,
    interfaces: DataFrame,
    country: DataFrame,
    merchant_activity: DataFrame,
    currency: DataFrame,
    acquier_bank_cmi: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
) -> DataFrame:
    is_retrait_gab = col("aut_tran_code") == "07000"
    is_confrere_bin = col("aut_bin_type") == "1"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_cdm_interface = col("int_ity_code") == "14"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(
        is_retrait_gab & is_confrere_bin & is_non_cancelled
    )
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    country = select_country(country)
    merchant_activity = select_merchant_activity(merchant_activity)
    currency = select_currency(currency)
    acquier_bank_cmi = select_acquier_bank_cmi(acquier_bank_cmi)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)

    return (
        authorization.join(interfaces, ["aut_sour_inte_code"])
        .join(country, ["cou_iden"])
        .join(merchant_activity, ["mac_iden"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .join(acquier_bank_cmi, ["abc_acq_code", "abc_fwd_code"], "left")
        .withColumn("numerotiers", lit(None))
        .withColumn("numerodecompte", lit(None))
        .withColumn("car_date_souscription", lit(None))
        .withColumn("car_date_renouvellement", lit(None))
        .withColumn("car_date_expiration", lit(None))
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn(
            "tra_categorie_transaction", lit("RETRAIT GAB CDM - CARTE CONFRERE")
        )
        .withColumn("tra_type_transaction", lit("RETRAIT NATIONAL"))
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumn("tra_type_carte", lit("CARTE CONFRERE"))
        .withColumnRenamed("aut_tran_amou_f004", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_bin_iden",
            "tra_pays_porteur",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def retrait_gab_etranger_cdm(
    authorization: DataFrame,
    country: DataFrame,
    merchant_activity: DataFrame,
    card: DataFrame,
    card_program: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    information_compte: DataFrame,
) -> DataFrame:
    is_retrait_gab = col("aut_tran_code") == "07000"
    is_cdm_bin = col("aut_bin_type") == "3"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_source_international = col("aut_sour_inte_code") == "79"
    is_international_network = col("aut_merc_dom") == "W"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(
        is_retrait_gab
        & is_cdm_bin
        & is_non_cancelled
        & is_source_international
        & is_international_network
    )
    country = select_country(country)
    merchant_activity = select_merchant_activity(merchant_activity)
    card = select_card(card)
    card_program = select_card_program(card_program)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    information_compte = select_information_compte(information_compte)

    return (
        authorization.join(information_compte, ["numerodecompte"], "left")
        .join(country, ["cou_iden"])
        .join(merchant_activity, ["mac_iden"])
        .join(card, ["tra_num_porteur"])
        .join(card_program, ["cpr_code"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn(
            "tra_categorie_transaction", lit("RETRAIT GAB ETRANGER - CARTE CDM")
        )
        .withColumn("tra_type_transaction", lit("RETRAIT INTERNATIONAL"))
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_terminal", lit("ETRANGER"))
        .withColumn("tra_bq_porteur", lit("CDM"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumnRenamed("aut_tran_amou_f004", "tra_mt_transaction")
        .withColumnRenamed("aut_bill_amou_f006", "tra_montant_dh")
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def retrait_gab_cdm_etranger(
    authorization: DataFrame,
    interfaces: DataFrame,
    country: DataFrame,
    merchant_activity: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
) -> DataFrame:
    is_retrait_gab = col("aut_tran_code") == "07000"
    is_international_bin = col("aut_bin_type") == "2"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_cert_numb_not_null = col("aut_cert_numb_f048_11").isNotNull()
    is_addi_data_not_null = col("aut_addi_data_f048").isNotNull()
    is_sub_addi_data_null = substring(col("aut_addi_data_f048"), 1, 2).isNull()
    is_cert_visa = col("aut_cert_numb_f048_11") == "0002"
    is_cert_visa_plus = col("aut_cert_numb_f048_11") == "0004"
    is_maestro = substring(col("aut_addi_data_f048"), 1, 2) == "MS"
    is_mastercard = substring(col("aut_addi_data_f048"), 1, 2) == "MC"
    is_cirrus = substring(col("aut_addi_data_f048"), 1, 2) == "CI"
    is_mastercard_debit = substring(col("aut_addi_data_f048"), 1, 2) == "MD"
    is_visa = substring(col("aut_addi_data_f048"), 1, 2) == "VI"
    is_visa_plus = substring(col("aut_addi_data_f048"), 1, 2) == "PL"
    is_cdm_interface = col("int_ity_code") == "14"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(
        is_retrait_gab
        & is_international_bin
        & is_non_cancelled
        & (is_cert_numb_not_null | is_addi_data_not_null)
    )
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    country = select_country(country)
    merchant_activity = select_merchant_activity(merchant_activity)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)

    return (
        authorization.join(interfaces, ["aut_sour_inte_code"])
        .join(country, ["cou_iden"])
        .join(merchant_activity, ["mac_iden"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .withColumn("numerotiers", lit(None))
        .withColumn("numerodecompte", lit(None))
        .withColumn("car_date_souscription", lit(None))
        .withColumn("car_date_renouvellement", lit(None))
        .withColumn("car_date_expiration", lit(None))
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn(
            "tra_categorie_transaction", lit("RETRAIT GAB CDM - CARTE ETRANGERE")
        )
        .withColumn("tra_type_transaction", lit("RETRAIT INTERNATIONAL"))
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_bq_porteur", lit("ETRANGER"))
        .withColumn("tra_pays_porteur", lit("ETRANGER"))
        .withColumn(
            "tra_type_carte",
            when(is_sub_addi_data_null & is_cert_visa, "VISA")
            .when(is_sub_addi_data_null & is_cert_visa_plus, "VISA PLUS")
            .when(is_maestro, "MAESTRO")
            .when(is_mastercard, "MASTER CARD")
            .when(is_cirrus, "CIRRUS")
            .when(is_mastercard_debit, "MASTER CARD DEBIT")
            .when(is_visa, "VISA")
            .when(is_visa_plus, "VISA PLUS")
            .otherwise("NF"),
        )
        .withColumnRenamed("aut_tran_amou_f004", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def achat_gab_cdm_cdm_iam_meditel(
    cdm_operator_reco_detail: DataFrame,
    authorization: DataFrame,
    interfaces: DataFrame,
    merchant_activity: DataFrame,
    card: DataFrame,
    card_program: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    cdm_operator_reconciliation: DataFrame,
    atm_utility_operator: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    information_compte: DataFrame,
) -> DataFrame:
    is_cdm_bin = col("aut_bin_type") == "3"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_cdm_interface = col("int_ity_code") == "14"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(is_cdm_bin & is_non_cancelled)
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    merchant_activity = select_merchant_activity(merchant_activity)
    card = select_card(card)
    card_program = select_card_program(card_program)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    information_compte = select_information_compte(information_compte)
    cdm_operator_reconciliation = select_cdm_operator_reconciliation(
        cdm_operator_reconciliation
    )
    cdm_operator_reco_detail = select_cdm_operator_reco_detail(
        cdm_operator_reco_detail
    )
    atm_utility_operator = select_atm_utility_operator(atm_utility_operator)

    return (
        authorization.join(information_compte, ["numerodecompte"], "left")
        .join(cdm_operator_reco_detail, ["tra_num_transaction"])
        .join(interfaces, ["aut_sour_inte_code"])
        .join(merchant_activity, ["mac_iden"])
        .join(card, ["tra_num_porteur"])
        .join(card_program, ["cpr_code"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .join(cdm_operator_reconciliation, ["cor_cor_code"], "left")
        .join(atm_utility_operator, ["auo_code"], "left")
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn("tra_categorie_transaction", lit("PAIEMENT GAB CDM - CARTE CDM"))
        .withColumn("tra_type_transaction", lit("PAIEMENT GAB"))
        .withColumn(
            "tra_lib_transaction",
            concat_ws(" ", col("tra_lib_transaction"), upper(col("auo_corp_name"))),
        )
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_num_terminal", concat_ws("", lit("0"), col("cor_ate_num")))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_pays_terminal", lit("MOROCCO"))
        .withColumn("tra_num_porteur", col("cor_card_numb"))
        .withColumn("tra_bq_porteur", lit("CDM"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumnRenamed("cor_amount", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def achat_gab_cdm_confrere_iam_meditel(
    cdm_operator_reco_detail: DataFrame,
    authorization: DataFrame,
    interfaces: DataFrame,
    merchant_activity: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    cdm_operator_reconciliation: DataFrame,
    atm_utility_operator: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    acquier_bank_cmi: DataFrame,
) -> DataFrame:
    is_confrere_bin = col("aut_bin_type") == "1"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_cdm_interface = col("int_ity_code") == "14"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(is_confrere_bin & is_non_cancelled)
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    merchant_activity = select_merchant_activity(merchant_activity)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    acquier_bank_cmi = select_acquier_bank_cmi(acquier_bank_cmi)
    cdm_operator_reconciliation = select_cdm_operator_reconciliation(
        cdm_operator_reconciliation
    )
    cdm_operator_reco_detail = select_cdm_operator_reco_detail(
        cdm_operator_reco_detail
    )
    atm_utility_operator = select_atm_utility_operator(atm_utility_operator)

    return (
        authorization.join(cdm_operator_reco_detail, ["tra_num_transaction"])
        .join(interfaces, ["aut_sour_inte_code"])
        .join(merchant_activity, ["mac_iden"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .join(cdm_operator_reconciliation, ["cor_cor_code"], "left")
        .join(atm_utility_operator, ["auo_code"], "left")
        .join(acquier_bank_cmi, ["abc_acq_code", "abc_fwd_code"], "left")
        .withColumn("numerotiers", lit(None))
        .withColumn("numerodecompte", lit(None))
        .withColumn("car_date_souscription", lit(None))
        .withColumn("car_date_renouvellement", lit(None))
        .withColumn("car_date_expiration", lit(None))
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn(
            "tra_categorie_transaction", lit("PAIEMENT GAB CDM - CARTE CONFRERE")
        )
        .withColumn("tra_type_transaction", lit("PAIEMENT GAB"))
        .withColumn(
            "tra_lib_transaction",
            concat_ws(" ", col("tra_lib_transaction"), upper(col("auo_corp_name"))),
        )
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_num_terminal", concat_ws("", lit("0"), col("cor_ate_num")))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_pays_terminal", lit("MOROCCO"))
        .withColumn("tra_num_porteur", col("cor_card_numb"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumn("tra_type_carte", lit("CARTE CONFRERE"))
        .withColumnRenamed("cor_amount", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def achat_gab_cdm_cdm_lydec(
    lydec_transaction_arrete: DataFrame,
    authorization: DataFrame,
    interfaces: DataFrame,
    merchant_activity: DataFrame,
    card: DataFrame,
    card_program: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    information_compte: DataFrame,
) -> DataFrame:
    is_cdm_bin = col("aut_bin_type") == "3"
    is_non_cancelled = col("aut_reve_stat") == "N"
    is_cdm_interface = col("int_ity_code") == "14"
    is_lydec_num_transaction_not_null = col("tra_num_transaction").isNotNull()

    authorization = select_authorization(authorization)
    authorization = authorization.filter(is_cdm_bin & is_non_cancelled)
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    merchant_activity = select_merchant_activity(merchant_activity)
    card = select_card(card)
    card_program = select_card_program(card_program)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    information_compte = select_information_compte(information_compte)
    lydec_transaction_arrete = select_lydec_transaction_arrete(
        lydec_transaction_arrete
    )
    lydec_transaction_arrete = lydec_transaction_arrete.filter(
        is_lydec_num_transaction_not_null
    )

    return (
        authorization.join(information_compte, ["numerodecompte"], "left")
        .join(lydec_transaction_arrete, ["tra_num_transaction"])
        .join(interfaces, ["aut_sour_inte_code"])
        .join(merchant_activity, ["mac_iden"])
        .join(card, ["tra_num_porteur"])
        .join(card_program, ["cpr_code"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn("tra_categorie_transaction", lit("PAIEMENT GAB CDM - CARTE CDM"))
        .withColumn("tra_type_transaction", lit("PAIEMENT GAB"))
        .withColumn(
            "tra_lib_transaction",
            concat_ws(" ", col("tra_lib_transaction"), lit("LYDEC")),
        )
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_pays_terminal", lit("MOROCCO"))
        .withColumn("tra_bq_porteur", lit("CDM"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumnRenamed("lygb_montant", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def achat_gab_cdm_confrere_lydec(
    lydec_transaction_arrete: DataFrame,
    authorization: DataFrame,
    interfaces: DataFrame,
    merchant_activity: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    authorization_response_f039: DataFrame,
    response_code_action: DataFrame,
    acquier_bank_cmi: DataFrame,
) -> DataFrame:
    is_cdm_interface = col("int_ity_code") == "14"
    is_lydec_num_transaction_not_null = col("tra_num_transaction").isNotNull()
    is_confrere_bin = col("aut_bin_type") == "1"
    is_non_cancelled = col("aut_reve_stat") == "N"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(is_confrere_bin & is_non_cancelled)
    interfaces = select_interfaces(interfaces)
    interfaces = interfaces.filter(is_cdm_interface)
    lydec_transaction_arrete = select_lydec_transaction_arrete(
        lydec_transaction_arrete
    )
    lydec_transaction_arrete = lydec_transaction_arrete.filter(
        is_lydec_num_transaction_not_null
    )
    merchant_activity = select_merchant_activity(merchant_activity)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    authorization_response_f039 = select_authorization_response_f039(
        authorization_response_f039
    )
    response_code_action = select_response_code_action(response_code_action)
    acquier_bank_cmi = select_acquier_bank_cmi(acquier_bank_cmi)

    return (
        authorization.join(lydec_transaction_arrete, ["tra_num_transaction"])
        .join(interfaces, ["aut_sour_inte_code"])
        .join(merchant_activity, ["mac_iden"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .join(response_code_action, ["rco_code"])
        .join(authorization_response_f039, ["are_code"])
        .join(acquier_bank_cmi, ["abc_acq_code", "abc_fwd_code"], "left")
        .withColumn("numerotiers", lit(None))
        .withColumn("numerodecompte", lit(None))
        .withColumn("car_date_souscription", lit(None))
        .withColumn("car_date_renouvellement", lit(None))
        .withColumn("car_date_expiration", lit(None))
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumn(
            "tra_categorie_transaction", lit("PAIEMENT GAB CDM - CARTE CONFRERE")
        )
        .withColumn("tra_type_transaction", lit("PAIEMENT GAB"))
        .withColumn(
            "tra_lib_transaction",
            concat_ws(" ", col("tra_lib_transaction"), lit("LYDEC")),
        )
        .withColumn("tra_type_terminal", lit("GAB"))
        .withColumn("tra_bq_terminal", lit("CDM"))
        .withColumn("tra_pays_terminal", lit("MOROCCO"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumn("tra_type_carte", lit("CARTE CONFRERE"))
        .withColumnRenamed("cur_alph_code", "tra_code_devise")
        .withColumnRenamed("lygb_montant", "tra_mt_transaction")
        .withColumn("tra_montant_dh", col("tra_mt_transaction"))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def achat_ecom_tpe(
    authorization: DataFrame,
    country: DataFrame,
    merchant_activity: DataFrame,
    card: DataFrame,
    card_program: DataFrame,
    currency: DataFrame,
    transaction_code: DataFrame,
    information_compte: DataFrame,
) -> DataFrame:
    is_purchase = col("aut_tran_code").isin(["05000", "15000", "25000", "35000"])
    is_success_response = col("aut_resp_code_f039") == "000"
    is_reve_stat_diff_f = col("aut_reve_stat") != "F"
    is_ecom = col("aut_card_data_inpt_mode_f22_09").isin(["9", "S", "T", "U", "V"])
    is_international = col("aut_sour_inte_code") == "79"
    is_national = col("aut_sour_inte_code") != "79"

    authorization = select_authorization(authorization)
    authorization = authorization.filter(
        is_purchase & is_success_response & is_reve_stat_diff_f
    )
    country = select_country(country)
    merchant_activity = select_merchant_activity(merchant_activity)
    card = select_card(card)
    card_program = select_card_program(card_program)
    currency = select_currency(currency)
    transaction_code = select_transaction_code(transaction_code)
    information_compte = select_information_compte(information_compte)

    return (
        authorization.join(information_compte, ["numerodecompte"], "left")
        .join(country, ["cou_iden"])
        .join(merchant_activity, ["mac_iden"])
        .join(card, ["tra_num_porteur"])
        .join(card_program, ["cpr_code"])
        .join(currency, ["cur_ide"])
        .join(transaction_code, ["tco_code"])
        .withColumn("tra_type_terminal", when(is_ecom, "INTERNET").otherwise("TPE"))
        .withColumn(
            "tra_categorie_transaction",
            when(is_ecom & is_international, "PAIEMENT E-COM INTERNATIONAL")
            .when(is_ecom & is_national, "PAIEMENT E-COM NATIONAL")
            .when(~is_ecom & is_international, "PAIEMENT TPE INTERNATIONAL")
            .when(~is_ecom & is_national, "PAIEMENT TPE NATIONAL"),
        )
        .withColumn(
            "tra_type_transaction",
            when(is_international, "PAIEMENT INTERNATIONAL").otherwise(
                "PAIEMENT NATIONAL"
            ),
        )
        .withColumn(
            "tra_bq_terminal", when(is_international, "ETRANGER").otherwise("CMI")
        )
        .withColumn("tra_reponse_autorisation", lit("Terminee avec succes"))
        .withColumn("tra_reponse_action", lit("Approve"))
        .withColumn("tra_bq_porteur", lit("CDM"))
        .withColumn("tra_pays_porteur", lit("MOROCCO"))
        .withColumn("tra_mode_transaction", lit("ON LINE"))
        .withColumnRenamed("aut_tran_amou_f004", "tra_mt_transaction")
        .withColumn(
            "tra_montant_dh",
            when(is_international, col("aut_bill_amou_f006")).otherwise(
                col("tra_mt_transaction")
            ),
        )
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )


def build_transaction_monetique(
    retrait_gab_cdm_cdm: DataFrame,
    retrait_gab_confrere_cdm: DataFrame,
    retrait_gab_etranger_cdm: DataFrame,
    achat_gab_cdm_cdm_lydec: DataFrame,
    achat_gab_cdm_cdm_iam_meditel: DataFrame,
    achat_ecom_tpe: DataFrame,
    retrait_gab_cdm_confrere: DataFrame,
    achat_gab_cdm_confrere_iam_meditel: DataFrame,
    achat_gab_cdm_confrere_lydec: DataFrame,
    retrait_gab_cdm_etranger: DataFrame,
) -> DataFrame:
    window_spec = Window.partitionBy("tra_num_porteur")
    return (
        retrait_gab_cdm_cdm.unionByName(retrait_gab_confrere_cdm)
        .unionByName(retrait_gab_etranger_cdm)
        .unionByName(achat_gab_cdm_cdm_iam_meditel)
        .unionByName(achat_gab_cdm_cdm_lydec)
        .unionByName(achat_ecom_tpe)
        .unionByName(retrait_gab_cdm_confrere)
        .unionByName(achat_gab_cdm_confrere_iam_meditel)
        .unionByName(achat_gab_cdm_confrere_lydec)
        .unionByName(retrait_gab_cdm_etranger)
        .withColumn(
            "car_date_premiere_transaction",
            when(
                col("car_date_souscription").isNotNull(),
                min(col("tra_date_transaction")).over(window_spec),
            ),
        )
        .withColumn(
            "tra_mt_transaction", col("tra_mt_transaction").cast(FloatType())
        )
        .withColumn("tra_montant_dh", col("tra_montant_dh").cast(FloatType()))
        .select(
            "tra_num_transaction",
            "numerotiers",
            "numerodecompte",
            "tra_mode_transaction",
            "tra_categorie_transaction",
            "tra_type_transaction",
            "tra_lib_transaction",
            "tra_type_terminal",
            "tra_num_terminal",
            "tra_bq_terminal",
            "tra_pays_terminal",
            "tra_activite_commercant",
            "tra_num_porteur",
            "car_date_souscription",
            "car_date_renouvellement",
            "car_date_expiration",
            "car_date_premiere_transaction",
            "tra_bq_porteur",
            "tra_pays_porteur",
            "tra_bin_iden",
            "tra_type_carte",
            "tra_code_devise",
            "tra_mt_transaction",
            "tra_date_transaction",
            "tra_mois_transaction",
            "tra_annee_transaction",
            "tra_date_traitement",
            "tra_reponse_autorisation",
            "tra_reponse_action",
            "tra_code_reponse",
            "tra_montant_dh",
        )
    )
