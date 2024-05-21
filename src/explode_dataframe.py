from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_extract, when

from src.utils import (
    extract_field_from_message,
    extract_field_with_brackets_from_message,
)


def explode_messages(df: DataFrame) -> DataFrame:
    # df = explode_activation_attijari_securite(df)
    # df = explode_affectation_profil_technique(df)
    # df = explode_affecter_role_seuil(df)
    # df = explode_ajouter_beneficiaire(df)
    # df = explode_ajouter_carte_beneficiaire(df)
    # df = explode_ajouter_compte(df)
    # df = explode_annuler_chequier_lcn(df)
    # df = explode_annuler_paiement(df)
    # df = explode_annuler_recharge(df)
    # df = explode_annuler_virement(df)
    # df = explode_annuler_virement_permanent(df)
    # df = explode_attijari_securite_envoi_push(df)
    # df = explode_attijari_securite_envoi_push_rejet(df)
    # df = explode_attijari_securite_envoi_push_timeout(df)
    # df = explode_attijari_securite_push_accept(df)
    # df = explode_bloquer_intervenant(df)
    # df = explode_cartes_convention(df)
    # df = explode_catalogue_attijaripay(df)
    # df = explode_changement_habilitations_fonctionnalites(df)
    # df = explode_changement_habilitation_compte(df)
    # df = explode_changement_habilitation_convention(df)
    # df = explode_change_mode_releve(df)
    # df = explode_change_password_expired_failed(df)
    # df = explode_change_password_expired_success(df)
    # df = explode_change_password_failed(df)
    # df = explode_change_password_success(df)
    # df = explode_commander_chequier(df)
    # df = explode_commander_lcn(df)
    # df = explode_consultation_carte(df)
    # df = explode_creation_intervenant(df)
    # df = explode_creation_prospect(df)
    # df = explode_delete_intervenant(df)
    # df = explode_demande_decharge_carte_webpay(df)
    # df = explode_demande_recharge_carte_webpay(df)
    # df = explode_desactivation_attijari_securite(df)
    # df = explode_desaffectation_profil_technique(df)
    # df = explode_effectuer_paiement(df)
    # df = explode_effectuer_recharge(df)
    # df = explode_effectuer_recharge_wallet(df)
    df = explode_effectuer_virement(df)
    # df = explode_effectuer_virement_permanent(df)
    # df = explode_envoi_mail_acces_complet(df)
    # df = explode_ereleve_activation(df)
    # df = explode_ereleve_telechargement(df)
    # df = explode_ereleve_visualisation(df)
    # df = explode_get_status_souscription_vente(df)
    # df = explode_impression_rib(df)
    # df = explode_initier_paiement(df)
    # df = explode_liste_operations_comptabilisees(df)
    # df = explode_liste_operations_par_carte(df)
    # df = explode_login(df)
    # df = explode_login_ko(df)
    # df = explode_logout(df)
    # df = explode_maj_intervenant(df)
    # df = explode_maj_operateur_intervenant(df)
    # df = explode_maj_statut_prospect(df)
    # df = explode_messagerie_nouveau_message(df)
    # df = explode_mise_a_disposition(df)
    # df = explode_mise_a_jour_nature_compte(df)
    # df = explode_modifier_validateur_virement(df)
    # df = explode_modifier_virement(df)
    # df = explode_opposition_carte(df)
    # df = explode_otp_envoi_sms(df)
    # df = explode_otp_envoi_voice(df)
    # df = explode_otp_verify_otp_ko(df)
    # df = explode_otp_verify_otp_ok(df)
    # df = explode_paiement_rejected(df)
    # df = explode_recharge_rejected(df)
    # df = explode_recharge_validated1(df)
    # df = explode_recharge_validated2(df)
    # df = explode_reset_password_success(df)
    # df = explode_reset_password_token_sent_success(df)
    # df = explode_soucription_docnet(df)
    # df = explode_soumettre_recharge(df)
    # df = explode_soumettre_virement(df)
    # df = explode_submit_after_sale_contract(df)
    # df = explode_submit_contrat_vente(df)
    # df = explode_supprimer_discussion(df)
    # df = explode_supprimer_role_seuil(df)
    # df = explode_supprime_beneficiaire(df)
    # df = explode_supprime_carte_beneficiaire(df)
    # df = explode_telechargement_rib(df)
    # df = explode_validate_after_sale_contract(df)
    # df = explode_validate_contrat_vente(df)
    # df = explode_validate_eligibility_contrat_vente(df)
    # df = explode_virement_canceled(df)
    # df = explode_virement_rejected(df)
    # df = explode_virement_rejected2(df)
    # df = explode_virement_validated1(df)
    # df = explode_virement_validated2(df)
    return df


# TODO: virement 6864164 commentaire Déjà fait. c'est un doublon
def explode_virement_rejected(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "virement_rejected",
        when(col("event_type") == lit("VIREMENT_REJECTED"), True),
    )


def explode_liste_operations_par_carte(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "liste_operations_par_carte",
        when(
            col("event_type") == lit("LISTE_OPERATIONS_PAR_CARTE"), col("audit_message")
        ),
    )


def explode_liste_operations_comptabilisees(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "liste_operations_comptabilisees",
        when(
            col("event_type") == lit("LISTE_OPERATIONS_COMPTABILISEES"),
            col("audit_message"),
        ),
    )


def explode_maj_statut_prospect(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "type_prospect",
            when(
                col("event_type") == lit("MAJ_STATUT_PROSPECT"),
                regexp_extract(
                    col("audit_message"),
                    "Type Prospect:" + r"(.*?)" + "changement",
                    1,
                ),
            ),
        )
        .withColumn(
            "changement_de_statut",
            when(
                col("event_type") == lit("MAJ_STATUT_PROSPECT"),
                regexp_extract(
                    col("audit_message"),
                    "du prospect de:" + r"(.*?)" + "date",
                    1,
                ),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("MAJ_STATUT_PROSPECT"),
                regexp_extract(
                    col("audit_message"),
                    "date :" + r"(.*?)",
                    1,
                ),
            ),
        )
    )


def explode_mise_a_disposition(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "montant",
        when(
            col("event_type") == lit("MISE_A_DISPOSITION"),
            regexp_extract(
                col("audit_message"),
                "montant=" + r"(.*?)" + ",",
                1,
            ),
        ),
    ).withColumn(
        "motif",
        when(
            col("event_type") == lit("MISE_A_DISPOSITION"),
            regexp_extract(
                col("audit_message"),
                "motif=" + r"(.*?)",
                1,
            ),
        ),
    )


def explode_opposition_carte(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "carte",
            when(
                col("event_type") == lit("OPPOSITION_CARTE"),
                regexp_extract(
                    col("audit_message"),
                    "Opposition sur carte n°" + r"(.*?)" + "pour",
                    1,
                ),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("OPPOSITION_CARTE"),
                regexp_extract(
                    col("audit_message"),
                    "pour motif de" + r"(.*?)" + "avec ",
                    1,
                ),
            ),
        )
        .withColumn(
            "commentaire",
            when(
                col("event_type") == lit("OPPOSITION_CARTE"),
                regexp_extract(
                    col("audit_message"),
                    "le commentaire" + r"(.*?)",
                    1,
                ),
            ),
        )
    )


def explode_ereleve_visualisation(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "image_url",
        when(
            col("event_type") == lit("ERELEVE_VISUALISATION"),
            regexp_extract(
                col("audit_message"),
                "Visualisation de l'image : " + r"(.*?)",
                1,
            ),
        ),
    )


def explode_ereleve_telechargement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "docname",
        when(
            col("event_type") == lit("ERELEVE_TELECHARGEMENT"),
            regexp_extract(
                col("audit_message"),
                "docname=" + r"(.*?)" + ",",
                1,
            ),
        ),
    ).withColumn(
        "docid",
        when(
            col("event_type") == lit("ERELEVE_TELECHARGEMENT "),
            regexp_extract(
                col("audit_message"),
                "id=" + r"(.*?)" + ",",
                1,
            ),
        ),
    )


def explode_desaffectation_profil_technique(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "administrateur_technique",
            when(
                col("event_type") == lit("DESAFFECTATION_PROFIL_TECHNIQUE"),
                regexp_extract(
                    col("audit_message"),
                    "Suppression de l'administrateur technique "
                    + r"(.*?)",  # + r" \(",
                    1,
                ),
            ),
        )
        .withColumn(
            "id_administrateur_technique",
            when(
                col("event_type") == lit("DESAFFECTATION_PROFIL_TECHNIQUE"),
                regexp_extract(
                    col("audit_message"),
                    r"\((.*?)\)",
                    1,
                ),
            ),
        )
        .withColumn(
            "contrat",
            when(
                col("event_type") == lit("DESAFFECTATION_PROFIL_TECHNIQUE"),
                regexp_extract(
                    col("audit_message"),
                    "pour le contrat n° " + r"(.*?)",
                    1,
                ),
            ),
        )
        .withColumn(
            "profil_technique",
            when(col("event_type") == lit("DESAFFECTATION_PROFIL_TECHNIQUE"), False),
        )
    )


def explode_creation_prospect(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "prospect",
        when(
            col("event_type") == lit("CREATION_PROSPECT"),
            regexp_extract(
                col("audit_message"),
                "creation prospect Confirmée de type :" + r"(.*?)",
                1,
            ),
        ),
    )


def explode_changement_habilitation_convention(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "lintervenant",
        when(
            col("event_type") == lit("CHANGEMENT_HABILITATION_CONVENTION"),
            regexp_extract(
                col("audit_message"),
                "Affectation de l'intervenant " + r"(.*?)" + " pour",
                1,
            ),
        ),
    ).withColumn(
        "convention_carte",
        when(
            col("event_type") == lit("CHANGEMENT_HABILITATION_CONVENTION"),
            regexp_extract(
                col("audit_message"),
                "la convention carte " + r"(.*?)" + " en transaction",
                1,
            ),
        ),
    )


def explode_changement_habilitation_compte(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "lintervenant",
        when(
            col("event_type") == lit("CHANGEMENT_HABILITATIONS_FONCTIONNALITES"),
            regexp_extract(
                col("audit_message"),
                "l'intervenant " + r"(.*?)" + " pour",
                1,
            ),
        ),
    ).withColumn(
        "compte",
        when(
            col("event_type") == lit("CHANGEMENT_HABILITATIONS_FONCTIONNALITES"),
            regexp_extract(
                col("audit_message"),
                "le compte " + r"(.*?)" + " :",
                1,
            ),
        ),
    )


def explode_changement_habilitations_fonctionnalites(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "lintervenant",
        when(
            col("event_type") == lit("CHANGEMENT_HABILITATIONS_FONCTIONNALITES"),
            regexp_extract(
                col("audit_message"),
                "Modification des habilitations de l'intervenant :" + r"(.*?)" + r"\.",
                1,
            ),
        ),
    ).withColumn(
        "fonctionnalites_affectees",
        when(
            col("event_type") == lit("CHANGEMENT_HABILITATIONS_FONCTIONNALITES"),
            regexp_extract(
                col("audit_message"),
                "Nouvelles fonctionnalites affectees :" + r"(.*?)",
                1,
            ),
        ),
    )


def explode_cartes_convention(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "cartes_convention",
        when(
            col("event_type") == lit("CARTES_CONVENTION"),
            regexp_extract(
                col("audit_message"),
                "Export des cartes de la convention " + r"(.*?)" + " ,",
                1,
            ),
        ),
    ).withColumn(
        "type_export",
        when(
            col("event_type") == lit("CARTES_CONVENTION"),
            regexp_extract(col("audit_message"), "Type d'export: " + r"(.*?)", 1),
        ),
    )


def explode_annuler_recharge(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "carte_annulee",
        when(
            col("event_type") == lit("ANNULER_RECHARGE"),
            regexp_extract(
                col("audit_message"), "Recharge de carte annulée: " + r"(.*?)", 1
            ),
        ),
    ).withColumn("recharge", when(col("event_type") == lit("ANNULER_RECHARGE"), False))


def explode_annuler_paiement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "paiement_annule",
        when(
            col("event_type") == lit("ANNULER_PAIEMENT"),
            regexp_extract(col("audit_message"), "Paiement annulé: " + r"(.*?)", 1),
        ),
    ).withColumn("paiement", when(col("event_type") == lit("ANNULER_PAIEMENT"), False))


def explode_ajouter_compte(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "compte",
        when(
            col("event_type") == lit("AJOUTER_COMPTE"),
            regexp_extract(col("audit_message"), "le numéro " + r"(.*?)" + " et", 1),
        ),
    ).withColumn(
        "nature",
        when(
            col("event_type") == lit("AJOUTER_COMPTE"),
            regexp_extract(col("audit_message"), "la nature " + r"(.*?)", 1),
        ),
    )


def explode_ajouter_beneficiaire(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "rib_ajoute",
        when(
            col("event_type") == lit("AJOUTER_BENEFICIAIRE"),
            regexp_extract(col("audit_message"), "RIB ajouté: " + r"(.*?)", 1),
        ),
    ).withColumn(
        "beneficiaire_state",
        when(
            col("event_type") == lit("AJOUTER_BENEFICIAIRE"),
            True,
        ),
    )


def explode_supprime_beneficiaire(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "rib_ajoute",
        when(
            col("event_type") == lit("SUPPRIME_BENEFICIAIRE"),
            regexp_extract(
                col("audit_message"), "Suppression du bénéficiaire " + r"(.*?)", 1
            ),
        ),
    ).withColumn(
        "beneficiaire_state",
        when(
            col("event_type") == lit("SUPPRIME_BENEFICIAIRE"),
            False,
        ),
    )


def explode_affectation_profil_technique(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "lintervenant",
            when(
                col("event_type") == lit("AFFECTATION_PROFIL_TECHNIQUE"),
                regexp_extract(col("audit_message"), "L'intervenant " + r"(.*?)\(", 1),
            ),
        )
        .withColumn(
            "designe_comme",
            when(
                col("event_type") == lit("AFFECTATION_PROFIL_TECHNIQUE"),
                regexp_extract(
                    col("audit_message"), "a été désigné comme " + r"(.*?)", 1
                ),
            ),
        )
        .withColumn(
            "profil_technique",
            when(col("event_type") == lit("AFFECTATION_PROFIL_TECHNIQUE"), True),
        )
    )


def explode_activation_attijari_securite(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "attijari_securite",
        when(col("event_type") == lit("ACTIVATION_ATTIJARI_SECURITE"), True),
    )


def explode_desactivation_attijari_securite(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "attijari_securite",
        when(col("event_type") == lit("DESACTIVATION_ATTIJARI_SECURITE"), False),
    )


# TODO: virement 3176783 commentaire modification du montant
def explode_virement_rejected2(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "virement_rejected2",
        when(col("event_type") == lit("VIREMENT_REJECTED2"), True),
    )


def explode_otp_verify_otp_ok(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "otp_verify_otp",
        when(col("event_type") == lit("OTP_VERIFY_OTP_OK"), True),
    )


def explode_consultation_carte(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "consultation_carte",
        when(col("event_type") == lit("CONSULTATION_CARTE"), True),
    )


def explode_validate_after_sale_contract(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "validate_after_sale_contract",
        when(col("event_type") == lit("VALIDATE_AFTER_SALE_CONTRACT"), True),
    )


def explode_submit_after_sale_contract(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "submit_after_sale_contract",
        when(col("event_type") == lit("SUBMIT_AFTER_SALE_CONTRACT"), True),
    )


def explode_paiement_rejected(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "paiement_rejected",
        when(col("event_type") == lit("PAIEMENT_REJECTED"), True),
    )


def explode_validate_contrat_vente(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "validate_contrat_vente",
        when(col("event_type") == lit("VALIDATE_CONTRAT_VENTE"), True),
    )


def explode_logout(df: DataFrame) -> DataFrame:
    return df.withColumn("logout", when(col("event_type") == lit("LOGOUT"), True))


def explode_reset_password_token_sent_success(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "reset_password_token_sent_success",
        when(col("event_type") == lit("RESET_PASSWORD_TOKEN_SENT_SUCCESS"), True),
    )


def explode_change_password_expired_success(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "change_password_expired",
        when(col("event_type") == lit("CHANGE_PASSWORD_EXPIRED_SUCCESS"), True),
    )


def explode_get_status_souscription_vente(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "get_status_souscription_vente",
        when(col("event_type") == lit("GET_STATUS_SOUSCRIPTION_VENTE"), True),
    )


def explode_attijari_securite_envoi_push_rejet(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "attijari_securite_push",
        when(col("event_type") == lit("ATTIJARI_SECURITE_ENVOI_PUSH_REJET"), True),
    )


def explode_ereleve_activation(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "ereleve_activation",
        when(col("event_type") == lit("ERELEVE_ACTIVATION"), True),
    )


def explode_validate_eligibility_contrat_vente(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "validate_eligibility_contrat_vente",
        when(col("event_type") == lit("VALIDATE_ELIGIBILITY_CONTRAT_VENTE"), True),
    )


def explode_change_password_failed(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "change_password",
        when(col("event_type") == lit("CHANGE_PASSWORD_FAILED"), False),
    )


def explode_submit_contrat_vente(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "submit_contrat_vente",
        when(col("event_type") == lit("SUBMIT_CONTRAT_VENTE"), True),
    )


def explode_maj_operateur_intervenant(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "maj_operateur_intervenant",
        when(col("event_type") == lit("MAJ_OPERATEUR_INTERVENANT"), True),
    )


def explode_change_password_expired_failed(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "change_password_expired",
        when(col("event_type") == lit("CHANGE_PASSWORD_EXPIRED_FAILED"), False),
    )


def explode_soucription_docnet(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "soucription_docnet",
        when(col("event_type") == lit("SOUCRIPTION_DOCNET"), True),
    )


def explode_telechargement_rib(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "telechargement_rib",
        when(col("event_type") == lit("TELECHARGEMENT_RIB"), True),
    )


def explode_attijari_securite_push_accept(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "attijari_securite_push",
        when(col("event_type") == lit("ATTIJARI_SECURITE_PUSH_ACCEPT"), True),
    )


# TODO: Always: Virement annulé: REF- 24880528
def explode_annuler_virement_permanent(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "virement_permanent",
        when(col("event_type") == lit("ANNULER_VIREMENT_PERMANENT"), False),
    )


def explode_attijari_securite_envoi_push_timeout(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "attijari_securite_envoi_push_timeout",
        when(col("event_type") == lit("ATTIJARI_SECURITE_ENVOI_PUSH_TIMEOUT"), True),
    )


def explode_bloquer_intervenant(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "bloquer_intervenant",
        when(col("event_type") == lit("BLOQUER_INTERVENANT"), True),
    )


def explode_creation_intervenant(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "lintervenant_admin",
        regexp_extract(
            col("audit_message"), "Création de l'intervenant admin " + r"(.*?)", 1
        ),
    ).withColumn(
        "intervenant_status",
        when(col("event_type") == lit("CREATION_INTERVENANT"), True),
    )


def explode_catalogue_attijaripay(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "catalogue_attijaripay",
        when(col("event_type") == lit("CATALOGUE_ATTIJARIPAY"), True),
    )


def explode_maj_intervenant(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "maj_intervenant",
        when(col("event_type") == lit("MAJ_INTERVENANT"), True),
    )


def explode_otp_verify_otp_ko(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "otp_verify_otp",
        when(col("event_type") == lit("OTP_VERIFY_OTP_KO"), False),
    )


def explode_supprimer_discussion(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "supprimer_discussion",
        when(col("event_type") == lit("SUPPRIMER_DISCUSSION"), True),
    )


def explode_change_password_success(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "change_password",
        when(col("event_type") == lit("CHANGE_PASSWORD_SUCCESS"), True),
    )


def explode_change_mode_releve(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "change_mode_releve",
        when(col("event_type") == lit("CHANGE_MODE_RELEVE"), True),
    )


def explode_annuler_chequier_lcn(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "annuler_chequier_lcn",
        when(col("event_type") == lit("ANNULER_CHEQUIER_LCN"), True),
    )


# TODO: Virement annulé: REF- 24880528
def explode_annuler_virement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "virement",
        when(col("event_type") == lit("ANNULER_VIREMENT"), True),
    )


def explode_attijari_securite_envoi_push(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "attijari_securite_envoi_push",
        when(col("event_type") == lit("ATTIJARI_SECURITE_ENVOI_PUSH"), True),
    )


def explode_impression_rib(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "impression_rib",
        when(col("event_type") == lit("IMPRESSION_RIB"), True),
    )


def explode_otp_envoi_voice(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "otp_envoi_voice",
        when(col("event_type") == lit("OTP_ENVOI_VOICE"), True),
    )


def explode_otp_envoi_sms(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "otp_envoi_sms",
        when(col("event_type") == lit("OTP_ENVOI_SMS"), True),
    )


def explode_effectuer_recharge(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "carte_beneficiaire",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE"),
                extract_field_with_brackets_from_message("carte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "recharge", when(col("event_type") == lit("ANNULER_RECHARGE"), True)
        )
    )


def explode_virement_validated1(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_beneficiaire_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Compte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
        .withColumn(
            "validateur2_virement_validated1",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED1"),
                extract_field_with_brackets_from_message("Validateur2"),
            ),
        )
    )


def explode_virement_validated2(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_beneficiaire_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Compte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
        .withColumn(
            "validateur2_virement_validated2",
            when(
                col("event_type") == lit("VIREMENT_VALIDATED2"),
                extract_field_with_brackets_from_message("Validateur2"),
            ),
        )
    )


def explode_soumettre_virement(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_beneficiaire_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Compte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
        .withColumn(
            "validateur2_soumettre_virement",
            when(
                col("event_type") == lit("SOUMETTRE_VIREMENT"),
                extract_field_with_brackets_from_message("Validateur2"),
            ),
        )
    )


def explode_ajouter_carte_beneficiaire(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "numero_de_carte",
            when(
                col("event_type") == lit("AJOUTER_CARTE_BENEFICIAIRE"),
                extract_field_with_brackets_from_message("Numéro de carte"),
            ),
        )
        .withColumn(
            "beneficiaire",
            when(
                col("event_type") == lit("AJOUTER_CARTE_BENEFICIAIRE"),
                extract_field_with_brackets_from_message("Bénéficiaire"),
            ),
        )
        .withColumn(
            "carte_beneficiaire_status",
            when(
                col("event_type") == lit("AJOUTER_CARTE_BENEFICIAIRE"),
                True,
            ),
        )
    )


def explode_supprime_carte_beneficiaire(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "beneficiaire",
        when(
            col("event_type") == lit("SUPPRIME_CARTE_BENEFICIAIRE "),
            extract_field_from_message("bénéficiaire "),
        ),
    ).withColumn(
        "carte_beneficiaire_status",
        when(
            col("event_type") == lit("SUPPRIME_CARTE_BENEFICIAIRE"),
            False,
        ),
    )


def explode_demande_recharge_carte_webpay(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("DEMANDE_RECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "ndeg_carte_web_pay",
            when(
                col("event_type") == lit("DEMANDE_RECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("N° Carte Web Pay"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("DEMANDE_RECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("DEMANDE_RECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
    )


def explode_soumettre_recharge(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "carte_beneficiaire",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Carte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
        .withColumn(
            "validateur2",
            when(
                col("event_type") == lit("SOUMETTRE_RECHARGE"),
                extract_field_with_brackets_from_message("Validateur2"),
            ),
        )
    )


# TODO: Référence virement [5998482] Ancien validateur 2 [Ngg75128452] Nouveau validateur 2 [Ngg75128452]
def explode_modifier_validateur_virement(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "reference_virement_modifier_validateur_virement",
            when(
                col("event_type") == lit("MODIFIER_VALIDATEUR_VIREMENT"),
                extract_field_with_brackets_from_message("Référence virement"),
            ),
        )
        .withColumn(
            "ancien_validateur_2_modifier_validateur_virement",
            when(
                col("event_type") == lit("MODIFIER_VALIDATEUR_VIREMENT"),
                extract_field_with_brackets_from_message("Ancien validateur 2"),
            ),
        )
        .withColumn(
            "nouveau_validateur_2_modifier_validateur_virement",
            when(
                col("event_type") == lit("MODIFIER_VALIDATEUR_VIREMENT"),
                extract_field_with_brackets_from_message("Nouveau validateur 2"),
            ),
        )
    )


# TODO: New
def explode_initier_paiement(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "paiement_facture",
            when(
                col("event_type") == lit("INITIER_PAIEMENT"),
                extract_field_with_brackets_from_message("Paiement Facture"),
            ),
        )
        .withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("INITIER_PAIEMENT"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("INITIER_PAIEMENT"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("INITIER_PAIEMENT"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("INITIER_PAIEMENT"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur",
            when(
                col("event_type") == lit("INITIER_PAIEMENT"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
    )


def explode_commander_chequier(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte",
            when(
                col("event_type") == lit("COMMANDER_CHEQUIER"),
                extract_field_with_brackets_from_message("Compte"),
            ),
        )
        .withColumn(
            "mention",
            when(
                col("event_type") == lit("COMMANDER_CHEQUIER"),
                extract_field_with_brackets_from_message("Mention"),
            ),
        )
        .withColumn(
            "type_chequier",
            when(
                col("event_type") == lit("COMMANDER_CHEQUIER"),
                extract_field_with_brackets_from_message("Type chéquier"),
            ),
        )
    )


def explode_affecter_role_seuil(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "affecter_role",
            when(
                col("event_type") == lit("AFFECTER_ROLE_SEUIL"),
                extract_field_with_brackets_from_message("Affecter rôle"),
            ),
        )
        .withColumn(
            "avec_seuil",
            when(
                col("event_type") == lit("AFFECTER_ROLE_SEUIL"),
                extract_field_with_brackets_from_message("avec seuil"),
            ),
        )
        .withColumn(
            "a_lintervenant",
            when(
                col("event_type") == lit("AFFECTER_ROLE_SEUIL"),
                extract_field_with_brackets_from_message("à l'intervenant"),
            ),
        )
        .withColumn(
            "role_seuil",
            when(
                col("event_type") == lit("AFFECTER_ROLE_SEUIL"),
                True,
            ),
        )
    )


def explode_login_ko(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "login",
        when(
            col("event_type") == lit("LOGIN_KO"),
            when(
                extract_field_with_brackets_from_message("connexion réussie")
                == lit("oui"),
                True,
            ),
        ),
    ).withColumn(
        "utilisateur_bloque",
        when(
            col("event_type") == lit("LOGIN_KO"),
            when(
                extract_field_with_brackets_from_message("utilisateur bloqué")
                == lit("oui"),
                True,
            ).otherwise(False),
        ),
    )


def explode_recharge_validated1(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "carte_beneficiaire",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Carte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
        .withColumn(
            "validateur2",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED1"),
                extract_field_with_brackets_from_message("Validateur2"),
            ),
        )
    )


def explode_login(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "login",
            when(
                col("event_type") == lit("LOGIN"),
                when(
                    extract_field_with_brackets_from_message("connexion réussie")
                    == lit("oui"),
                    True,
                ).otherwise(False),
            ),
        )
        .withColumn(
            "utilisateur_bloque",
            when(
                col("event_type") == lit("LOGIN"),
                when(
                    extract_field_with_brackets_from_message("utilisateur bloqué")
                    == lit("oui"),
                    True,
                ).otherwise(False),
            ),
        )
        .withColumn(
            "clientbankalik",
            when(
                col("event_type") == lit("LOGIN"),
                extract_field_with_brackets_from_message("clientBankaLik"),
            ),
        )
        .withColumn(
            "appname",
            when(
                col("event_type") == lit("LOGIN"),
                extract_field_with_brackets_from_message("appName:"),
            ),
        )
    )


# TODO: virement annulé [9142225] initiateur supprimé [Okj34516368]
# TODO: Annulé recharge [17722711] à cause de la suppression du rôle [Initier] à intervenant [Xhv43159324]
def explode_virement_canceled(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "annule_recharge_virement_canceled",
            when(
                col("event_type") == lit("VIREMENT_CANCELED"),
                extract_field_with_brackets_from_message("Annulé recharge"),
            ),
        )
        .withColumn(
            "a_cause_de_la_suppression_du_role_virement_canceled",
            when(
                col("event_type") == lit("VIREMENT_CANCELED"),
                extract_field_with_brackets_from_message(
                    "à cause de la suppression du rôle"
                ),
            ),
        )
        .withColumn(
            "a_intervenant_virement_canceled",
            when(
                col("event_type") == lit("VIREMENT_CANCELED"),
                extract_field_with_brackets_from_message("à intervenant"),
            ),
        )
    )


def explode_demande_decharge_carte_webpay(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_beneficiaire",
            when(
                col("event_type") == lit("DEMANDE_DECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("Compte bénéficiaire"),
            ),
        )
        .withColumn(
            "ndeg_carte_web_pay",
            when(
                col("event_type") == lit("DEMANDE_DECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("N° Carte Web Pay"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("DEMANDE_DECHARGE_CARTE_WEBPAY"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
    )


def explode_envoi_mail_acces_complet(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "envoi_mail_acces_complet",
        when(
            col("event_type") == lit("ENVOI_MAIL_ACCES_COMPLET"),
            True,
        ),
    )


def explode_messagerie_nouveau_message(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "theme",
            when(
                col("event_type") == lit("MESSAGERIE_NOUVEAU_MESSAGE"),
                extract_field_with_brackets_from_message("Théme="),
            ),
        )
        .withColumn(
            "_objet",
            when(
                col("event_type") == lit("MESSAGERIE_NOUVEAU_MESSAGE"),
                extract_field_with_brackets_from_message(", Objet="),
            ),
        )
        .withColumn(
            "_message",
            when(
                col("event_type") == lit("MESSAGERIE_NOUVEAU_MESSAGE"),
                extract_field_with_brackets_from_message(", Message="),
            ),
        )
    )


def explode_mise_a_jour_nature_compte(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "mise_a_jour_nature_compte",
        when(
            col("event_type") == lit("MISE_A_JOUR_NATURE_COMPTE"),
            extract_field_with_brackets_from_message(
                "Les comptes avec les identifiants:"
            ),
        ),
    )


def explode_supprimer_role_seuil(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "supprimer_role",
            when(
                col("event_type") == lit("SUPPRIMER_ROLE_SEUIL"),
                extract_field_with_brackets_from_message("Supprimer rôle"),
            ),
        )
        .withColumn(
            "avec_seuil",
            when(
                col("event_type") == lit("SUPPRIMER_ROLE_SEUIL"),
                extract_field_with_brackets_from_message("avec seuil"),
            ),
        )
        .withColumn(
            "a_lintervenant",
            when(
                col("event_type") == lit("SUPPRIMER_ROLE_SEUIL"),
                extract_field_with_brackets_from_message("à l'intervenant"),
            ),
        )
        .withColumn(
            "role_seuil",
            when(
                col("event_type") == lit("SUPPRIMER_ROLE_SEUIL"),
                False,
            ),
        )
    )


def explode_modifier_virement(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "ancienne_reference_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Ancienne référence"),
            ),
        )
        .withColumn(
            "nouvelle_reference_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Nouvelle référence"),
            ),
        )
        .withColumn(
            "compte_beneficiaire_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Compte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1_modifier_virement",
            when(
                col("event_type") == lit("MODIFIER_VIREMENT"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
    )


def explode_effectuer_recharge_wallet(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "mobile_wallet",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE_WALLET"),
                extract_field_with_brackets_from_message("mobile Wallet"),
            ),
        )
        .withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE_WALLET"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE_WALLET"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE_WALLET"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE_WALLET"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur",
            when(
                col("event_type") == lit("EFFECTUER_RECHARGE_WALLET"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
    )


def explode_commander_lcn(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "commande_lcn",
            when(
                col("event_type") == lit("COMMANDER_LCN"),
                extract_field_with_brackets_from_message("commande LCN"),
            ),
        )
        .withColumn(
            "compte",
            when(
                col("event_type") == lit("COMMANDER_LCN"),
                extract_field_with_brackets_from_message("Compte"),
            ),
        )
        .withColumn(
            "formule_lcn",
            when(
                col("event_type") == lit("COMMANDER_LCN"),
                extract_field_with_brackets_from_message("Formule LCN"),
            ),
        )
        .withColumn(
            "nombre",
            when(
                col("event_type") == lit("COMMANDER_LCN"),
                extract_field_with_brackets_from_message("nombre"),
            ),
        )
    )


# TODO: New
def explode_effectuer_paiement(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "paiement_facture_effectuer_paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                extract_field_with_brackets_from_message("Paiement Facture"),
            ),
        )
        .withColumn(
            "compte_emetteur_effectuer_paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant_effectuer_paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_effectuer_paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif_effectuer_paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur_effectuer_paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "paiement",
            when(
                col("event_type") == lit("EFFECTUER_PAIEMENT"),
                True,
            ),
        )
    )


def explode_recharge_rejected(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "rejete_recharge",
            when(
                col("event_type") == lit("RECHARGE_REJECTED"),
                extract_field_with_brackets_from_message("Rejeté recharge"),
            ),
        )
        .withColumn(
            "a_cause_de_la_suppression_du_role",
            when(
                col("event_type") == lit("RECHARGE_REJECTED"),
                extract_field_with_brackets_from_message(
                    "à cause de la suppression du rôle"
                ),
            ),
        )
        .withColumn(
            "a_intervenant",
            when(
                col("event_type") == lit("RECHARGE_REJECTED"),
                extract_field_with_brackets_from_message("à intervenant"),
            ),
        )
    )


# TODO: sometimes: "Rib bénéficiaire recalcule pour DAS:[ma.awb.ebk.bel.domain.Rib@516e7773]"
def explode_effectuer_virement(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_beneficiaire_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                extract_field_with_brackets_from_message("Compte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "virement_effectuer_virement",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT"),
                True,
            ),
        )
    )


def explode_recharge_validated2(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "carte_beneficiaire",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Carte bénéficiaire"),
            ),
        )
        .withColumn(
            "compte_emetteur",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "montant",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Date"),
            ),
        )
        .withColumn(
            "motif",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "initiateur",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Initiateur"),
            ),
        )
        .withColumn(
            "validateur1",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Validateur1"),
            ),
        )
        .withColumn(
            "validateur2",
            when(
                col("event_type") == lit("RECHARGE_VALIDATED2"),
                extract_field_with_brackets_from_message("Validateur2"),
            ),
        )
    )


# TODO: How to model this?
def explode_reset_password_success(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "reset_password_success",
        when(
            col("event_type") == lit("RESET_PASSWORD_SUCCESS"),
            True,
        ),
    )


def explode_delete_intervenant(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "intervenant_supprime",
        when(
            col("event_type") == lit("DELETE_INTERVENANT"),
            extract_field_with_brackets_from_message("Intervenant supprimé:"),
        ),
    ).withColumn(
        "intervenant_status",
        when(col("event_type") == lit("CREATION_INTERVENANT"), False),
    )


def explode_effectuer_virement_permanent(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "compte_emetteur_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Compte émetteur"),
            ),
        )
        .withColumn(
            "beneficiaire_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Bénéficiaire"),
            ),
        )
        .withColumn(
            "montant_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Montant"),
            ),
        )
        .withColumn(
            "date_validation_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Date validation"),
            ),
        )
        .withColumn(
            "motif_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Motif"),
            ),
        )
        .withColumn(
            "date_debut_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Date début"),
            ),
        )
        .withColumn(
            "date_fin_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Date fin"),
            ),
        )
        .withColumn(
            "frequence_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Fréquence"),
            ),
        )
        .withColumn(
            "nombre_decheances_effectuer_virement_permanent",
            when(
                col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"),
                extract_field_with_brackets_from_message("Nombre d'échéances"),
            ),
        )
        .withColumn(
            "virement_permanent",
            when(col("event_type") == lit("EFFECTUER_VIREMENT_PERMANENT"), True),
        )
    )
