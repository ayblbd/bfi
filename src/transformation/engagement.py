from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    min,
    when,
)


def select_l2do_dgen(l2do_dgen: DataFrame) -> DataFrame:
    """
    Sélectionne et renomme les colonnes principales du DataFrame `l2do_dgen`.

    Cette fonction extrait les colonnes principales liées à la gestion des dossiers, notamment
    le numéro de dossier, le numéro de compte, et l'entité de gestion.
    """
    return (
        l2do_dgen.withColumnRenamed("nodoss", "numero_dossier")
        .withColumnRenamed("rfemp_11", "numero_de_compte")
        .withColumnRenamed("entgest_11", "entite_gestion")
        .select(
            "numero_dossier",
            "numero_de_compte",
            "entite_gestion",
        )
    )


def select_l2do_dlanc(l2do_dlanc: DataFrame) -> DataFrame:
    """
    Sélectionne et renomme les colonnes principales du DataFrame `l2do_dlanc`.

    Cette fonction extrait les informations relatives au lancement des dossiers, telles que
    le code produit, les dates de proposition, de réalisation et d'échéance, ainsi que
    les données financières comme le nominal, la devise et le taux d'intérêt.
    """
    return (
        l2do_dlanc.withColumnRenamed("nodoss", "numero_dossier")
        .withColumnRenamed("cdpdt_12", "code_produit")
        .withColumnRenamed("dtpropo_12", "date_proposition")
        .withColumnRenamed("cddevise_12", "devise_pret")
        .withColumnRenamed("nominal_12", "nominal")
        .withColumnRenamed("duree_12", "duree_dossier")
        .withColumnRenamed("dtreal_12", "date_realisation")
        .withColumnRenamed("dtech_12", "date_premiere_echeance")
        .withColumnRenamed("dtaccor_12", "date_accord_dossier")
        .withColumnRenamed("tie_12", "taux_interet_effectif_initial")
        .select(
            "numero_dossier",
            "code_produit",
            "date_proposition",
            "devise_pret",
            "nominal",
            "duree_dossier",
            "date_realisation",
            "date_premiere_echeance",
            "date_accord_dossier",
            "taux_interet_effectif_initial",
        )
    )


def select_l2do_tfonc(l2do_tfonc: DataFrame) -> DataFrame:
    """
    Sélectionne et renomme les colonnes principales du DataFrame `l2do_tfonc`.

    Cette fonction extrait les informations financières et temporelles liées aux dossiers en cours,
    telles que la durée restante, le capital restant dû, les montants débloqués, et les échéances.
    Elle renomme également les colonnes pour harmoniser les noms.
    """
    return (
        l2do_tfonc.withColumnRenamed("nodoss", "numero_dossier")
        .withColumnRenamed("durcfd_22", "duree_restante")
        .withColumnRenamed("nopac_e22", "numero_palier_en_cours")
        .withColumnRenamed("nbeche_22", "nombre_echeance_echues")
        .withColumnRenamed("dtfdp_22", "date_fin_previsionnelle")
        .withColumnRenamed("dtfdr_22", "date_fin_reelle")
        .withColumnRenamed("stdu_22", "sommes_totales_restant_dues")
        .withColumnRenamed("crdu_22", "capital_restant_du")
        .withColumnRenamed("mttdbl_22", "montant_total_debloque_depuis_l_origine")
        .withColumnRenamed("dtaddbl_22", "date_avant_dernier_deblocage")
        .withColumnRenamed("dtddbl_22", "date_dernier_deblocage")
        .withColumnRenamed("dtpdbl_22", "date_prochain_deblocage")
        .withColumnRenamed("mtpdbl_22", "montant_prochain_deblocage")
        .withColumnRenamed("camechb_22", "capital_total_amorti_en_echeances")
        .withColumnRenamed(
            "ipechb_22", "capital_total_recue_en_interets_part_banque"
        )
        .withColumnRenamed("dtadech_22", "date_avant_derniere_echeance_traitee")
        .withColumnRenamed("dtdech_22", "date_derniere_echeance")
        .withColumnRenamed("nopech_22", "numero_prochain_echeance")
        .withColumnRenamed("mtdblmns_22", "montant_debloque_dans_mois_global")
        .withColumnRenamed("dtecmoy_22", "date_dernier_calcul_encours_calcule")
        .withColumnRenamed("mtecmoy_22", "montant_dernier_encours_calcule")
        .withColumnRenamed("dtpechit_22", "date_prochaine_echeance")
        .withColumnRenamed("dtplit_22", "date_passage_pret_douteux_compromis")
        .withColumnRenamed("dtpctx_22", "date_passage_contentieux")
        .withColumnRenamed(
            "cvnominal_22", "nominal_en_contre_valeur_devise_nationale"
        )
        .withColumnRenamed("dtrach_22", "date_rachat")
        .withColumn("date_anticipation_remboursement_anticipe", col("date_rachat"))
        .withColumnRenamed("tie_22", "taux_interets_effectif_courant")
        .withColumn(
            "lib_etat_dossier",
            when(col("cdsitdos_22") == "1", "ACCORDE_ATTENTE_REALISATION")
            .when(col("cdsitdos_22") == "2", "EN_COURS_AMORTISSEMENT")
            .when(
                col("cdsitdos_22") == "3", "ATTENTE_MODIFCATION_DONNEES_FINANCIERES"
            )
            .when(col("cdsitdos_22") == "4", "ATTENTE_REPRISE")
            .when(col("cdsitdos_22") == "5", "SOLDE"),
        )
        .withColumnRenamed("cdsitdos_22", "code_situation_dossier")
        .select(
            "numero_dossier",
            "duree_restante",
            "nombre_echeance_echues",
            "date_fin_previsionnelle",
            "date_fin_reelle",
            "sommes_totales_restant_dues",
            "capital_restant_du",
            "montant_total_debloque_depuis_l_origine",
            "date_avant_dernier_deblocage",
            "date_dernier_deblocage",
            "date_prochain_deblocage",
            "montant_prochain_deblocage",
            "capital_total_amorti_en_echeances",
            "capital_total_recue_en_interets_part_banque",
            "date_avant_derniere_echeance_traitee",
            "date_derniere_echeance",
            "numero_prochain_echeance",
            "montant_debloque_dans_mois_global",
            "date_dernier_calcul_encours_calcule",
            "montant_dernier_encours_calcule",
            "numero_palier_en_cours",
            "date_prochaine_echeance",
            "date_passage_pret_douteux_compromis",
            "date_passage_contentieux",
            "nominal_en_contre_valeur_devise_nationale",
            "date_rachat",
            "date_anticipation_remboursement_anticipe",
            "taux_interets_effectif_courant",
            "lib_etat_dossier",
            "code_situation_dossier",
        )
    )


def select_l2do_dcomm(l2do_dcomm: DataFrame) -> DataFrame:
    """
    Sélectionne et renomme les colonnes principales du DataFrame `l2do_dcomm`.

    Cette fonction extrait les informations relatives à la catégorie et au type de prêt.
    """
    return (
        l2do_dcomm.withColumnRenamed("nodoss", "numero_dossier")
        .withColumnRenamed("ctdoss_13", "code_categorie_pret")
        .withColumnRenamed("typret_13", "type_pret")
        .select(
            "numero_dossier",
            "code_categorie_pret",
            "type_pret",
        )
    )


def select_l2do_dpal(l2do_dpal: DataFrame) -> DataFrame:
    """
    Sélectionne et traite les colonnes principales du DataFrame `l2do_dpal`.

    Cette fonction gère les informations liées aux paliers de déblocage, comme les montants débloqués
    et amortis, et identifie le montant du premier déblocage pour chaque dossier.
    """
    window = Window.partitionBy("numero_dossier")
    return (
        l2do_dpal.withColumnRenamed("nodoss", "numero_dossier")
        .withColumnRenamed("mtdebloc_18", "montant_deblocage")
        .withColumnRenamed("capamort_18", "capital_amortit_palier")
        .withColumnRenamed("mtint_18", "interet_a_percevoir")
        .withColumn("min_nopal_e18", min(col("nopal_e18")).over(window))
        .withColumn(
            "montant_premier_deblocage",
            when(
                col("nopal_e18") == col("min_nopal_e18"),
                col("montant_deblocage"),
            ),
        )
        .filter(col("montant_premier_deblocage").isNotNull())
        .select(
            "numero_dossier",
            "montant_deblocage",
            "capital_amortit_palier",
            "interet_a_percevoir",
            "montant_premier_deblocage",
        )
    )


def select_l2hd_rbt_nat(l2hd_rbt_nat: DataFrame) -> DataFrame:
    """
    Sélectionne et traite les colonnes principales du DataFrame `l2hd_rbt_nat`.

    Cette fonction extrait les informations des rachats, comme les montants et taxes associés, et
    ajoute une colonne pour différencier les types de rachats (RAT, RAP, ou AUTRE).
    """
    return (
        l2hd_rbt_nat.withColumnRenamed("nodoss", "numero_dossier")
        .withColumnRenamed("aspb_14", "montant_rachat")
        .withColumnRenamed("tvaspb_14", "taxe_montant_rachat")
        .withColumn(
            "type_rachat",
            when(col("tyeven_g") == "014", "RAT")
            .when(col("tyeven_g") == "015", "RAP")
            .otherwise("AUTRE"),
        )
        .select(
            "numero_dossier",
            "montant_rachat",
            "taxe_montant_rachat",
            "tyeven_g",
            "type_rachat",
        )
    )


def build_credit(
    l2do_dgen: DataFrame,
    l2do_dlanc: DataFrame,
    l2do_tfonc: DataFrame,
    l2do_dcomm: DataFrame,
    l2do_dpal: DataFrame,
    l2hd_rbt_nat: DataFrame,
) -> DataFrame:
    """
    Construit une vue consolidée des données de crédit.

    Cette fonction combine les données provenant de plusieurs sources (gestion des dossiers,
    lancement, fonctionnement, commercialisation, paliers, et rachats) pour produire un
    DataFrame complet avec les informations de crédit.

    Arguments :
        l2do_dgen : DataFrame contenant les informations générales des dossiers.
        l2do_dlanc : DataFrame contenant les informations de lancement des dossiers.
        l2do_tfonc : DataFrame contenant les informations financières des dossiers.
        l2do_dcomm : DataFrame contenant les informations commerciales des dossiers.
        l2do_dpal : DataFrame contenant les informations des paliers de déblocage.
        l2hd_rbt_nat : DataFrame contenant les informations de rachat.

    Retourne :
        Un DataFrame consolidé avec toutes les informations liées aux crédits.
    """
    l2do_dgen = l2do_dgen.transform(select_l2do_dgen)
    l2do_dlanc = l2do_dlanc.transform(select_l2do_dlanc)
    l2do_tfonc = l2do_tfonc.transform(select_l2do_tfonc)
    l2do_dcomm = l2do_dcomm.transform(select_l2do_dcomm)
    l2do_dpal = l2do_dpal.transform(select_l2do_dpal)
    l2hd_rbt_nat = l2hd_rbt_nat.transform(select_l2hd_rbt_nat)

    return (
        l2do_dgen.join(l2do_dlanc, ["numero_dossier"], how="inner")
        .join(l2do_tfonc, ["numero_dossier"], how="inner")
        .join(l2do_dcomm, ["numero_dossier"], how="inner")
        .join(l2do_dpal, ["numero_dossier"], how="inner")
        .join(l2hd_rbt_nat, ["numero_dossier"], how="inner")
        .withColumn(
            "type_deblocage",
            when(
                l2do_dpal["montant_deblocage"] != l2do_dlanc["nominal"],
                "DEBLOCAGE_PARTIEL",
            ).otherwise("DEBLOCAGE TOTAL"),
        )
        .select(
            "numero_dossier",
            "numero_de_compte",
            "entite_gestion",
            "code_produit",
            "date_proposition",
            "devise_pret",
            "nominal",
            "duree_dossier",
            "date_realisation",
            "date_premiere_echeance",
            "date_accord_dossier",
            "taux_interet_effectif_initial",
            "duree_restante",
            "nombre_echeance_echues",
            "date_fin_previsionnelle",
            "date_fin_reelle",
            "sommes_totales_restant_dues",
            "capital_restant_du",
            "montant_total_debloque_depuis_l_origine",
            "date_avant_dernier_deblocage",
            "date_dernier_deblocage",
            "date_prochain_deblocage",
            "montant_prochain_deblocage",
            "capital_total_amorti_en_echeances",
            "capital_total_recue_en_interets_part_banque",
            "date_avant_derniere_echeance_traitee",
            "date_derniere_echeance",
            "numero_prochain_echeance",
            "montant_debloque_dans_mois_global",
            "date_dernier_calcul_encours_calcule",
            "montant_dernier_encours_calcule",
            "date_prochaine_echeance",
            "date_passage_pret_douteux_compromis",
            "date_passage_contentieux",
            "nominal_en_contre_valeur_devise_nationale",
            "date_rachat",
            "date_anticipation_remboursement_anticipe",
            "taux_interets_effectif_courant",
            "numero_palier_en_cours",
            "lib_etat_dossier",
            "code_situation_dossier",
            "code_categorie_pret",
            "type_pret",
            "montant_deblocage",
            "montant_premier_deblocage",
            "capital_amortit_palier",
            "interet_a_percevoir",
            "montant_rachat",
            "taxe_montant_rachat",
            "tyeven_g",
            "type_deblocage",
            "type_rachat",
        )
    )
