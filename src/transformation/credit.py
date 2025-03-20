from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    min,
    when,
    count,
    lit,
    date_format,
    to_date,
    trim,
    concat_ws,
    coalesce,
    crc32,
    sum,
    substring,
    max,
    date_sub,
    broadcast,
    dayofweek,
)
from src.transformation.common import (
    get_is_during_a_period_for_equipement,
    build_day_holiday,
)


def select_l2do_dgen(l2do_dgen: DataFrame) -> DataFrame:
    return (
        l2do_dgen.withColumn("situationpret", lit("ACCORDE"))
        .withColumn("numero_dossier", trim(col("nodoss")))
        .withColumn("numerodecompte", trim(substring(col("rfemp_11"), 1, 11)))
        .withColumn("entite_gestion", trim(col("entgest_11")))
        .select(
            "numero_dossier",
            "numerodecompte",
            "entite_gestion",
            "situationpret",
        )
    )


def select_l2do_dpal(l2do_dpal: DataFrame) -> DataFrame:
    window = Window.partitionBy("numero_dossier")
    return (
        l2do_dpal.withColumn("numero_dossier", trim(col("nodoss")))
        .withColumn("PretCreditBrute", col("mtdebloc_18").cast("float"))
        .withColumn("min_nopal_e18", min(col("nopal_e18")).over(window))
        .withColumn(
            "montant_premier_deblocage",
            when(
                col("nopal_e18") == col("min_nopal_e18"),
                col("PretCreditBrute"),
            ),
        )
        .filter(col("montant_premier_deblocage").isNotNull())
        .select("numero_dossier", "PretCreditBrute")
        .dropDuplicates()
    )

def select_l2hd_debl(l2hd_debl: DataFrame, joursferie: DataFrame, partitiondate) -> DataFrame:
    joursferie_processed = joursferie.filter(col("dfval") > partitiondate)
    l2hd_debl_processed = (l2hd_debl
                           .withColumn("PretCreditBrute", col("mtdebloc_06").cast("float"))
                           .withColumn(
                                "datedeblocage",
                                date_format(to_date(col("dtdevent_g"), "yyyyMMdd"), "yyyy-MM-dd"),
                            )
                           .withColumn("numero_dossier", trim(col("nodoss")))
                           .withColumn("datedeblocage_1", col("datedeblocage"))
                           .select(col("PretCreditBrute"), col("datedeblocage"), col("numero_dossier"), col("datedeblocage_1")))
    
    l2hd_debl_processed = build_day_holiday(
        l2hd_debl_processed,
        joursferie_processed,
        partitiondate="datedeblocage_1",
        nm=1,
    )
    return l2hd_debl_processed


def select_l2do_dlanc(
    l2do_dlanc: DataFrame, joursferie: DataFrame, partitiondate
) -> DataFrame:
    joursferie_processed = joursferie.filter(col("dfval") > partitiondate)

    l2do_dlanc_process = (
        l2do_dlanc.withColumn("numero_dossier", trim(col("nodoss")))
        .withColumn("code_produit", trim(col("cdpdt_12")))
        .withColumn("devise_pret", trim(col("cddevise_12")))
        .withColumn("nominal", col("nominal_12").cast("float"))
        .withColumn("duree_dossier", trim("duree_12"))
        # .withColumn(
        #     "datedeblocage",
        #     date_format(to_date(col("dtreal_12"), "yyyyMMdd"), "yyyy-MM-dd"),
        # )
        # .withColumn("datedeblocage_1", col("datedeblocage"))
        .select(
            "numero_dossier",
            "code_produit",
            "devise_pret",
            # "nominal",
            "duree_dossier",
            # "datedeblocage",
            # "datedeblocage_1",
        )
    )
    # l2do_dlanc_process = build_day_holiday(
    #     l2do_dlanc_process,
    #     joursferie_processed,
    #     partitiondate="datedeblocage_1",
    #     nm=1,
    # )

    return l2do_dlanc_process


def select_l2do_tfonc(l2do_tfonc: DataFrame) -> DataFrame:
    return (
        l2do_tfonc.withColumn("numero_dossier", trim("nodoss"))
        .withColumn(
            "dateecheance",
            date_format(to_date(col("dtfdp_22"), "yyyyMMdd"), "yyyy-MM-dd"),
        )
        .withColumn("code_situation_dossier", trim("cdsitdos_22"))
        .withColumn("montantecheance", col("mtcrpac_22").cast("float"))
        .withColumn("montant_cumule_debloque", col("mttdbl_22"))
        .withColumn("nominal", col("cvnominal_22").cast("float"))
        .withColumn("taux", trim("txintc_22"))
        .withColumn("restdu", trim("crdu_22"))
        .select(
            "numero_dossier",
            "code_situation_dossier",
            "montantecheance",
            "taux",
            "dateecheance",
            "nominal",
            "montant_cumule_debloque",
            "restdu",
        )
    )


def select_l2do_dcomm(l2do_dcomm: DataFrame) -> DataFrame:
    return (
        l2do_dcomm.withColumn("numero_dossier", trim("nodoss"))
        .withColumn("code_categorie_pret", trim("ctdoss_13"))
        .withColumn("type_pret", trim("typret_13"))
        .select(
            "numero_dossier",
            "code_categorie_pret",
            "type_pret",
        )
    )


def select_l2ei_echeanche(l2ei_echeanche: DataFrame) -> DataFrame:
    return (
        l2ei_echeanche.withColumn("numero_dossier", trim("nodoss"))
        .groupBy("numero_dossier")
        .agg(count(col("numero_dossier")).alias("nombreimpaye"))
        .select("numero_dossier", "nombreimpaye")
    )


def select_ressource_emploi(
    ressource_emploi: DataFrame, compte: DataFrame, map_ressource_emploi: DataFrame
) -> DataFrame:
    join_clause = (
        ressource_emploi["fk_mapressourceemploi"]
        == map_ressource_emploi["pk_mapressourceemploi"]
    )

    map_ressource_emploi = map_ressource_emploi.select(
        "pk_mapressourceemploi", "famille_tdb_banque"
    )
    compte = compte.select("numerocompteorigine", "pk_compte")

    rsemploi_filtered = (
        ressource_emploi.withColumn("datesolde_j_1", col("partitiondate"))
        .join(map_ressource_emploi, join_clause, "left")
        .filter(col("famille_tdb_banque").isin("Consommation", "Immobilier"))
    )

    join_compte = rsemploi_filtered["fk_compte"] == compte["pk_compte"]

    resource_emploi_processed = (
        rsemploi_filtered.join(compte, join_compte, "left")
        .groupBy("numerocompteorigine", "datesolde_j_1", "famille_tdb_banque")
        .agg(sum(col("soldejourdb_devise")).alias("encours_conso"))
    )

    return resource_emploi_processed


def credit_ls_transformation(
    l2do_dgen: DataFrame,
    l2do_dlanc: DataFrame,
    l2do_tfonc: DataFrame,
    l2do_dcomm: DataFrame,
    l2hd_debl: DataFrame,
    l2ei_echeanche: DataFrame,
    ressource_emploi: DataFrame,
    compte: DataFrame,
    produit: DataFrame,
    joursferie: DataFrame,
    partitiondate: str,
    name: str = "production",
) -> DataFrame:
    l2do_dgen = l2do_dgen.transform(select_l2do_dgen)
    l2do_dlanc = select_l2do_dlanc(l2do_dlanc, joursferie, partitiondate)
    l2do_tfonc = l2do_tfonc.transform(select_l2do_tfonc)
    l2do_dcomm = l2do_dcomm.transform(select_l2do_dcomm)
    l2hd_debl = l2hd_debl.transform(select_l2hd_debl, joursferie, partitiondate)
    l2ei_echeanche = l2ei_echeanche.transform(select_l2ei_echeanche)
    compte = compte.select("numerocompteorigine", "numerotiers").dropDuplicates()
    produit = produit.withColumn(
        "famille_du_pret",
        when(col("famille_du_pret") == "CREDIT IMMOBILIER", lit("Immobilier"))
        .when(col("famille_du_pret") == "CREDIT CONSOMMATION", lit("Consommation"))
        .otherwise(col("famille_du_pret")),
    )

    clause_ressource_join = (
        (ressource_emploi["numerocompteorigine"] == l2do_dgen["numerodecompte"])
        & (ressource_emploi["datesolde_j_1"] == l2hd_debl["datedeblocage_1"])
        & (ressource_emploi["famille_tdb_banque"] == produit["famille_du_pret"])
    )

    clause_join_produit = (
        l2do_dlanc["code_produit"] == produit["code_produit_prets"]
    ) & (l2do_dcomm["code_categorie_pret"] == produit["code_categorie_du_pret"])
    return (
        l2do_dgen.join(l2do_dlanc, ["numero_dossier"])
        .join(l2do_tfonc, ["numero_dossier"], "left")
        .join(l2do_dcomm, ["numero_dossier"], "left")
        .join(l2hd_debl, ["numero_dossier"], "left")
        .join(l2ei_echeanche, ["numero_dossier"], "left")
        .join(produit, clause_join_produit, "left")
        .join(ressource_emploi, clause_ressource_join, "left")
        .join(
            compte,
            compte["numerocompteorigine"] == l2do_dgen["numerodecompte"],
            "left",
        )
        .withColumn("etat", lit("-1"))
        .withColumn("numero_dossier", trim(col("numero_dossier")))
        .withColumn("code_source", lit("1"))
        .withColumn(
            "pk_numerodossier",
            concat_ws("", col("numero_dossier"), col("code_source")),
        )
        .withColumn("code_produit", coalesce(trim("code_produit"), lit("INC")))
        .withColumn("code_categorie_pret", trim(col("code_categorie_pret")))
        .withColumn(
            "fk_produit",
            crc32(concat_ws("", col("code_categorie_pret"), col("code_produit"))),
        )
        .withColumn(
            "PretCreditNet",
            (col("PretCreditBrute") - coalesce(col("encours_conso"), lit(0))).cast(
                "float"
            ),
        )
        .transform(
            get_is_during_a_period_for_equipement,
            by="datedeblocage",
            name=name,
            partition_date=partitiondate,
        )
        .select(
            "pk_numerodossier",
            "code_source",
            "numero_dossier",
            "numerodecompte",
            "numerotiers",
            "PretCreditBrute",
            "PretCreditNet",
            "datedeblocage",
            "dateecheance",
            "montantecheance",
            "duree_dossier",
            "code_categorie_pret",
            "code_produit",
            "code_situation_dossier",
            "restdu",
            "taux",
            "nombreimpaye",
            "situationpret",
            "etat",
            "fk_produit",
            "nominal",
            "montant_cumule_debloque",
            f"is_{name}_s",
            f"is_{name}_s_1",
            f"is_{name}_m",
            f"is_{name}_m_1",
            f"is_{name}_t",
            f"is_{name}_y",
        )
    )


def credit_wafasalaf_transformation(
    deblocage_oav: DataFrame,
    compte: DataFrame,
    partitiondate: str,
    name: str = "production",
) -> DataFrame:
    compte = compte.select("numerocompteorigine", "numerotiers").dropDuplicates()
    window = Window.partitionBy("numero_dossier")
    deblocage_oav_processed = (
        deblocage_oav.filter(
            (col("datedeblocage").isNotNull()) & (col("etat") == "1")
        )
        .withColumn("numero_dossier", trim("numaffaire"))
        .withColumn("dtdeblocage_max", max(col("datedeblocage")).over(window))
        .filter(col("datedeblocage") == col("dtdeblocage_max"))
        .withColumn(
            "datedeblocage",
            date_format(col("datedeblocage"), "yyyy-MM-dd"),
        )
        .withColumn("numerodecompte", substring(col("rib"), 11, 11))
        .withColumn("code_produit", lit("INC"))
        .withColumn("duree_dossier", trim("dureepret"))
        .withColumn("code_categorie_pret", trim("categoriepret"))
        .withColumn(
            "fk_produit",
            crc32(concat_ws("", col("code_categorie_pret"), col("code_produit"))),
        )
        .withColumn("PretCreditBrute", col("montantpret").cast("float"))
        .withColumn("mt_cred_net", col("montantaffairerat1").cast("float"))
        .withColumn(
            "PretCreditNet",
            col("PretCreditBrute") - coalesce(col("mt_cred_net"), lit(0)),
        )
        .withColumn("code_situation_dossier", lit("-1"))
        .withColumn("code_source", lit("2"))
        .withColumn("restdu", lit("-1"))
        .withColumn("taux", lit("-1"))
        .withColumn("nombreimpaye", lit(-1))
        .withColumn("situationpret", lit("ACCORDE"))
        .withColumn("dateecheance", lit("-1"))
        .withColumn("montantecheance", lit(-1))
        .withColumn(
            "pk_numerodossier",
            concat_ws("", col("numero_dossier"), col("code_source")),
        )
        .withColumn("nominal", lit(-1))
        .withColumn("montant_cumule_debloque", lit("-1"))
        .select(
            col("datedeblocage"),
            col("numero_dossier"),
            col("numerodecompte"),
            col("code_produit"),
            col("duree_dossier"),
            col("code_categorie_pret"),
            col("fk_produit"),
            col("PretCreditBrute"),
            col("mt_cred_net"),
            col("PretCreditNet"),
            col("code_situation_dossier"),
            col("code_source"),
            col("pk_numerodossier"),
            col("restdu"),
            col("taux"),
            col("nombreimpaye"),
            col("situationpret"),
            col("etat"),
            col("dateecheance"),
            col("montantecheance"),
            col("nominal"),
            col("montant_cumule_debloque")
        )
    )

    wafasalaf_input_columns = (
        deblocage_oav_processed.transform(
            get_is_during_a_period_for_equipement,
            by="datedeblocage",
            name=name,
            partition_date=partitiondate,
        )
        .join(
            compte,
            compte["numerocompteorigine"]
            == deblocage_oav_processed["numerodecompte"],
            "left",
        )
        .select(
            "pk_numerodossier",
            "code_source",
            "numero_dossier",
            "numerodecompte",
            "numerotiers",
            "PretCreditBrute",
            "PretCreditNet",
            "datedeblocage",
            "dateecheance",
            "montantecheance",
            "duree_dossier",
            "code_categorie_pret",
            "code_produit",
            "code_situation_dossier",
            "restdu",
            "taux",
            "nombreimpaye",
            "situationpret",
            "etat",
            "fk_produit",
            "nominal",
            "montant_cumule_debloque",
            f"is_{name}_s",
            f"is_{name}_s_1",
            f"is_{name}_m",
            f"is_{name}_m_1",
            f"is_{name}_t",
            f"is_{name}_y",
        )
    )
    return wafasalaf_input_columns


def produit_transformation(
    produit: DataFrame, produit_credit: DataFrame
) -> DataFrame:
    join_clause = ["code_produit"]
    produit_processed = (
        produit.withColumn(
            "code_produit", coalesce(col("code_produit_prets"), lit("INC"))
        )
        .join(produit_credit, join_clause, "left")
        .withColumn(
            "pk_produit",
            crc32(concat_ws("", col("code_categorie_du_pret"), col("code_produit"))),
        )
        .select(
            "code_categorie_du_pret",
            "code_produit",
            "libelle_produit",
            "pk_produit",
            "categorie_du_pret",
            "famille_du_pret",
        )
    )
    return produit_processed
