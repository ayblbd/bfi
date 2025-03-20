from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    collect_set,
    concat_ws,
    date_add,
    date_format,
    lag,
    last,
    lit,
    max,
    month,
    quarter,
    round,
    size,
    substring,
    sum,
    to_date,
    trim,
    weekofyear,
    when,
    year,
)


def select_information_tier(information_tier: DataFrame) -> DataFrame:
    window = Window.partitionBy("cd_fdc_gest")

    return (
        information_tier.withColumn(
            "total_tier_par_fdc",
            size(collect_set((col("numerotiers"))).over(window)),
        )
        .withColumn(
            "total_tier_actif_par_fdc",
            size(
                collect_set(
                    when(col("statutclient") == "ACTIF", col("numerotiers"))
                ).over(window)
            ),
        )
        .select(
            "numerotiers",
            "marche",
            col("agencegest").alias("codeagence"),
            col("cd_fdc_gest").alias("fdc_gest"),
            "new_seg",
            "libellesegment",
            "new_sous_seg",
            "libellesoussegment",
            "libelle_marche",
            "gamme",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
        )
    )


def select_information_compte_client(
    information_compte_client: DataFrame,
) -> DataFrame:
    return information_compte_client.select(
        "numerotiers", "numerodecompte", col("cd_fdc_gest").alias("fdc_gest_compte")
    )


def select_niveau_regroupement(niveau_regroupement: DataFrame) -> DataFrame:
    return niveau_regroupement.select(
        "codeagence",
        "nomagence",
        "succursale",
        "nomsuccursale",
        "coderegion",
        "codebanque",
        "nomregion",
        "nombanque",
    ).drop_duplicates()


def get_last_year_csc_etat_produit(df):
    window = Window.partitionBy("numerodecompte").orderBy("partitiondate")

    return (
        df.withColumn("rank", date_format(col("partitiondate"), "yyyy"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_target", lag(col("etatcompte")).over(window))
        .withColumn(
            "annee_prec_etat_compte",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "etatproduit_annee_prec",
            when(col("annee_prec_etat_compte").isNull(), None)
            .when(col("annee_prec_etat_compte").isin(["0", "1"]), "VALIDE")
            .otherwise("RESILIE"),
        )
        .drop("rank", "prev_target", "rank_diff", "annee_prec_etat_compte")
    )


def get_last_week_csc_etat_produit(df):
    window = Window.partitionBy("numerodecompte").orderBy("partitiondate")
    return (
        df.withColumn(
            "rank",
            concat_ws(
                "",
                date_format(col("partitiondate"), "yyyy"),
                weekofyear(to_date(col("partitiondate"))),
            ),
        )
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_target", lag(col("etatcompte")).over(window))
        .withColumn(
            "semaine_prec_etat_compte",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "etatproduit_semaine_prec",
            when(col("semaine_prec_etat_compte").isNull(), None)
            .when(col("semaine_prec_etat_compte").isin(["0", "1"]), "VALIDE")
            .otherwise("RESILIE"),
        )
        .drop("rank", "prev_target", "rank_diff", "semaine_prec_etat_compte")
    )


def get_last_month_csc_etat_produit(df):
    window = Window.partitionBy("numerodecompte").orderBy("partitiondate")

    return (
        df.withColumn("rank", date_format(col("partitiondate"), "yyyyMM"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_target", lag(col("etatcompte")).over(window))
        .withColumn(
            "mois_prec_etat_compte",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "etatproduit_mois_prec",
            when(col("mois_prec_etat_compte").isNull(), None)
            .when(col("mois_prec_etat_compte").isin(["0", "1"]), "VALIDE")
            .otherwise("RESILIE"),
        )
        .drop("rank", "prev_target", "rank_diff", "mois_prec_etat_compte")
    )


def get_last_year_contrat_etat_produit(df):
    window = Window.partitionBy("numerocontratinterne").orderBy("partitiondate")

    return (
        df.withColumn("rank", date_format(col("partitiondate"), "yyyy"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_target", lag(col("etatcontrat")).over(window))
        .withColumn(
            "annee_prec_etat_contrat",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "etatproduit_annee_prec",
            when(
                col("annee_prec_etat_contrat").isin(
                    ["3", "7", "8", "D", "E", "S", "X"]
                ),
                "VALIDE",
            )
            .when(col("annee_prec_etat_contrat") == "5", "RESILIE")
            .when(col("annee_prec_etat_contrat") == "2", "ECHUE")
            .when(col("annee_prec_etat_contrat").isin(["1", "A", "C"]), "NONVALIDE")
            .otherwise(None),
        )
        .drop("rank", "prev_target", "rank_diff", "annee_prec_etat_contrat")
    )


def get_last_month_contrat_etat_produit(df):
    window = Window.partitionBy("numerocontratinterne").orderBy("partitiondate")

    return (
        df.withColumn("rank", date_format(col("partitiondate"), "yyyyMM"))
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_target", lag(col("etatcontrat")).over(window))
        .withColumn(
            "mois_prec_etat_contrat",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "etatproduit_mois_prec",
            when(
                col("mois_prec_etat_contrat").isin(
                    ["3", "7", "8", "D", "E", "S", "X"]
                ),
                "VALIDE",
            )
            .when(col("mois_prec_etat_contrat") == "5", "RESILIE")
            .when(col("mois_prec_etat_contrat") == "2", "ECHUE")
            .when(col("mois_prec_etat_contrat").isin(["1", "A", "C"]), "NONVALIDE")
            .otherwise(None),
        )
        .drop("rank", "prev_target", "rank_diff", "mois_prec_etat_contrat")
    )


def get_last_week_contrat_etat_produit(df):
    window = Window.partitionBy("numerocontratinterne").orderBy("partitiondate")
    return (
        df.withColumn(
            "rank",
            concat_ws(
                "",
                date_format(col("partitiondate"), "yyyy"),
                weekofyear(to_date(col("partitiondate"))),
            ),
        )
        .withColumn("rank_diff", lag("rank").over(window))
        .withColumn("prev_target", lag(col("etatcontrat")).over(window))
        .withColumn(
            "semaine_prec_etat_contrat",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .withColumn(
            "etatproduit_semaine_prec",
            when(
                col("semaine_prec_etat_contrat").isin(
                    ["3", "7", "8", "D", "E", "S", "X"]
                ),
                "VALIDE",
            )
            .when(col("semaine_prec_etat_contrat") == "5", "RESILIE")
            .when(col("semaine_prec_etat_contrat") == "2", "ECHUE")
            .when(
                col("semaine_prec_etat_contrat").isin(["1", "A", "C"]), "NONVALIDE"
            )
            .otherwise(None),
        )
        .drop("rank", "prev_target", "rank_diff", "semaine_prec_etat_contrat")
    )


def get_contrat_history(df):
    return (
        df.select("numerocontratinterne", "etatcontrat", "partitiondate")
        .transform(get_last_week_contrat_etat_produit)
        .transform(get_last_month_contrat_etat_produit)
        .transform(get_last_year_contrat_etat_produit)
        .cache()
    )


def build_objectifs_equipement(
    objectifs_tro: DataFrame, saisonalite: DataFrame
) -> DataFrame:
    objectifs_tro = objectifs_tro.select(
        "code_region",
        "code_groupe",
        "code_agence",
        "code_emploi",
        "code_fdc",
        "cartes",
        "delta_encours_carte",
        "master_card",
        "cdmpass",
        "visa_infinite",
        "titanum",
        "cdm_silver",
        "platinum",
        "cdmtawfir",
        "ebuy",
        "business_pro",
        "business_pro_premium",
        "total_assurances",
        "assurance_delta_encours",
        "lam",
        "isaaf",
        "lsm",
        "lsi",
        "liberis_education",
        "liberis_compte",
        "liberis_carte",
        "liberis_vie",
        "liberis_habitation",
        "lpa",
        "liberis_pro",
        "assurance_vie",
        "assurance_vie_delta_encours",
        "liberis_retraite",
        "liberis_patrimoine",
        "liberis_epargne",
        "digital",
        "delta_encours_digital",
        "epargnes",
        "delta_encours_epargnes",
    )

    result = objectifs_tro.crossJoin(saisonalite)

    window_spec_mois = Window.partitionBy(
        "code_region",
        "code_groupe",
        "code_agence",
        "code_emploi",
        "code_fdc",
        "annee",
        "mois",
    )
    window_spec_trimestre = Window.partitionBy(
        "code_region",
        "code_groupe",
        "code_agence",
        "code_emploi",
        "code_fdc",
        "annee",
        "trimestre",
    )

    objectifs_equipement_mois_trimestre = (
        result.withColumn("numero_semaine", col("numero_semaine").cast("int"))
        .withColumn("pourc_semaine", col("pourc_semaine").cast("float"))
        .withColumn(
            "trimestre",
            when(col("numero_semaine").between(1, 13), 1)
            .when(col("numero_semaine").between(14, 26), 2)
            .when(col("numero_semaine").between(27, 39), 3)
            .when(col("numero_semaine").between(40, 53), 4)
            .otherwise(None),
        )
        .withColumn(
            "mois",
            when(col("numero_semaine").between(1, 4), 1)
            .when(col("numero_semaine").between(5, 8), 2)
            .when(col("numero_semaine").between(9, 13), 3)
            .when(col("numero_semaine").between(14, 17), 4)
            .when(col("numero_semaine").between(18, 21), 5)
            .when(col("numero_semaine").between(22, 26), 6)
            .when(col("numero_semaine").between(27, 30), 7)
            .when(col("numero_semaine").between(31, 35), 8)
            .when(col("numero_semaine").between(36, 39), 9)
            .when(col("numero_semaine").between(40, 43), 10)
            .when(col("numero_semaine").between(44, 48), 11)
            .when(col("numero_semaine").between(49, 53), 12)
            .otherwise(None),
        )
    )

    objectifs_equipement_cartes = (
        objectifs_equipement_mois_trimestre.withColumn(
            "objectif_cartes_annuel", round(col("cartes").cast("float"), 2)
        )
        .withColumn(
            "objectif_cartes_hebdo",
            round(col("objectif_cartes_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_cartes_mensuel",
            round(sum(col("objectif_cartes_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_cartes_trimestre",
            round(sum(col("objectif_cartes_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_delta_encours_cartes_annuel",
            round(col("delta_encours_carte").cast("float"), 2),
        )
        .withColumn(
            "objectif_delta_encours_cartes_hebdo",
            round(
                col("objectif_delta_encours_cartes_annuel") * col("pourc_semaine"), 2
            ),
        )
        .withColumn(
            "objectif_delta_encours_cartes_mensuel",
            round(
                sum(col("objectif_delta_encours_cartes_hebdo")).over(
                    window_spec_mois
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_cartes_trimestre",
            round(
                sum(col("objectif_delta_encours_cartes_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_master_card_annuel", round(col("master_card").cast("float"), 2)
        )
        .withColumn(
            "objectif_master_card_hebdo",
            round(col("objectif_master_card_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_master_card_mensuel",
            round(sum(col("objectif_master_card_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_master_card_trimestre",
            round(
                sum(col("objectif_master_card_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_cdmpass_annuel", round(col("cdmpass").cast("float"), 2)
        )
        .withColumn(
            "objectif_cdmpass_hebdo",
            round(col("objectif_cdmpass_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_cdmpass_mensuel",
            round(sum(col("objectif_cdmpass_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_cdmpass_trimestre",
            round(sum(col("objectif_cdmpass_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_visa_infinite_annuel",
            round(col("visa_infinite").cast("float"), 2),
        )
        .withColumn(
            "objectif_visa_infinite_hebdo",
            round(col("objectif_visa_infinite_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_visa_infinite_mensuel",
            round(
                sum(col("objectif_visa_infinite_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_visa_infinite_trimestre",
            round(
                sum(col("objectif_visa_infinite_hebdo")).over(window_spec_trimestre),
                2,
            ),
        )
        .withColumn(
            "objectif_titanum_annuel", round(col("titanum").cast("float"), 2)
        )
        .withColumn(
            "objectif_titanum_hebdo",
            round(col("objectif_titanum_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_titanum_mensuel",
            round(sum(col("objectif_titanum_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_titanum_trimestre",
            round(sum(col("objectif_titanum_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_cdm_silver_annuel", round(col("cdm_silver").cast("float"), 2)
        )
        .withColumn(
            "objectif_cdm_silver_hebdo",
            round(col("objectif_cdm_silver_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_cdm_silver_mensuel",
            round(sum(col("objectif_cdm_silver_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_cdm_silver_trimestre",
            round(
                sum(col("objectif_cdm_silver_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_platinum_annuel", round(col("platinum").cast("float"), 2)
        )
        .withColumn(
            "objectif_platinum_hebdo",
            round(col("objectif_platinum_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_platinum_mensuel",
            round(sum(col("objectif_platinum_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_platinum_trimestre",
            round(
                sum(col("objectif_platinum_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_cdmtawfir_annuel", round(col("cdmtawfir").cast("float"), 2)
        )
        .withColumn(
            "objectif_cdmtawfir_hebdo",
            round(col("objectif_cdmtawfir_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_cdmtawfir_mensuel",
            round(sum(col("objectif_cdmtawfir_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_cdmtawfir_trimestre",
            round(
                sum(col("objectif_cdmtawfir_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn("objectif_ebuy_annuel", round(col("ebuy").cast("float"), 2))
        .withColumn(
            "objectif_ebuy_hebdo",
            round(col("objectif_ebuy_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_ebuy_mensuel",
            round(sum(col("objectif_ebuy_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_ebuy_trimestre",
            round(sum(col("objectif_ebuy_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_business_pro_annuel",
            round(col("business_pro").cast("float"), 2),
        )
        .withColumn(
            "objectif_business_pro_hebdo",
            round(col("objectif_business_pro_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_business_pro_mensuel",
            round(sum(col("objectif_business_pro_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_business_pro_trimestre",
            round(
                sum(col("objectif_business_pro_hebdo")).over(window_spec_trimestre),
                2,
            ),
        )
        .withColumn(
            "objectif_business_pro_premium_annuel",
            round(col("business_pro_premium").cast("float"), 2),
        )
        .withColumn(
            "objectif_business_pro_premium_hebdo",
            round(
                col("objectif_business_pro_premium_annuel") * col("pourc_semaine"), 2
            ),
        )
        .withColumn(
            "objectif_business_pro_premium_mensuel",
            round(
                sum(col("objectif_business_pro_premium_hebdo")).over(
                    window_spec_mois
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_business_pro_premium_trimestre",
            round(
                sum(col("objectif_business_pro_premium_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_assurances_annuel",
            round(col("total_assurances").cast("float"), 2),
        )
        .withColumn(
            "objectif_assurances_hebdo",
            round(col("objectif_assurances_annuel") * col("pourc_semaine"), 2),
        )
    )

    objectifs_equipement = (
        objectifs_equipement_cartes.withColumn(
            "objectif_assurances_mensuel",
            round(sum(col("objectif_assurances_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_assurances_trimestre",
            round(
                sum(col("objectif_assurances_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_delta_encours_assurances_annuel",
            round(col("assurance_delta_encours").cast("float"), 2),
        )
        .withColumn(
            "objectif_delta_encours_assurances_hebdo",
            round(
                col("objectif_delta_encours_assurances_annuel")
                * col("pourc_semaine"),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_assurances_mensuel",
            round(
                sum(col("objectif_delta_encours_assurances_hebdo")).over(
                    window_spec_mois
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_assurances_trimestre",
            round(
                sum(col("objectif_delta_encours_assurances_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn("objectif_lam_annuel", round(col("lam").cast("float"), 2))
        .withColumn(
            "objectif_lam_hebdo",
            round(col("objectif_lam_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_lam_mensuel",
            round(sum(col("objectif_lam_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_lam_trimestre",
            round(sum(col("objectif_lam_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn("objectif_isaaf_annuel", round(col("isaaf").cast("float"), 2))
        .withColumn(
            "objectif_isaaf_hebdo",
            round(col("objectif_isaaf_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_isaaf_mensuel",
            round(sum(col("objectif_isaaf_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_isaaf_trimestre",
            round(sum(col("objectif_isaaf_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn("objectif_lsm_annuel", round(col("lsm").cast("float"), 2))
        .withColumn(
            "objectif_lsm_hebdo",
            round(col("objectif_lsm_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_lsm_mensuel",
            round(sum(col("objectif_lsm_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_lsm_trimestre",
            round(sum(col("objectif_lsm_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn("objectif_lsi_annuel", round(col("lsi").cast("float"), 2))
        .withColumn(
            "objectif_lsi_hebdo",
            round(col("objectif_lsi_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_lsi_mensuel",
            round(sum(col("objectif_lsi_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_lsi_trimestre",
            round(sum(col("objectif_lsi_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_liberis_compte_annuel",
            round(col("liberis_compte").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_compte_hebdo",
            round(col("objectif_liberis_compte_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_liberis_compte_mensuel",
            round(
                sum(col("objectif_liberis_compte_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_liberis_compte_trimestre",
            round(
                sum(col("objectif_liberis_compte_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_vie_annuel", round(col("liberis_vie").cast("float"), 2)
        )
        .withColumn(
            "objectif_liberis_vie_hebdo",
            round(col("objectif_liberis_vie_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_liberis_vie_mensuel",
            round(sum(col("objectif_liberis_vie_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_liberis_vie_trimestre",
            round(
                sum(col("objectif_liberis_vie_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_liberis_carte_annuel",
            round(col("liberis_carte").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_carte_hebdo",
            round(col("objectif_liberis_carte_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_liberis_carte_mensuel",
            round(
                sum(col("objectif_liberis_carte_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_liberis_carte_trimestre",
            round(
                sum(col("objectif_liberis_carte_hebdo")).over(window_spec_trimestre),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_habitation_annuel",
            round(col("liberis_habitation").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_habitation_hebdo",
            round(
                col("objectif_liberis_habitation_annuel") * col("pourc_semaine"), 2
            ),
        )
        .withColumn(
            "objectif_liberis_habitation_mensuel",
            round(
                sum(col("objectif_liberis_habitation_hebdo")).over(window_spec_mois),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_habitation_trimestre",
            round(
                sum(col("objectif_liberis_habitation_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn("objectif_lpa_annuel", round(col("lpa").cast("float"), 2))
        .withColumn(
            "objectif_lpa_hebdo",
            round(col("objectif_lpa_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_lpa_mensuel",
            round(sum(col("objectif_lpa_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_lpa_trimestre",
            round(sum(col("objectif_lpa_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_liberis_pro_annuel", round(col("liberis_pro").cast("float"), 2)
        )
        .withColumn(
            "objectif_liberis_pro_hebdo",
            round(col("objectif_liberis_pro_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_liberis_pro_mensuel",
            round(sum(col("objectif_liberis_pro_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_liberis_pro_trimestre",
            round(
                sum(col("objectif_liberis_pro_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_assurance_vie_annuel",
            round(col("assurance_vie").cast("float"), 2),
        )
        .withColumn(
            "objectif_assurance_vie_hebdo",
            round(col("objectif_assurance_vie_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_assurance_vie_mensuel",
            round(
                sum(col("objectif_assurance_vie_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_assurance_vie_trimestre",
            round(
                sum(col("objectif_assurance_vie_hebdo")).over(window_spec_trimestre),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_assurance_vie_annuel",
            round(col("assurance_vie_delta_encours").cast("float"), 2),
        )
        .withColumn(
            "objectif_delta_encours_assurance_vie_hebdo",
            round(
                col("objectif_delta_encours_assurance_vie_annuel")
                * col("pourc_semaine"),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_assurance_vie_mensuel",
            round(
                sum(col("objectif_delta_encours_assurance_vie_hebdo")).over(
                    window_spec_mois
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_assurance_vie_trimestre",
            round(
                sum(col("objectif_delta_encours_assurance_vie_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_retraite_annuel",
            round(col("liberis_retraite").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_retraite_hebdo",
            round(col("objectif_liberis_retraite_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_liberis_retraite_mensuel",
            round(
                sum(col("objectif_liberis_retraite_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_liberis_retraite_trimestre",
            round(
                sum(col("objectif_liberis_retraite_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_education_annuel",
            round(col("liberis_education").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_education_hebdo",
            round(
                col("objectif_liberis_education_annuel") * col("pourc_semaine"), 2
            ),
        )
        .withColumn(
            "objectif_liberis_education_mensuel",
            round(
                sum(col("objectif_liberis_education_hebdo")).over(window_spec_mois),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_education_trimestre",
            round(
                sum(col("objectif_liberis_education_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_patrimoine_annuel",
            round(col("liberis_patrimoine").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_patrimoine_hebdo",
            round(
                col("objectif_liberis_patrimoine_annuel") * col("pourc_semaine"), 2
            ),
        )
        .withColumn(
            "objectif_liberis_patrimoine_mensuel",
            round(
                sum(col("objectif_liberis_patrimoine_hebdo")).over(window_spec_mois),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_patrimoine_trimestre",
            round(
                sum(col("objectif_liberis_patrimoine_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_liberis_epargne_annuel",
            round(col("liberis_epargne").cast("float"), 2),
        )
        .withColumn(
            "objectif_liberis_epargne_hebdo",
            round(col("objectif_liberis_epargne_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_liberis_epargne_mensuel",
            round(
                sum(col("objectif_liberis_epargne_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_liberis_epargne_trimestre",
            round(
                sum(col("objectif_liberis_epargne_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_digital_annuel", round(col("digital").cast("float"), 2)
        )
        .withColumn(
            "objectif_digital_hebdo",
            round(col("objectif_digital_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_digital_mensuel",
            round(sum(col("objectif_digital_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_digital_trimestre",
            round(sum(col("objectif_digital_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_delta_encours_digital_annuel",
            round(col("delta_encours_digital").cast("float"), 2),
        )
        .withColumn(
            "objectif_delta_encours_digital_hebdo",
            round(
                col("objectif_delta_encours_digital_annuel") * col("pourc_semaine"),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_digital_mensuel",
            round(
                sum(col("objectif_delta_encours_digital_hebdo")).over(
                    window_spec_mois
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_digital_trimestre",
            round(
                sum(col("objectif_delta_encours_digital_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_epargnes_annuel", round(col("epargnes").cast("float"), 2)
        )
        .withColumn(
            "objectif_epargnes_hebdo",
            round(col("objectif_epargnes_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_epargnes_mensuel",
            round(sum(col("objectif_epargnes_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_epargnes_trimestre",
            round(
                sum(col("objectif_epargnes_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_delta_encours_epargnes_annuel",
            round(col("delta_encours_epargnes").cast("float"), 2),
        )
        .withColumn(
            "objectif_delta_encours_epargnes_hebdo",
            round(
                col("objectif_delta_encours_epargnes_annuel") * col("pourc_semaine"),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_epargnes_mensuel",
            round(
                sum(col("objectif_delta_encours_epargnes_hebdo")).over(
                    window_spec_mois
                ),
                2,
            ),
        )
        .withColumn(
            "objectif_delta_encours_epargnes_trimestre",
            round(
                sum(col("objectif_delta_encours_epargnes_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .withColumnRenamed("code_fdc", "fdc_gest")
        .withColumnRenamed("numero_semaine", "semaine")
    )

    return objectifs_equipement.select(
        "code_region",
        "code_groupe",
        "code_agence",
        "code_emploi",
        "fdc_gest",
        "semaine",
        "mois",
        "trimestre",
        "annee",
        "dds",
        "dfs",
        "pourc_semaine",
        "pourc_cumul",
        "objectif_cartes_annuel",
        "objectif_cartes_hebdo",
        "objectif_cartes_mensuel",
        "objectif_cartes_trimestre",
        "objectif_delta_encours_cartes_annuel",
        "objectif_delta_encours_cartes_hebdo",
        "objectif_delta_encours_cartes_mensuel",
        "objectif_delta_encours_cartes_trimestre",
        "objectif_master_card_annuel",
        "objectif_master_card_hebdo",
        "objectif_master_card_mensuel",
        "objectif_master_card_trimestre",
        "objectif_cdmpass_annuel",
        "objectif_cdmpass_hebdo",
        "objectif_cdmpass_mensuel",
        "objectif_cdmpass_trimestre",
        "objectif_visa_infinite_annuel",
        "objectif_visa_infinite_hebdo",
        "objectif_visa_infinite_mensuel",
        "objectif_visa_infinite_trimestre",
        "objectif_titanum_annuel",
        "objectif_titanum_hebdo",
        "objectif_titanum_mensuel",
        "objectif_titanum_trimestre",
        "objectif_cdm_silver_annuel",
        "objectif_cdm_silver_hebdo",
        "objectif_cdm_silver_mensuel",
        "objectif_cdm_silver_trimestre",
        "objectif_platinum_annuel",
        "objectif_platinum_hebdo",
        "objectif_platinum_mensuel",
        "objectif_platinum_trimestre",
        "objectif_cdmtawfir_annuel",
        "objectif_cdmtawfir_hebdo",
        "objectif_cdmtawfir_mensuel",
        "objectif_cdmtawfir_trimestre",
        "objectif_ebuy_annuel",
        "objectif_ebuy_hebdo",
        "objectif_ebuy_mensuel",
        "objectif_ebuy_trimestre",
        "objectif_business_pro_annuel",
        "objectif_business_pro_hebdo",
        "objectif_business_pro_mensuel",
        "objectif_business_pro_trimestre",
        "objectif_business_pro_premium_annuel",
        "objectif_business_pro_premium_hebdo",
        "objectif_business_pro_premium_mensuel",
        "objectif_business_pro_premium_trimestre",
        "objectif_assurances_annuel",
        "objectif_assurances_hebdo",
        "objectif_assurances_mensuel",
        "objectif_assurances_trimestre",
        "objectif_delta_encours_assurances_annuel",
        "objectif_delta_encours_assurances_hebdo",
        "objectif_delta_encours_assurances_mensuel",
        "objectif_delta_encours_assurances_trimestre",
        "objectif_lam_annuel",
        "objectif_lam_hebdo",
        "objectif_lam_mensuel",
        "objectif_lam_trimestre",
        "objectif_isaaf_annuel",
        "objectif_isaaf_hebdo",
        "objectif_isaaf_mensuel",
        "objectif_isaaf_trimestre",
        "objectif_lsm_annuel",
        "objectif_lsm_hebdo",
        "objectif_lsm_mensuel",
        "objectif_lsm_trimestre",
        "objectif_lsi_annuel",
        "objectif_lsi_hebdo",
        "objectif_lsi_mensuel",
        "objectif_lsi_trimestre",
        "objectif_liberis_compte_annuel",
        "objectif_liberis_compte_hebdo",
        "objectif_liberis_compte_mensuel",
        "objectif_liberis_compte_trimestre",
        "objectif_liberis_vie_annuel",
        "objectif_liberis_vie_hebdo",
        "objectif_liberis_vie_mensuel",
        "objectif_liberis_vie_trimestre",
        "objectif_liberis_carte_annuel",
        "objectif_liberis_carte_hebdo",
        "objectif_liberis_carte_mensuel",
        "objectif_liberis_carte_trimestre",
        "objectif_liberis_habitation_annuel",
        "objectif_liberis_habitation_hebdo",
        "objectif_liberis_habitation_mensuel",
        "objectif_liberis_habitation_trimestre",
        "objectif_lpa_annuel",
        "objectif_lpa_hebdo",
        "objectif_lpa_mensuel",
        "objectif_lpa_trimestre",
        "objectif_liberis_pro_annuel",
        "objectif_liberis_pro_hebdo",
        "objectif_liberis_pro_mensuel",
        "objectif_liberis_pro_trimestre",
        "objectif_assurance_vie_annuel",
        "objectif_assurance_vie_hebdo",
        "objectif_assurance_vie_mensuel",
        "objectif_assurance_vie_trimestre",
        "objectif_delta_encours_assurance_vie_annuel",
        "objectif_delta_encours_assurance_vie_hebdo",
        "objectif_delta_encours_assurance_vie_mensuel",
        "objectif_delta_encours_assurance_vie_trimestre",
        "objectif_liberis_retraite_annuel",
        "objectif_liberis_retraite_hebdo",
        "objectif_liberis_retraite_mensuel",
        "objectif_liberis_retraite_trimestre",
        "objectif_liberis_education_annuel",
        "objectif_liberis_education_hebdo",
        "objectif_liberis_education_mensuel",
        "objectif_liberis_education_trimestre",
        "objectif_liberis_patrimoine_annuel",
        "objectif_liberis_patrimoine_hebdo",
        "objectif_liberis_patrimoine_mensuel",
        "objectif_liberis_patrimoine_trimestre",
        "objectif_liberis_epargne_annuel",
        "objectif_liberis_epargne_hebdo",
        "objectif_liberis_epargne_mensuel",
        "objectif_liberis_epargne_trimestre",
        "objectif_digital_annuel",
        "objectif_digital_hebdo",
        "objectif_digital_mensuel",
        "objectif_digital_trimestre",
        "objectif_delta_encours_digital_annuel",
        "objectif_delta_encours_digital_hebdo",
        "objectif_delta_encours_digital_mensuel",
        "objectif_delta_encours_digital_trimestre",
        "objectif_epargnes_annuel",
        "objectif_epargnes_hebdo",
        "objectif_epargnes_mensuel",
        "objectif_epargnes_trimestre",
        "objectif_delta_encours_epargnes_annuel",
        "objectif_delta_encours_epargnes_hebdo",
        "objectif_delta_encours_epargnes_mensuel",
        "objectif_delta_encours_epargnes_trimestre",
    ).cache()


def select_objectifs_carte(df: DataFrame) -> DataFrame:
    return df.select(
        "fdc_gest",
        "annee",
        "dds",
        "dfs",
        "semaine",
        "objectif_cartes_annuel",
        "objectif_cartes_hebdo",
        "objectif_cartes_mensuel",
        "objectif_cartes_trimestre",
        "objectif_delta_encours_cartes_annuel",
        "objectif_delta_encours_cartes_hebdo",
        "objectif_delta_encours_cartes_mensuel",
        "objectif_delta_encours_cartes_trimestre",
        "objectif_master_card_annuel",
        "objectif_master_card_hebdo",
        "objectif_master_card_mensuel",
        "objectif_master_card_trimestre",
        "objectif_cdmpass_annuel",
        "objectif_cdmpass_hebdo",
        "objectif_cdmpass_mensuel",
        "objectif_cdmpass_trimestre",
        "objectif_visa_infinite_annuel",
        "objectif_visa_infinite_hebdo",
        "objectif_visa_infinite_mensuel",
        "objectif_visa_infinite_trimestre",
        "objectif_titanum_annuel",
        "objectif_titanum_hebdo",
        "objectif_titanum_mensuel",
        "objectif_titanum_trimestre",
        "objectif_cdm_silver_annuel",
        "objectif_cdm_silver_hebdo",
        "objectif_cdm_silver_mensuel",
        "objectif_cdm_silver_trimestre",
        "objectif_platinum_annuel",
        "objectif_platinum_hebdo",
        "objectif_platinum_mensuel",
        "objectif_platinum_trimestre",
        "objectif_cdmtawfir_annuel",
        "objectif_cdmtawfir_hebdo",
        "objectif_cdmtawfir_mensuel",
        "objectif_cdmtawfir_trimestre",
        "objectif_ebuy_annuel",
        "objectif_ebuy_hebdo",
        "objectif_ebuy_mensuel",
        "objectif_ebuy_trimestre",
        "objectif_business_pro_annuel",
        "objectif_business_pro_hebdo",
        "objectif_business_pro_mensuel",
        "objectif_business_pro_trimestre",
        "objectif_business_pro_premium_annuel",
        "objectif_business_pro_premium_hebdo",
        "objectif_business_pro_premium_mensuel",
        "objectif_business_pro_premium_trimestre",
    )


def select_objectifs_assurance(df: DataFrame) -> DataFrame:
    return df.select(
        "fdc_gest",
        "annee",
        "dds",
        "dfs",
        "semaine",
        "objectif_assurances_annuel",
        "objectif_assurances_hebdo",
        "objectif_assurances_mensuel",
        "objectif_assurances_trimestre",
        "objectif_delta_encours_assurances_annuel",
        "objectif_delta_encours_assurances_hebdo",
        "objectif_delta_encours_assurances_mensuel",
        "objectif_delta_encours_assurances_trimestre",
        "objectif_lam_annuel",
        "objectif_lam_hebdo",
        "objectif_lam_mensuel",
        "objectif_lam_trimestre",
        "objectif_isaaf_annuel",
        "objectif_isaaf_hebdo",
        "objectif_isaaf_mensuel",
        "objectif_isaaf_trimestre",
        "objectif_lsm_annuel",
        "objectif_lsm_hebdo",
        "objectif_lsm_mensuel",
        "objectif_lsm_trimestre",
        "objectif_lsi_annuel",
        "objectif_lsi_hebdo",
        "objectif_lsi_mensuel",
        "objectif_lsi_trimestre",
        "objectif_liberis_compte_annuel",
        "objectif_liberis_compte_hebdo",
        "objectif_liberis_compte_mensuel",
        "objectif_liberis_compte_trimestre",
        "objectif_liberis_vie_annuel",
        "objectif_liberis_vie_hebdo",
        "objectif_liberis_vie_mensuel",
        "objectif_liberis_vie_trimestre",
        "objectif_liberis_carte_annuel",
        "objectif_liberis_carte_hebdo",
        "objectif_liberis_carte_mensuel",
        "objectif_liberis_carte_trimestre",
        "objectif_liberis_habitation_annuel",
        "objectif_liberis_habitation_hebdo",
        "objectif_liberis_habitation_mensuel",
        "objectif_liberis_habitation_trimestre",
        "objectif_lpa_annuel",
        "objectif_lpa_hebdo",
        "objectif_lpa_mensuel",
        "objectif_lpa_trimestre",
        "objectif_liberis_pro_annuel",
        "objectif_liberis_pro_hebdo",
        "objectif_liberis_pro_mensuel",
        "objectif_liberis_pro_trimestre",
        "objectif_assurance_vie_annuel",
        "objectif_assurance_vie_hebdo",
        "objectif_assurance_vie_mensuel",
        "objectif_assurance_vie_trimestre",
        "objectif_delta_encours_assurance_vie_annuel",
        "objectif_delta_encours_assurance_vie_hebdo",
        "objectif_delta_encours_assurance_vie_mensuel",
        "objectif_delta_encours_assurance_vie_trimestre",
        "objectif_liberis_retraite_annuel",
        "objectif_liberis_retraite_hebdo",
        "objectif_liberis_retraite_mensuel",
        "objectif_liberis_retraite_trimestre",
        "objectif_liberis_education_annuel",
        "objectif_liberis_education_hebdo",
        "objectif_liberis_education_mensuel",
        "objectif_liberis_education_trimestre",
        "objectif_liberis_patrimoine_annuel",
        "objectif_liberis_patrimoine_hebdo",
        "objectif_liberis_patrimoine_mensuel",
        "objectif_liberis_patrimoine_trimestre",
        "objectif_liberis_epargne_annuel",
        "objectif_liberis_epargne_hebdo",
        "objectif_liberis_epargne_mensuel",
        "objectif_liberis_epargne_trimestre",
    )


def select_objectifs_digital(df: DataFrame) -> DataFrame:
    return df.select(
        "fdc_gest",
        "annee",
        "dds",
        "dfs",
        "semaine",
        "objectif_digital_annuel",
        "objectif_digital_hebdo",
        "objectif_digital_mensuel",
        "objectif_digital_trimestre",
        "objectif_delta_encours_digital_annuel",
        "objectif_delta_encours_digital_hebdo",
        "objectif_delta_encours_digital_mensuel",
        "objectif_delta_encours_digital_trimestre",
    )


def select_objectifs_csc(df: DataFrame) -> DataFrame:
    return df.select(
        "fdc_gest",
        "annee",
        "dds",
        "dfs",
        "semaine",
        "objectif_epargnes_annuel",
        "objectif_epargnes_hebdo",
        "objectif_epargnes_mensuel",
        "objectif_epargnes_trimestre",
        "objectif_delta_encours_epargnes_annuel",
        "objectif_delta_encours_epargnes_hebdo",
        "objectif_delta_encours_epargnes_mensuel",
        "objectif_delta_encours_epargnes_trimestre",
    )


def get_latest_cards(elt_stock: DataFrame) -> DataFrame:
    latest_modified_card = elt_stock.groupBy("idtexterne").agg(
        max("dtmodif").alias("dtmodif")
    )

    return (
        elt_stock.filter(col("typeeltstock") == 2)
        .join(latest_modified_card, ["idtexterne", "dtmodif"])
        .withColumnRenamed("idtexterne", "numerocarte")
        .withColumn(
            "libetatagence",
            when(col("etateltstock") == "1", "DEMANDE")
            .when(col("etateltstock") == "2", "VALIDE")
            .when(col("etateltstock") == "3", "REJETE")
            .when(col("etateltstock") == "4", "RECU A VALIDER")
            .when(col("etateltstock") == "5", "NON ENCORE RECU")
            .when(col("etateltstock") == "6", "RECU")
            .when(col("etateltstock") == "7", "A DETRUIR")
            .when(col("etateltstock") == "8", "RETIRE")
            .when(col("etateltstock") == "9", "DETRUIT"),
        )
        .select("numerocarte", "libetatagence")
        .drop_duplicates()
    )


def get_weekly_corporate(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_weekly",
        (col("is_valid") & col("this_week") & col("is_corporate")).cast("integer"),
    )


def get_monthy_corporate(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_monthly",
        (col("is_valid") & col("this_month") & col("is_corporate")).cast("integer"),
    )


def get_yearly_corporate(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_yearly",
        (col("is_valid") & col("this_year") & col("is_corporate")).cast("integer"),
    )


def build_client_e_corpo(
    contrat_produit: DataFrame,
    elt_contrat: DataFrame,
    relation_commerciale: DataFrame,
) -> DataFrame:
    elt_contrat = elt_contrat.select(
        "numerocontratinterne",
        "partiespecifique",
    ).drop_duplicates()

    contrat_produit = (
        contrat_produit.filter(col("codeproduit").isin("EBAY", "CN01"))
        .select(
            "numerotiers",
            "numerodecompte",
            "numerocontratinterne",
            "etatproduit",
            "numerocarte",
            "datesouscription",
            "codeproduit",
            "dateecheance",
            "partitiondate",
        )
        .drop_duplicates()
    )

    relation_commerciale = (
        relation_commerciale.withColumnRenamed("identifiantrc", "numerotiers")
        .withColumn("statut_relation_commerciale", lit(True))
        .select("numerotiers", "statut_relation_commerciale")
        .drop_duplicates()
    )

    is_ecorpo = (
        (col("codeproduit").isNotNull())
        & (col("codeproduit") == lit("CN01"))
        & (col("option").isNotNull())
        & (col("option").isin("7", "8"))
    )

    is_ebay = (col("codeproduit").isNotNull()) & (col("codeproduit") == lit("EBAY"))

    is_relation_commerciale = (
        (col("statut_relation_commerciale")) & (~is_ecorpo) & (~is_ebay)
    )

    return (
        contrat_produit.join(relation_commerciale, ["numerotiers"], "full")
        .join(elt_contrat, ["numerocontratinterne"], "left")
        .withColumn("option", substring(col("partiespecifique"), 143, 1))
        .filter(is_ebay | is_ecorpo | is_relation_commerciale)
        .select(
            contrat_produit["numerotiers"],
            contrat_produit["numerodecompte"],
            contrat_produit["numerocontratinterne"],
            contrat_produit["etatproduit"],
            contrat_produit["numerocarte"],
            contrat_produit["datesouscription"],
            contrat_produit["codeproduit"],
            contrat_produit["dateecheance"],
            contrat_produit["partitiondate"],
        )
        .drop_duplicates()
    )


def build_equipement_corporate(
    contrat_produit: DataFrame,
    information_tier: DataFrame,
    niveau_regroupement: DataFrame,
    contrat_history: DataFrame,
    relation_commercial: DataFrame,
    categ_filtre: str,
    val_filtre: str,
    elt_contrat: DataFrame,
) -> DataFrame:
    contrat_produit = contrat_produit.select(
        "numerotiers",
        "numerodecompte",
        "numerocontratinterne",
        "etatproduit",
        "numerocarte",
        "datesouscription",
        "codeproduit",
        "dateecheance",
        "partitiondate",
    ).drop_duplicates()
    contrat_history = (
        contrat_history.select(
            "numerocontratinterne", "etatcontrat", "partitiondate"
        )
        .transform(get_last_week_contrat_etat_produit)
        .transform(get_last_month_contrat_etat_produit)
        .transform(get_last_year_contrat_etat_produit)
    )
    information_tier = select_information_tier(information_tier)
    information_tier = information_tier.withColumnRenamed("agencegest", "codeagence")

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)
    contrat_produit = (
        build_client_e_corpo(contrat_produit, elt_contrat, relation_commercial)
        if categ_filtre == "EBAY"
        else contrat_produit.filter(col("CODEPRODUIT") == categ_filtre)
    )

    contrat_produit = (
        contrat_produit.join(
            contrat_history, ["numerocontratinterne", "partitiondate"], "left"
        )
        .join(information_tier, ["numerotiers"], "left")
        .join(niveau_regroupement, ["codeagence"], "left")
    )

    contrat_produit = (
        contrat_produit.withColumn(
            "is_corporate", col("codeproduit") == categ_filtre
        )
        .withColumn("is_valid", col("etatproduit") == "VALIDE")
        .withColumn(
            "this_year", year(col("partitiondate")) == year(col("datesouscription"))
        )
        .withColumn(
            "this_week",
            col("this_year")
            & (
                weekofyear(col("partitiondate"))
                == weekofyear(col("datesouscription"))
            ),
        )
        .withColumn(
            "this_month",
            col("this_year")
            & (month(col("partitiondate")) == month(col("datesouscription"))),
        )
        .withColumn("categorie", lit(val_filtre))
        .withColumn("libproduit", lit(val_filtre))
    )

    return contrat_produit.withColumnRenamed("numerocarte", "numerocontrat").select(
        "numerotiers",
        "numerodecompte",
        "numerocontrat",
        "datesouscription",
        "dateecheance",
        "partitiondate",
        "libproduit",
        "marche",
        "libelle_marche",
        "fdc_gest",
        "codeagence",
        "nomagence",
        "coderegion",
        "codebanque",
        "nomregion",
        "nombanque",
        "succursale",
        "nomsuccursale",
        "total_tier_actif_par_fdc",
        "total_tier_par_fdc",
        "codeproduit",
        "etatproduit",
        "is_valid",
        "this_week",
        "this_month",
        "this_year",
        "categorie",
        "is_corporate",
        "new_seg",
        "new_sous_seg",
        "libellesegment",
        "libellesoussegment",
        "gamme",
        "etatproduit_semaine_prec",
        "etatproduit_mois_prec",
        "etatproduit_annee_prec",
    )


def build_equipement_corporate_kpis(
    ebanking_corporate: DataFrame,
    eswift: DataFrame,
    etrade: DataFrame,
    edocument: DataFrame,
    etebac_ip: DataFrame,
    cdm_cheque_express: DataFrame,
    mt101: DataFrame,
    mt940: DataFrame,
) -> DataFrame:
    ebanking_corporate = (
        ebanking_corporate.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    eswift = (
        eswift.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    etrade = (
        etrade.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    edocument = (
        edocument.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    etebac_ip = (
        etebac_ip.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    cdm_cheque_express = (
        cdm_cheque_express.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    mt101 = (
        mt101.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )
    mt940 = (
        mt940.transform(get_weekly_corporate)
        .transform(get_monthy_corporate)
        .transform(get_yearly_corporate)
    )

    return (
        ebanking_corporate.union(eswift)
        .union(etrade)
        .union(edocument)
        .union(etebac_ip)
        .union(cdm_cheque_express)
        .union(mt101)
        .union(mt940)
        .select(
            "numerotiers",
            "numerodecompte",
            "numerocontrat",
            "datesouscription",
            "dateecheance",
            "partitiondate",
            "marche",
            "libelle_marche",
            "libproduit",
            "fdc_gest",
            "codeagence",
            "nomagence",
            "coderegion",
            "codebanque",
            "categorie",
            "nomregion",
            "nombanque",
            "succursale",
            "nomsuccursale",
            "total_tier_actif_par_fdc",
            "total_tier_par_fdc",
            "codeproduit",
            "etatproduit",
            "is_valid",
            "this_week",
            "this_month",
            "this_year",
            "is_weekly",
            "is_monthly",
            "is_yearly",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "gamme",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
        )
    )


def get_weekly_electronic_cards(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_weekly",
        (col("is_valid") & col("this_week") & col("is_card")).cast("integer"),
    )


def get_monthly_electronic_cards(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_monthly",
        (col("is_valid") & col("this_month") & col("is_card")).cast("integer"),
    )


def get_yearly_electronic_cards(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_yearly",
        (col("is_valid") & col("this_year") & col("is_card")).cast("integer"),
    )


def get_last_card_transaction_date(authorization: DataFrame) -> DataFrame:
    is_reve_stat_diff_f = col("aut_reve_stat") != "F"
    is_success_response = col("aut_resp_code_f039") == "000"
    is_cdm_bin = col("aut_bin_type") == "3"
    authorization = (
        authorization.filter(is_reve_stat_diff_f & is_success_response & is_cdm_bin)
        .withColumnRenamed("aut_prim_acct_numb_f002", "tra_num_porteur")
        .withColumnRenamed("aut_requ_syst_time", "tra_date_transaction")
        .select("tra_num_porteur", "tra_date_transaction")
    )
    return authorization.groupBy("tra_num_porteur").agg(
        max("tra_date_transaction").alias("date_derniere_transaction")
    )


def get_card_activity(df: DataFrame, n_jours: int):
    col_name = f"is_card_active_{n_jours}j"
    transaction_last_day = to_date(col("partitiondate"))
    transaction_first_day = date_add(to_date(col("partitiondate")), -n_jours)
    return df.withColumn(
        col_name,
        col("date_derniere_transaction").isNotNull()
        & to_date(col("date_derniere_transaction")).between(
            transaction_first_day, transaction_last_day
        ),
    )


def build_carte_kpis(df: DataFrame, objectifs_equipement: DataFrame) -> DataFrame:
    objectifs_carte = select_objectifs_carte(objectifs_equipement)

    return (
        df.transform(get_weekly_electronic_cards)
        .transform(get_monthly_electronic_cards)
        .transform(get_yearly_electronic_cards)
        .select(
            "codeproduit",
            "numerotiers",
            "numerocarte",
            "numeropackage",
            "numerodecompte",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "datesouscription",
            "dateecheance",
            "fdc_gest",
            "fdc_gest_compte",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "libetatagence",
            "libproduit",
            "is_valid",
            "has_pack",
            "is_pack",
            "statut_pack",
            "is_withdrawn",
            "is_backed_by_insurance",
            "this_week",
            "this_month",
            "this_year",
            "is_weekly",
            "is_monthly",
            "is_yearly",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "date_derniere_transaction",
            "is_card_active_30j",
            "is_card_active_90j",
            "partitiondate",
        )
        .withColumn("annee", year(col("partitiondate")))
        .withColumn("semaine", weekofyear(col("partitiondate")))
        .withColumn("trimestre", quarter(col("partitiondate")))
        .withColumn("mois", month(col("partitiondate")))
        .join(objectifs_carte, ["fdc_gest", "semaine", "annee"], how="left")
    )


def build_carte(
    contrat_produit: DataFrame,
    contrat: DataFrame,
    elt_stock: DataFrame,
    type_elt_contrat: DataFrame,
    information_tier: DataFrame,
    information_compte_client: DataFrame,
    niveau_regroupement: DataFrame,
    authorizarion: DataFrame,
    assurance: DataFrame,
) -> DataFrame:
    latest_cards = get_latest_cards(elt_stock)

    type_elt_contrat = (
        type_elt_contrat.withColumnRenamed("typedeproduit", "codeproduit")
        .withColumn("libproduit", trim("libproduit"))
        .select(
            "codeproduit",
            "libproduit",
        )
    )

    transaction_monetique = get_last_card_transaction_date(authorizarion)

    information_tier = select_information_tier(information_tier)

    information_compte = select_information_compte_client(information_compte_client)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    assurance = (
        assurance.filter(
            (col("codeproduit") == "SH05") & (col("etatproduit") == "VALIDE")
        )
        .withColumnRenamed("numerodecompte", "numero_compte")
        .withColumnRenamed("etatproduit", "etat_produit")
        .select("numero_compte", "etat_produit")
        .drop_duplicates()
    )

    contart_produit_with_etat_agence = (
        contrat_produit.join(latest_cards, ["numerocarte"], "left")
        .join(type_elt_contrat, ["codeproduit"], "left")
        .join(information_tier, ["numerotiers"], "left")
        .join(information_compte, ["numerotiers", "numerodecompte"], "left")
        .join(
            assurance,
            contrat_produit["numerodecompte"] == assurance["numero_compte"],
            "left",
        )
        .join(niveau_regroupement, ["codeagence"], "left")
        .join(
            transaction_monetique,
            (
                substring(contrat_produit["numerocarte"], 4, 16)
                == transaction_monetique["tra_num_porteur"]
            ),
            "left",
        )
        .transform(get_card_activity, 30)
        .transform(get_card_activity, 90)
    )

    card_products = (
        contart_produit_with_etat_agence.filter(col("libetatagence").isNotNull())
        .drop_duplicates(["codeproduit"])
        .withColumn("is_card", lit(True))
        .select("codeproduit", "is_card")
    )

    card_columns = [
        "codeproduit",
        "numerotiers",
        "numerocarte",
        "numeropackage",
        "numerodecompte",
        "etatproduit",
        "etatproduit_semaine_prec",
        "etatproduit_mois_prec",
        "etatproduit_annee_prec",
        "datesouscription",
        "dateecheance",
        "fdc_gest",
        "fdc_gest_compte",
        "new_seg",
        "new_sous_seg",
        "libellesegment",
        "libellesoussegment",
        "codeagence",
        "nomagence",
        "succursale",
        "nomsuccursale",
        "coderegion",
        "codebanque",
        "nomregion",
        "nombanque",
        "marche",
        "libelle_marche",
        "gamme",
        "libetatagence",
        "libproduit",
        "is_valid",
        "is_withdrawn",
        "is_backed_by_insurance",
        "is_card",
        "this_week",
        "this_month",
        "this_year",
        "total_tier_par_fdc",
        "total_tier_actif_par_fdc",
        "categorie",
        "date_derniere_transaction",
        "is_card_active_30j",
        "is_card_active_90j",
        "partitiondate",
    ]

    contrat_produit_with_is_card = (
        contart_produit_with_etat_agence.join(
            contrat, ["numerocontratinterne", "partitiondate"], "left"
        )
        .join(card_products, "codeproduit", "left")
        .withColumn("is_valid", col("etatproduit") == "VALIDE")
        .withColumn("is_card", coalesce(col("is_card"), lit(False)))
        .withColumn("is_withdrawn", col("libetatagence") == "RETIRE")
        .withColumn("is_backed_by_insurance", col("numero_compte").isNotNull())
        .filter(col("is_card"))
        .withColumn(
            "this_year", year(col("partitiondate")) == year(col("datesouscription"))
        )
        .withColumn(
            "this_week",
            col("this_year")
            & (
                weekofyear(col("partitiondate"))
                == weekofyear(col("datesouscription"))
            ),
        )
        .withColumn(
            "this_month",
            col("this_year")
            & (month(col("partitiondate")) == month(col("datesouscription"))),
        )
        .withColumn("categorie", lit("CARTE"))
        .select(*card_columns)
    )

    contrat_produit_small = (
        contrat_produit.filter(col("etatproduit") == "VALIDE")
        .withColumnRenamed("etatproduit", "etat_produit")
        .select("numerocarte", "etat_produit")
        .drop_duplicates()
    )

    return (
        contrat_produit_with_is_card.join(
            contrat_produit_small,
            contrat_produit_with_is_card["numeropackage"]
            == contrat_produit_small["numerocarte"],
            "left",
        )
        .withColumn("statut_pack", contrat_produit_small["etat_produit"])
        .withColumn(
            "has_pack", contrat_produit_with_is_card["numeropackage"].isNotNull()
        )
        .withColumn(
            "is_pack",
            (contrat_produit_with_is_card["numeropackage"].isNotNull())
            & (contrat_produit_small["etat_produit"].isNotNull()),
        )
        .select(
            contrat_produit_with_is_card["*"], "statut_pack", "has_pack", "is_pack"
        )
        .drop_duplicates()
    )


def get_type_contract_for_package(type_contrat: DataFrame) -> DataFrame:
    return (
        type_contrat.filter(col("dfval") > col("partitiondate"))
        .drop_duplicates(["typecontrat", "libcontrat"])
        .withColumnRenamed("typecontrat", "codeproduit")
        .select("codeproduit", "libcontrat")
    )


def get_weekly_package(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_weekly", (col("is_valid") & col("this_week")).cast("integer")
    )


def get_monthy_package(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_monthly", (col("is_valid") & col("this_month")).cast("integer")
    )


def get_yearly_package(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_yearly", (col("is_valid") & col("this_year")).cast("integer")
    )


def build_package_kpis(df: DataFrame) -> DataFrame:
    return (
        df.transform(get_weekly_package)
        .transform(get_monthy_package)
        .transform(get_yearly_package)
        .withColumnRenamed("numerocarte", "numerocontrat")
        .select(
            "numerotiers",
            "codeproduit",
            "numerodecompte",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "numerocontrat",
            "datesouscription",
            "numeropackage",
            "dateecheance",
            "fdc_gest",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "libcontrat",
            "is_valid",
            "this_week",
            "this_month",
            "this_year",
            "is_weekly",
            "is_monthly",
            "is_yearly",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "partitiondate",
        )
    )


def build_package(
    contrat_produit: DataFrame,
    contrat: DataFrame,
    relpackagecontrat: DataFrame,
    type_contrat: DataFrame,
    information_tier: DataFrame,
    niveau_regroupement: DataFrame,
) -> DataFrame:
    type_contrat = get_type_contract_for_package(type_contrat)

    information_tier = select_information_tier(information_tier)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    relpackagecontrat = (
        relpackagecontrat.filter(col("dfval") > col("partitiondate"))
        .select("typepackage")
        .drop_duplicates()
    )

    contrat_produit_with_packages = (
        contrat_produit.join(
            contrat, ["numerocontratinterne", "partitiondate"], "left"
        )
        .join(
            relpackagecontrat,
            contrat_produit["codeproduit"] == relpackagecontrat["typepackage"],
            "inner",
        )
        .join(type_contrat, ["codeproduit"], "left")
        .join(information_tier, ["numerotiers"], "left")
        .join(niveau_regroupement, ["codeagence"], "left")
        .withColumn("is_valid", col("etatproduit") == "VALIDE")
        .withColumn(
            "this_year", year(col("partitiondate")) == year(col("datesouscription"))
        )
        .withColumn(
            "this_week",
            col("this_year")
            & (
                weekofyear(col("partitiondate"))
                == weekofyear(col("datesouscription"))
            ),
        )
        .withColumn(
            "this_month",
            col("this_year")
            & (month(col("partitiondate")) == month(col("datesouscription"))),
        )
        .withColumn("categorie", lit("PACKAGE"))
    )

    return (
        contrat_produit_with_packages.withColumnRenamed(
            "numerocarte", "numerocontrat"
        )
        .select(
            "numerotiers",
            "codeproduit",
            "numerodecompte",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "numerocontrat",
            "datesouscription",
            "numeropackage",
            "dateecheance",
            "fdc_gest",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "libcontrat",
            "is_valid",
            "this_week",
            "this_month",
            "this_year",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "partitiondate",
        )
        .drop_duplicates()
    )


def get_weekly_assurance(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_weekly",
        (col("is_valid") & col("this_week") & col("is_assurance")).cast("integer"),
    )


def get_monthy_assurance(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_monthly",
        (col("is_valid") & col("this_month") & col("is_assurance")).cast("integer"),
    )


def get_yearly_assurance(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_yearly",
        (col("is_valid") & col("this_year") & col("is_assurance")).cast("integer"),
    )


def build_assurance_kpis(
    df: DataFrame, objectifs_equipement: DataFrame
) -> DataFrame:
    objectifs_assurance = select_objectifs_assurance(objectifs_equipement)
    return (
        df.transform(get_weekly_assurance)
        .transform(get_monthy_assurance)
        .transform(get_yearly_assurance)
        .withColumnRenamed("numerocarte", "numerocontrat")
        .select(
            "numerotiers",
            "codeproduit",
            "numerodecompte",
            "numeropackage",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "numerocontrat",
            "datesouscription",
            "dateecheance",
            "fdc_gest",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "libcontrat",
            "gamme_assurance",
            "is_valid",
            "has_pack",
            "is_pack",
            "statut_pack",
            "this_week",
            "this_month",
            "this_year",
            "is_weekly",
            "is_monthly",
            "is_yearly",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "partitiondate",
        )
        .withColumn("annee", year(col("partitiondate")))
        .withColumn("semaine", weekofyear(col("partitiondate")))
        .withColumn("trimestre", quarter(col("partitiondate")))
        .withColumn("mois", month(col("partitiondate")))
        .join(objectifs_assurance, ["fdc_gest", "semaine", "annee"], how="left")
    )


def get_type_contract_for_assurance(type_contrat: DataFrame) -> DataFrame:
    return (
        type_contrat.filter(col("dfval") > col("partitiondate"))
        .filter(col("codecompagnie").isNotNull())
        .drop_duplicates(["typecontrat", "libcontrat"])
        .select("typecontrat", "libcontrat")
    )


def build_assurance(
    contrat_produit: DataFrame,
    contrat: DataFrame,
    type_contrat: DataFrame,
    information_tier: DataFrame,
    gamme_assurance: DataFrame,
    niveau_regroupement: DataFrame,
) -> DataFrame:
    type_contrat = get_type_contract_for_assurance(type_contrat)

    information_tier = select_information_tier(information_tier)

    gamme_assurance = (
        gamme_assurance.withColumnRenamed("code_produit", "codeproduit")
        .withColumnRenamed("libelle_assurance", "libelleassurance")
        .withColumnRenamed("gamme", "gamme_assurance")
        .select(
            "codeproduit",
            "libelleassurance",
            "gamme_assurance",
        )
    )

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    assurance_columns = [
        "numerotiers",
        "codeproduit",
        "numerodecompte",
        "numeropackage",
        "etatproduit",
        "etatproduit_semaine_prec",
        "etatproduit_mois_prec",
        "etatproduit_annee_prec",
        "numerocontrat",
        "datesouscription",
        "dateecheance",
        "fdc_gest",
        "new_seg",
        "new_sous_seg",
        "libellesegment",
        "libellesoussegment",
        "codeagence",
        "nomagence",
        "succursale",
        "nomsuccursale",
        "coderegion",
        "codebanque",
        "nomregion",
        "nombanque",
        "marche",
        "libelle_marche",
        "gamme",
        "libcontrat",
        "gamme_assurance",
        "is_valid",
        "is_assurance",
        "this_week",
        "this_month",
        "this_year",
        "total_tier_par_fdc",
        "total_tier_actif_par_fdc",
        "categorie",
        "partitiondate",
    ]

    contrat_produit_with_assurance = (
        contrat_produit.join(
            contrat, ["numerocontratinterne", "partitiondate"], "left"
        )
        .join(
            type_contrat,
            contrat_produit["codeproduit"] == type_contrat["typecontrat"],
            "left",
        )
        .join(gamme_assurance, ["codeproduit"], "left")
        .join(information_tier, ["numerotiers"], "left")
        .join(niveau_regroupement, ["codeagence"], "left")
        .withColumn("is_valid", col("etatproduit") == "VALIDE")
        .withColumn("is_assurance", col("libcontrat").isNotNull())
        .filter(col("is_assurance"))
        .withColumn(
            "this_year", year(col("partitiondate")) == year(col("datesouscription"))
        )
        .withColumn(
            "this_week",
            col("this_year")
            & (
                weekofyear(col("partitiondate"))
                == weekofyear(col("datesouscription"))
            ),
        )
        .withColumn(
            "this_month",
            col("this_year")
            & (month(col("partitiondate")) == month(col("datesouscription"))),
        )
        .withColumn("categorie", lit("ASSURANCE"))
        .withColumnRenamed("numerocarte", "numerocontrat")
        .select(*assurance_columns)
    )

    contrat_produit_small = (
        contrat_produit.filter(col("etatproduit") == "VALIDE")
        .withColumnRenamed("etatproduit", "etat_produit")
        .select("numerocarte", "etat_produit")
        .drop_duplicates()
    )

    return (
        contrat_produit_with_assurance.join(
            contrat_produit_small,
            contrat_produit_with_assurance["numeropackage"]
            == contrat_produit_small["numerocarte"],
            "left",
        )
        .withColumn("statut_pack", contrat_produit_small["etat_produit"])
        .withColumn(
            "has_pack", contrat_produit_with_assurance["numeropackage"].isNotNull()
        )
        .withColumn(
            "is_pack",
            (contrat_produit_with_assurance["numeropackage"].isNotNull())
            & (contrat_produit_small["etat_produit"].isNotNull()),
        )
        .select(
            contrat_produit_with_assurance["*"], "statut_pack", "has_pack", "is_pack"
        )
        .drop_duplicates()
    )


def get_weekly_csc(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_weekly",
        (col("is_valid") & col("this_week") & col("is_csc")).cast("integer"),
    )


def get_monthy_csc(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_monthly",
        (col("is_valid") & col("this_month") & col("is_csc")).cast("integer"),
    )


def get_yearly_csc(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_yearly",
        (col("is_valid") & col("this_year") & col("is_csc")).cast("integer"),
    )


def build_csc_kpis(df: DataFrame, objectifs_equipement: DataFrame) -> DataFrame:
    objectifs_csc = select_objectifs_csc(objectifs_equipement)
    return (
        df.transform(get_weekly_csc)
        .transform(get_monthy_csc)
        .transform(get_yearly_csc)
        .select(
            "numerotiers",
            "codeproduit",
            "numerodecompte",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "datesouscription",
            "dateecheance",
            "fdc_gest",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "is_valid",
            "is_backed_by_insurance",
            "is_pack",
            "statut_pack",
            "this_week",
            "this_month",
            "this_year",
            "is_weekly",
            "is_monthly",
            "is_yearly",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "partitiondate",
        )
        .withColumn("annee", year(col("partitiondate")))
        .withColumn("semaine", weekofyear(col("partitiondate")))
        .withColumn("trimestre", quarter(col("partitiondate")))
        .withColumn("mois", month(col("partitiondate")))
        .join(objectifs_csc, ["fdc_gest", "semaine", "annee"], how="left")
    )


def build_csc(
    contrat_produit: DataFrame,
    sous_compte: DataFrame,
    information_tier: DataFrame,
    niveau_regroupement: DataFrame,
    package: DataFrame,
    carte: DataFrame,
) -> DataFrame:
    information_tier = select_information_tier(information_tier)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    sous_compte = (
        sous_compte.select("numerodecompte", "etatcompte", "partitiondate")
        .transform(get_last_week_csc_etat_produit)
        .transform(get_last_month_csc_etat_produit)
        .transform(get_last_year_csc_etat_produit)
    )

    package = (
        package.filter(col("etatproduit") == "VALIDE")
        .withColumnRenamed("etatproduit", "etat_produit")
        .select("numerodecompte", "etat_produit")
        .drop_duplicates()
    )

    carte = (
        carte.filter(
            (col("codeproduit") == "TAWF") & (col("etatproduit") == "VALIDE")
        )
        .withColumnRenamed("codeproduit", "code_produit")
        .select("numerodecompte", "code_produit")
        .drop_duplicates()
    )

    contrat_produit_with_csc = (
        contrat_produit.join(information_tier, ["numerotiers"], "left")
        .join(niveau_regroupement, ["codeagence"], "left")
        .withColumn("is_valid", col("etatproduit") == "VALIDE")
        .withColumn("is_csc", col("codeproduit").isin("CSC1", "CSC2"))
        .filter(col("is_csc"))
        .withColumn(
            "this_year", year(col("partitiondate")) == year(col("datesouscription"))
        )
        .withColumn(
            "this_week",
            col("this_year")
            & (
                weekofyear(col("partitiondate"))
                == weekofyear(col("datesouscription"))
            ),
        )
        .withColumn(
            "this_month",
            col("this_year")
            & (month(col("partitiondate")) == month(col("datesouscription"))),
        )
        .withColumn("categorie", lit("CSC"))
    )

    return (
        contrat_produit_with_csc.join(
            sous_compte, ["numerodecompte", "partitiondate"], "left"
        )
        .join(package, ["numerodecompte"], "left")
        .join(carte, ["numerodecompte"], "left")
        .withColumn("statut_pack", package["etat_produit"])
        .withColumn(
            "is_pack",
            (contrat_produit_with_csc["numerodecompte"].isNotNull())
            & (package["etat_produit"].isNotNull()),
        )
        .withColumn("is_backed_by_insurance", col("code_produit").isNotNull())
        .select(
            "numerotiers",
            "codeproduit",
            "numerodecompte",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "datesouscription",
            "dateecheance",
            "fdc_gest",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "is_valid",
            "is_csc",
            "is_backed_by_insurance",
            "is_pack",
            "statut_pack",
            "this_week",
            "this_month",
            "this_year",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "partitiondate",
        )
    )


def get_weekly_digital(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_weekly",
        (col("is_valid") & col("this_week") & col("is_digital")).cast("integer"),
    )


def get_monthy_digital(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_monthly",
        (col("is_valid") & col("this_month") & col("is_digital")).cast("integer"),
    )


def get_yearly_digital(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_yearly",
        (col("is_valid") & col("this_year") & col("is_digital")).cast("integer"),
    )


def build_digital_kpis(df: DataFrame, objectifs_equipement: DataFrame) -> DataFrame:
    objectifs_digital = select_objectifs_digital(objectifs_equipement)
    return (
        df.transform(get_weekly_digital)
        .transform(get_monthy_digital)
        .transform(get_yearly_digital)
        .withColumnRenamed("numerocarte", "numerocontrat")
        .select(
            "numerotiers",
            "codeproduit",
            "numerodecompte",
            "etatproduit",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
            "numerocontrat",
            "datesouscription",
            "dateecheance",
            "fdc_gest",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "marche",
            "libelle_marche",
            "gamme",
            "is_digital_active_30j",
            "is_digital_active_90j",
            "date_derniere_connexion",
            "is_valid",
            "has_pack",
            "is_pack",
            "statut_pack",
            "this_week",
            "this_month",
            "this_year",
            "is_weekly",
            "is_monthly",
            "is_yearly",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "categorie",
            "partitiondate",
        )
        .withColumn("annee", year(col("partitiondate")))
        .withColumn("semaine", weekofyear(col("partitiondate")))
        .withColumn("trimestre", quarter(col("partitiondate")))
        .withColumn("mois", month(col("partitiondate")))
        .join(objectifs_digital, ["fdc_gest", "semaine", "annee"], how="left")
    )


def select_audit_log_event(df: DataFrame) -> DataFrame:
    return (
        df.filter(
            (col("codebanqueassocie") == "021") & (col("eventname") == "ConnexionOK")
        )
        .withColumnRenamed("actor", "numerotiers")
        .select("numerotiers", "datecreated")
    )


def select_utilisateur(df: DataFrame) -> DataFrame:
    return df.filter(col("typeprofile") == "CL").select("typeprofile", "username")


def get_last_connection_date(
    audit_log_event: DataFrame, utilisateur: DataFrame
) -> DataFrame:
    return (
        audit_log_event.join(
            utilisateur,
            audit_log_event["numerotiers"] == utilisateur["username"],
        )
        .groupBy("numerotiers")
        .agg(max("datecreated").alias("date_derniere_connexion"))
    )


def select_contrat_produit_digital(df: DataFrame) -> DataFrame:
    return df.select(
        "numerotiers",
        "numerodecompte",
        "numeropackage",
        "numerocontratinterne",
        "etatproduit",
        "numerocarte",
        "datesouscription",
        "codeproduit",
        "dateecheance",
        "partitiondate",
    ).drop_duplicates()


def build_digital_activity(df: DataFrame, n_jours: int) -> DataFrame:
    col_name = f"is_digital_active_{n_jours}j"
    connection_last_day = to_date(col("partitiondate"))
    connection_first_day = date_add(to_date(col("partitiondate")), -n_jours)
    return df.withColumn(
        col_name,
        col("date_derniere_connexion").isNotNull()
        & to_date(col("date_derniere_connexion")).between(
            connection_first_day, connection_last_day
        ),
    )


def build_digital(
    contrat_produit: DataFrame,
    contrat: DataFrame,
    information_tier: DataFrame,
    niveau_regroupement: DataFrame,
    audit_log_event: DataFrame,
    utilisateur: DataFrame,
) -> DataFrame:
    contrat_produit = select_contrat_produit_digital(contrat_produit)

    information_tier = select_information_tier(information_tier)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    audit_log_event = select_audit_log_event(audit_log_event)

    utilisateur = select_utilisateur(utilisateur)

    last_connection_date = get_last_connection_date(audit_log_event, utilisateur)

    digital_columns = [
        "numerotiers",
        "codeproduit",
        "numeropackage",
        "numerodecompte",
        "etatproduit",
        "etatproduit_semaine_prec",
        "etatproduit_mois_prec",
        "etatproduit_annee_prec",
        "numerocontrat",
        "datesouscription",
        "dateecheance",
        "fdc_gest",
        "new_seg",
        "new_sous_seg",
        "libellesegment",
        "libellesoussegment",
        "codeagence",
        "nomagence",
        "succursale",
        "nomsuccursale",
        "coderegion",
        "codebanque",
        "nomregion",
        "nombanque",
        "marche",
        "libelle_marche",
        "gamme",
        "is_valid",
        "is_digital",
        "is_digital_active_30j",
        "is_digital_active_90j",
        "date_derniere_connexion",
        "this_week",
        "this_month",
        "this_year",
        "total_tier_par_fdc",
        "total_tier_actif_par_fdc",
        "categorie",
        "partitiondate",
    ]

    contrat_produit_with_digital = (
        contrat_produit.withColumn("is_digital", col("codeproduit").isin("CN01"))
        .filter(col("is_digital"))
        .join(contrat, ["numerocontratinterne", "partitiondate"], "left")
        .join(information_tier, ["numerotiers"], "left")
        .join(last_connection_date, ["numerotiers"], "left")
        .join(niveau_regroupement, ["codeagence"], "left")
        .withColumn("is_valid", col("etatproduit") == "VALIDE")
        .withColumn(
            "this_year", year(col("partitiondate")) == year(col("datesouscription"))
        )
        .withColumn(
            "this_week",
            col("this_year")
            & (
                weekofyear(col("partitiondate"))
                == weekofyear(col("datesouscription"))
            ),
        )
        .withColumn(
            "this_month",
            col("this_year")
            & (month(col("partitiondate")) == month(col("datesouscription"))),
        )
        .withColumn("categorie", lit("DIGITAL"))
        .withColumnRenamed("numerocarte", "numerocontrat")
        .transform(build_digital_activity, 30)
        .transform(build_digital_activity, 90)
        .select(*digital_columns)
    )

    contrat_produit_small = (
        contrat_produit.filter(col("etatproduit") == "VALIDE")
        .withColumnRenamed("etatproduit", "etat_produit")
        .select("numerocarte", "etat_produit")
        .drop_duplicates()
    )

    return (
        contrat_produit_with_digital.join(
            contrat_produit_small,
            contrat_produit_with_digital["numeropackage"]
            == contrat_produit_small["numerocarte"],
            "left",
        )
        .withColumn("statut_pack", contrat_produit_small["etat_produit"])
        .withColumn(
            "has_pack", contrat_produit_with_digital["numeropackage"].isNotNull()
        )
        .withColumn(
            "is_pack",
            (contrat_produit_with_digital["numeropackage"].isNotNull())
            & (contrat_produit_small["etat_produit"].isNotNull()),
        )
        .select(
            contrat_produit_with_digital["*"],
            "statut_pack",
            "has_pack",
            "is_pack",
        )
        .drop("numeropackage")
        .drop_duplicates()
    )
