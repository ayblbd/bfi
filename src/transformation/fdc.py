from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    date_format,
    lag,
    last,
    lit,
    max,
    min,
    month,
    months_between,
    quarter,
    round,
    sum,
    to_date,
    weekofyear,
    when,
    year,
)
from pyspark.sql.window import Window


def get_last_year_infos(df):
    window = Window.partitionBy("numero_tiers").orderBy("date_traitement")

    df = df.withColumn("rank", col("ANNEE")).withColumn(
        "rank_diff", lag("rank").over(window)
    )

    return (
        df.withColumn("prev_target", lag(col("statut_client")).over(window))
        .withColumn(
            "annee_prec_statut_client",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop("rank", "prev_target", "rank_diff")
    )


def get_last_week_infos(df):
    window = Window.partitionBy("numero_tiers").orderBy("date_traitement")

    df = df.withColumn(
        "rank",
        concat_ws(
            "",
            lit(date_format(col("date_traitement"), "yyyy")),
            weekofyear(to_date(col("date_traitement"))),
        ),
    ).withColumn("rank_diff", lag("rank").over(window))

    return (
        df.withColumn("prev_target", lag(col("statut_client")).over(window))
        .withColumn(
            "semaine_prec_statut_client",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop("rank", "prev_target", "rank_diff")
    )


def get_last_month_infos(df):
    window = Window.partitionBy("numero_tiers").orderBy("date_traitement")

    df = df.withColumn(
        "rank", date_format(col("date_traitement"), "yyyyMM")
    ).withColumn("rank_diff", lag("rank").over(window))

    return (
        df.withColumn("prev_target", lag(col("statut_client")).over(window))
        .withColumn(
            "mois_prec_statut_client",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop("rank", "prev_target", "rank_diff")
    )


def get_last_quarter_infos(df):
    window = Window.partitionBy("numero_tiers").orderBy("date_traitement")

    df = df.withColumn(
        "rank",
        concat_ws(
            "",
            lit(date_format(col("date_traitement"), "yyyy")),
            quarter(to_date(col("date_traitement"))),
        ),
    ).withColumn("rank_diff", lag("rank").over(window))

    return (
        df.withColumn("prev_target", lag(col("statut_client")).over(window))
        .withColumn(
            "trimestre_prec_statut_client",
            last(
                when(col("rank") != col("rank_diff"), col("prev_target")),
                ignorenulls=True,
            ).over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        .drop("rank", "prev_target", "rank_diff")
    )


def get_week_conquete_en_cours(df):
    windowSpec = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "DATE_OUVERTURE_PREMIER_COMPTE",
        min(when(col("is_client_account_adult"), col("DATE_OUVERTURE"))).over(
            windowSpec
        ),
    )

    return df.withColumn(
        "CONQUETE_EN_COURS_SEMAINE",
        when(
            (col("ANNEE") == year(col("DATE_OUVERTURE_PREMIER_COMPTE")))
            & (col("SEMAINE") == weekofyear(col("DATE_OUVERTURE_PREMIER_COMPTE"))),
            lit(1),
        ).otherwise(lit(0)),
    )


def get_month_conquete_en_cours(df):
    windowSpec = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "DATE_OUVERTURE_PREMIER_COMPTE",
        min(when(col("is_client_account_adult"), col("DATE_OUVERTURE"))).over(
            windowSpec
        ),
    )

    return df.withColumn(
        "CONQUETE_EN_COURS_MOIS",
        when(
            (col("ANNEE") == year(col("DATE_OUVERTURE_PREMIER_COMPTE")))
            & (col("MOIS") == month(col("DATE_OUVERTURE_PREMIER_COMPTE"))),
            lit(1),
        ).otherwise(lit(0)),
    )


def get_quarter_conquete_en_cours(df):
    windowSpec = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "DATE_OUVERTURE_PREMIER_COMPTE",
        min(when(col("is_client_account_adult"), col("DATE_OUVERTURE"))).over(
            windowSpec
        ),
    )

    return df.withColumn(
        "CONQUETE_EN_COURS_TRIMESTRE",
        when(
            (col("ANNEE") == year(col("DATE_OUVERTURE_PREMIER_COMPTE")))
            & (col("trimestre") == quarter(col("DATE_OUVERTURE_PREMIER_COMPTE"))),
            lit(1),
        ).otherwise(lit(0)),
    )


def get_cumul_conquete_en_cours(df):
    windowSpec = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "DATE_OUVERTURE_PREMIER_COMPTE",
        min(when(col("is_client_account_adult"), col("DATE_OUVERTURE"))).over(
            windowSpec
        ),
    )

    return df.withColumn(
        "CONQUETE_EN_COURS_CUMUL",
        when(
            (col("ANNEE") == year(col("DATE_OUVERTURE_PREMIER_COMPTE"))), lit(1)
        ).otherwise(lit(0)),
    )


def get_week_conquete_active(df):
    return df.withColumn(
        "CONQUETE_ACTIVE_SEMAINE",
        when(
            (col("CONQUETE_EN_COURS_SEMAINE") == 1)
            & (col("statut_client") == "ACTIF")
            & col("is_pack_and_mycdm"),
            1,
        ).otherwise(0),
    )


def get_month_conquete_active(df):
    return df.withColumn(
        "CONQUETE_ACTIVE_MOIS",
        when(
            (col("CONQUETE_EN_COURS_MOIS") == 1)
            & (col("statut_client") == "ACTIF")
            & col("is_pack_and_mycdm"),
            1,
        ).otherwise(0),
    )


def get_quarter_conquete_active(df):
    return df.withColumn(
        "CONQUETE_ACTIVE_TRIMESTRE",
        when(
            (col("CONQUETE_EN_COURS_TRIMESTRE") == 1)
            & (col("statut_client") == "ACTIF")
            & col("is_pack_and_mycdm"),
            1,
        ).otherwise(0),
    )


def get_cumul_conquete_active(df):
    return df.withColumn(
        "CONQUETE_ACTIVE_CUMUL",
        when(
            (col("CONQUETE_EN_COURS_CUMUL") == 1)
            & (col("statut_client") == "ACTIF")
            & col("is_pack_and_mycdm"),
            1,
        ).otherwise(0),
    )


def get_date_cloture_dernier_compte(df):
    window = Window.partitionBy("numero_tiers")

    return df.withColumn(
        "date_cloture_dernier_compte", max(col("date_cloture")).over(window)
    )


def get_week_rupture_relation(df):
    window = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "ALL_CLOSED", (col("ETAT_COMPTE") == "OUVERT").cast("integer")
    ).withColumn("ALL_CLOSED", max(col("ALL_CLOSED")).over(window) == 0)

    return df.withColumn(
        "RUPTURE_RELATION_SEMAINE",
        when(
            (col("ANNEE") == year(col("DATE_CLOTURE")))
            & (col("SEMAINE") == weekofyear(col("DATE_CLOTURE")))
            & col("ALL_CLOSED"),
            1,
        ).otherwise(0),
    ).withColumn(
        "RUPTURE_RELATION_SEMAINE", max(col("RUPTURE_RELATION_SEMAINE")).over(window)
    )


def get_month_rupture_relation(df):
    window = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "ALL_CLOSED", (col("ETAT_COMPTE") == "OUVERT").cast("integer")
    ).withColumn("ALL_CLOSED", max(col("ALL_CLOSED")).over(window) == 0)

    return df.withColumn(
        "RUPTURE_RELATION_MOIS",
        when(
            (col("ANNEE") == year(col("DATE_CLOTURE")))
            & (col("mois") == month(col("DATE_CLOTURE")))
            & col("ALL_CLOSED"),
            1,
        ).otherwise(0),
    ).withColumn(
        "RUPTURE_RELATION_MOIS", max(col("RUPTURE_RELATION_MOIS")).over(window)
    )


def get_quarter_rupture_relation(df):
    window = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "ALL_CLOSED", (col("ETAT_COMPTE") == "OUVERT").cast("integer")
    ).withColumn("ALL_CLOSED", max(col("ALL_CLOSED")).over(window) == 0)

    return df.withColumn(
        "RUPTURE_RELATION_TRIMESTRE",
        when(
            (col("ANNEE") == year(col("DATE_CLOTURE")))
            & (col("TRIMESTRE") == quarter(col("DATE_CLOTURE")))
            & col("ALL_CLOSED"),
            1,
        ).otherwise(0),
    ).withColumn(
        "RUPTURE_RELATION_TRIMESTRE",
        max(col("RUPTURE_RELATION_TRIMESTRE")).over(window),
    )


def get_cumul_rupture_relation(df):
    window = Window.partitionBy("numero_tiers")

    df = df.withColumn(
        "ALL_CLOSED", (col("ETAT_COMPTE") == "OUVERT").cast("integer")
    ).withColumn("ALL_CLOSED", max(col("ALL_CLOSED")).over(window) == 0)

    return df.withColumn(
        "RUPTURE_RELATION_CUMUL",
        when(
            (col("ANNEE") == year(col("DATE_CLOTURE"))) & col("ALL_CLOSED"), 1
        ).otherwise(0),
    ).withColumn(
        "RUPTURE_RELATION_CUMUL", max(col("RUPTURE_RELATION_CUMUL")).over(window)
    )


def get_week_reactivation(df):
    return df.withColumn(
        "REACTIVATION_SEMAINE",
        when(
            (col("statut_client") == "ACTIF")
            & (
                (col("semaine_prec_statut_client") != "ACTIF")
                | col("semaine_prec_statut_client").isNull()
            ),
            1,
        ).otherwise(0),
    )


def get_month_reactivation(df):
    return df.withColumn(
        "REACTIVATION_MOIS",
        when(
            (col("statut_client") == "ACTIF")
            & (
                (col("mois_prec_statut_client") != "ACTIF")
                | col("mois_prec_statut_client").isNull()
            ),
            1,
        ).otherwise(0),
    )


def get_quarter_reactivation(df):
    return df.withColumn(
        "REACTIVATION_TRIMESTRE",
        when(
            (col("statut_client") == "ACTIF")
            & (
                (col("trimestre_prec_statut_client") != "ACTIF")
                | col("trimestre_prec_statut_client").isNull()
            ),
            1,
        ).otherwise(0),
    )


def get_cumul_reactivation(df):
    return df.withColumn(
        "REACTIVATION_CUMUL",
        when(
            (col("statut_client") == "ACTIF")
            & (
                (col("annee_prec_statut_client") != "ACTIF")
                | col("annee_prec_statut_client").isNull()
            ),
            1,
        ).otherwise(0),
    )


def get_week_inactivite(df):
    return df.withColumn(
        "INACTIVITE_SEMAINE",
        when(
            ((col("statut_client") != "ACTIF") | col("statut_client").isNull())
            & (col("semaine_prec_statut_client") == "ACTIF"),
            1,
        ).otherwise(0),
    )


def get_month_inactivite(df):
    return df.withColumn(
        "INACTIVITE_MOIS",
        when(
            ((col("statut_client") != "ACTIF") | col("statut_client").isNull())
            & (col("mois_prec_statut_client") == "ACTIF"),
            1,
        ).otherwise(0),
    )


def get_quarter_inactivite(df):
    return df.withColumn(
        "INACTIVITE_TRIMESTRE",
        when(
            ((col("statut_client") != "ACTIF") | col("statut_client").isNull())
            & (col("trimestre_prec_statut_client") == "ACTIF"),
            1,
        ).otherwise(0),
    )


def get_cumul_inactivite(df):
    return df.withColumn(
        "INACTIVITE_CUMUL",
        when(
            ((col("statut_client") != "ACTIF") | col("statut_client").isNull())
            & (col("annee_prec_statut_client") == "ACTIF"),
            1,
        ).otherwise(0),
    )


def get_week_client_actif_delta(df):
    return df.withColumn(
        "CLIENT_ACTIF_DELTA_SEMAINE",
        when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "INACTIF"),
            1,
        )
        .when(
            (col("ETAT_COMPTE") == "OUVERT")
            & (col("statut_client") == "INACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            -1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client").isNull()),
            1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            0,
        )
        .otherwise(0),
    )


def get_month_client_actif_delta(df):
    return df.withColumn(
        "CLIENT_ACTIF_DELTA_MOIS",
        when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "INACTIF"),
            1,
        )
        .when(
            (col("statut_client") == "INACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            -1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client").isNull()),
            1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            0,
        )
        .otherwise(0),
    )


def get_quarter_client_actif_delta(df):
    return df.withColumn(
        "CLIENT_ACTIF_DELTA_TRIMESTRE",
        when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "INACTIF"),
            1,
        )
        .when(
            (col("statut_client") == "INACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            -1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client").isNull()),
            1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            0,
        )
        .otherwise(0),
    )


def get_cumul_client_actif_delta(df):
    return df.withColumn(
        "CLIENT_ACTIF_DELTA_CUMUL",
        when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "INACTIF"),
            1,
        )
        .when(
            (col("statut_client") == "INACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            -1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client").isNull()),
            1,
        )
        .when(
            (col("statut_client") == "ACTIF")
            & (col("ANNEE_PREC_statut_client") == "ACTIF"),
            0,
        )
        .otherwise(0),
    )


def get_fdc_kpi(df: DataFrame, partition_date: str) -> DataFrame:
    df = (
        df.transform(get_last_year_infos)
        .transform(get_last_quarter_infos)
        .transform(get_last_month_infos)
        .transform(get_last_week_infos)
        .filter(col("date_traitement") == to_date(lit(partition_date)))
        .transform(get_date_cloture_dernier_compte)
        .transform(get_week_conquete_en_cours)
        .transform(get_month_conquete_en_cours)
        .transform(get_quarter_conquete_en_cours)
        .transform(get_cumul_conquete_en_cours)
        .transform(get_week_conquete_active)
        .transform(get_month_conquete_active)
        .transform(get_quarter_conquete_active)
        .transform(get_cumul_conquete_active)
        .transform(get_week_rupture_relation)
        .transform(get_month_rupture_relation)
        .transform(get_quarter_rupture_relation)
        .transform(get_cumul_rupture_relation)
        .transform(get_week_reactivation)
        .transform(get_month_reactivation)
        .transform(get_quarter_reactivation)
        .transform(get_cumul_reactivation)
        .transform(get_week_inactivite)
        .transform(get_month_inactivite)
        .transform(get_quarter_inactivite)
        .transform(get_cumul_inactivite)
        # .transform(get_week_client_actif_delta)
        # .transform(get_month_client_actif_delta)
        # .transform(get_quarter_client_actif_delta)
        # .transform(get_cumul_client_actif_delta)
    )

    return (
        df.withColumnRenamed("numero_tiers", "numerotiers")
        .select(
            "numerotiers",
            "fdc_gest",
            "semaine",
            "annee",
            "statut_client",
            "marche",
            "libelle_marche",
            "gamme",
            "date_ouverture_premier_compte",
            "date_cloture_dernier_compte",
            "trimestre",
            "mois",
            "code_agence",
            "nom_agence",
            "code_groupe",
            "nom_groupe",
            "code_region",
            "nom_region",
            "code_banque",
            "nom_banque",
            "new_seg",
            "libellesegment",
            "new_sous_seg",
            "libellesoussegment",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "semaine_prec_statut_client",
            "mois_prec_statut_client",
            "trimestre_prec_statut_client",
            "annee_prec_statut_client",
            "conquete_en_cours_semaine",
            "conquete_en_cours_mois",
            "conquete_en_cours_trimestre",
            "conquete_en_cours_cumul",
            "conquete_active_semaine",
            "conquete_active_mois",
            "conquete_active_trimestre",
            "conquete_active_cumul",
            "all_closed",
            "is_pack",
            "is_mycdm",
            "is_engaged",
            "rupture_relation_semaine",
            "rupture_relation_mois",
            "rupture_relation_trimestre",
            "rupture_relation_cumul",
            "reactivation_semaine",
            "reactivation_mois",
            "reactivation_trimestre",
            "reactivation_cumul",
            "inactivite_semaine",
            "inactivite_mois",
            "inactivite_trimestre",
            "inactivite_cumul",
            "objectif_conquete_annuel",
            "objectif_conquete_hebdo",
            "objectif_conquete_mensuel",
            "objectif_conquete_trimestre",
            "objectif_mass_market_annuel",
            "objectif_mass_market_hebdo",
            "objectif_mass_market_mensuel",
            "objectif_mass_market_trimestre",
            "objectif_medium_annuel",
            "objectif_medium_hebdo",
            "objectif_medium_mensuel",
            "objectif_medium_trimestre",
            "objectif_premium_annuel",
            "objectif_premium_hebdo",
            "objectif_premium_mensuel",
            "objectif_premium_trimestre",
            "objectif_clients_actifs_annuel",
            "objectif_clients_actifs_hebdo",
            "objectif_clients_actifs_mensuel",
            "objectif_clients_actifs_trimestre",
            "objectif_pro_tpe_annuel",
            "objectif_pro_tpe_hebdo",
            "objectif_pro_tpe_mensuel",
            "objectif_pro_tpe_trimestre",
            "partitiondate",
        )
        .drop_duplicates()
    )


def select_information_tier(information_tier: DataFrame) -> DataFrame:
    window = Window.partitionBy("cd_fdc_gest")

    return (
        information_tier.withColumn(
            "total_tier_par_fdc", count(col("numerotiers")).over(window)
        )
        .withColumn(
            "total_tier_actif_par_fdc",
            count(when(col("statutclient") == "ACTIF", col("numerotiers"))).over(
                window
            ),
        )
        .select(
            "partitiondate",
            "numerotiers",
            "marche",
            col("datedenaissance").alias("date_naissance"),
            col("agencegest").alias("code_agence"),
            col("cd_fdc_gest").alias("fdc_gest"),
            "new_seg",
            "libellesegment",
            "new_sous_seg",
            "libelle_marche",
            "libellesoussegment",
            "gamme",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
        )
    )


def select_information_compte_client(
    information_compte_client: DataFrame,
) -> DataFrame:
    return information_compte_client.select(
        "numerotiers",
        "numerodecompte",
        "etat_compte",
        col("dateeffet").alias("date_ouverture"),
        col("dateecheance").alias("date_cloture"),
        "partitiondate",
    )


def select_donnee_client(donnee_client: DataFrame) -> DataFrame:
    return donnee_client.select(
        "numerotiers",
        col("statutclient").alias("statut_client"),
        col("partitiondate").alias("date_traitement"),
        "partitiondate",
    )


def select_niveau_regroupement(niveau_regroupement: DataFrame) -> DataFrame:
    return niveau_regroupement.select(
        col("CODEAGENCE").alias("code_agence"),
        col("NOMAGENCE").alias("nom_agence"),
        col("succursale").alias("code_groupe"),
        col("nomsuccursale").alias("nom_groupe"),
        col("CODEREGION").alias("code_region"),
        col("NOMREGION").alias("nom_region"),
        col("CODEBANQUE").alias("code_banque"),
        col("NOMBANQUE").alias("nom_banque"),
        "partitiondate",
    )


def select_segmentation(segmentation: DataFrame) -> DataFrame:
    return (
        segmentation.filter(col("dfval") >= col("partitiondate"))
        .withColumnRenamed("codesegment", "new_seg")
        .select("new_seg", "libellesegment", "partitiondate")
        .drop_duplicates()
    )


def select_ref_sous_segment(ref_sous_segment: DataFrame) -> DataFrame:
    return (
        ref_sous_segment.filter(col("dfval") >= col("partitiondate"))
        .withColumnRenamed("codesoussegment", "new_sous_seg")
        .withColumnRenamed("libsoussegment", "libellesoussegment")
        .select("new_sous_seg", "libellesoussegment", "partitiondate")
        .drop_duplicates()
    )


def build_objectifs_fdc(
    objectifs_tro: DataFrame, saisonalite: DataFrame
) -> DataFrame:
    objectifs_tro = objectifs_tro.select(
        "code_region",
        "code_groupe",
        "code_agence",
        "code_emploi",
        "code_fdc",
        "conquete",
        "mass_market",
        "medium",
        "premium",
        "pro_tpe",
        "clients_actifs",
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

    return (
        result.withColumn("numero_semaine", col("numero_semaine").cast("int"))
        .withColumn("pourc_semaine", col("pourc_semaine").cast("float"))
        .withColumn(
            "objectif_conquete_annuel", round(col("conquete").cast("float"), 2)
        )
        .withColumn(
            "objectif_conquete_hebdo",
            round(col("objectif_conquete_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_mass_market_annuel", round(col("mass_market").cast("float"), 2)
        )
        .withColumn(
            "objectif_mass_market_hebdo",
            round(col("objectif_mass_market_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn("objectif_medium_annuel", round(col("medium").cast("float"), 2))
        .withColumn(
            "objectif_medium_hebdo",
            round(col("objectif_medium_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_premium_annuel", round(col("premium").cast("float"), 2)
        )
        .withColumn(
            "objectif_premium_hebdo",
            round(col("objectif_premium_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_pro_tpe_annuel", round(col("pro_tpe").cast("float"), 2)
        )
        .withColumn(
            "objectif_pro_tpe_hebdo",
            round(col("objectif_pro_tpe_annuel") * col("pourc_semaine"), 2),
        )
        .withColumn(
            "objectif_clients_actifs_annuel",
            round(col("clients_actifs").cast("float"), 2),
        )
        .withColumn(
            "objectif_clients_actifs_hebdo",
            round(col("objectif_clients_actifs_annuel") * col("pourc_semaine"), 2),
        )
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
        .withColumn(
            "objectif_conquete_mensuel",
            round(sum(col("objectif_conquete_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_mass_market_mensuel",
            round(sum(col("objectif_mass_market_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_medium_mensuel",
            round(sum(col("objectif_medium_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_premium_mensuel",
            round(sum(col("objectif_premium_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_pro_tpe_mensuel",
            round(sum(col("objectif_pro_tpe_hebdo")).over(window_spec_mois), 2),
        )
        .withColumn(
            "objectif_clients_actifs_mensuel",
            round(
                sum(col("objectif_clients_actifs_hebdo")).over(window_spec_mois), 2
            ),
        )
        .withColumn(
            "objectif_conquete_trimestre",
            round(
                sum(col("objectif_conquete_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_mass_market_trimestre",
            round(
                sum(col("objectif_mass_market_hebdo")).over(window_spec_trimestre), 2
            ),
        )
        .withColumn(
            "objectif_medium_trimestre",
            round(sum(col("objectif_medium_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_premium_trimestre",
            round(sum(col("objectif_premium_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_pro_tpe_trimestre",
            round(sum(col("objectif_pro_tpe_hebdo")).over(window_spec_trimestre), 2),
        )
        .withColumn(
            "objectif_clients_actifs_trimestre",
            round(
                sum(col("objectif_clients_actifs_hebdo")).over(
                    window_spec_trimestre
                ),
                2,
            ),
        )
        .orderBy("code_fdc", "numero_semaine")
        .select(
            col("code_fdc").alias("fdc_gest"),
            "annee",
            "dds",
            "dfs",
            col("numero_semaine").alias("semaine"),
            "mois",
            "trimestre",
            "objectif_conquete_annuel",
            "objectif_conquete_hebdo",
            "objectif_conquete_mensuel",
            "objectif_conquete_trimestre",
            "objectif_mass_market_annuel",
            "objectif_mass_market_hebdo",
            "objectif_mass_market_mensuel",
            "objectif_mass_market_trimestre",
            "objectif_medium_annuel",
            "objectif_medium_hebdo",
            "objectif_medium_mensuel",
            "objectif_medium_trimestre",
            "objectif_premium_annuel",
            "objectif_premium_hebdo",
            "objectif_premium_mensuel",
            "objectif_premium_trimestre",
            "objectif_clients_actifs_annuel",
            "objectif_clients_actifs_hebdo",
            "objectif_clients_actifs_mensuel",
            "objectif_clients_actifs_trimestre",
            "objectif_pro_tpe_annuel",
            "objectif_pro_tpe_hebdo",
            "objectif_pro_tpe_mensuel",
            "objectif_pro_tpe_trimestre",
        )
    )


def build_fdc(
    objectifs_tro: DataFrame,
    information_tier: DataFrame,
    information_compte_client: DataFrame,
    donnee_client: DataFrame,
    contrat_produit: DataFrame,
    niveau_regroupement: DataFrame,
    rel_package_contrat: DataFrame,
    ressource_emploi: DataFrame,
    partition_date: str,
) -> DataFrame:
    MY_CDM_PRODUCT_CODE = ["CN01"]

    information_tier = select_information_tier(information_tier)

    information_compte_client = select_information_compte_client(
        information_compte_client
    )

    infos_comptes_tiers = information_tier.join(
        information_compte_client, ["numerotiers", "partitiondate"], "left"
    )

    donnee_client = select_donnee_client(donnee_client)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    donnees_clients = donnee_client.join(
        infos_comptes_tiers, ["partitiondate", "NUMEROTIERS"], "left"
    )

    donnees_clients = donnees_clients.withColumnRenamed(
        "numerotiers", "numero_tiers"
    ).withColumnRenamed("numerodecompte", "numero_compte")

    donnnes_clients_infos = (
        donnees_clients.withColumn("date_naissance", to_date(col("date_naissance")))
        .withColumn("date_traitement", to_date(col("date_traitement")))
        .withColumn(
            "AGE",
            when(
                col("date_naissance").isNotNull(),
                months_between(col("date_traitement"), col("date_naissance")) / 12,
            ),
        )
        .withColumn(
            "is_client_account_adult",
            when(
                col("date_naissance").isNotNull(),
                (
                    (
                        months_between(col("date_ouverture"), col("date_naissance"))
                        / 12
                    )
                    >= 18
                ),
            ).otherwise(lit(True)),
        )
    )

    contrat_produit = contrat_produit.select(
        col("numerotiers").alias("numero_tiers"),
        col("numerodecompte").alias("numero_compte"),
        col("codeproduit").alias("code_produit"),
        "partitiondate",
    )

    ressource_emploi = (
        ressource_emploi.filter(
            (col("categorie") == "EMPLOI") & (col("soldejourdb_devise") > 0)
        )
        .withColumn("is_engaged", lit(True))
        .select(
            col("numerotiers").alias("numero_tiers"),
            col("is_engaged"),
        )
        .drop_duplicates()
    )

    comptes_produits_df = donnnes_clients_infos.join(
        contrat_produit, ["numero_tiers", "numero_compte", "partitiondate"], "left"
    )

    window = Window.partitionBy("numero_tiers", "partitiondate")

    rel_package_contrat = (
        rel_package_contrat.select("typecontrat")
        .drop_duplicates()
        .withColumn("is_pack", lit(1))
        .withColumnRenamed("typecontrat", "code_produit")
        .select("code_produit", "is_pack")
    )

    comptes_produits_df = (
        comptes_produits_df.join(rel_package_contrat, ["code_produit"], "left")
        .na.fill({"is_pack": 0})
        .withColumn("is_pack", max(col("is_pack")).over(window))
        .withColumn(
            "is_mycdm", col("CODE_PRODUIT").isin(MY_CDM_PRODUCT_CODE).cast("integer")
        )
        .withColumn("is_mycdm", max(col("is_mycdm")).over(window))
        .withColumn("is_pack_and_mycdm", (col("is_pack") + col("is_mycdm")) >= 2)
    )

    comptes_produits_df = comptes_produits_df.join(
        niveau_regroupement, ["code_agence", "partitiondate"], "left"
    )

    comptes_produits = (
        comptes_produits_df.withColumn(
            "date_traitement", to_date(col("partitiondate"))
        )
        .withColumn("date_ouverture", to_date(col("date_ouverture")))
        .withColumn("date_cloture", to_date(col("date_cloture")))
        .withColumn("annee", year(col("partitiondate")))
        .withColumn("trimestre", quarter(col("partitiondate")))
        .withColumn("mois", month(col("partitiondate")))
        .withColumn("semaine", weekofyear(col("partitiondate")))
        .join(
            objectifs_tro.drop("mois", "trimestre"),
            ["fdc_gest", "semaine", "annee"],
            how="left",
        )
    )
    comptes_produits = comptes_produits.join(
        ressource_emploi, ["numero_tiers"], "left"
    )
    df_kpi_final = get_fdc_kpi(comptes_produits, partition_date)

    return df_kpi_final
