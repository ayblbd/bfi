from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    concat_ws,
    crc32,
    date_format,
    lit,
    max,
    min,
    months_between,
    substring,
    upper,
    when,
)


def tiers_client_joins(tiers_df: DataFrame, client_df: DataFrame) -> DataFrame:
    join_clause = ["numerotiers"]
    tier_join = client_df.join(tiers_df, join_clause, "left")
    return tier_join.cache()


def tiers_transformation(
    tier_client_joins_df: DataFrame,
    segmentation_processed: DataFrame,
    marche_processed_intermediaire: DataFrame,
    fdctiers: DataFrame,
    donnee_client: DataFrame,
    compte: DataFrame,
    elt_contrat: DataFrame,
    titulaire: DataFrame,
    personne_physique: DataFrame,
    csp: DataFrame,
    partitiondate: str,
) -> DataFrame:
    fdctiers_transformation = fdctiers.filter(col("typefdctiers") == 2).withColumn(
        "fk_fondsdecommerce",
        crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdctiers"))),
    )

    elt_contrat_filtered = elt_contrat.filter(col("typedeproduit") == "CA03").select(
        col("numeroelementinterne"), col("dateeffet"), col("numerocontratinterne")
    )

    titulaire_filtered = titulaire.filter(col("titulaireprincipal") == "1").select(
        col("numerotiers"), col("numerocontratinterne")
    )

    personne_physique = personne_physique.select("numerotiers", "categoriecsp")

    csp = csp.filter(col("dfval") > partitiondate).select("cdcsp", "libelcsp")
    donnee_client = donnee_client.select(
        col("numerotiers"), col("statutclient")
    ).dropDuplicates()

    join_clause_seg = ["new_seg", "new_sous_seg"]
    join_clause_marche = ["numerotiers"]
    join_clause_fdctiers = ["numerotiers"]
    join_compte_elt_contrat = ["numeroelementinterne"]
    join_titulaire_contrat = ["numerocontratinterne"]
    join_clause_csp = personne_physique["categoriecsp"] == csp["cdcsp"]

    compte_filtered = (
        compte.withColumn("naturecompte", substring(col("numerodecompte"), 4, 3))
        .join(elt_contrat_filtered, join_compte_elt_contrat, "left")
        .join(titulaire_filtered, join_titulaire_contrat, "left")
        .withColumn("is_mre", col("naturecompte").isin("006", "014").cast("integer"))
        .groupBy(col("numerotiers"))
        .agg(
            min(col("dateeffet")).alias("dateeffet"),
            max(col("is_mre")).alias("is_mre"),
        )
        .select("numerotiers", "dateeffet", "is_mre")
        .drop_duplicates()
    )

    tiers_processed = (
        tier_client_joins_df.join(compte_filtered, join_clause_marche, "left")
        .join(personne_physique, join_clause_marche, "left")
        .join(broadcast(csp), join_clause_csp, "left")
        .join(broadcast(segmentation_processed), join_clause_seg, "left")
        .join(marche_processed_intermediaire, join_clause_marche, "left")
        .join(fdctiers_transformation, join_clause_fdctiers, "left")
        .join(donnee_client, join_clause_fdctiers, "left")
        .withColumn(
            "is_client_account_adult",
            when(
                col("datedenaissance").isNotNull(),
                (
                    (months_between(col("dateeffet"), col("datedenaissance")) / 12)
                    >= 18
                ),
            ).otherwise(lit(True)),
        )
        .withColumn(
            "date_ouverture_premier_compte",
            when(
                col("is_client_account_adult"),
                date_format(col("dateeffet"), "yyyy-MM-dd"),
            ),
        )
        .withColumn(
            "lebelle_ppm",
            when(col("codepppm") == "2", lit("PPH"))
            .when(col("codepppm") == "1", lit("PMO"))
            .when(col("codepppm") == "3", lit("PRO")),
        )
        .withColumnRenamed("categoriecsp", "code_csp")
        .withColumnRenamed("libelcsp", "libelle_csp")
        .withColumnRenamed("dtcreat", "date_creation")
        .select(
            col("numerotiers"),
            col("nomreduit"),
            col("datemajkyc"),
            col("clientbp"),
            col("lieukyc"),
            col("codepppm"),
            col("lebelle_ppm"),
            col("typeidentite"),
            col("numeroidentite"),
            col("nationalite"),
            col("agenteconomique"),
            col("datefininterdcompte"),
            col("zonederesidence"),
            col("paysderesidence"),
            col("code_segment"),
            col("qualite"),
            col("americain"),
            col("dateprochmajkyc"),
            col("code_marche"),
            col("codefondsdecommerce"),
            col("statutclient"),
            col("fk_fondsdecommerce"),
            col("code_csp"),
            col("libelle_csp"),
            col("is_mre"),
            col("date_creation"),
            col("date_ouverture_premier_compte"),
        )
        .fillna(-1)
    )

    return tiers_processed


def segmentation_transformation(
    marche_processed_intermediaire: DataFrame,
    segmentation: DataFrame,
    sous_segmentation: DataFrame,
    partitiondate: str,
) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    defaut_value = [(-1, -1, -1, -1, -1)]
    schema_segmentation = [
        "new_seg",
        "new_sous_seg",
        "gamme",
        "libellesegment",
        "libellesoussegment",
    ]
    df_default_value = spark.createDataFrame(defaut_value, schema_segmentation)

    join_segment = ["new_seg"]
    join_sous_segment = ["new_sous_seg"]

    segmentation = (
        segmentation.filter(col("dfval") > partitiondate)
        .withColumnRenamed("codesegment", "new_seg")
        .select("new_seg", "libellesegment")
    )

    sous_segmentation = (
        sous_segmentation.filter(col("dfval") > partitiondate)
        .withColumnRenamed("codesoussegment", "new_sous_seg")
        .withColumnRenamed("libsoussegment", "libellesoussegment")
        .select("new_sous_seg", "libellesoussegment")
    )

    segmentation_processed = (
        marche_processed_intermediaire.join(segmentation, join_segment, "left")
        .join(sous_segmentation, join_sous_segment, "left")
        .filter(~col("gamme").isin("GE", "PME", "PIM"))
        .select(
            col("new_seg"),
            col("new_sous_seg"),
            col("gamme"),
            col("libellesegment"),
            col("libellesoussegment"),
        )
        .dropDuplicates()
        .dropna()
        .union(df_default_value)
        .withColumn(
            "code_segment",
            crc32(concat_ws("/", col("new_seg"), col("new_sous_seg"))),
        )
        .select(
            col("new_seg"),
            col("new_sous_seg"),
            col("gamme"),
            col("libellesegment"),
            col("libellesoussegment"),
            col("code_segment"),
        )
    )

    return segmentation_processed.cache()


def marche_transformation(
    tier_client_join: DataFrame,
    marche_mapping_pme_ge: DataFrame,
    marche_mapping_pro_retail: DataFrame,
    fdctiers: DataFrame,
) -> DataFrame:
    marche_mapping_pro_retail_seg = (
        marche_mapping_pro_retail.withColumnRenamed("marche", "marche_seg")
        .withColumnRenamed("libelle_marche", "libelle_marche_seg")
        .withColumnRenamed("gamme", "gamme_seg")
        .select(
            col("new_seg"),
            col("marche_seg"),
            col("libelle_marche_seg"),
            col("gamme_seg"),
        )
        .dropDuplicates()
    )

    marche_mapping_pro_retail_sous_seg = (
        marche_mapping_pro_retail.withColumnRenamed("marche", "marche_sous_seg")
        .withColumnRenamed("libelle_marche", "libelle_marche_sous_seg")
        .withColumnRenamed("gamme", "gamme_sous_seg")
        .select(
            col("new_sous_seg"),
            col("marche_sous_seg"),
            col("libelle_marche_sous_seg"),
            col("gamme_sous_seg"),
        )
        .dropDuplicates()
    )

    join_clause_fdc = ["numerotiers"]

    fdctiers = fdctiers.filter(col("typefdctiers") == "2")

    tiers_lieux = tier_client_join.join(fdctiers, join_clause_fdc, "left").select(
        tier_client_join["numerotiers"],
        fdctiers["lieufdctiers"],
        tier_client_join["new_seg"],
        tier_client_join["new_sous_seg"],
    )

    join_clause_ge_pme = (
        tiers_lieux["lieufdctiers"] == marche_mapping_pme_ge["code_lieu_gest"]
    )
    join_clause_pro_retail_seg = ["new_seg"]
    join_clause_pro_retail_sous_seg = ["new_sous_seg"]

    marche = (
        tiers_lieux.join(
            broadcast(marche_mapping_pme_ge), join_clause_ge_pme, "left"
        )
        .join(
            broadcast(marche_mapping_pro_retail_seg),
            join_clause_pro_retail_seg,
            "left",
        )
        .join(
            broadcast(marche_mapping_pro_retail_sous_seg),
            join_clause_pro_retail_sous_seg,
            "left",
        )
        .withColumn("gamme_ge", col("marche"))
        .withColumn(
            "marche",
            upper(
                coalesce(
                    when(
                        col("code_lieu_gest").isNotNull()
                        & (col("gamme_seg") == "TPE"),
                        col("marche_seg"),
                    ),
                    col("marche"),
                    col("marche_seg"),
                    col("marche_sous_seg"),
                    lit("RETAIL"),
                )
            ),
        )
        .withColumn(
            "libelle_marche",
            upper(
                coalesce(
                    when(
                        col("code_lieu_gest").isNotNull()
                        & (col("gamme_seg") == "TPE"),
                        col("libelle_marche_seg"),
                    ),
                    col("libelle_marche"),
                    col("libelle_marche_seg"),
                    col("libelle_marche_sous_seg"),
                    lit("Particulier"),
                )
            ),
        )
        .withColumn(
            "gamme",
            upper(
                coalesce(
                    when(
                        col("code_lieu_gest").isNotNull()
                        & (col("gamme_seg") == "TPE"),
                        col("gamme_seg"),
                    ),
                    col("gamme_ge"),
                    col("gamme_seg"),
                    col("gamme_sous_seg"),
                    lit("Non Segmenté"),
                )
            ),
        )
        .withColumn("code_marche", crc32(col("marche")))
        .select(
            col("code_marche"),
            col("numerotiers"),
            col("marche"),
            col("libelle_marche"),
            col("gamme"),
            tiers_lieux["new_seg"],
            tiers_lieux["new_sous_seg"],
        )
    )

    return marche.persist()


def marche_transformation_alim(
    marche_processed_intermediaire: DataFrame,
) -> DataFrame:
    return (
        marche_processed_intermediaire.select(
            col("code_marche"), col("marche"), col("libelle_marche")
        )
        .dropDuplicates(["marche"])
        .na.fill(-1)
    )
