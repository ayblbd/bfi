from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    concat_ws,
    crc32,
    date_format,
    lit,
    row_number,
    substring,
    date_format,
    when,
    to_date,
)


def etat_compte_transformation() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    etat_compte = [
        (6, "Bloqué"),
        (0, "Ouvert,non confirmé"),
        (8, "Demandé à la cloture"),
        (7, "A Arrêter"),
        (4, "A surveiller"),
        (5, "A clore"),
        (1, "Ouvert Confirmé"),
        (2, "Candidat à la cloture"),
        (3, "Clôturé"),
    ]

    schema_etat = ["code_etatcompte", "libelle_etatcompte"]

    return spark.createDataFrame(etat_compte, schema_etat)


def compte_transformation(
    compte: DataFrame,
    elt_contrat: DataFrame,
    titulaire: DataFrame,
    compte_commercial: DataFrame,
    fdccompte: DataFrame,
    fdcgcompte_ctx: DataFrame,
    t605: DataFrame,
    donnee_compte: DataFrame,
    fdctiers: DataFrame,
    partitiondate: str,
) -> DataFrame:
    # Filtering DataFrames with target data
    titulaire_filtered = titulaire.filter(col("titulaireprincipal") == "1").select(
        col("numerocontratinterne"), col("numerotiers")
    )

    t605_filtered = t605.filter(
        (col("dfval") > partitiondate) & (col("typmar") == "C")
    ).select(col("coddev"), col("irdevi"))

    window = Window.partitionBy("numerodecompte").orderBy(col("dtmodif").desc())

    fdccompte_filtered = (
        fdccompte.filter(col("typefdccompte") == "2")
        .withColumn(
            "fk_fondsdecommerce",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdccompte"))),
        )
        .withColumn("is_fdc", lit(True))
        .select(col("numerodecompte"), col("fk_fondsdecommerce"), col("is_fdc"))
    )

    fdcgcompte_ctx_filtered = (
        fdcgcompte_ctx.withColumn("rn", row_number().over(window))
        .filter(col("rn") == "1")
        .filter(col("typefdccompte") == "2")
        .withColumn(
            "fk_fondsdecommerce_ctx",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdccompte"))),
        )
        .withColumn("is_ctx", lit(True))
        .select(col("numerodecompte"), col("fk_fondsdecommerce_ctx"), col("is_ctx"))
    )

    elt_contrat_filtered = elt_contrat.filter(col("typedeproduit") == "CA03").select(
        col("numeroelementinterne"), col("numerocontratinterne"), col("dateeffet")
    )
    compte = compte.withColumn(
        "naturecompte", substring(col("numerodecompte"), 4, 3)
    ).select(
        col("numerodecompte"),
        col("devise"),
        col("naturecompte"),
        col("etatcompte"),
        col("numeroelementinterne"),
    )
    compte_commercial = compte_commercial.filter(
        col("representantetranger").isNotNull()
    ).select(
        col("numeroelementinterne"), col("representantetranger"), col("intitule")
    )

    donnee_compte = donnee_compte.select(col("numerodecompte"), col("statutcompte"))
    fdctiers = (
        fdctiers.filter(col("typefdctiers") == "2")
        .withColumn(
            "fk_fondsdecommerce_tiers",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdctiers"))),
        )
        .select(col("numerotiers"), col("fk_fondsdecommerce_tiers"))
    )

    # Define join conditions
    join_compte_elt_contrat = ["numeroelementinterne"]
    join_elt_contrat_contrat = ["numerocontratinterne"]
    join_fdccompte = ["numerodecompte"]
    join_compte_devise = compte["devise"] == t605["irdevi"]
    join_tiers = ["numerotiers"]

    compte_processed = (
        compte.join(fdccompte_filtered, join_fdccompte, "left")
        .join(fdcgcompte_ctx_filtered, join_fdccompte, "left")
        .join(donnee_compte, join_fdccompte, "left")
        .join(elt_contrat_filtered, join_compte_elt_contrat, "left")
        .join(titulaire_filtered, join_elt_contrat_contrat, "left")
        .join(compte_commercial, join_compte_elt_contrat, "left")
        .join(broadcast(t605_filtered), join_compte_devise, "left")
        .join(fdctiers, join_tiers, "left")
        .withColumn(
            "fk_fondsdecommerce_merge",
            when(col("is_ctx"), col("fk_fondsdecommerce_ctx")).otherwise(
                col("fk_fondsdecommerce_tiers")
            ),
        )
        .withColumn(
            "pk_compte",
            concat_ws("", col("numerodecompte"), coalesce(col("coddev"), lit("0"))),
        )
        .withColumn(
            "date_ouverture_compte",
            date_format(col("dateeffet"), "yyyy-MM-dd"),
        )
        .withColumnRenamed("coddev", "code_devise")
        .withColumnRenamed("etatcompte", "code_etatcompte")
        .withColumn("typecompte", lit("origine"))
        .withColumn("numerocompteorigine", col("numerodecompte"))
        .select(
            col("numerodecompte"),
            col("numerocompteorigine"),
            col("numerotiers"),
            col("numeroelementinterne"),
            col("code_devise"),
            col("code_etatcompte"),
            col("naturecompte"),
            col("intitule"),
            col("statutcompte"),
            col("representantetranger"),
            col("fk_fondsdecommerce_ctx"),
            col("fk_fondsdecommerce"),
            col("pk_compte"),
            col("typecompte"),
            col("date_ouverture_compte"),
            col("fk_fondsdecommerce_merge"),
        )
        .fillna(-1)
    )
    return compte_processed


def devise_transformation(t605: DataFrame) -> DataFrame:
    devise_processed = (
        t605.withColumnRenamed("coddev", "code_devise")
        .select(col("code_devise"), col("irdevi"))
        .dropDuplicates()
    )
    return devise_processed


def cpttenu_transformation(
    cpt_tenu: DataFrame,
    t605: DataFrame,
    compte: DataFrame,
    elt_contrat: DataFrame,
    titulaire: DataFrame,
    fdccompte: DataFrame,
    fdcgcompte_ctx: DataFrame,
    fdctiers: DataFrame,
    partitiondate,
) -> DataFrame:
    t605_filtered = t605.filter(
        (col("dfval") > partitiondate) & (col("typmar") == "C")
    ).select(col("coddev"), col("irdevi"))

    elt_contrat_filtered = elt_contrat.filter(col("typedeproduit") == "CA03").select(
        col("numeroelementinterne"), col("numerocontratinterne"), col("dateeffet")
    )
    titulaire_filtered = titulaire.filter(col("titulaireprincipal") == "1").select(
        col("numerocontratinterne"), col("numerotiers")
    )
    compte = compte.withColumnRenamed(
        "numerodecompte", "numerocompteorigine"
    ).select(col("numerocompteorigine"), col("numeroelementinterne"))

    window = Window.partitionBy("numerodecompte").orderBy(col("dtmodif").desc())
    fdccompte_filtered = (
        fdccompte.filter(col("typefdccompte") == "2")
        .withColumn(
            "fk_fondsdecommerce",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdccompte"))),
        )
        .withColumnRenamed("numerodecompte", "numerocompteorigine")
        .select(col("numerocompteorigine"), col("fk_fondsdecommerce"))
    )

    fdcgcompte_ctx_filtered = (
        fdcgcompte_ctx.withColumn("rn", row_number().over(window))
        .filter(col("rn") == "1")
        .filter(col("typefdccompte") == "2")
        .withColumn(
            "fk_fondsdecommerce_ctx",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdccompte"))),
        )
        .withColumn("is_ctx", lit(True))
        .withColumnRenamed("numerodecompte", "numerocompteorigine")
        .select(
            col("numerocompteorigine"), col("fk_fondsdecommerce_ctx"), col("is_ctx")
        )
    )

    cpt_tenu = (
        cpt_tenu.withColumn(
            "devisecompteorigine",
            when(col("devisecompteorigine") == "000", "MAD").otherwise(
                col("devisecompteorigine")
            ),
        )
        .select(
            col("devisecompteorigine"),
            col("numerodecompte"),
            col("numerocompteorigine"),
        )
        .drop_duplicates()
    )

    fdctiers = (
        fdctiers.filter(col("typefdctiers") == "2")
        .withColumn(
            "fk_fondsdecommerce_tiers",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdctiers"))),
        )
        .select(col("numerotiers"), col("fk_fondsdecommerce_tiers"))
    )

    join_devise = cpt_tenu["devisecompteorigine"] == t605["irdevi"]
    join_compte = ["numerocompteorigine"]
    join_compte_elt_contrat = ["numeroelementinterne"]
    join_elt_contrat_contrat = ["numerocontratinterne"]
    join_fdc = ["numerocompteorigine"]
    join_fdctiers = ["numerotiers"]
    cpt_tenu_processed = (
        cpt_tenu.withColumn("naturecompte", substring(col("numerodecompte"), 4, 3))
        .filter(col("naturecompte") > "299")
        .join(broadcast(t605_filtered), join_devise, "left")
        .join(compte, join_compte, "left")
        .join(elt_contrat_filtered, join_compte_elt_contrat, "left")
        .join(titulaire_filtered, join_elt_contrat_contrat, "left")
        .join(fdccompte_filtered, join_fdc, "left")
        .join(fdcgcompte_ctx_filtered, join_fdc, "left")
        .join(fdctiers, join_fdctiers, "left")
        .withColumn(
            "pk_compte",
            concat_ws("", col("numerodecompte"), coalesce(col("coddev"), lit("0"))),
        )
        .withColumn(
            "fk_fondsdecommerce_merge",
            when(col("is_ctx"), col("fk_fondsdecommerce_ctx")).otherwise(
                col("fk_fondsdecommerce_tiers")
            ),
        )
        .withColumn(
            "date_ouverture_compte",
            date_format(col("dateeffet"), "yyyy-MM-dd"),
        )
        .withColumnRenamed("coddev", "code_devise")
        .withColumn("typecompte", lit("tenu"))
        .withColumn("numeroelementinterne", lit("-1"))
        .withColumn("code_etatcompte", lit("-1"))
        .withColumn("intitule", lit("-1"))
        .withColumn("representantetranger", lit("-1"))
        .withColumn("statutcompte", lit("-1"))
        .select(
            col("numerodecompte"),
            col("numerocompteorigine"),
            col("numerotiers"),
            col("numeroelementinterne"),
            col("code_devise"),
            col("code_etatcompte"),
            col("naturecompte"),
            col("intitule"),
            col("statutcompte"),
            col("representantetranger"),
            col("fk_fondsdecommerce_ctx"),
            col("fk_fondsdecommerce"),
            col("pk_compte"),
            col("typecompte"),
            col("date_ouverture_compte"),
            col("fk_fondsdecommerce_merge"),
        )
        .na.fill("-1")
    )
    return cpt_tenu_processed


def ressource_emploi_transformation(ressource_emploi: DataFrame) -> DataFrame:
    ressource_emploi_processed = (
        ressource_emploi.withColumn(
            "categorie",
            when(col("categorie") == "EMP", lit("EMPLOI")).when(
                col("categorie") == "RESS", lit("RESSOURCE")
            ),
        )
        .withColumn(
            "pk_mapressourceemploi",
            concat_ws("", col("pci"), col("nature"), crc32(col("categorie"))),
        )
        .select(
            col("pk_mapressourceemploi"),
            col("pci"),
            col("nature"),
            col("type_ressource_emploi"),
            col("sens_mapping"),
            col("categorie"),
            col("famille_tdb_banque"),
            col("libelle_type"),
        )
        .drop_duplicates()
        .na.fill(-1)
    )
    return ressource_emploi_processed


def compte_sld_transformation(
    sld_cli: DataFrame,
    compte_processed: DataFrame,
    cpt_tenu_processed: DataFrame,
):
    sld_cli_filtred = sld_cli.withColumn(
        "code_devise", substring(col("devise"), 1, 3)
    ).select(col("numerodecompte"), col("code_devise"))
    compte_tenu_filtred = cpt_tenu_processed.select(
        col("numerodecompte"), col("code_devise")
    )
    compte_filtred = (
        compte_processed.withColumnRenamed("code_devise", "devise_origine")
        .select(
            col("numerodecompte"), col("numeroelementinterne"), col("devise_origine")
        )
        .withColumnRenamed("numerodecompte", "numerocompteorigine")
    )

    join_clause_tenu = ["numerodecompte", "code_devise"]
    join_clause_compte = (
        sld_cli_filtred["numerodecompte"] == compte_filtred["numerocompteorigine"]
    ) & (sld_cli_filtred["code_devise"] == compte_filtred["devise_origine"])

    sld_cli_processed = (
        sld_cli_filtred.join(compte_tenu_filtred, join_clause_tenu, "left")
        .filter(compte_tenu_filtred["numerodecompte"].isNull())
        .join(compte_filtred, join_clause_compte, "left")
        .filter(compte_filtred["numerocompteorigine"].isNull())
        .withColumn("naturecompte", substring(col("numerodecompte"), 4, 3))
        .withColumn(
            "pk_compte",
            concat_ws(
                "", col("numerodecompte"), coalesce(col("code_devise"), lit("0"))
            ),
        )
        .withColumn("numerocompteorigine", lit("-1"))
        .withColumn("numerotiers", lit("-1"))
        .withColumn("typecompte", lit("-1"))
        .withColumn("fk_fondsdecommerce", lit(-1))
        .withColumn("fk_fondsdecommerce_ctx", lit(-1))
        .withColumn("fk_fondsdecommerce_merge", lit(-1))
        .withColumn("numeroelementinterne", lit("-1"))
        .withColumn("code_etatcompte", lit("-1"))
        .withColumn("intitule", lit("-1"))
        .withColumn("representantetranger", lit("-1"))
        .withColumn("statutcompte", lit("-1"))
        .withColumn("date_ouverture_compte", lit("-1"))
        .select(
            col("numerodecompte"),
            col("numerocompteorigine"),
            col("numerotiers"),
            col("numeroelementinterne"),
            col("code_devise"),
            col("code_etatcompte"),
            col("naturecompte"),
            col("intitule"),
            col("statutcompte"),
            col("representantetranger"),
            col("fk_fondsdecommerce_ctx"),
            col("fk_fondsdecommerce"),
            col("pk_compte"),
            col("typecompte"),
            col("date_ouverture_compte"),
            col("fk_fondsdecommerce_merge"),
        )
        .dropDuplicates()
    )

    return sld_cli_processed


def compte_origine_transformation(compte: DataFrame) -> DataFrame:
    compte_processed = (
        compte.filter(col("typecompte") == "origine")
        .select(
            col("numerocompteorigine"),
            col("numerotiers"),
            col("numeroelementinterne"),
            col("code_devise"),
            col("code_etatcompte"),
            col("naturecompte"),
            col("intitule"),
            col("statutcompte"),
            col("representantetranger"),
            col("fk_fondsdecommerce_ctx"),
            col("fk_fondsdecommerce"),
            col("pk_compte"),
            col("date_ouverture_compte"),
        )
        .dropDuplicates()
    )
    return compte_processed
