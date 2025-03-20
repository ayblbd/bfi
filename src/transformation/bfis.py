from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    crc32,
    first,
    lit,
    lpad,
    regexp_replace,
    row_number,
    rpad,
    substring,
    to_date,
    when,
)

from src.transformation.common import (
    get_last_recorded_sold_per_client_from,
    get_last_recorded_sold_per_client_from_last_month,
    get_last_recorded_sold_per_client_from_last_quarter,
    get_last_recorded_sold_per_client_from_last_week,
    get_last_recorded_sold_per_client_from_last_year,
    get_last_recorded_sold_per_client_from_same_day_last_year,
    map_account_balance,
)


def build_ancien_mapping_type_ressource_emploi(
    capjour_param2_oe: DataFrame,
) -> DataFrame:
    capjour_param2_oe = capjour_param2_oe.withColumn("pci", rpad(col("pci"), 6, "0"))
    # First part of the UNION - Working with LIGNE_CREDIT
    part1 = capjour_param2_oe.filter(col("LIGNE_CREDIT").isNotNull()).select(
        col("PCI"),
        col("NATURE"),
        col("LIGNE_CREDIT").alias("CODE_LIGNE"),
        lit("RESS").alias("CATEGORIE"),
        when(substring(col("LIGNE_CREDIT"), 1, 2) == "50", "R.BAM/TRES/CCP")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "51", "DET/EC")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "52", "DET/SF")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "61", "RAV")
        .when(substring(col("LIGNE_CREDIT"), 1, 3).isin(["624", "627"]), "REP")
        .when(substring(col("LIGNE_CREDIT"), 1, 3).isin(["625", "626"]), "RAT")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "65", "DEP.DIV")
        .when(substring(col("LIGNE_CREDIT"), 1, 2).isin(["67", "68"]), "DEP.DIV")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "73", "CD")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "10", "E.BAM/TRES/CCP")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "11", "CAV/EC")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "12", "CAV/EF")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "21", "CAV")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "22", "TRESO")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "23", "EQUIPI")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "24", "CONS")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "25", "IMMO")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "27", "DEPOTS DIV")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "28", "CRE.DIV")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "29", "CRE.SOUF")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "31", "TITRES")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "91", "HB.ENG/EC")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "92", "HB.ENG/CLI")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "95", "HB.AVAL/EC")
        .when(substring(col("LIGNE_CREDIT"), 1, 2) == "96", "HB.CAUT.AVAL")
        .otherwise("AUTRES")
        .alias("Type_RESS_EMPO"),
    )

    # Second part of the UNION - Working with LIGNE_Debit
    part2 = capjour_param2_oe.filter(col("LIGNE_Debit").isNotNull()).select(
        col("PCI"),
        col("NATURE"),
        col("LIGNE_Debit").alias("CODE_LIGNE"),
        lit("EMP").alias("CATEGORIE"),
        when(substring(col("LIGNE_Debit"), 1, 2) == "50", "R.BAM/TRES/CCP")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "51", "DET/EC")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "52", "DET/SF")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "61", "RAV")
        .when(substring(col("LIGNE_Debit"), 1, 3).isin(["624", "627"]), "REP")
        .when(substring(col("LIGNE_Debit"), 1, 3).isin(["625", "626"]), "RAT")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "65", "DEP.DIV")
        .when(substring(col("LIGNE_Debit"), 1, 2).isin(["67", "68"]), "DEP.DIV")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "73", "CD")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "10", "E.BAM/TRES/CCP")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "11", "CAV/EC")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "12", "CAV/EF")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "21", "CAV")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "22", "TRESO")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "23", "EQUIPI")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "24", "CONS")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "25", "IMMO")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "27", "DEPOTS DIV")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "28", "CRE.DIV")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "29", "CRE.SOUF")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "31", "TITRES")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "91", "HB.ENG/EC")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "92", "HB.ENG/CLI")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "95", "HB.AVAL/EC")
        .when(substring(col("LIGNE_Debit"), 1, 2) == "96", "HB.CAUT.AVAL")
        .otherwise("AUTRES")
        .alias("Type_RESS_EMPO"),
    )

    # Union the two parts
    union_df = part1.union(part2)
    return union_df.distinct()


def build_solde_client_devise(
    dm_sldcli: DataFrame, rp_gl_bct: DataFrame, t601: DataFrame
) -> DataFrame:
    window_spec = Window.partitionBy("NACMPT").orderBy(col("DFVAL").desc())
    exp_t601_processed = (
        t601.withColumn("RUBRIC", first("RUBRIC").over(window_spec))
        .select("NACMPT", "RUBRIC")
        .distinct()
    )

    joined_df = (
        dm_sldcli.join(
            rp_gl_bct,
            dm_sldcli["NUMERODECOMPTE"] == rp_gl_bct["COMPTE_CLIENT"],
            how="left",
        )
        .withColumn(
            "NATURE_EXTERNE",
            coalesce(
                rp_gl_bct["nature_externe"],
                substring(dm_sldcli["NUMERODECOMPTE"], 4, 3),
            ),
        )
        .join(
            exp_t601_processed,
            col("NATURE_EXTERNE") == exp_t601_processed["NACMPT"],
            how="left",
        )
    )

    return (
        joined_df.withColumn("PCI", rpad(substring(col("RUBRIC"), 3, 5), 6, "0"))
        .withColumn(
            "TYPE",
            when(
                substring(col("RUBRIC"), 3, 5).isin(
                    "11130", "11140", "11211", "11212", "46110", "46210", "46613"
                ),
                lit("BILLET"),
            ).otherwise(lit("VIREMENT")),
        )
        .withColumn("SOLDEJOURCR", dm_sldcli["SOLDEJOURCR"].cast("decimal(38, 4)"))
        .withColumn("SOLDEJOURDB", dm_sldcli["SOLDEJOURDB"].cast("decimal(38, 4)"))
        .select(
            dm_sldcli["DTTRTM"],
            col("PCI"),
            joined_df["NATURE_EXTERNE"],
            dm_sldcli["NUMERODECOMPTE"],
            dm_sldcli["DEVISE"],
            col("TYPE"),
            col("SOLDEJOURCR"),
            col("SOLDEJOURDB"),
        )
    )


def build_taux_change_bbe_bam(
    cours_bbe_bam: DataFrame,
) -> DataFrame:
    return cours_bbe_bam.withColumn(
        "moyen_par_unite",
        (col("venteClientele") + col("achatClientele") / 2) / col("uniteDevise"),
    ).select(
        "libDevise",
        "venteClientele",
        "uniteDevise",
        "achatClientele",
        "moyen_par_unite",
        "date",
        "partitiondate",
    )


def build_taux_change_virement_bam(
    virement_bam: DataFrame,
) -> DataFrame:
    return virement_bam.withColumn(
        "moyen_par_unite", col("moyen") / col("uniteDevise")
    ).select(
        "libDevise",
        "moyen",
        "uniteDevise",
        "moyen_par_unite",
        "date",
        "partitiondate",
    )


def build_taux_change_bam(
    exploit_ref_devise: DataFrame,
    virement_bam: DataFrame,
    cours_bbe_bam: DataFrame,
) -> DataFrame:
    selected_virement_bam = (
        virement_bam.withColumn("moyen_par_unite", col("moyen") / col("uniteDevise"))
        .withColumnRenamed("date", "date_cours_virement")
        .select(
            "libDevise",
            "moyen_par_unite",
            "date_cours_virement",
        )
    )

    selected_cours_bbe = (
        cours_bbe_bam.withColumn(
            "venteClientele", col("venteClientele") / col("uniteDevise")
        )
        .withColumn("achatClientele", col("achatClientele") / col("uniteDevise"))
        .withColumnRenamed("date", "date_cours_bbe")
        .select(
            "libDevise",
            "venteClientele",
            "achatClientele",
            "date_cours_bbe",
        )
    )

    return (
        exploit_ref_devise.join(
            selected_virement_bam,
            selected_virement_bam["libDevise"] == exploit_ref_devise["CD_DEVISE"],
            "left_outer",
        )
        .join(
            selected_cours_bbe,
            selected_cours_bbe["libDevise"] == exploit_ref_devise["CD_DEVISE"],
            "left_outer",
        )
        .withColumn(
            "devise",
            coalesce(
                selected_virement_bam["libDevise"],
                selected_cours_bbe["libDevise"],
                exploit_ref_devise["CD_DEVISE"],
            ),
        )
        .withColumn(
            "date_cours",
            coalesce(
                selected_virement_bam["date_cours_virement"],
                selected_cours_bbe["date_cours_bbe"],
            ),
        )
        .select(
            exploit_ref_devise["CD_DEV_OPER"],
            col("devise").alias("CODE_DEVISE"),
            col("lb_devise").alias("DEVISE_LIBELLE"),
            col("moyen_par_unite").alias("MIDBAM"),
            col("date_cours"),
            col("achatClientele").alias("COURSMIN"),
            col("venteClientele").alias("COURSMAX"),
            col("partitiondate"),
        )
    )


def build_solde_client(
    sldcli_nature: DataFrame,
    taux_change_bam: DataFrame,
) -> DataFrame:
    return (
        sldcli_nature.join(
            taux_change_bam,
            sldcli_nature["DEVISE"] == taux_change_bam["CD_DEV_OPER"],
            "left_outer",
        )
        .withColumn(
            "CODE_DEVISE",
            when(sldcli_nature["DEVISE"] == lit("000C"), "MAD").otherwise(
                taux_change_bam["CODE_DEVISE"]
            ),
        )
        .withColumn(
            "average_cours",
            when(col("DEVISE") == lit("000C"), 1).otherwise(
                (col("COURSMIN") + col("COURSMAX")) / 2
            ),
        )
        .withColumn(
            "MIDBAM", when(col("DEVISE") == lit("000C"), 1).otherwise(col("MIDBAM"))
        )
        .withColumn(
            "SOLDEJOURCR_DEVISE",
            when(
                col("TYPE") == lit("VIREMENT"),
                col("SOLDEJOURCR") * col("MIDBAM"),
            ).otherwise(col("SOLDEJOURCR") * col("average_cours")),
        )
        .withColumn(
            "SOLDEJOURDB_DEVISE",
            when(
                col("TYPE") == lit("VIREMENT"),
                col("SOLDEJOURDB") * col("MIDBAM"),
            ).otherwise(col("SOLDEJOURDB") * col("average_cours")),
        )
        .select(
            "DTTRTM",
            "PCI",
            "NATURE_EXTERNE",
            "NUMERODECOMPTE",
            "CODE_DEVISE",
            "DEVISE",
            "TYPE",
            "SOLDEJOURCR",
            "SOLDEJOURCR_DEVISE",
            "SOLDEJOURDB",
            "SOLDEJOURDB_DEVISE",
        )
    )


def map_ressource_emploi(df: DataFrame) -> DataFrame:
    return (
        df.transform(
            map_account_balance,
            column_debit="soldejourdb",
            column_credit="soldejourcr",
        )
        .transform(
            map_account_balance,
            column_debit="soldejourdb_devise",
            column_credit="soldejourcr_devise",
        )
        .withColumn(
            "categorie",
            when(col("sens_mapping") == "AA", lit("EMPLOI"))
            .when(col("sens_mapping") == "PP", lit("RESSOURCE"))
            .when(
                (col("sens_mapping") == "AP") & (col("soldejourcr") >= 0),
                lit("RESSOURCE"),
            )
            .when(
                (col("sens_mapping") == "AP") & (col("soldejourdb") >= 0),
                lit("EMPLOI"),
            ),
        )
    )


def selected_nouveau_mapping_ressource_emploi(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("nature", lpad(col("nature"), 3, "0"))
        .withColumnRenamed("sens", "sens_mapping")
        .withColumn(
            "categorie",
            when(col("categorie") == "EMP", lit("EMPLOI")).otherwise(
                lit("RESSOURCE")
            ),
        )
    )


def build_encours_ressource(
    sldcli_nature_daily_devises: DataFrame,
    nouveau_mapping_ressource_emploi: DataFrame,
) -> DataFrame:
    nouveau_mapping_ressource_emploi = selected_nouveau_mapping_ressource_emploi(
        nouveau_mapping_ressource_emploi
    )

    cleaned_mapping = nouveau_mapping_ressource_emploi.select(
        "nature",
        "pci",
        "sens_mapping",
    ).drop_duplicates()

    type_ressource_emploi = nouveau_mapping_ressource_emploi.select(
        "nature",
        "pci",
        "sens_mapping",
        "categorie",
        "type_ressource_emploi",
        "famille_tdb_banque",
        "libelle_type",
    )

    sldci_with_mapping = (
        sldcli_nature_daily_devises.withColumnRenamed("nature_externe", "nature")
        .join(
            cleaned_mapping,
            ["pci", "nature"],
            "left",
        )
        .transform(map_ressource_emploi)
        .join(
            type_ressource_emploi,
            ["pci", "nature", "sens_mapping", "categorie"],
            "left",
        )
    )

    return sldci_with_mapping.filter(
        (col("categorie") == lit("RESSOURCE")) | col("categorie").isNull()
    ).select(
        "dttrtm",
        "pci",
        "nature",
        "numerodecompte",
        "devise",
        "soldejourcr",
        "soldejourdb",
        "soldejourcr_devise",
        "soldejourdb_devise",
        "sens_mapping",
        "type_ressource_emploi",
        "categorie",
        "famille_tdb_banque",
        "libelle_type",
    )


def build_encours_emplois(
    sldcli_nature_daily_devises: DataFrame,
    nouveau_mapping_ressource_emploi: DataFrame,
) -> DataFrame:
    nouveau_mapping_ressource_emploi = selected_nouveau_mapping_ressource_emploi(
        nouveau_mapping_ressource_emploi
    )

    cleaned_mapping = nouveau_mapping_ressource_emploi.select(
        "nature",
        "pci",
        "sens_mapping",
    ).drop_duplicates()

    type_ressource_emploi = nouveau_mapping_ressource_emploi.select(
        "nature",
        "pci",
        "sens_mapping",
        "categorie",
        "type_ressource_emploi",
        "famille_tdb_banque",
        "libelle_type",
    )

    sldci_with_mapping = (
        sldcli_nature_daily_devises.withColumnRenamed("nature_externe", "nature")
        .join(
            cleaned_mapping,
            ["pci", "nature"],
            "left",
        )
        .transform(map_ressource_emploi)
        .join(
            type_ressource_emploi,
            ["pci", "nature", "sens_mapping", "categorie"],
            "left",
        )
    )

    return sldci_with_mapping.filter((col("categorie") == lit("EMPLOI"))).select(
        "dttrtm",
        "pci",
        "nature",
        "numerodecompte",
        "devise",
        "soldejourcr",
        "soldejourdb",
        "soldejourcr_devise",
        "soldejourdb_devise",
        "sens_mapping",
        "type_ressource_emploi",
        "categorie",
        "famille_tdb_banque",
        "libelle_type",
    )


def build_contentieux(
    t705: DataFrame,
    fdcg_compte_ctx: DataFrame,
    niveau_regroupement: DataFrame,
    partition_date: str,
) -> DataFrame:
    window_t705 = Window.partitionBy("rattache", "codefondsdecommerce").orderBy(
        "dtmodif"
    )

    filtered_t705 = (
        t705.filter(col("dfval") > to_date(lit(partition_date)))
        .withColumn("tn", row_number().over(window_t705))
        .filter(col("tn") == 1)
        .withColumnRenamed("rattache", "lieufdccompte")
        .select("lieufdccompte", "codefondsdecommerce", "cdgest")
    )

    window_ctx = Window.partitionBy("numerodecompte").orderBy("dtmodif")

    filtered_fdcg_compte_ctx = (
        fdcg_compte_ctx.withColumn("rn", row_number().over(window_ctx))
        .filter(col("rn") == 1)
        .filter(col("TYPEFDCCOMPTE") == "2")
        .withColumn("is_contentieux", lit(True))
        .select(
            "lieufdccompte",
            "codefondsdecommerce",
            "numerodecompte",
            "dtmodif",
            "typefdccompte",
        )
    )

    ctx_with_fdcgest = (
        filtered_fdcg_compte_ctx.join(
            filtered_t705, ["codefondsdecommerce", "lieufdccompte"], "left"
        )
        .withColumn("etat", lit(True))
        .withColumnRenamed("lieufdccompte", "codeagence_ctx")
        .withColumnRenamed("cdgest", "cdgest_ctx")
        .withColumnRenamed("numerodecompte", "numerocompteorigine")
        .select("numerocompteorigine", "codeagence_ctx", "cdgest_ctx", "etat")
    )

    return ctx_with_fdcgest


def build_ressource_emplois(
    encours_ressource: DataFrame,
    encours_emplois: DataFrame,
    information_compte_tenu_par_client: DataFrame,
    information_compte_client: DataFrame,
    niveau_regroupement: DataFrame,
    contentieux: DataFrame,
    information_tier: DataFrame,
    utilisateur: DataFrame,
) -> DataFrame:
    open_accounts = (
        information_compte_tenu_par_client.filter(
            substring(col("numerodecompte"), 4, 3) >= lit("300")
        )
        .withColumnRenamed("cdfdc_op", "cdfdc_op_compte")
        .withColumnRenamed("agenceop", "agenceop_compte")
        .withColumnRenamed("cdfdc_gest", "cdfdc_gest_compte")
        .withColumnRenamed("agencegest", "agencegest_compte")
        .select(
            "numerotiers",
            "numerodecompte",
            "naturecompte",
            "devise",
            "numerocompteorigine",
            "cdfdc_op_compte",
            "agenceop_compte",
            "cdfdc_gest_compte",
            "agencegest_compte",
            "etat_compte",
        )
    )

    filtered_compte = (
        information_compte_client.withColumn(
            "numerocompteorigine", col("numerodecompte")
        )
        .withColumnRenamed("cd_fdc_op", "cdfdc_op_compte")
        .withColumnRenamed("agenceop", "agenceop_compte")
        .withColumnRenamed("cd_fdc_gest", "cdfdc_gest_compte")
        .withColumnRenamed("agencegest", "agencegest_compte")
        .select(
            "numerotiers",
            "numerodecompte",
            "naturecompte",
            "devise",
            "numerocompteorigine",
            "cdfdc_op_compte",
            "agenceop_compte",
            "cdfdc_gest_compte",
            "agencegest_compte",
            "etat_compte",
        )
    )

    union_account_and_client_account = open_accounts.unionByName(filtered_compte)

    grouped_soldes = encours_ressource.unionByName(encours_emplois)

    joined_soldes_comptes = (
        grouped_soldes.join(
            union_account_and_client_account,
            ["numerodecompte", "devise"],
            "left_outer",
        )
        .withColumn(
            "agencegest_compte",
            when(
                col("cdfdc_gest_compte").isNull(),
                substring(col("numerodecompte"), 1, 3),
            ).otherwise(col("agencegest_compte")),
        )
        .withColumn(
            "agenceop_compte",
            when(
                col("cdfdc_op_compte").isNull(),
                substring(col("numerodecompte"), 1, 3),
            ).otherwise(col("agenceop_compte")),
        )
    )

    selected_information_tier = (
        information_tier.withColumn("codeagence", col("agencegest"))
        .withColumnRenamed("cd_fdc_op", "cdfdc_op")
        .withColumnRenamed("cd_fdc_gest", "cdfdc_gest")
        .select(
            "numerotiers",
            "codeagence",
            "agenceop",
            "cdfdc_op",
            "gestionnaire_op",
            "agencegest",
            "cdfdc_gest",
            "gestionnaire_gest",
            "marche",
            "libelle_marche",
            "nomraisonsociale",
            "prenomenscom",
            "nomreduit",
            "new_sous_seg",
            "libellesoussegment",
            "new_seg",
            "libellesegment",
            "gamme",
        )
    )

    selected_utilisatuer = (
        utilisateur.filter(col("dfval") >= col("partitiondate"))
        .withColumnRenamed("identifiant", "gestionnaire_gest")
        .select(
            "gestionnaire_gest",
            "nomuti",
            "qualiteuti",
            "prenom",
            "fonction",
        )
    )

    selected_niveau_regroupement = niveau_regroupement.select(
        "codeagence",
        "nomagence",
        "succursale",
        "nomsuccursale",
        "codebanque",
        "nombanque",
        "coderegion",
        "nomregion",
    ).drop_duplicates()

    joined_soldes_comptes_with_agence = (
        joined_soldes_comptes.join(
            selected_information_tier,
            ["numerotiers"],
            "left",
        )
        .join(
            selected_niveau_regroupement,
            ["codeagence"],
            "left",
        )
        .join(selected_utilisatuer, ["gestionnaire_gest"], "left")
        .withColumn("dttrtm", to_date("dttrtm"))
        .drop("partitiondate")
    )

    renamed_niveau_regroupement = (
        selected_niveau_regroupement.withColumnRenamed(
            "codeagence", "codeagence_ctx"
        )
        .withColumnRenamed("nomagence", "nomagence_ctx")
        .withColumnRenamed("succursale", "succursale_ctx")
        .withColumnRenamed("nomsuccursale", "nomsuccursale_ctx")
        .withColumnRenamed("coderegion", "coderegion_ctx")
        .withColumnRenamed("codebanque", "codebanque_ctx")
        .withColumnRenamed("nomregion", "nomregion_ctx")
        .withColumnRenamed("nombanque", "nombanque_ctx")
        .select(
            "codeagence_ctx",
            "nomagence_ctx",
            "succursale_ctx",
            "nomsuccursale_ctx",
            "coderegion_ctx",
            "codebanque_ctx",
            "nomregion_ctx",
            "nombanque_ctx",
        )
        .drop_duplicates()
    )

    with_contentieux = (
        joined_soldes_comptes_with_agence.join(
            contentieux, ["numerocompteorigine"], "left"
        )
        .withColumn(
            "etat",
            when(col("etat"), lit("CONTENTIEUX")).otherwise("NON CONTENTIEUX"),
        )
        .withColumn("cdgest_ctx", coalesce(col("cdgest_ctx"), col("cdfdc_gest")))
        .withColumn(
            "codeagence_ctx", coalesce(col("codeagence_ctx"), col("agencegest"))
        )
        .join(renamed_niveau_regroupement, ["codeagence_ctx"], "left")
        .withColumn(
            "gestionnaire_gest_uniformed",
            regexp_replace("gestionnaire_gest", "i", "u"),
        )
    )

    return with_contentieux


def build_information_tier(
    client: DataFrame,
    fdc_tier: DataFrame,
    t705: DataFrame,
    t706: DataFrame,
    mapping_marche_ge_pme: DataFrame,
    mapping_pro_retail: DataFrame,
    donnee_client: DataFrame,
    segmentation: DataFrame,
    ref_sous_segment: DataFrame,
    partition_date: str,
) -> DataFrame:
    filtered_fdc_tier = fdc_tier.select(
        "numerotiers", "lieufdctiers", "codefondsdecommerce", "typefdctiers"
    )

    client_columns = [
        "numerotiers",
        "qualite",
        "nomraisonsociale",
        "typeidentite",
        "numeroidentite",
        "prenomenscom",
        "datedenaissance",
        "numerois",
        "numregcom",
        "numerodepatente",
        "anneedecreation",
        "nomreduit",
        "nationalite",
        "agenteconomique",
        "natjur",
        "dateentreeaumaroc",
        "categorieetablissement",
        "devise",
        "langue",
        "codeinvalidation",
        "interditdecompte",
        "datefininterdcompte",
        "codemultilieux",
        "zonederesidence",
        "paysderesidence",
        "apporteur",
        "apparentegroupe",
        "apparentebanque",
        "cotation",
        "foyerfiscal",
        "pnb",
        "soumisatva",
        "soumisafisc",
        "signatureecartee",
        "coteactivite",
        "cotecredit",
        "cotepaiement",
        "numeroprovisoire",
        "situaexceptiers",
        "datemajsitex",
        "indicfournisseur",
        "indicsousdeleg",
        "dtmodif",
        "atracer",
        "cotationrisque",
        "agent_economique",
        "tiersgarant",
        "sensible",
        "justsensible",
        "kyceditee",
        "lieukyc",
        "justvip",
        "vip",
        "americain",
        "sousmarche",
        "numrc",
        "controlesensibilite",
        "dtsegmarche",
        "datemajkyc",
        "dateprochmajkyc",
        "datefinrelation",
        "indiceclientautreca",
        "clientbp",
        "apporteuraffairebp",
        "gestionnairebp",
        "dtenvoibp",
        "entitegroupeca",
        "clientleasing",
        "codefjcdm",
        "statut",
        "indicapporteuraffaires",
        "indicassureursreassureurs",
        "indicparticipantpret",
        "americain_parcontagion",
        "segment",
        "parrain",
        "new_seg",
        "new_sous_seg",
    ]

    filtered_fdc_tier_gest = filtered_fdc_tier.filter(col("typefdctiers") == 2)

    client_with_fdc_tier = client.join(
        filtered_fdc_tier_gest, "numerotiers", "left_outer"
    )

    filtered_t705 = (
        t705.filter(col("DFVAL") > to_date(lit(partition_date)))
        .withColumnRenamed("rattache", "lieufdctiers")
        .select("lieufdctiers", "codefondsdecommerce", "cdgest", "gestionnaire")
    )

    client_with_gest = (
        client_with_fdc_tier.join(
            filtered_t705, ["lieufdctiers", "codefondsdecommerce"], "left_outer"
        )
        .withColumnRenamed("lieufdctiers", "agencegest")
        .withColumnRenamed("cdgest", "cd_fdc_gest")
        .withColumnRenamed("gestionnaire", "gestionnaire_gest")
        .select("agencegest", "cd_fdc_gest", "gestionnaire_gest", *client_columns)
    )

    filtered_fdc_tier_op = filtered_fdc_tier.filter(col("typefdctiers") == 1)

    client_with_gest_and_op = (
        client_with_gest.join(filtered_fdc_tier_op, "numerotiers", "left_outer")
        .join(filtered_t705, ["lieufdctiers", "codefondsdecommerce"], "left_outer")
        .withColumnRenamed("lieufdctiers", "agenceop")
        .withColumnRenamed("cdgest", "cd_fdc_op")
        .withColumnRenamed("gestionnaire", "gestionnaire_op")
        .select(
            "agenceop",
            "cd_fdc_op",
            "gestionnaire_op",
            "agencegest",
            "cd_fdc_gest",
            "gestionnaire_gest",
            *client_columns,
        )
    )

    renamed_mapping_marche_ge_pme = (
        mapping_marche_ge_pme.select("code_lieu_gest", "marche", "libelle_marche")
        .withColumn("gamme_ge_pme", col("marche"))
        .withColumnRenamed("code_lieu_gest", "agencegest")
        .withColumnRenamed("marche", "marche_ge_pme")
        .withColumnRenamed("libelle_marche", "libelle_marche_ge_pme")
    )

    renamed_mapping_pro_retail_new_seg = (
        mapping_pro_retail.select("new_seg", "marche", "libelle_marche", "gamme")
        .withColumnRenamed("marche", "marche_pro_retail_new_seg")
        .withColumnRenamed("new_seg", "new_seg_pr")
        .withColumnRenamed("libelle_marche", "libelle_marche_pro_retail_new_seg")
        .withColumnRenamed("gamme", "gamme_new_seg")
        .drop_duplicates()
    )

    renamed_mapping_pro_retail_new_sous_seg = (
        mapping_pro_retail.select(
            "new_sous_seg", "marche", "libelle_marche", "gamme"
        )
        .withColumnRenamed("marche", "marche_pro_retail_new_sous_seg")
        .withColumnRenamed("new_sous_seg", "new_sous_seg_pr")
        .withColumnRenamed(
            "libelle_marche", "libelle_marche_pro_retail_new_sous_seg"
        )
        .withColumnRenamed("gamme", "gamme_new_sous_seg")
        .drop_duplicates()
    )

    new_seg_condition = client_with_gest_and_op["new_seg"].isNotNull() & (
        client_with_gest_and_op["new_seg"]
        == renamed_mapping_pro_retail_new_seg["new_seg_pr"]
    )

    new_sous_seg_condition = (
        client_with_gest_and_op["new_seg"].isNull()
        & client_with_gest_and_op["new_sous_seg"].isNotNull()
        & (
            client_with_gest_and_op["new_sous_seg"]
            == renamed_mapping_pro_retail_new_sous_seg["new_sous_seg_pr"]
        )
    )

    donnee_client = donnee_client.select(
        "numerotiers",
        "statutclient",
    )

    ref_sous_segment = (
        ref_sous_segment.filter(col("dfval") >= col("partitiondate"))
        .withColumnRenamed("codesoussegment", "new_sous_seg")
        .withColumnRenamed("libsoussegment", "libellesoussegment")
        .select(
            "new_sous_seg",
            "libellesoussegment",
        )
        .drop_duplicates()
    )

    segmentation = (
        segmentation.filter(col("dfval") >= col("partitiondate"))
        .withColumnRenamed("codesegment", "new_seg")
        .select(
            "new_seg",
            "libellesegment",
        )
        .drop_duplicates()
    )

    filtered_t706 = t706.filter(col("DFVAL") > col("partitiondate")).select(
        "cdqual", "qualite", "libqua"
    )

    return (
        client_with_gest_and_op.join(segmentation, ["new_seg"], "left")
        .join(ref_sous_segment, ["new_sous_seg"], "left")
        .join(donnee_client, ["numerotiers"], "left")
        .join(renamed_mapping_marche_ge_pme, "agencegest", "left_outer")
        .join(renamed_mapping_pro_retail_new_seg, new_seg_condition, "left_outer")
        .join(
            renamed_mapping_pro_retail_new_sous_seg,
            new_sous_seg_condition,
            "left_outer",
        )
        .withColumn(
            "marche_pro_retail",
            coalesce(
                col("marche_pro_retail_new_seg"),
                col("marche_pro_retail_new_sous_seg"),
            ),
        )
        .withColumn(
            "libelle_marche_pro_retail",
            coalesce(
                col("libelle_marche_pro_retail_new_seg"),
                col("libelle_marche_pro_retail_new_sous_seg"),
            ),
        )
        .withColumn(
            "libelle_marche",
            coalesce(col("libelle_marche_ge_pme"), col("libelle_marche_pro_retail")),
        )
        .withColumn(
            "gamme_pro_retail",
            coalesce(col("gamme_new_seg"), col("gamme_new_sous_seg")),
        )
        .withColumn("gamme", coalesce(col("gamme_ge_pme"), col("gamme_pro_retail")))
        .withColumn(
            "marche", coalesce(col("marche_ge_pme"), col("marche_pro_retail"))
        )
        .withColumnRenamed("qualite", "cdqual")
        .join(filtered_t706, ["cdqual"], "left")
        .withColumnRenamed("libqua", "libelle_qualite")
        .select(
            "numerotiers",
            "prenomenscom",
            "nomreduit",
            "numeroidentite",
            "typeidentite",
            "datedenaissance",
            "nationalite",
            "anneedecreation",
            "numerodepatente",
            "agenceop",
            "cd_fdc_op",
            "gestionnaire_op",
            "agencegest",
            "cd_fdc_gest",
            "gestionnaire_gest",
            "marche",
            "libelle_marche",
            "sousmarche",
            "gamme",
            "statutclient",
            "segment",
            "libellesegment",
            "new_seg",
            "new_sous_seg",
            "libellesoussegment",
            "qualite",
            "libelle_qualite",
            "nomraisonsociale",
            "agenteconomique",
            "devise",
            "langue",
            "zonederesidence",
            "paysderesidence",
            "cotation",
            "cotationrisque",
            "agent_economique",
            "tiersgarant",
            "sensible",
            "justsensible",
            "kyceditee",
            "lieukyc",
            "justvip",
            "vip",
            "americain",
            "statut",
        )
    )


def build_solde_client_devise_history(
    dm_sldcli: DataFrame, rp_gl_bct: DataFrame, t601: DataFrame
) -> DataFrame:
    window_spec = Window.partitionBy("NACMPT", "partitiondate").orderBy(
        col("DFVAL").desc()
    )
    exp_t601_processed = (
        t601.withColumn("RUBRIC", first("RUBRIC").over(window_spec))
        .select("NACMPT", "RUBRIC", "partitiondate")
        .distinct()
    )

    joined_df = (
        dm_sldcli.join(
            rp_gl_bct,
            (dm_sldcli["NUMERODECOMPTE"] == rp_gl_bct["COMPTE_CLIENT"])
            & (dm_sldcli["partitiondate"] == rp_gl_bct["partitiondate"]),
            how="left",
        )
        .withColumn(
            "NATURE_EXTERNE",
            coalesce(
                rp_gl_bct["nature_externe"],
                substring(dm_sldcli["NUMERODECOMPTE"], 4, 3),
            ),
        )
        .join(
            exp_t601_processed,
            (col("nature_externe") == exp_t601_processed["NACMPT"])
            & (dm_sldcli["partitiondate"] == exp_t601_processed["partitiondate"]),
            how="left",
        )
    )

    return (
        joined_df.withColumn("PCI", rpad(substring(col("RUBRIC"), 3, 5), 6, "0"))
        .withColumn(
            "TYPE",
            when(
                substring(col("RUBRIC"), 3, 5).isin(
                    "11130", "11140", "11211", "11212", "46110", "46210", "46613"
                ),
                lit("BILLET"),
            ).otherwise(lit("VIREMENT")),
        )
        .withColumn("SOLDEJOURCR", dm_sldcli["SOLDEJOURCR"].cast("decimal(38, 4)"))
        .withColumn("SOLDEJOURDB", dm_sldcli["SOLDEJOURDB"].cast("decimal(38, 4)"))
        .withColumn("dttrtm", to_date(dm_sldcli["DTTRTM"]))
        .select(
            col("DTTRTM"),
            col("PCI"),
            joined_df["NATURE_EXTERNE"],
            dm_sldcli["NUMERODECOMPTE"],
            dm_sldcli["DEVISE"],
            col("TYPE"),
            col("SOLDEJOURCR"),
            col("SOLDEJOURDB"),
            dm_sldcli["partitiondate"],
        )
    )


def build_taux_change_bam_history(
    exploit_ref_devise: DataFrame,
    virement_bam_history: DataFrame,
    cours_bbe_bam_history: DataFrame,
) -> DataFrame:
    selected_virement_bam = (
        virement_bam_history.withColumn(
            "moyen_par_unite", col("moyen") / col("uniteDevise")
        )
        .withColumnRenamed("date", "date_cours_virement")
        .select(
            "libDevise",
            "moyen_par_unite",
            "date_cours_virement",
        )
    )

    selected_cours_bbe = (
        cours_bbe_bam_history.withColumn(
            "venteClientele", col("venteClientele") / col("uniteDevise")
        )
        .withColumn("achatClientele", col("achatClientele") / col("uniteDevise"))
        .withColumnRenamed("date", "date_cours_bbe")
        .select(
            "libDevise",
            "venteClientele",
            "achatClientele",
            "date_cours_bbe",
        )
    )

    joined_virement_bbe = (
        selected_virement_bam.join(
            selected_cours_bbe,
            (selected_virement_bam["libDevise"] == selected_cours_bbe["libDevise"])
            & (
                selected_virement_bam["date_cours_virement"]
                == selected_cours_bbe["date_cours_bbe"]
            ),
            "left_outer",
        )
        .withColumn(
            "devise",
            coalesce(
                selected_virement_bam["libDevise"], selected_cours_bbe["libDevise"]
            ),
        )
        .withColumn(
            "date_cours",
            coalesce(
                selected_virement_bam["date_cours_virement"],
                selected_cours_bbe["date_cours_bbe"],
            ),
        )
    )

    return (
        joined_virement_bbe.join(
            exploit_ref_devise,
            joined_virement_bbe["devise"] == exploit_ref_devise["CD_DEVISE"],
            "left_outer",
        )
        .withColumn(
            "devise",
            coalesce(joined_virement_bbe["devise"], exploit_ref_devise["CD_DEVISE"]),
        )
        .select(
            exploit_ref_devise["CD_DEV_OPER"],
            col("devise").alias("CODE_DEVISE"),
            col("lb_devise").alias("DEVISE_LIBELLE"),
            col("moyen_par_unite").alias("MIDBAM"),
            col("date_cours"),
            col("achatClientele").alias("COURSMIN"),
            col("venteClientele").alias("COURSMAX"),
            col("partitiondate"),
        )
    )


def build_solde_client_history(
    sldcli_nature: DataFrame,
    taux_change_bam: DataFrame,
) -> DataFrame:
    window_asc = (
        Window.partitionBy("code_devise")
        .orderBy(col("date_cours"))
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    window_desc = (
        Window.partitionBy("code_devise")
        .orderBy(col("date_cours").desc())
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    taux_change_bam = (
        sldcli_nature.select("dttrtm")
        .distinct()
        .withColumnRenamed("dttrtm", "date_cours")
        .join(taux_change_bam, ["date_cours"], "left")
        .withColumn(
            "midbam",
            coalesce(
                col("midbam"),
                first(col("midbam"), ignorenulls=True).over(window_asc),
                first(col("midbam"), ignorenulls=True).over(window_desc),
            ),
        )
        .withColumn(
            "coursmin",
            coalesce(
                col("coursmin"),
                first(col("coursmin"), ignorenulls=True).over(window_asc),
                first(col("coursmin"), ignorenulls=True).over(window_desc),
            ),
        )
        .withColumn(
            "coursmax",
            coalesce(
                col("coursmax"),
                first(col("coursmax"), ignorenulls=True).over(window_asc),
                first(col("coursmax"), ignorenulls=True).over(window_desc),
            ),
        )
    )

    return (
        sldcli_nature.join(
            taux_change_bam,
            (sldcli_nature["DEVISE"] == taux_change_bam["CD_DEV_OPER"])
            & (sldcli_nature["dttrtm"] == taux_change_bam["date_cours"]),
            "left_outer",
        )
        .withColumn(
            "CODE_DEVISE",
            when(sldcli_nature["DEVISE"] == lit("000C"), "MAD").otherwise(
                taux_change_bam["CODE_DEVISE"]
            ),
        )
        .withColumn(
            "average_cours",
            when(col("DEVISE") == lit("000C"), 1).otherwise(
                (col("COURSMIN") + col("COURSMAX")) / 2
            ),
        )
        .withColumn(
            "MIDBAM", when(col("DEVISE") == lit("000C"), 1).otherwise(col("MIDBAM"))
        )
        .withColumn(
            "SOLDEJOURCR_DEVISE",
            when(
                col("TYPE") == lit("VIREMENT"),
                col("SOLDEJOURCR") * col("MIDBAM"),
            ).otherwise(col("SOLDEJOURCR") * col("average_cours")),
        )
        .withColumn(
            "SOLDEJOURDB_DEVISE",
            when(
                col("TYPE") == lit("VIREMENT"),
                col("SOLDEJOURDB") * col("MIDBAM"),
            ).otherwise(col("SOLDEJOURDB") * col("average_cours")),
        )
        .withColumn(
            "solde", coalesce(col("soldejourcr_devise"), col("soldejourdb_devise"))
        )
        .select(
            "numerodecompte",
            "devise",
            "pci",
            "nature_externe",
            "soldejourcr",
            "soldejourcr_devise",
            "soldejourdb",
            "soldejourdb_devise",
            "dttrtm",
        )
    )


def build_solde_client_history_with_propagation(
    dm_sldcli_history: DataFrame, partition_date: str
) -> DataFrame:
    selected_dm_sldcli = dm_sldcli_history.select(
        "numerodecompte",
        "devise",
        "soldejourcr_devise",
        "soldejourdb_devise",
        "dttrtm",
    )
    return (
        selected_dm_sldcli.transform(
            get_last_recorded_sold_per_client_from, by="soldejourcr_devise", days=1
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourdb_devise", days=1
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourcr_devise", days=2
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourdb_devise", days=2
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourcr_devise", days=3
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourdb_devise", days=3
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourcr_devise", days=4
        )
        .transform(
            get_last_recorded_sold_per_client_from, by="soldejourdb_devise", days=4
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourcr_devise",
            partition_date=partition_date,
            months=1,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourdb_devise",
            partition_date=partition_date,
            months=1,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourcr_devise",
            partition_date=partition_date,
            months=2,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourdb_devise",
            partition_date=partition_date,
            months=2,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourcr_devise",
            partition_date=partition_date,
            months=3,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourdb_devise",
            partition_date=partition_date,
            months=3,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourcr_devise",
            partition_date=partition_date,
            months=4,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="soldejourdb_devise",
            partition_date=partition_date,
            months=4,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="soldejourcr_devise",
            partition_date=partition_date,
            years=1,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="soldejourdb_devise",
            partition_date=partition_date,
            years=1,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="soldejourcr_devise",
            partition_date=partition_date,
            years=2,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="soldejourdb_devise",
            partition_date=partition_date,
            years=2,
        )
        .transform(
            get_last_recorded_sold_per_client_from_same_day_last_year,
            by="soldejourcr_devise",
            partition_date=partition_date,
        )
        .transform(
            get_last_recorded_sold_per_client_from_same_day_last_year,
            by="soldejourdb_devise",
            partition_date=partition_date,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="soldejourcr_devise",
            partition_date=partition_date,
            weeks=1,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="soldejourdb_devise",
            partition_date=partition_date,
            weeks=1,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="soldejourcr_devise",
            partition_date=partition_date,
            weeks=2,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="soldejourdb_devise",
            partition_date=partition_date,
            weeks=2,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="soldejourcr_devise",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="soldejourdb_devise",
        )
        .select(
            "numerodecompte",
            "devise",
            "dttrtm",
            "last_day_soldejourcr_devise",
            "last_day_soldejourdb_devise",
            "last_week_soldejourcr_devise",
            "last_week_soldejourdb_devise",
            "last_2_weeks_soldejourcr_devise",
            "last_2_weeks_soldejourdb_devise",
            "last_2_days_soldejourcr_devise",
            "last_2_days_soldejourdb_devise",
            "last_3_days_soldejourcr_devise",
            "last_3_days_soldejourdb_devise",
            "last_4_days_soldejourcr_devise",
            "last_4_days_soldejourdb_devise",
            "last_month_soldejourcr_devise",
            "last_month_soldejourdb_devise",
            "last_2_months_soldejourcr_devise",
            "last_2_months_soldejourdb_devise",
            "last_3_months_soldejourcr_devise",
            "last_3_months_soldejourdb_devise",
            "last_4_months_soldejourcr_devise",
            "last_4_months_soldejourdb_devise",
            "last_year_soldejourcr_devise",
            "last_year_soldejourdb_devise",
            "last_2_years_soldejourcr_devise",
            "last_2_years_soldejourdb_devise",
            "same_day_last_year_soldejourcr_devise",
            "same_day_last_year_soldejourdb_devise",
            "last_quarter_soldejourcr_devise",
            "last_quarter_soldejourdb_devise",
        )
    )


def build_fact_table(
    ressource_emplois: DataFrame,
    dm_sldcli_solde_history: DataFrame,
    nouveau_mapping_ressource_emploi: DataFrame,
) -> DataFrame:
    fact_table = (
        ressource_emplois.join(
            dm_sldcli_solde_history,
            ["numerodecompte", "devise", "dttrtm"],
            "left_outer",
        )
        .transform(
            map_account_balance,
            column_debit="last_day_soldejourdb_devise",
            column_credit="last_day_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_2_days_soldejourdb_devise",
            column_credit="last_2_days_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_3_days_soldejourdb_devise",
            column_credit="last_3_days_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_4_days_soldejourdb_devise",
            column_credit="last_4_days_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_week_soldejourdb_devise",
            column_credit="last_week_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_2_weeks_soldejourdb_devise",
            column_credit="last_2_weeks_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_month_soldejourdb_devise",
            column_credit="last_month_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_2_months_soldejourdb_devise",
            column_credit="last_2_months_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_3_months_soldejourdb_devise",
            column_credit="last_3_months_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_4_months_soldejourdb_devise",
            column_credit="last_4_months_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_year_soldejourdb_devise",
            column_credit="last_year_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_2_years_soldejourdb_devise",
            column_credit="last_2_years_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="same_day_last_year_soldejourdb_devise",
            column_credit="same_day_last_year_soldejourcr_devise",
        )
        .transform(
            map_account_balance,
            column_debit="last_quarter_soldejourdb_devise",
            column_credit="last_quarter_soldejourcr_devise",
        )
    )

    aa_pp_fact_table = fact_table.filter(
        col("sens_mapping").isin("AA", "PP") | col("sens_mapping").isNull()
    )

    nouveau_mapping_ressource_emploi = selected_nouveau_mapping_ressource_emploi(
        nouveau_mapping_ressource_emploi
    ).select(
        "pci",
        "nature",
        "type_ressource_emploi",
        "sens_mapping",
        "categorie",
        "famille_tdb_banque",
        "libelle_type",
    )

    nouveau_mapping_ressource_emploi_with_ressource_only = (
        nouveau_mapping_ressource_emploi.filter(
            col("sens_mapping") == lit("AP")
        ).filter(col("categorie") == lit("RESSOURCE"))
    )

    ap_ressource_fact_table = (
        fact_table.filter(col("sens_mapping").isin("AP"))
        .withColumn("categorie", lit("RESSOURCE"))
        .withColumn("soldejourdb", lit(None))
        .withColumn("soldejourdb_devise", lit(None))
        .withColumn("last_day_soldejourdb_devise", lit(None))
        .withColumn("last_2_days_soldejourdb_devise", lit(None))
        .withColumn("last_3_days_soldejourdb_devise", lit(None))
        .withColumn("last_4_days_soldejourdb_devise", lit(None))
        .withColumn("last_week_soldejourdb_devise", lit(None))
        .withColumn("last_2_weeks_soldejourdb_devise", lit(None))
        .withColumn("last_month_soldejourdb_devise", lit(None))
        .withColumn("last_2_months_soldejourdb_devise", lit(None))
        .withColumn("last_3_months_soldejourdb_devise", lit(None))
        .withColumn("last_4_months_soldejourdb_devise", lit(None))
        .withColumn("last_year_soldejourdb_devise", lit(None))
        .withColumn("last_2_years_soldejourdb_devise", lit(None))
        .withColumn("same_day_last_year_soldejourdb_devise", lit(None))
        .withColumn("last_quarter_soldejourdb_devise", lit(None))
        .drop("famille_tdb_banque", "libelle_type", "type_ressource_emploi")
        .join(
            nouveau_mapping_ressource_emploi_with_ressource_only,
            ["pci", "nature", "sens_mapping", "categorie"],
            "left",
        )
    )

    nouveau_mapping_ressource_emploi_with_emploi_only = (
        nouveau_mapping_ressource_emploi.filter(
            col("sens_mapping") == lit("AP")
        ).filter(col("categorie") == lit("EMPLOI"))
    )

    ap_emploi_fact_table = (
        fact_table.filter(col("sens_mapping").isin("AP"))
        .withColumn("categorie", lit("EMPLOI"))
        .withColumn("soldejourcr", lit(None))
        .withColumn("soldejourcr_devise", lit(None))
        .withColumn("last_day_soldejourcr_devise", lit(None))
        .withColumn("last_2_days_soldejourcr_devise", lit(None))
        .withColumn("last_3_days_soldejourcr_devise", lit(None))
        .withColumn("last_4_days_soldejourcr_devise", lit(None))
        .withColumn("last_week_soldejourcr_devise", lit(None))
        .withColumn("last_2_weeks_soldejourcr_devise", lit(None))
        .withColumn("last_month_soldejourcr_devise", lit(None))
        .withColumn("last_2_months_soldejourcr_devise", lit(None))
        .withColumn("last_3_months_soldejourcr_devise", lit(None))
        .withColumn("last_4_months_soldejourcr_devise", lit(None))
        .withColumn("last_year_soldejourcr_devise", lit(None))
        .withColumn("last_2_years_soldejourcr_devise", lit(None))
        .withColumn("same_day_last_year_soldejourcr_devise", lit(None))
        .withColumn("last_quarter_soldejourdb_devise", lit(None))
        .drop("famille_tdb_banque", "libelle_type", "type_ressource_emploi")
        .join(
            nouveau_mapping_ressource_emploi_with_emploi_only,
            ["pci", "nature", "sens_mapping", "categorie"],
            "left",
        )
    )

    fact_table = aa_pp_fact_table.unionByName(ap_ressource_fact_table).unionByName(
        ap_emploi_fact_table
    )
    fact_table = (
        fact_table.withColumn("code_devise", substring(col("devise"), 1, 3))
        .withColumn(
            "fk_compte",
            concat_ws(
                "", col("numerodecompte"), coalesce(col("code_devise"), lit("0"))
            ),
        )
        .withColumn(
            "fk_mapressourceemploi",
            concat_ws("", col("pci"), col("nature"), crc32(col("categorie"))),
        )
        .select(
            col("numerodecompte"),
            col("numerotiers"),
            col("dttrtm"),
            col("categorie"),
            col("soldejourcr"),
            col("soldejourdb"),
            col("soldejourcr_devise"),
            col("soldejourdb_devise"),
            col("fk_compte"),
            col("fk_mapressourceemploi"),
            col("last_day_soldejourcr_devise"),
            col("last_day_soldejourdb_devise"),
            col("last_2_days_soldejourcr_devise"),
            col("last_2_days_soldejourdb_devise"),
            col("last_3_days_soldejourdb_devise"),
            col("last_3_days_soldejourcr_devise"),
            col("last_4_days_soldejourcr_devise"),
            col("last_4_days_soldejourdb_devise"),
            col("last_week_soldejourcr_devise"),
            col("last_week_soldejourdb_devise"),
            col("last_2_weeks_soldejourcr_devise"),
            col("last_2_weeks_soldejourdb_devise"),
            col("last_month_soldejourcr_devise"),
            col("last_month_soldejourdb_devise"),
            col("last_2_months_soldejourcr_devise"),
            col("last_2_months_soldejourdb_devise"),
            col("last_3_months_soldejourcr_devise"),
            col("last_3_months_soldejourdb_devise"),
            col("last_4_months_soldejourcr_devise"),
            col("last_4_months_soldejourdb_devise"),
            col("last_year_soldejourcr_devise"),
            col("last_year_soldejourdb_devise"),
            col("last_2_years_soldejourcr_devise"),
            col("last_2_years_soldejourdb_devise"),
            col("same_day_last_year_soldejourcr_devise"),
            col("same_day_last_year_soldejourdb_devise"),
            col("last_quarter_soldejourcr_devise"),
            col("last_quarter_soldejourdb_devise"),
        )
    )

    return fact_table.persist()
