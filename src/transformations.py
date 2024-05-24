from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, expr, lit, substring, when


def build_compte_tenu(
    spark: SparkSession,
    compte_commercial: DataFrame,
    ret_documents: DataFrame,
    client: DataFrame,
    titulaire: DataFrame,
    sous_compte: DataFrame,
    elt_contrat: DataFrame,
    contrat: DataFrame,
    fdc_compte: DataFrame,
    t705: DataFrame,
    cpte_tenu_par_client: DataFrame,
) -> DataFrame:
    compte_commercial.createOrReplaceTempView("COMPTECOMMERCIAL")
    ret_documents.createOrReplaceTempView("RETDOCUMENTS")
    client.createOrReplaceTempView("CLIENT")
    titulaire.createOrReplaceTempView("TITULAIRE")
    sous_compte.createOrReplaceTempView("SOUSCOMPTE")
    elt_contrat.createOrReplaceTempView("ELT_CONTRAT")
    contrat.createOrReplaceTempView("CONTRAT")
    fdc_compte.createOrReplaceTempView("FDCCOMPTE")
    t705.createOrReplaceTempView("T705")
    cpte_tenu_par_client.createOrReplaceTempView("CPTETENUPARCLIENT")
    return spark.sql(
        """
SELECT DISTINCT NUMEROTIERS,
        NUMERODECOMPTE,
        NUMEROCOMPTEORIGINE,
        DEVISE,
        NATURECOMPTE,
        INTITULE,
        REPRESENTANTETRANGER,
        ETAT_COMPTE,
        DATEEFFET,
        DATEECHEANCE,
        AgenceGEST,
        CDFDC_GEST,
        GESTIONNAIRE_GEST,
        NOMGEST_GEST,
        QUALITEPTF,
        AgenceOP,
        CDFDC_OP,
        GESTIONNAIRE_OP,
        NOMGEST_OP
FROM
(SELECT core2.*,
    b.RATTACHE AS AgenceOP,
    b.CDGEST AS CDFDC_OP,
    b.GESTIONNAIRE AS GESTIONNAIRE_OP,
    b.NOMGEST AS NOMGEST_OP
FROM
(SELECT core.*,
        b.RATTACHE AS AgenceGEST,
        b.CDGEST AS CDFDC_GEST,
        b.GESTIONNAIRE AS GESTIONNAIRE_GEST,
        b.NOMGEST AS NOMGEST_GEST,
        b.QUALITEPTF
FROM
(SELECT EXPLOIT_CLIENT.NUMEROTIERS,
        q.NUMERODECOMPTE,
        q.NUMEROCOMPTEORIGINE,
        EXPLOIT_CLIENT.DEVISE,
        q.NATURECOMPTE,
        q.INTITULE,
        q.REPRESENTANTETRANGER,
        CASE EXPLOIT_SOUSCOMPTE.ETATCOMPTE
            WHEN '0' THEN 'OUVERT'
            WHEN '1' THEN 'OUVERT'
            WHEN '3' THEN 'CLOTURE'
            WHEN '5' THEN 'ACLORE'
            ELSE 'DEMCLOT'
        END AS ETAT_COMPTE,
        EXPLOIT_ELT_CONTRAT.DATEEFFET,
        EXPLOIT_ELT_CONTRAT.DATEECHEANCE,
        q.NATURECOMPTE
    FROM
    (SELECT CPTTENUPARCLIENT.NUMERODECOMPTE,
            CPTTENUPARCLIENT.NUMEROCOMPTEORIGINE,
            EXPLOIT_COMPTECOMMERCIAL.NATURECOMPTE,
            EXPLOIT_COMPTECOMMERCIAL.INTITULE,
            EXPLOIT_COMPTECOMMERCIAL.REPRESENTANTETRANGER
    FROM CPTETENUPARCLIENT AS CPTTENUPARCLIENT
    LEFT JOIN COMPTECOMMERCIAL AS EXPLOIT_COMPTECOMMERCIAL
ON CPTTENUPARCLIENT.NUMEROCOMPTEORIGINE = EXPLOIT_COMPTECOMMERCIAL.NUMERODECOMPTE) q
    LEFT JOIN SOUSCOMPTE AS EXPLOIT_SOUSCOMPTE
    ON EXPLOIT_SOUSCOMPTE.numerodecompte = q.NUMEROCOMPTEORIGINE,
                                                CLIENT AS EXPLOIT_CLIENT,
                                                TITULAIRE AS EXPLOIT_TITULAIRE,
                                                ELT_CONTRAT AS EXPLOIT_ELT_CONTRAT,
                                                CONTRAT AS EXPLOIT_CONTRAT
    WHERE EXPLOIT_CLIENT.numerotiers = EXPLOIT_TITULAIRE.numerotiers
AND EXPLOIT_SOUSCOMPTE.numeroelementinterne = EXPLOIT_ELT_CONTRAT.numeroelementinterne
AND EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE = EXPLOIT_TITULAIRE.NUMEROCONTRATINTERNE
AND EXPLOIT_TITULAIRE.TITULAIREPRINCIPAL = 1
AND EXPLOIT_ELT_CONTRAT.TYPEDEPRODUIT = 'CA03'
AND EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE = EXPLOIT_CONTRAT.NUMEROCONTRATINTERNE) core,
    FDCCOMPTE a,
    T705 b
WHERE b.RATTACHE = a.LIEUFDCCOMPTE
AND b.CODEFONDSDECOMMERCE = a.CODEFONDSDECOMMERCE
AND b.DFVAL > CURRENT_DATE
AND a.TYPEFDCCOMPTE=1
AND core.NUMEROCOMPTEORIGINE=a.NUMERODECOMPTE) core2,
FDCCOMPTE a,
T705 b
WHERE b.RATTACHE = a.LIEUFDCCOMPTE
AND b.CODEFONDSDECOMMERCE = a.CODEFONDSDECOMMERCE
AND b.DFVAL > CURRENT_DATE
AND a.TYPEFDCCOMPTE=2
AND core2.NUMEROCOMPTEORIGINE=a.NUMERODECOMPTE);
"""
    )


def build_dim_agence(
    spark: SparkSession,
    exp_t608_2: DataFrame,
    niveau_1: DataFrame,
    niveau_0: DataFrame,
    niveau_3: DataFrame,
) -> DataFrame:
    exp_t608_2.createOrReplaceTempView("EXP_T608_2")
    niveau_1.createOrReplaceTempView("ENTITE_NIVEAU_1")
    niveau_0.createOrReplaceTempView("ENTITE_NIVEAU_0")
    niveau_3.createOrReplaceTempView("ENTITE_NIVEAU_3")

    return spark.sql(
        """
WITH niv_1 AS
  (SELECT ENTITE AS CODEENTITE,
          NOMENTIT AS NOMENTITE,
          DIRZONE AS CODEBANQUE,
          GRREG AS CODEREGION,
          LOCALI,
          CVILLE,
          SECTGEO,
          OPERATIONNELLE
   FROM EXP_T608_2
   WHERE (ENTITE IS NOT NULL
          AND GRREG IS NOT NULL
          AND DFVAL>CURRENT_DATE()
          AND SUCCURSALE IS NULL
          AND ENTITE=GRREG)
   UNION ALL SELECT CODEENTITE,
                    NOMENTITE,
                    CODEBANQUE,
                    CODEREGION,
                    LOCALI,
                    CVILLE,
                    SECTGEO,
                    OPERATIONNELLE
   FROM ENTITE_NIVEAU_1)
SELECT tmp2.*,
       d.NOMBANQUE
FROM
  (SELECT tmp1.*,
          c.NOMENTIT AS NOMREGION
   FROM
     (SELECT (CASE
                  WHEN CODEAGENCE IS NULL THEN CODESUCC
                  ELSE CODEAGENCE
              END) AS CODEAGENCE,
             (CASE
                  WHEN NOMAGENCE IS NULL THEN NOMSUCC
                  ELSE NOMAGENCE
              END) AS NOMAGENCE,
             (CASE
                  WHEN CODEAGENCE IS NULL THEN NULL
                  ELSE CODESUCC
              END) AS CODESUCC,
             (CASE
                  WHEN NOMAGENCE IS NULL THEN NULL
                  ELSE NOMSUCC
              END) AS NOMSUCC,
             (CASE
                  WHEN CODEREGION0 IS NULL THEN CODEREGION1
                  ELSE CODEREGION0
              END) AS CODEREGION,
             (CASE
                  WHEN CODEBANQUE0 IS NULL THEN CODEBANQUE1
                  ELSE CODEBANQUE0
              END) AS CODEBANQUE
      FROM
        (SELECT a.CODEAGENCE,
                a.NOMAGENCE,
                a.CODEREGION AS CODEREGION0,
                b.CODEREGION AS CODEREGION1,
                b.CODEENTITE AS CODESUCC,
                b.NOMENTITE AS NOMSUCC,
                a.CODEBANQUE AS CODEBANQUE0,
                b.CODEBANQUE AS CODEBANQUE1
         FROM ENTITE_NIVEAU_0 a
         RIGHT JOIN niv_1 b ON a.CODESUCCURSALE=b.CODEENTITE)) tmp1
   LEFT JOIN
     (SELECT ENTITE,
             NOMENTIT,
             DIRZONE AS CODEBANQUE,
             LOCALI,
             CVILLE,
             SECTGEO,
             OPERATIONNELLE
      FROM EXP_T608_2
      WHERE (GRREG IS NULL
             OR GRREG=ENTITE
             OR GRREG not in
               (SELECT CODEREGION
                FROM ENTITE_NIVEAU_0)
             OR GRREG not in
               (SELECT CODEREGION
                FROM niv_1))
        AND (DFVAL > CURRENT_DATE())
        AND (DIRZONE IS NOT NULL
             OR ENTITE in
               (SELECT CODEBANQUE
                FROM ENTITE_NIVEAU_3)) ) c ON tmp1.CODEREGION=c.ENTITE) tmp2
LEFT JOIN ENTITE_NIVEAU_3 d ON tmp2.CODEBANQUE=d.CODEBANQUE"""
    )


def build_dim_compte(
    compte_commercial: DataFrame,
    client: DataFrame,
    titulaire: DataFrame,
    sous_compte: DataFrame,
    elt_contrat: DataFrame,
    contrat: DataFrame,
    fdc_compte: DataFrame,
    t705: DataFrame,
) -> DataFrame:
    base_query = (
        compte_commercial.alias("q")
        .join(
            client.alias("EXPLOIT_CLIENT"),
            col("EXPLOIT_CLIENT.NUMEROTIERS") == col("EXPLOIT_CLIENT.NUMEROTIERS"),
        )
        .join(
            titulaire.alias("EXPLOIT_TITULAIRE"),
            col("EXPLOIT_CLIENT.NUMEROTIERS") == col("EXPLOIT_TITULAIRE.NUMEROTIERS"),
        )
        .join(
            sous_compte.alias("EXPLOIT_SOUSCOMPTE"),
            (col("q.NUMERODECOMPTE") == col("EXPLOIT_SOUSCOMPTE.NUMERODECOMPTE")),
        )
        .join(
            elt_contrat.alias("EXPLOIT_ELT_CONTRAT"),
            (
                (
                    col("EXPLOIT_TITULAIRE.NUMEROCONTRATINTERNE")
                    == col("EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE")
                )
                & (
                    col("EXPLOIT_SOUSCOMPTE.NUMEROELEMENTINTERNE")
                    == col("EXPLOIT_ELT_CONTRAT.NUMEROELEMENTINTERNE")
                )
            ),
        )
        .join(
            contrat.alias("EXPLOIT_CONTRAT"),
            col("EXPLOIT_ELT_CONTRAT.NUMEROCONTRATINTERNE")
            == col("EXPLOIT_CONTRAT.NUMEROCONTRATINTERNE"),
        )
        .where(
            (col("EXPLOIT_TITULAIRE.TITULAIREPRINCIPAL") == 1)
            & (col("EXPLOIT_ELT_CONTRAT.TYPEDEPRODUIT") == "CA03")
        )
        .select(
            col("EXPLOIT_CLIENT.NUMEROTIERS").alias("NUMEROTIERS"),
            col("q.NUMERODECOMPTE").alias("NUMERODECOMPTE"),
            col("EXPLOIT_CLIENT.DEVISE").alias("DEVISE"),
            col("q.INTITULE").alias("INTITULE"),
            when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == lit("0"), "OUVERT")
            .when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == lit("1"), "OUVERT")
            .when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == lit("3"), "CLOTURE")
            .when(col("EXPLOIT_SOUSCOMPTE.ETATCOMPTE") == lit("5"), "ACLORE")
            .otherwise("DEMCLOT")
            .alias("ETAT_COMPTE"),
            col("EXPLOIT_ELT_CONTRAT.DATEEFFET").alias("DATEEFFET"),
            col("EXPLOIT_ELT_CONTRAT.DATEECHEANCE").alias("DATEECHEANCE"),
            col("q.NATURECOMPTE").alias("NATURECOMPTE"),
        )
    )

    # Adding FDCTIERS
    core_with_fdctiers = base_query.join(
        fdc_compte.alias("a"),
        (
            (base_query["NUMERODECOMPTE"] == col("a.NUMERODECOMPTE"))
            & (col("a.TYPEFDCCOMPTE") == 1)
        ),
    ).select(
        base_query["NUMEROTIERS"],
        base_query["NUMERODECOMPTE"],
        base_query["DEVISE"],
        base_query["INTITULE"],
        base_query["ETAT_COMPTE"],
        base_query["DATEEFFET"],
        base_query["DATEECHEANCE"],
        base_query["NATURECOMPTE"],
        col("a.LIEUFDCCOMPTE").alias("LIEUFDCCOMPTE"),
        col("a.CODEFONDSDECOMMERCE").alias("CODEFONDSDECOMMERCE"),
    )

    # First LEFT JOIN with T705
    core2 = core_with_fdctiers.join(
        t705.alias("b"),
        (col("b.RATTACHE") == core_with_fdctiers["LIEUFDCCOMPTE"])
        & (col("b.CODEFONDSDECOMMERCE") == core_with_fdctiers["CODEFONDSDECOMMERCE"])
        & (col("b.DFVAL") > current_date()),
    ).select(
        "NUMEROTIERS",
        "NUMERODECOMPTE",
        "NATURECOMPTE",
        "INTITULE",
        "ETAT_COMPTE",
        "DEVISE",
        col("b.RATTACHE").alias("AGENCEGEST"),
        col("b.CDGEST").alias("CD_FDC_GEST"),
        col("b.GESTIONNAIRE").alias("GESTIONNAIRE_GEST"),
    )

    # Second FDCTIERS join and renaming for clarity
    core3 = (
        core2.alias("core2")
        .join(
            fdc_compte.alias("a"),
            (col("core2.NUMERODECOMPTE") == col("a.NUMERODECOMPTE"))
            & (col("a.TYPEFDCCOMPTE") == 2),
        )
        .select(
            col("a.NUMERODECOMPTE").alias("NUMERODECOMPTE"),
            "NUMEROTIERS",
            "NATURECOMPTE",
            "INTITULE",
            "ETAT_COMPTE",
            "DEVISE",
            "AGENCEGEST",
            "CD_FDC_GEST",
            "GESTIONNAIRE_GEST",
            col("a.LIEUFDCCOMPTE").alias("LIEUFDCCOMPTE"),
            col("a.CODEFONDSDECOMMERCE").alias("CODEFONDSDECOMMERCE"),
        )
    )

    # Second LEFT JOIN with T705 for core4 and final selection
    core4 = core3.join(
        t705.alias("b"),
        (core3["LIEUFDCCOMPTE"] == col("b.RATTACHE"))
        & (core3["CODEFONDSDECOMMERCE"] == col("b.CODEFONDSDECOMMERCE"))
        & (col("b.DFVAL") > current_date()),
    ).select(
        core3["NUMEROTIERS"],
        core3["NUMERODECOMPTE"],
        core3["NATURECOMPTE"],
        core3["INTITULE"],
        core3["ETAT_COMPTE"],
        core3["DEVISE"],
        core3["AGENCEGEST"],
        core3["CD_FDC_GEST"],
        core3["GESTIONNAIRE_GEST"],
        col("b.RATTACHE").alias("AGENCEOP"),
        col("b.CDGEST").alias("CD_FDC_OP"),
        col("b.GESTIONNAIRE").alias("GESTIONNAIRE_OP"),
    )

    # Since the SQL query specifies DISTINCT, let's apply a distinct operation
    # to the final DataFrame.

    distinct_core4 = core4.distinct()
    return distinct_core4


def build_dim_level0(exp_t608_2: DataFrame) -> DataFrame:
    return exp_t608_2.selectExpr(
        "ENTITE as CODEAGENCE",
        "NOMENTIT as NOMAGENCE",
        "GRREG as CODEREGION",
        "DIRZONE as CODEBANQUE",
        "SUCCURSALE AS CODESUCCURSALE",
        "TELAGE AS TELEPHONE",
        "LOCALI",
        "CVILLE",
        "SECTGEO",
        "OPERATIONNELLE",
    ).filter(
        (col("Entite").isNotNull())
        & (col("GRREG").isNotNull())
        & (col("DFVAL") > current_date())
        & (col("SUCCURSALE").isNotNull())
    )


def build_dim_level1(exp_t608_2: DataFrame) -> DataFrame:
    # Define the conditions
    conditions = (
        col("SUCCURSALE").isNull()
        & col("ENTITE").isNotNull()
        & col("GRREG").isNotNull()
        & (col("ENTITE") < 800)
        & (col("DFVAL") > current_date())
        & (col("ENTITE") != col("GRREG"))
    )

    # Subquery to get distinct GRREG values
    subquery = (
        exp_t608_2.filter(col("GRREG").isNotNull()).select("GRREG").distinct().collect()
    )
    grreg_list = [row["GRREG"] for row in subquery]

    # Apply the conditions and perform the subquery filter
    return (
        exp_t608_2.filter(conditions)
        .filter(~col("ENTITE").isin(grreg_list))
        .select(
            col("ENTITE").alias("CODEENTITE"),
            col("NOMENTIT").alias("NOMENTITE"),
            col("GRREG").alias("CODEREGION"),
            col("DIRZONE").alias("CODEBANQUE"),
            col("TELAGE").alias("TELEPHONE"),
            col("LOCALI"),
            col("CVILLE"),
            col("SECTGEO"),
            col("OPERATIONNELLE"),
        )
    )


def build_dim_level3(exp_t608_2: DataFrame) -> DataFrame:
    conditions = (
        col("GRREG").isNull()
        & (col("DFVAL") > current_date())
        & col("DIRZONE").isNull()
    )

    # Apply the conditions, select the required columns, and order by ENTITE
    return exp_t608_2.filter(conditions).select(
        col("ENTITE").alias("CODEBANQUE"),
        col("NOMENTIT").alias("NOMBANQUE"),
        col("LOCALI"),
        col("CVILLE"),
        col("SECTGEO"),
        col("OPERATIONNELLE"),
    )


def build_dim_type_ress_emp(capjour_param2_oe: DataFrame) -> DataFrame:
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


def build_emplois_daily_hist(
    capjour_param2_oe: DataFrame,
    type_ress_emp: DataFrame,
    mapping_type_emploi: DataFrame,
) -> DataFrame:
    return (
        capjour_param2_oe.alias("a")
        .join(
            mapping_type_emploi.alias("c"),
            expr("a.PCI = c.PCI AND a.NATURE_EXTERNE = c.NATURE_EXTERNE"),
            "left_outer",
        )
        .join(
            type_ress_emp.alias("e"),
            expr(
                "a.PCI = concat(e.PCI, '0')"
                "AND a.NATURE_EXTERNE = e.NATURE AND e.categorie = 'EMP'"
            ),
            "left_outer",
        )
        .select(
            col("a.DTTRTM"),
            col("a.PCI"),
            col("a.NATURE_EXTERNE").alias("NATURE"),
            col("a.numerodecompte"),
            col("a.devise"),
            col("a.soldejourcr"),
            col("a.soldejourdb"),
            col("a.soldejourcr_devise"),
            col("a.soldejourdb_devise"),
            col("c.TYPE_EMPLOI").alias("NOUVEAU_TYPE_EMPLOI"),
            col("e.Type_RESS_EMPO").alias("ANCIEN_TYPE_EMPLOI"),
        )
        .filter(capjour_param2_oe["soldejourdb"].isNotNull())
    )


def build_ress_daily_hist(
    sldcli_nature_daily_devises: DataFrame,
    mapping_type_ressource: DataFrame,
    type_ress_emp: DataFrame,
) -> DataFrame:
    return (
        sldcli_nature_daily_devises.alias("a")
        .join(
            mapping_type_ressource.alias("c"),
            expr("a.PCI = concat(c.PCI, '0') AND a.NATURE_EXTERNE = c.NATURE"),
            "left_outer",
        )
        .join(
            type_ress_emp.alias("e"),
            expr(
                "a.PCI = concat(e.PCI, '0')"
                "AND a.NATURE_EXTERNE = e.NATURE and e.CATEGORIE='RESS'"
            ),
            "left_outer",
        )
        .select(
            col("a.DTTRTM"),
            col("a.PCI"),
            col("a.NATURE_EXTERNE").alias("NATURE"),
            col("a.numerodecompte"),
            col("a.devise"),
            col("a.soldejourcr"),
            col("a.soldejourdb"),
            col("a.soldejourcr_devise"),
            col("a.soldejourdb_devise"),
            col("c.TYPE_RESSOURCE").alias("NOUVEAU_TYPE"),
            col("e.Type_RESS_EMPO").alias("ANCIEN_TYPE"),
        )
        .filter(sldcli_nature_daily_devises["soldejourcr"].isNotNull())
    )


def build_sldcli_nature_daily_devises(
    spark: SparkSession,
    dm_sldcli: DataFrame,
    rp_gl_bct: DataFrame,
    reference_nature_compte_pci: DataFrame,
    taux_change_bam_hist: DataFrame,
    exploit_ref_devise: DataFrame,
) -> DataFrame:
    dm_sldcli.createOrReplaceTempView("dm_sldcli")
    rp_gl_bct.createOrReplaceTempView("rp_gl_bct")
    reference_nature_compte_pci.createOrReplaceTempView("reference_nature_compte_pci")
    taux_change_bam_hist.createOrReplaceTempView("taux_change_bam_hist")
    exploit_ref_devise.createOrReplaceTempView("exploit_ref_devise")

    return spark.sql(
        """
WITH devises AS
(SELECT a.CODE_DEVISE,
        b.CD_DEV_OPER,
        a.TAUXDECHANGE,
        a.TYPE,
        a.DATE
FROM TAUX_CHANGE_BAM_HIST a
JOIN EXPLOIT_REF_DEVISE b ON a.CODE_DEVISE=b.CD_DEVISE)
SELECT DISTINCT
tmp1.DTTRTM,
tmp1.PCI,
tmp1.NATURE_EXTERNE,
tmp1.NUMERODECOMPTE ,
tmp1.DEVISE,
tmp2.TYPE,
CAST(tmp1.SOLDEJOURCR AS DECIMAL(38, 4)) AS SOLDEJOURCR,
CAST(tmp1.SOLDEJOURCR AS DECIMAL(38, 4)) * CAST(tmp2.TAUXDECHANGE AS DECIMAL(38, 4)) AS SOLDEJOURCR_devise,
CAST(tmp1.SOLDEJOURDB AS DECIMAL(38, 4)) AS SOLDEJOURDB,
CAST(tmp1.SOLDEJOURDB AS DECIMAL(38, 4)) * CAST(tmp2.TAUXDECHANGE AS DECIMAL(38, 4)) AS SOLDEJOURDB_devise
FROM
(SELECT c.DTTRTM,
        f.PCI,
        (CASE
            WHEN d.nature_externe IS NULL THEN SUBSTRING(c.NUMERODECOMPTE, 4, 3)
            ELSE d.nature_externe
        END) AS NATURE_EXTERNE,
        c.NUMERODECOMPTE,
        c.DEVISE,
        c.SOLDEJOURCR,
        c.SOLDEJOURDB,
        (CASE
            WHEN f.PCI IN ('111300',
                            '111400',
                            '112110',
                            '112120',
                            '461100',
                            '462100',
                            '466130') THEN 'BILLET'
            ELSE 'VIREMENT'
        END) AS TYPE
FROM DM_SLDCLI c
LEFT JOIN RP_GL_BCT d ON c.NUMERODECOMPTE=d.COMPTE_CLIENT
LEFT JOIN REFERENCE_NATURE_COMPTE_PCI f ON c.NTCPTE=f.Nature) tmp1
LEFT JOIN devises tmp2 ON tmp1.DEVISE=tmp2.CD_DEV_OPER
AND tmp1.TYPE=tmp2.TYPE
AND CAST(tmp1.DTTRTM AS DATE)=CAST(tmp2.DATE AS DATE);
    """
    )


def build_taux_change_bam_hist(
    spark: SparkSession,
    taux_change_billet_hist: DataFrame,
    taux_change_virement_hist: DataFrame,
) -> DataFrame:
    df_virement = taux_change_virement_hist.withColumn("TYPE", lit("VIREMENT"))
    df_billet = taux_change_billet_hist.withColumn("TYPE", lit("BILLET"))

    df_billet.createOrReplaceTempView("BILLET")
    df_virement.createOrReplaceTempView("VIREMENT")

    return spark.sql(
        """
        SELECT * FROM BILLET
        UNION
        SELECT * FROM VIREMENT ;
    """
    )
