from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    ceil,
    col,
    concat,
    concat_ws,
    date_format,
    dayofmonth,
    dayofweek,
    expr,
    lit,
    max,
    min,
    month,
    quarter,
    rank,
    regexp_replace,
    rtrim,
    substring,
    sum,
    to_date,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import FloatType, IntegerType

from src.transformation.common import (
    get_last_recorded_sold_per_client_from,
    get_last_recorded_sold_per_client_from_last_month,
    get_last_recorded_sold_per_client_from_last_quarter,
    get_last_recorded_sold_per_client_from_last_year,
)


def build_pourcent_quarter_saisonnality(df: DataFrame) -> DataFrame:
    """
    La fonction permet d'ajouter la colonne POURC_TRIMESTRE dans la table
    NBR_SAISONNALITE. Cette colonne est indispensable pour le calcul de TRO TRIMESTRE

    INPUT: df (dataframe contenant les infos de NBR_SAISONNALITE)
    OUTPUT: NBR_SAISONNALITE + colonne POURC_TRIMESTRE
    """

    """
    - Traitement de colonnes POURC_SEMAINE, POURC_CUMUL qui sont normalement des nombres
       mais ont été enregistré comme une chaine de caractère. Donc on remplace ',' par '.'
    - NUMERO_SEMAINE (weekofyear) doit etre également casté en Float
    - Ajout de la colonne TRIMESTRE en se basant sur la date de fin de la semaine
    """
    df = (
        df.withColumn(
            "POURC_SEMAINE",
            regexp_replace("POURC_SEMAINE", ",", ".").cast(FloatType()),
        )
        .withColumn(
            "POURC_CUMUL", regexp_replace("POURC_CUMUL", ",", ".").cast(FloatType())
        )
        .withColumn("NUMERO_SEMAINE", col("NUMERO_SEMAINE").cast(IntegerType()))
        .withColumn("TRIMESTRE", quarter(to_date(col("DFS"), "dd/MM/yyyy")))
    )

    """
    - Pour chaque trimestre de l'année, l'on veut juste recuperer la dernière semaine du meme trimestre
    """
    window_spec = Window.partitionBy("partitiondate", "ANNEE", "TRIMESTRE")
    max_week_trimestre = (
        df.withColumn(
            "MAX_NUMERO_SEMAINE", max(col("NUMERO_SEMAINE")).over(window_spec)
        )
        .filter(col("NUMERO_SEMAINE") == col("MAX_NUMERO_SEMAINE"))
        .select("partitiondate", "ANNEE", "TRIMESTRE", "NUMERO_SEMAINE")
    )

    """
    - Calcul du pourcentage de l'objectif trimestre qui n'est autre que le pourcentage
        cumul de jusqu'à la dernière semaine du trimestre
    """
    pour_trimestre = max_week_trimestre.join(
        df, ["ANNEE", "TRIMESTRE", "NUMERO_SEMAINE", "partitiondate"], how="inner"
    ).select(
        "ANNEE",
        "TRIMESTRE",
        col("POURC_CUMUL").alias("POURC_TRIMESTRE"),
        "partitiondate",
    )

    """
    Recuperer d'autres colonnes initiales puis retourner le résultat
    """
    return df.join(
        pour_trimestre, ["ANNEE", "TRIMESTRE", "partitiondate"], how="left"
    ).select(
        "DDS",
        "DFS",
        "ANNEE",
        "NUMERO_SEMAINE",
        "NOMBRE_JOURS",
        "POURC_SEMAINE",
        "POURC_CUMUL",
        "POURC_TRIMESTRE",
        "partitiondate",
    )


def build_opcvm(
    v_corres_val: DataFrame,
    v_cours_ref: DataFrame,
    v_solde_clt: DataFrame,
    infos_clients: DataFrame,
    compte_client: DataFrame,
    compte_tenu: DataFrame,
    agence_infos: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    La fonction permet de construire la table enrichie OPCVM qui sera stockée dans le SOCLE.
    Cette dernière est construite en se basant sur les règles de gestion de DATAWAREHOUSE
    INPUT:  - v_corres_val
            - v_cours_ref
            - v_cours_ref
            - infos_clients: contient les infos du clients (TIERS)
            - compte_client: contient les infos des comptes de clients
            - compte_tenu: contient les infos des comptes tenus par clients (c-à-d comptes absents dans compte_client)
            - agence_infos: contient les infos liées aux agences et leur niveau de regroupement (REGION, GROUPE, etc.)
            - partition_date: est la date de partitionnement (ou date cible de traitement de données)

    OUTPUT: la table ENCOURS_OPCVM à date du partition_date
    """

    """
    - Selection et renommage des colonnes utiles pour la construction de la table enrichie
    """
    compte_client = compte_client.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AGENCEGEST").alias("AGENCE_GEST"),
        col("CD_FDC_GEST").alias("CODE_FDC_GEST"),
        col("AGENCEOP").alias("AGENCE_OP"),
        col("CD_FDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    compte_tenu = compte_tenu.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AgenceGEST").alias("AGENCE_GEST"),
        col("CDFDC_GEST").alias("CODE_FDC_GEST"),
        col("AgenceOP").alias("AGENCE_OP"),
        col("CDFDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    agence_infos = agence_infos.select(
        "partitiondate",
        col("CODEAGENCE").alias("CODE_AGENCE"),
        col("NOMAGENCE").alias("NOM_AGENCE"),
        col("succursale").alias("CODE_GROUPE"),
        col("nomsuccursale").alias("NOM_GROUPE"),
        col("CODEREGION").alias("CODE_REGION"),
        col("NOMREGION").alias("NOM_REGION"),
        col("CODEBANQUE").alias("CODE_BANQUE"),
        col("NOMBANQUE").alias("NOM_BANQUE"),
    )

    infos_clients = infos_clients.select(
        "partitiondate",
        col("numerotiers").alias("NUMEROTIERS"),
        col("marche").alias("MARCHE"),
        col("segment").alias("SEGMENT"),
    )

    """
    - Extraction de numéro de compte à base de la colonne TITULAIRE_CODE_CDM de v_solde_clt
    - Suppression d'espace vide (de trop) dans CODE_VALEUR_CDM
    - Calcul de l'encours: SD_CR - SD_DB
    """
    query = (
        v_solde_clt.withColumn(
            "NUMERO_COMPTE", substring(col("TITULAIRE_CODE_CDM"), 2, 11)
        )
        .withColumn("CODE_VALEUR", rtrim(col("CODE_VALEUR_CDM")))
        .withColumn("ENCOURS", col("SD_CR") - col("SD_DB"))
    )

    """
    - Pour chaque combinaison TITULAIRE_CODE_CDM, NUMERO_COMPTE, CODE_VALEUR, SD_MAJ (DATE_REALISATION),
        on calcule NOMBRE_VALEUR = sum("ENCOURS")
    """
    window_spec = Window.partitionBy(
        "partitiondate",
        "TITULAIRE_CODE_CDM",
        "NUMERO_COMPTE",
        "CODE_VALEUR",
        "SD_MAJ",
    )
    query_1 = (
        query.withColumn("NOMBRE_VALEUR", sum("ENCOURS").over(window_spec))
        .withColumn(
            "SD_MAJ", to_date(date_format(col("SD_MAJ"), format="yyyy-MM-dd"))
        )
        .select(
            "partitiondate",
            "TITULAIRE_CODE_CDM",
            "NUMERO_COMPTE",
            "CODE_VALEUR",
            col("SD_MAJ").alias("DATE_REALISATION"),
            "NOMBRE_VALEUR",
        )
    )

    """
    - Jointure entre v_cours_ref et v_corres_val avec v_cours_ref.COURS_REF_ISIN == v_corres_val.VAL_ISIN_CODE
    - Pour chaque combinaison VAL_ISIN_CODE, CAT_LIB_COURT, TYPE_LIB_COURT_FR, CODE_VALEUR_CDM, COURS_REF_ISIN,
      on détermine COURS_REF_DATE = max("COURS_REF_DATE")
    """
    window_spec = Window.partitionBy(
        "partitiondate",
        "VAL_ISIN_CODE",
        "CAT_LIB_COURT",
        "TYPE_LIB_COURT_FR",
        "CODE_VALEUR_CDM",
        "COURS_REF_ISIN",
    )
    query_3 = (
        v_cours_ref.join(
            v_corres_val,
            (v_cours_ref.COURS_REF_ISIN == v_corres_val.VAL_ISIN_CODE)
            & (v_cours_ref.partitiondate == v_corres_val.partitiondate),
        )
        .select(
            v_cours_ref["partitiondate"],
            "VAL_ISIN_CODE",
            "CAT_LIB_COURT",
            "TYPE_LIB_COURT_FR",
            "CODE_VALEUR_CDM",
            "COURS_REF_ISIN",
            "COURS_REF_DATE",
        )
        .withColumn("MAX_COURS_REF_DATE", max("COURS_REF_DATE").over(window_spec))
        .filter(col("COURS_REF_DATE") == col("MAX_COURS_REF_DATE"))
        .select(
            "partitiondate",
            "VAL_ISIN_CODE",
            "CAT_LIB_COURT",
            "TYPE_LIB_COURT_FR",
            "CODE_VALEUR_CDM",
            "COURS_REF_ISIN",
            "COURS_REF_DATE",
        )
    )

    """
    - Jointure entre query_3 et v_cours_ref
    - Redefinition de la colonne COURS_REF:
        Si COURS_REF_ISIN='BE0009525883' alors COURS_REF = 0
        Sinon COURS_REF = COURS_REF de V_COURS_REF
    """
    query_4 = query_3.join(
        v_cours_ref,
        ["COURS_REF_DATE", "COURS_REF_ISIN", "partitiondate"],
        how="left",
    ).withColumn(
        "COURS_REF",
        when(col("COURS_REF_ISIN") == "BE0009525883", 0).otherwise(col("COURS_REF")),
    )

    """
    - Ajout de COURS_REF_M = max("COURS_REF") pour chaque VAL_ISIN_CODE, CAT_LIB_COURT, TYPE_LIB_COURT_FR, CODE_VALEUR_CDM
    """
    window_spec = Window.partitionBy(
        "partitiondate",
        "VAL_ISIN_CODE",
        "CAT_LIB_COURT",
        "TYPE_LIB_COURT_FR",
        "CODE_VALEUR_CDM",
    )
    query_m = (
        query_4.withColumn("COURS_REF_M", max("COURS_REF").over(window_spec))
        .filter(col("COURS_REF_M") == col("COURS_REF"))
        .select(
            col("partitiondate").alias("partition_date"),
            "VAL_ISIN_CODE",
            "CAT_LIB_COURT",
            "TYPE_LIB_COURT_FR",
            "CODE_VALEUR_CDM",
            "COURS_REF_M",
        )
    )

    """
    - Nouvelles colonnes COURS et LIBELLE_VALEUR
        - COURS = COURS_REF_M si NUMERO_COMPTE n'est pas null
            COURS = Null si NUMERO_COMPTE est null
        - LIBELLE_VALEUR = VAL_ISIN_CODE si NUMERO_COMPTE n'est pas null
            LIBELLE_VALEUR = Null si NUMERO_COMPTE est null
    - ENCOURS = NOMBRE_VALEUR * COURS
    - Filtrage sur partitiondate == partition_date
    - Selection et renommage de colonnes
    """
    df_final = (
        query_1.join(
            query_m,
            (query_1.CODE_VALEUR == rtrim(query_m.CODE_VALEUR_CDM))
            & (query_1.partitiondate == query_m.partition_date),
            how="left",
        )
        .withColumn(
            "COURS",
            when(col("NUMERO_COMPTE").isNull(), None).otherwise(col("COURS_REF_M")),
        )
        .withColumn(
            "LIBELLE_VALEUR",
            when(col("NUMERO_COMPTE").isNull(), None).otherwise(
                col("VAL_ISIN_CODE")
            ),
        )
        .withColumn("DATE_TRAITEMENT", col("partitiondate"))
        .withColumn("ENCOURS", col("NOMBRE_VALEUR") * col("COURS"))
        .filter(col("partitiondate") == partition_date)
        .select(
            "DATE_TRAITEMENT",
            "DATE_REALISATION",
            "NUMERO_COMPTE",
            "CODE_VALEUR",
            "LIBELLE_VALEUR",
            col("CAT_LIB_COURT").alias("CATEGORIE_LIBELLE"),
            col("TYPE_LIB_COURT_FR").alias("TYPE_LIBELLE"),
            "NOMBRE_VALEUR",
            "COURS",
            "ENCOURS",
            "partitiondate",
        )
    )

    """
    - Ajout des infos de comptes aux precedentes df_final:
        - Jointure avec compte_client et ne garder que les comptes matchés (c-à-d INTITULE existe)
        - Jointure avec compte_tenu pour les comptes n'apparaissont pas dans compte_client (c-à-d INTITULE null)
        - Regrouper les deux parties des infos comptes liées à df_final
    """
    df_final_compte = df_final.join(
        compte_client, ["NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df_final_compte_non_null = df_final_compte.filter(col("INTITULE").isNotNull())
    df_final_2 = df_final_compte_non_null.select(*list(df_final.columns))
    df_final_comptetenu = df_final.exceptAll(df_final_2).join(
        compte_tenu, ["NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df = df_final_compte_non_null.unionByName(df_final_comptetenu)

    """
    - Ajout des infos d'agences et de niveau de regroupement
    - Ajout des infos de clients (MARCHE, SEGMENT)

    NB: substring(df_final.NUMERO_COMPTE, 1, 3) peut etre remplacé par FDC_GEST/FDC_OP selon le besoin
    """
    agence_infos = agence_infos.withColumnRenamed("partitiondate", "date")
    infos_clients = infos_clients.withColumnRenamed("partitiondate", "dttrm")

    return (
        df.join(
            agence_infos,
            (substring(df_final.NUMERO_COMPTE, 1, 3) == agence_infos.CODE_AGENCE)
            & (df_final.partitiondate == agence_infos.date),
            how="left",
        )
        .join(
            infos_clients,
            (df.NUMERO_TIERS == infos_clients.NUMEROTIERS)
            & (df.partitiondate == infos_clients.dttrm),
            how="left",
        )
        .select(
            "DATE_TRAITEMENT",
            "DATE_REALISATION",
            "NUMERO_TIERS",
            "NUMERO_COMPTE",
            "INTITULE",
            "CODE_VALEUR",
            "LIBELLE_VALEUR",
            "CATEGORIE_LIBELLE",
            "TYPE_LIBELLE",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "AGENCE_OP",
            "AGENCE_GEST",
            "MARCHE",
            "SEGMENT",
            "CODE_FDC_GEST",
            "CODE_FDC_OP",
            "NOMBRE_VALEUR",
            "COURS",
            "ENCOURS",
            "partitiondate",
        )
    )


def build_encours_assurance_axa(
    epargne_axa: DataFrame,
    contrat_produit: DataFrame,
    compte_client: DataFrame,
    compte_tenu: DataFrame,
    infos_clients: DataFrame,
    agence_infos: DataFrame,
) -> DataFrame:
    """
    La fonction permet de construire la table enrichie E.
    Cette dernière est construite en se basant sur les règles de gestion de DATAWAREHOUSE
    INPUT:  - epargne_axa: contient les données provenant de fichier ingéré (Epargne_AXA.txt)
            - contrat_produit: contient les données de produits (CARTES, PACKAGES, etc.)
            - infos_clients: contient les infos du clients (TIERS)
            - compte_client: contient les infos des comptes de clients
            - compte_tenu: contient les infos des comptes tenus par clients (c-à-d comptes absents dans compte_client)
            - agence_infos: contient les infos liées aux agences et leur niveau de regroupement (REGION, GROUPE, etc.)

    OUTPUT: la table ENCOURS_AXA_ASSURANCE
    """

    """
    - Selection et renommage des colonnes utiles pour la construction de la table enrichie
    """
    contrat_produit = contrat_produit.select(
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        "NUMEROCARTE",
        "ETATPRODUIT",
        "partitiondate",
    )

    compte_client = compte_client.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AGENCEGEST").alias("AGENCE_GEST"),
        col("CD_FDC_GEST").alias("CODE_FDC_GEST"),
        col("AGENCEOP").alias("AGENCE_OP"),
        col("CD_FDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    compte_tenu = compte_tenu.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AgenceGEST").alias("AGENCE_GEST"),
        col("CDFDC_GEST").alias("CODE_FDC_GEST"),
        col("AgenceOP").alias("AGENCE_OP"),
        col("CDFDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    agence_infos = agence_infos.select(
        "partitiondate",
        col("CODEAGENCE").alias("CODE_AGENCE"),
        col("NOMAGENCE").alias("NOM_AGENCE"),
        col("succursale").alias("CODE_GROUPE"),
        col("nomsuccursale").alias("NOM_GROUPE"),
        col("CODEREGION").alias("CODE_REGION"),
        col("NOMREGION").alias("NOM_REGION"),
        col("CODEBANQUE").alias("CODE_BANQUE"),
        col("NOMBANQUE").alias("NOM_BANQUE"),
    )

    infos_clients = infos_clients.select(
        "partitiondate",
        col("numerotiers").alias("NUMEROTIERS"),
        col("marche").alias("MARCHE"),
        col("segment").alias("SEGMENT"),
    )

    """
    - Reconstruction de la colonne PRODUIT à travers des filtres
    - Ajout de la colonne IDENTIFIANT = '0' & Field2
    - Cast de la colonne ENCOURS en Float
    """
    epargne_axa = (
        epargne_axa.select(
            "ENTREPRISE", "PRODUIT", "Field2", "DATE", "ENCOURS", "partitiondate"
        )
        .withColumn(
            "PRODUIT",
            when(col("PRODUIT").isin("223", "2223"), "SE01")
            .when(col("PRODUIT").isin("320", "2320"), "SR01")
            .otherwise(None),
        )
        .withColumn("IDENTIFIANT", concat(lit("0"), epargne_axa.Field2))
        .withColumn("DATE_TRAITEMENT", col("partitiondate"))
        .withColumn("DATE", to_date(col("DATE"), "yyyyMMdd"))
        .withColumn("ENCOURS", col("ENCOURS").cast(FloatType()))
    )

    """
    - Jointure avec contrat_produit pour recuperer NUMERO_TIERS et NUMERO_COMPTE
    - Clé de jointure IDENTIFIANT = NUMEROCARTE et ETATPRODUIT = VALIDE
    """
    contrat_produit = contrat_produit.withColumnRenamed("partitiondate", "dttrm")
    df_final = epargne_axa.join(
        contrat_produit,
        (epargne_axa.IDENTIFIANT == contrat_produit.NUMEROCARTE)
        & (contrat_produit.ETATPRODUIT == "VALIDE")
        & (epargne_axa.partitiondate == contrat_produit.dttrm),
        how="inner",
    ).select(
        "ENTREPRISE",
        "PRODUIT",
        "NUMERO_TIERS",
        "NUMERO_COMPTE",
        "IDENTIFIANT",
        "ENCOURS",
        "DATE_TRAITEMENT",
        "partitiondate",
    )

    """
    - Ajout des infos de comptes aux precedentes df_final:
        - Jointure avec compte_client et ne garder que les comptes matchés (c-à-d INTITULE existe)
        - Jointure avec compte_tenu pour les comptes n'apparaissont pas dans compte_client (c-à-d INTITULE null)
        - Regrouper les deux parties des infos comptes liées à df_final
    """
    df_final_compte = df_final.join(
        compte_client, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df_final_compte_non_null = df_final_compte.filter(col("INTITULE").isNotNull())
    df_final_2 = df_final_compte_non_null.select(*list(df_final.columns))
    df_final_comptetenu = df_final.exceptAll(df_final_2).join(
        compte_tenu, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df = df_final_compte_non_null.unionByName(df_final_comptetenu)

    """
    - Ajout des infos d'agences et de niveau de regroupement
    - Ajout des infos de clients (MARCHE, SEGMENT)

    NB: substring(df_final.NUMERO_COMPTE, 1, 3) peut etre remplacé par FDC_GEST/FDC_OP selon le besoin
    """
    agence_infos = agence_infos.withColumnRenamed("partitiondate", "date")
    infos_clients = infos_clients.withColumnRenamed("partitiondate", "dttrm")

    return (
        df.join(
            agence_infos,
            (substring(df_final.NUMERO_COMPTE, 1, 3) == agence_infos.CODE_AGENCE)
            & (df_final.partitiondate == agence_infos.date),
            how="left",
        )
        .join(
            infos_clients,
            (df.NUMERO_TIERS == infos_clients.NUMEROTIERS)
            & (df.partitiondate == infos_clients.dttrm),
            how="left",
        )
        .select(
            "DATE_TRAITEMENT",
            "NUMERO_TIERS",
            "NUMERO_COMPTE",
            "INTITULE",
            "ENTREPRISE",
            "PRODUIT",
            "IDENTIFIANT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "AGENCE_OP",
            "AGENCE_GEST",
            "MARCHE",
            "SEGMENT",
            "CODE_FDC_GEST",
            "CODE_FDC_OP",
            "ENCOURS",
            "partitiondate",
        )
    )


def build_encours_assurance_rma(
    epargne_rma: DataFrame,
    contrat_produit: DataFrame,
    compte_client: DataFrame,
    compte_tenu: DataFrame,
    infos_clients: DataFrame,
    agence_infos: DataFrame,
) -> DataFrame:
    contrat_produit = contrat_produit.select(
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        "NUMEROCARTE",
        col("CODEPRODUIT").alias("PRODUIT"),
        "ETATPRODUIT",
        "partitiondate",
    )

    compte_client = compte_client.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AGENCEGEST").alias("AGENCE_GEST"),
        col("CD_FDC_GEST").alias("CODE_FDC_GEST"),
        col("AGENCEOP").alias("AGENCE_OP"),
        col("CD_FDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    compte_tenu = compte_tenu.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AgenceGEST").alias("AGENCE_GEST"),
        col("CDFDC_GEST").alias("CODE_FDC_GEST"),
        col("AgenceOP").alias("AGENCE_OP"),
        col("CDFDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    agence_infos = agence_infos.select(
        "partitiondate",
        col("CODEAGENCE").alias("CODE_AGENCE"),
        col("NOMAGENCE").alias("NOM_AGENCE"),
        col("succursale").alias("CODE_GROUPE"),
        col("nomsuccursale").alias("NOM_GROUPE"),
        col("CODEREGION").alias("CODE_REGION"),
        col("NOMREGION").alias("NOM_REGION"),
        col("CODEBANQUE").alias("CODE_BANQUE"),
        col("NOMBANQUE").alias("NOM_BANQUE"),
    )

    infos_clients = infos_clients.select(
        "partitiondate",
        col("numerotiers").alias("NUMEROTIERS"),
        col("marche").alias("MARCHE"),
        col("segment").alias("SEGMENT"),
    )

    """
    - Ajout de la colonne IDENTIFIANT = NUMERO_CONTRAT
    - Cast de la colonne ENCOURS en Float puis divisé par 100
    """
    epargne_rma = (
        epargne_rma.select(
            "ENTREPRISE", "NUMERO_CONTRAT", "ENCOURS", "partitiondate"
        )
        .withColumn("IDENTIFIANT", col("NUMERO_CONTRAT"))
        .withColumn("DATE_TRAITEMENT", col("partitiondate"))
        .withColumn("ENCOURS", col("ENCOURS").cast(FloatType()) / 100.0)
    )

    """
    - Jointure avec contrat_produit pour recuperer NUMERO_TIERS et NUMERO_COMPTE
    - Clé de jointure IDENTIFIANT = NUMEROCARTE et ETATPRODUIT = VALIDE
    """
    contrat_produit = contrat_produit.withColumnRenamed("partitiondate", "dttrm")
    df_final = epargne_rma.join(
        contrat_produit,
        (epargne_rma.IDENTIFIANT == contrat_produit.NUMEROCARTE)
        & (contrat_produit.ETATPRODUIT == "VALIDE")
        & (epargne_rma.partitiondate == contrat_produit.dttrm),
        how="inner",
    ).select(
        "ENTREPRISE",
        "PRODUIT",
        "NUMERO_TIERS",
        "NUMERO_COMPTE",
        "IDENTIFIANT",
        "ENCOURS",
        "DATE_TRAITEMENT",
        "partitiondate",
    )

    """
    - Ajout des infos de comptes aux precedentes df_final:
        - Jointure avec compte_client et ne garder que les comptes matchés (c-à-d INTITULE existe)
        - Jointure avec compte_tenu pour les comptes n'apparaissont pas dans compte_client (c-à-d INTITULE null)
        - Regrouper les deux parties des infos comptes liées à df_final
    """
    df_final_compte = df_final.join(
        compte_client, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df_final_compte_non_null = df_final_compte.filter(col("INTITULE").isNotNull())
    df_final_2 = df_final_compte_non_null.select(*list(df_final.columns))
    df_final_comptetenu = df_final.exceptAll(df_final_2).join(
        compte_tenu, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df = df_final_compte_non_null.unionByName(df_final_comptetenu)

    """
    - Ajout des infos d'agences et de niveau de regroupement
    - Ajout des infos de clients (MARCHE, SEGMENT)

    NB: substring(df_final.NUMERO_COMPTE, 1, 3) peut etre remplacé par FDC_GEST/FDC_OP selon le besoin
    """
    agence_infos = agence_infos.withColumnRenamed("partitiondate", "date")
    infos_clients = infos_clients.withColumnRenamed("partitiondate", "dttrm")

    return (
        df.join(
            agence_infos,
            (substring(df_final.NUMERO_COMPTE, 1, 3) == agence_infos.CODE_AGENCE)
            & (df_final.partitiondate == agence_infos.date),
            how="left",
        )
        .join(
            infos_clients,
            (df.NUMERO_TIERS == infos_clients.NUMEROTIERS)
            & (df.partitiondate == infos_clients.dttrm),
            how="left",
        )
        .select(
            "DATE_TRAITEMENT",
            "NUMERO_TIERS",
            "NUMERO_COMPTE",
            "INTITULE",
            "ENTREPRISE",
            "PRODUIT",
            "IDENTIFIANT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "AGENCE_OP",
            "AGENCE_GEST",
            "MARCHE",
            "SEGMENT",
            "CODE_FDC_OP",
            "CODE_FDC_GEST",
            "ENCOURS",
            "partitiondate",
        )
    )


def build_encours_assurance_saham(
    epargne_liberis: DataFrame,
    contrat_produit: DataFrame,
    compte_client: DataFrame,
    compte_tenu: DataFrame,
    infos_clients: DataFrame,
    agence_infos: DataFrame,
) -> DataFrame:
    contrat_produit = contrat_produit.select(
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        "NUMEROCARTE",
        "CODEPRODUIT",
        "ETATPRODUIT",
        "partitiondate",
    )

    compte_client = compte_client.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AGENCEGEST").alias("AGENCE_GEST"),
        col("CD_FDC_GEST").alias("CODE_FDC_GEST"),
        col("AGENCEOP").alias("AGENCE_OP"),
        col("CD_FDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    compte_tenu = compte_tenu.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AgenceGEST").alias("AGENCE_GEST"),
        col("CDFDC_GEST").alias("CODE_FDC_GEST"),
        col("AgenceOP").alias("AGENCE_OP"),
        col("CDFDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    agence_infos = agence_infos.select(
        "partitiondate",
        col("CODEAGENCE").alias("CODE_AGENCE"),
        col("NOMAGENCE").alias("NOM_AGENCE"),
        col("succursale").alias("CODE_GROUPE"),
        col("nomsuccursale").alias("NOM_GROUPE"),
        col("CODEREGION").alias("CODE_REGION"),
        col("NOMREGION").alias("NOM_REGION"),
        col("CODEBANQUE").alias("CODE_BANQUE"),
        col("NOMBANQUE").alias("NOM_BANQUE"),
    )

    infos_clients = infos_clients.select(
        "partitiondate",
        col("numerotiers").alias("NUMEROTIERS"),
        col("marche").alias("MARCHE"),
        col("segment").alias("SEGMENT"),
    )

    """
    - Ajout de la colonne IDENTIFIANT = CONTRAT
    - PRODUIT = substring(CONTRAT, 4, 4))
    - Cast de la colonne ENCOURS_NET en Float puis divisé par 100
    - ENCOURS = ENCOURS_NET
    """
    epargne_liberis = (
        epargne_liberis.select(
            "ENTREPRISE", "CONTRAT", "ENCOURS_NET", "partitiondate"
        )
        .withColumn("IDENTIFIANT", col("CONTRAT"))
        .withColumn("PRODUIT", substring(col("CONTRAT"), 4, 4))
        .withColumn("ENCOURS", col("ENCOURS_NET").cast(FloatType()) / 100.0)
        .withColumn("DATE_TRAITEMENT", col("partitiondate"))
    )

    """
    - Jointure avec contrat_produit pour recuperer NUMERO_TIERS et NUMERO_COMPTE
    - Clé de jointure:
        NUMEROCARTE = IDENTIFIANT
        ETATPRODUIT = VALIDE
        CODEPRODUIT = PRODUIT
    """
    contrat_produit = contrat_produit.withColumnRenamed("partitiondate", "dttrm")
    df_final = epargne_liberis.join(
        contrat_produit,
        (epargne_liberis.IDENTIFIANT == contrat_produit.NUMEROCARTE)
        & (epargne_liberis.PRODUIT == contrat_produit.CODEPRODUIT)
        & (contrat_produit.ETATPRODUIT == "VALIDE")
        & (epargne_liberis.partitiondate == contrat_produit.dttrm),
        how="inner",
    ).select(
        "ENTREPRISE",
        "PRODUIT",
        "NUMERO_TIERS",
        "NUMERO_COMPTE",
        "IDENTIFIANT",
        "ENCOURS",
        "DATE_TRAITEMENT",
        "partitiondate",
    )

    """
    - Ajout des infos de comptes aux precedentes df_final:
        - Jointure avec compte_client et ne garder que les comptes matchés (c-à-d INTITULE existe)
        - Jointure avec compte_tenu pour les comptes n'apparaissont pas dans compte_client (c-à-d INTITULE null)
        - Regrouper les deux parties des infos comptes liées à df_final
    """
    df_final_compte = df_final.join(
        compte_client, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df_final_compte_non_null = df_final_compte.filter(col("INTITULE").isNotNull())
    df_final_2 = df_final_compte_non_null.select(*list(df_final.columns))
    df_final_comptetenu = df_final.exceptAll(df_final_2).join(
        compte_tenu, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df = df_final_compte_non_null.unionByName(df_final_comptetenu)

    """
    - Ajout des infos d'agences et de niveau de regroupement
    - Ajout des infos de clients (MARCHE, SEGMENT)
    NB: substring(df_final.NUMERO_COMPTE, 1, 3) peut etre remplacé par FDC_GEST/FDC_OP selon le besoin
    """
    agence_infos = agence_infos.withColumnRenamed("partitiondate", "date")
    infos_clients = infos_clients.withColumnRenamed("partitiondate", "dttrm")

    return (
        df.join(
            agence_infos,
            (substring(df_final.NUMERO_COMPTE, 1, 3) == agence_infos.CODE_AGENCE)
            & (df_final.partitiondate == agence_infos.date),
            how="left",
        )
        .join(
            infos_clients,
            (df.NUMERO_TIERS == infos_clients.NUMEROTIERS)
            & (df.partitiondate == infos_clients.dttrm),
            how="left",
        )
        .select(
            "DATE_TRAITEMENT",
            "NUMERO_TIERS",
            "NUMERO_COMPTE",
            "INTITULE",
            "ENTREPRISE",
            "PRODUIT",
            "IDENTIFIANT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "AGENCE_OP",
            "AGENCE_GEST",
            "MARCHE",
            "SEGMENT",
            "CODE_FDC_OP",
            "CODE_FDC_GEST",
            "ENCOURS",
            "partitiondate",
        )
    )


def build_encours_assurance_wafa(
    encours_epargne: DataFrame,
    contrat_produit: DataFrame,
    compte_client: DataFrame,
    compte_tenu: DataFrame,
    infos_clients: DataFrame,
    agence_infos: DataFrame,
) -> DataFrame:
    contrat_produit = contrat_produit.select(
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        "NUMEROCARTE",
        "CODEPRODUIT",
        "ETATPRODUIT",
        "partitiondate",
    )

    compte_client = compte_client.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AGENCEGEST").alias("AGENCE_GEST"),
        col("CD_FDC_GEST").alias("CODE_FDC_GEST"),
        col("AGENCEOP").alias("AGENCE_OP"),
        col("CD_FDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    compte_tenu = compte_tenu.select(
        "partitiondate",
        col("NUMEROTIERS").alias("NUMERO_TIERS"),
        col("NUMERODECOMPTE").alias("NUMERO_COMPTE"),
        col("AgenceGEST").alias("AGENCE_GEST"),
        col("CDFDC_GEST").alias("CODE_FDC_GEST"),
        col("AgenceOP").alias("AGENCE_OP"),
        col("CDFDC_OP").alias("CODE_FDC_OP"),
        "INTITULE",
    )

    agence_infos = agence_infos.select(
        "partitiondate",
        col("CODEAGENCE").alias("CODE_AGENCE"),
        col("NOMAGENCE").alias("NOM_AGENCE"),
        col("succursale").alias("CODE_GROUPE"),
        col("nomsuccursale").alias("NOM_GROUPE"),
        col("CODEREGION").alias("CODE_REGION"),
        col("NOMREGION").alias("NOM_REGION"),
        col("CODEBANQUE").alias("CODE_BANQUE"),
        col("NOMBANQUE").alias("NOM_BANQUE"),
    )

    infos_clients = infos_clients.select(
        "partitiondate",
        col("numerotiers").alias("NUMEROTIERS"),
        col("marche").alias("MARCHE"),
        col("segment").alias("SEGMENT"),
    )

    """
    - Filtrage  TYPE_D != 'Z'
    - Renommage de NUMERO_ADHESION en IDENTIFIANT
    - Redefinition de la colonne PRODUIT à base de filtres sur CODE_PRODUIT
    - ENCOURS = ENCOURS_NET
    """
    encours_epargne = (
        encours_epargne.filter(encours_epargne.TYPE_D != "Z")
        .select(
            "ENTREPRISE",
            "EPARGNE",
            "CODE_PRODUIT",
            "DATE_CALCULEE",
            "NUMERO_ADHESION",
            "partitiondate",
        )
        .withColumnRenamed("NUMERO_ADHESION", "IDENTIFIANT")
        .withColumn("ENCOURS", col("EPARGNE").cast(FloatType()) / 100.0)
        .withColumn("DATE", to_date("DATE_CALCULEE", "ddMMyyyy"))
        .withColumn(
            "PRODUIT",
            when(substring(col("CODE_PRODUIT"), 1, 3) == "100", lit("WR01"))
            .when(substring(col("CODE_PRODUIT"), 1, 3) == "101", lit("WE01"))
            .otherwise(None),
        )
        .withColumn("DATE_TRAITEMENT", col("partitiondate"))
    )

    """
    - Jointure avec contrat_produit pour recuperer NUMERO_TIERS et NUMERO_COMPTE
    - Clé de jointure:
        NUMEROCARTE = IDENTIFIANT
        ETATPRODUIT = VALIDE
        CODEPRODUIT = PRODUIT
    """
    contrat_produit = contrat_produit.withColumnRenamed("partitiondate", "dttrm")
    df_final = encours_epargne.join(
        contrat_produit,
        (encours_epargne.IDENTIFIANT == contrat_produit.NUMEROCARTE)
        & (encours_epargne.PRODUIT == contrat_produit.CODEPRODUIT)
        & (contrat_produit.ETATPRODUIT == "VALIDE")
        & (encours_epargne.partitiondate == contrat_produit.dttrm),
        how="inner",
    ).select(
        "ENTREPRISE",
        "PRODUIT",
        "NUMERO_TIERS",
        "NUMERO_COMPTE",
        "IDENTIFIANT",
        "ENCOURS",
        "DATE_TRAITEMENT",
        "partitiondate",
    )

    """
    - Ajout des infos de comptes aux precedentes df_final:
        - Jointure avec compte_client et ne garder que les comptes matchés (c-à-d INTITULE existe)
        - Jointure avec compte_tenu pour les comptes n'apparaissont pas dans compte_client (c-à-d INTITULE null)
        - Regrouper les deux parties des infos comptes liées à df_final
    """
    df_final_compte = df_final.join(
        compte_client, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df_final_compte_non_null = df_final_compte.filter(col("INTITULE").isNotNull())
    df_final_2 = df_final_compte_non_null.select(*list(df_final.columns))
    df_final_comptetenu = df_final.exceptAll(df_final_2).join(
        compte_tenu, ["NUMERO_TIERS", "NUMERO_COMPTE", "partitiondate"], how="left"
    )
    df = df_final_compte_non_null.unionByName(df_final_comptetenu)

    """
    - Ajout des infos d'agences et de niveau de regroupement
    - Ajout des infos de clients (MARCHE, SEGMENT)
    NB: substring(df_final.NUMERO_COMPTE, 1, 3) peut etre remplacé par FDC_GEST/FDC_OP selon le besoin
    """
    agence_infos = agence_infos.withColumnRenamed("partitiondate", "date")
    infos_clients = infos_clients.withColumnRenamed("partitiondate", "dttrm")

    return (
        df.join(
            agence_infos,
            (substring(df_final.NUMERO_COMPTE, 1, 3) == agence_infos.CODE_AGENCE)
            & (df_final.partitiondate == agence_infos.date),
            how="left",
        )
        .join(
            infos_clients,
            (df.NUMERO_TIERS == infos_clients.NUMEROTIERS)
            & (df.partitiondate == infos_clients.dttrm),
            how="left",
        )
        .select(
            "DATE_TRAITEMENT",
            "NUMERO_TIERS",
            "NUMERO_COMPTE",
            "INTITULE",
            "ENTREPRISE",
            "PRODUIT",
            "IDENTIFIANT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "AGENCE_OP",
            "AGENCE_GEST",
            "MARCHE",
            "SEGMENT",
            "CODE_FDC_OP",
            "CODE_FDC_GEST",
            "ENCOURS",
            "partitiondate",
        )
    )


def build_encours_assurances(
    encours_axa: DataFrame,
    encours_rma: DataFrame,
    encours_saham: DataFrame,
    encours_wafa: DataFrame,
) -> DataFrame:
    """
    La fonction permet de construire la table enrichie ENCOURS_ASSURANCES.
    INPUT:  - encours_axa: les encours de l'entreprise AXA ASSURANCE
            - encours_rma: les encours de l'entreprise RMA WATANYA
            - encours_saham: les encours de l'entreprise SAHAM ASSURANCE
            - encours_wafa: les encours de l'entreprise WAFA ASSURANCE

    OUTPUT: la table ENCOURS_ASSURANCES qui est donc l'union des tables en input
    """
    return (
        encours_axa.unionByName(encours_rma)
        .unionByName(encours_saham)
        .unionByName(encours_wafa)
    )


def build_leasing(
    rib: DataFrame,
    actrib: DataFrame,
    acteur: DataFrame,
    dosrubrique: DataFrame,
    dossier: DataFrame,
    dosrubecheancier: DataFrame,
    facture: DataFrame,
) -> DataFrame:
    # Selection et casting de champs permettant de construire la table enrichie au niveau du socle.
    rib = rib.select("partitiondate", "RIBID", "RIBCOMPTE").withColumn(
        "RIBID", col("RIBID").cast(IntegerType())
    )
    actrib = (
        actrib.select("partitiondate", "RIBID", "ACTID")
        .withColumn("RIBID", col("RIBID").cast(IntegerType()))
        .withColumn("ACTID", col("ACTID").cast(IntegerType()))
    )
    acteur = acteur.select("partitiondate", "ACTID", "ACTCODE").withColumn(
        "ACTID", col("ACTID").cast(IntegerType())
    )
    dosrubrique = dosrubrique.select(
        "partitiondate", "DRUDTDEB", "DOSID", "DRUCLASSE", "DRUORDRE"
    ).withColumn("DOSID", col("DOSID").cast(IntegerType()))
    dossier = (
        dossier.select("partitiondate", "ACTID", "DOSID", "DOSNUM", "TACCODE")
        .withColumn("ACTID", col("ACTID").cast(IntegerType()))
        .withColumn("DOSID", col("DOSID").cast(IntegerType()))
    )
    dosrubecheancier = (
        dosrubecheancier.select(
            "partitiondate", "DOSID", "DREMTECF", "FACID", "DRETYPE"
        )
        .withColumn("DOSID", col("DOSID").cast(IntegerType()))
        .withColumn("FACID", col("FACID").cast(IntegerType()))
    )
    facture = facture.select(
        "partitiondate", "FACID", "FACNUM", "FACMTTTC"
    ).withColumn("FACID", col("FACID").cast(IntegerType()))

    window_spec = Window.partitionBy(
        "ACTID",
        "DOSID",
        "TYPE_LEASING",
        "NUMERODE_CONTRAT",
        "DATE_PREMIER_LOYER",
        "partitiondate",
    )

    """
    - Jointure des tables en input
    - Filter les données dont DRUCLASSE de la table DOSRUBRIQUE = F et
        DRETYPE de la table DOSRUBECHEANCIER = 'LOYER'
    - Extraire le numero de compte à partir de RIBCOMPTE
    - Renommer certains champs
    - Ne garder que les lignes dont DRUORDRE = min("DRUORDRE") pour toute combinaison de
        ACTID, DOSID, TYPE_LEASING, NUMERODE_CONTRAT, DATE_PREMIER_LOYER, partitiondate
    """
    return (
        rib.join(actrib, ["partitiondate", "RIBID"], how="inner")
        .join(acteur, ["partitiondate", "ACTID"], how="inner")
        .join(dossier, ["partitiondate", "ACTID"], how="inner")
        .join(dosrubrique, ["partitiondate", "DOSID"], how="inner")
        .join(dosrubecheancier, ["partitiondate", "DOSID"], how="inner")
        .join(facture, ["partitiondate", "FACID"], how="inner")
        .withColumn("NUMERO_COMPTE", substring("RIBCOMPTE", 11, 11))
        .withColumnRenamed("ACTCODE", "CODE_CLIENT")
        .withColumnRenamed("DOSNUM", "NUMERODE_CONTRAT")
        .withColumnRenamed("TACCODE", "TYPE_LEASING")
        .withColumnRenamed("DRUDTDEB", "DATE_PREMIER_LOYER")
        .withColumnRenamed("FACNUM", "NUMERO_FACTURE")
        .withColumnRenamed("DREMTECF", "ENCOURS")
        .withColumnRenamed("FACMTTTC", "MONTANT_LOYER")
        .withColumn("MIN_DRUORDRE", min(col("DRUORDRE")).over(window_spec))
        .filter(
            (col("DRUCLASSE") == "F")
            & (col("DRETYPE") == "LOYER")
            & (col("DRUCLASSE") == col("MIN_DRUORDRE"))
        )
        .drop("MIN_DRUORDRE")
    )


def build_kpi(
    df: DataFrame,
    partition_date: str,
    by: str,
    has_delta: bool = True,
    partition_columns: list = ["numerodecompte", "devise"],
    order_column: str = "dttrtm",
) -> DataFrame:
    """
    INPUT:
        - df: contient permettant de calcul les KPIs
        - by: colonne cible sur laquelle on calcule le KPI (ex: soldejourcr_devise)
        - has_delta: permet de distinguer si l'on doit aussi calculer les KPIs pour les deltas
            DELTA_CIBLE = CIBLE - LAST_YEAR_CIBLE
        - partition_columns: liste de colonnes permettant de partitionner les données et calculer les KPIs
            Par defaut, nous utilisons les colonnes de partitionnement relatives aux ressources/emplois
        - order_column: colonne utilisée dans le tri de données, également utile dans le partitionnement

    OUTPUT:
        - Dataframe avec les KPIs ci-après

    Cette fonction généralise le calcul de KPIs pour tout type de besoins (RESSOURCES/EMPLOIS, OPCVM, ASSURANCES, etc)
    """
    df_kpi = (
        df.transform(
            get_last_recorded_sold_per_client_from,
            by=by,
            days=5,  # calcul de J-7 (sans weekends)
            partition_columns=partition_columns,
            order_column=order_column,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            partition_date=partition_date,
            by=by,  # calcul de M-1
            partition_columns=partition_columns,
            order_column=order_column,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by=by,  # calcul de T-1
            partition_columns=partition_columns,
            order_column=order_column,
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            partition_date=partition_date,
            by=by,  # calcul de A-1
            partition_columns=partition_columns,
            order_column=order_column,
        )
    )
    df_kpi = (
        df_kpi.withColumn(  # var_semaine = by -last_5_days_by (gérer aussi le cas de null)
            f"variation_semaine_{by}",
            when(col(by).isNull() & col(f"last_5_days_{by}").isNull(), 0)
            .when(
                col(by).isNull() & col(f"last_5_days_{by}").isNotNull(),
                col(f"last_5_days_{by}"),
            )
            .when(col(by).isNotNull() & col(f"last_5_days_{by}").isNull(), col(by))
            .otherwise(col(by) - col(f"last_5_days_{by}")),
        )  # var_semaine_pct = var_semaine/last_5_days_by
        .withColumn(
            f"variation_semaine_pct_{by}",
            when(
                (col(f"last_5_days_{by}").isNotNull())
                & (col(f"last_5_days_{by}") != 0),
                col(f"variation_semaine_{by}") / col(f"last_5_days_{by}"),
            ).otherwise(None),
        )  # var_mois = by - last_month_by
        .withColumn(
            f"variation_mois_{by}",
            when(col(by).isNull() & col(f"last_month_{by}").isNull(), 0)
            .when(
                col(by).isNull() & col(f"last_month_{by}").isNotNull(),
                col(f"last_month_{by}"),
            )
            .when(col(by).isNotNull() & col(f"last_month_{by}").isNull(), col(by))
            .otherwise(col(by) - col(f"last_month_{by}")),
        )  # var_mois_pct = var_mois / last_month_by
        .withColumn(
            f"variation_mois_pct_{by}",
            when(
                (col(f"last_month_{by}").isNotNull())
                & (col(f"last_month_{by}") != 0),
                col(f"variation_mois_{by}") / col(f"last_month_{by}"),
            ).otherwise(None),
        )  # var_trimestre = by - last_quarter_by
        .withColumn(
            f"variation_trimestre_{by}",
            when(col(by).isNull() & col(f"last_quarter_{by}").isNull(), 0)
            .when(
                col(by).isNull() & col(f"last_quarter_{by}").isNotNull(),
                col(f"last_quarter_{by}"),
            )
            .when(col(by).isNotNull() & col(f"last_quarter_{by}").isNull(), col(by))
            .otherwise(col(by) - col(f"last_quarter_{by}")),
        )  # var_trimestre_pct = var_trimestre / last_quarter_by
        .withColumn(
            f"variation_trimestre_pct_{by}",
            when(
                (col(f"last_quarter_{by}").isNotNull())
                & (col(f"last_quarter_{by}") != 0),
                col(f"variation_trimestre_{by}") / col(f"last_quarter_{by}"),
            ).otherwise(None),
        )  # var_annee = by - last_year_by
        .withColumn(
            f"variation_annee_{by}",
            when(col(by).isNull() & col(f"last_year_{by}").isNull(), 0)
            .when(
                col(by).isNull() & col(f"last_year_{by}").isNotNull(),
                col(f"last_year_{by}"),
            )
            .when(col(by).isNotNull() & col(f"last_year_{by}").isNull(), col(by))
            .otherwise(col(by) - col(f"last_year_{by}")),
        )  # var_annee_pct = var_annee / last_year_by
        .withColumn(
            f"variation_annee_pct_{by}",
            when(
                (col(f"last_year_{by}").isNotNull()) & (col(f"last_year_{by}") != 0),
                col(f"variation_annee_{by}") / col(f"last_year_{by}"),
            ).otherwise(None),
        )
    )
    # Refaire les memes calculs pour les deltas
    if has_delta:
        df_kpi = (
            df_kpi.withColumn(
                f"delta_{by}",
                when(col(f"{by}").isNull() & col(f"last_year_{by}").isNull(), 0)
                .when(
                    col(f"{by}").isNull() & col(f"last_year_{by}").isNotNull(),
                    col(f"last_year_{by}"),
                )
                .when(
                    col(f"{by}").isNotNull() & col(f"last_year_{by}").isNull(),
                    col(f"{by}"),
                )
                .otherwise(col(f"{by}") - col(f"last_year_{by}")),
            )
            .transform(
                get_last_recorded_sold_per_client_from,
                by=f"delta_{by}",
                days=5,
                partition_columns=partition_columns,
                order_column=order_column,
            )
            .transform(
                get_last_recorded_sold_per_client_from_last_month,
                partition_date=partition_date,
                by=f"delta_{by}",
                partition_columns=partition_columns,
                order_column=order_column,
            )
            .transform(
                get_last_recorded_sold_per_client_from_last_quarter,
                by=f"delta_{by}",
                partition_columns=partition_columns,
                order_column=order_column,
            )
            .transform(
                get_last_recorded_sold_per_client_from_last_year,
                partition_date=partition_date,
                by=f"delta_{by}",
                partition_columns=partition_columns,
                order_column=order_column,
            )
            .withColumn(
                f"variation_semaine_delta_{by}",
                when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_5_days_delta_{by}").isNull(),
                    0,
                )
                .when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_5_days_delta_{by}").isNotNull(),
                    col(f"last_5_days_delta_{by}"),
                )
                .when(
                    col(f"delta_{by}").isNotNull()
                    & col(f"last_5_days_delta_{by}").isNull(),
                    col(f"delta_{by}"),
                )
                .otherwise(col(f"delta_{by}") - col(f"last_5_days_delta_{by}")),
            )
            .withColumn(
                f"variation_semaine_pct_delta_{by}",
                when(
                    (col(f"last_5_days_delta_{by}").isNotNull())
                    & (col(f"last_5_days_delta_{by}") != 0),
                    col(f"variation_semaine_delta_{by}")
                    / col(f"last_5_days_delta_{by}"),
                ).otherwise(None),
            )
            .withColumn(
                f"variation_mois_delta_{by}",
                when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_month_delta_{by}").isNull(),
                    0,
                )
                .when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_month_delta_{by}").isNotNull(),
                    col(f"last_month_delta_{by}"),
                )
                .when(
                    col(f"delta_{by}").isNotNull()
                    & col(f"last_month_delta_{by}").isNull(),
                    col(f"delta_{by}"),
                )
                .otherwise(col(f"delta_{by}") - col(f"last_month_delta_{by}")),
            )
            .withColumn(
                f"variation_mois_pct_delta_{by}",
                when(
                    (col(f"last_month_delta_{by}").isNotNull())
                    & (col(f"last_month_delta_{by}") != 0),
                    col(f"variation_mois_delta_{by}")
                    / col(f"last_month_delta_{by}"),
                ).otherwise(None),
            )
            .withColumn(
                f"variation_trimestre_delta_{by}",
                when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_quarter_delta_{by}").isNull(),
                    0,
                )
                .when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_quarter_delta_{by}").isNotNull(),
                    col(f"last_quarter_delta_{by}"),
                )
                .when(
                    col(f"delta_{by}").isNotNull()
                    & col(f"last_quarter_delta_{by}").isNull(),
                    col(f"delta_{by}"),
                )
                .otherwise(col(f"delta_{by}") - col(f"last_quarter_delta_{by}")),
            )
            .withColumn(
                f"variation_trimestre_pct_delta_{by}",
                when(
                    (col(f"last_quarter_delta_{by}").isNotNull())
                    & (col(f"last_quarter_delta_{by}") != 0),
                    col(f"variation_trimestre_delta_{by}")
                    / col(f"last_quarter_delta_{by}"),
                ).otherwise(None),
            )
            .withColumn(
                f"variation_annee_delta_{by}",
                when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_year_delta_{by}").isNull(),
                    0,
                )
                .when(
                    col(f"delta_{by}").isNull()
                    & col(f"last_year_delta_{by}").isNotNull(),
                    col(f"last_year_delta_{by}"),
                )
                .when(
                    col(f"delta_{by}").isNotNull()
                    & col(f"last_year_delta_{by}").isNull(),
                    col(f"delta_{by}"),
                )
                .otherwise(col(f"delta_{by}") - col(f"last_year_delta_{by}")),
            )
            .withColumn(
                f"variation_annee_pct_delta_{by}",
                when(
                    (col(f"last_year_delta_{by}").isNotNull())
                    & (col(f"last_year_delta_{by}") != 0),
                    col(f"variation_annee_delta_{by}")
                    / col(f"last_year_delta_{by}"),
                ).otherwise(None),
            )
        )
    return df_kpi


def build_data_week_of_month(df: DataFrame, by: str) -> DataFrame:
    # Cette fonction permet de ne travailler qu'avec les semaines du mois et non de l'année
    df_week_of_month = (
        df.withColumn("ANNEE", year(to_date(col(by))))
        .withColumn(
            "MOIS",
            when(
                month(to_date(col(by))) < 10,
                concat_ws("", lit(0), month(to_date(col(by)))),
            ).otherwise(month(to_date(col(by)))),
        )
        .withColumn("JOUR", dayofmonth(by))
        .withColumn("JOUR_SEMAINE", dayofweek(by))
        .withColumn("SEMAINE", ceil((col("JOUR") + col("JOUR_SEMAINE") - 1) / 7))
        .drop("JOUR", "JOUR_SEMAINE")
    )

    group_cols = [
        c
        for c in df.columns
        if ("soldejour" not in c) and (c not in [by, "MOIS", "SEMAINE", "ANNEE"])
    ]
    window_spec = Window.partitionBy(
        *(group_cols + ["ANNEE", "MOIS", "SEMAINE"])
    ).orderBy(col(by).desc())

    return (
        df_week_of_month.withColumn("rank", rank().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank")
        .select(*list(df.columns))
    )


def build_fact_table_ressource_emplois(
    ressource_emplois: DataFrame, information_tier: DataFrame
) -> DataFrame:
    """
    - Construction de la table de faits en ajoutant le marché et le segment du client
    - Renommer les champs
    """
    selected_information_tier = information_tier.select(
        "numerotiers",
        # "marche",
        "segment",
    )
    return (
        ressource_emplois.join(
            selected_information_tier, "numerotiers", "left_outer"
        )
        .withColumnRenamed("numerodecompte", "NUMERO_COMPTE")
        .withColumnRenamed("NUMEROTIERS", "NUMERO_TIERS")
        .withColumnRenamed("dttrtm", "DATE_TRAITEMENT")
        .withColumnRenamed("AGENCEGEST", "AGENCE_GEST")
        .withColumnRenamed("CDFDC_GEST", "CODE_FDC_GEST")
        .withColumnRenamed("AGENCEOP", "AGENCE_OP")
        .withColumnRenamed("CDFDC_OP", "CODE_FDC_OP")
        .withColumnRenamed("CODEAGENCE", "CODE_AGENCE")
        .withColumnRenamed("NOMAGENCE", "NOM_AGENCE")
        .withColumnRenamed("succursale", "CODE_GROUPE")
        .withColumnRenamed("nomsuccursale", "NOM_GROUPE")
        .withColumnRenamed("CODEREGION", "CODE_REGION")
        .withColumnRenamed("NOMREGION", "NOM_REGION")
        .withColumnRenamed("CODEBANQUE", "CODE_BANQUE")
        .withColumnRenamed("NOMBANQUE", "NOM_BANQUE")
        # .withColumnRenamed("marche", "MARCHE")
        .withColumnRenamed("segment", "SEGMENT")
        .withColumnRenamed("devise", "DEVISE")
        .withColumnRenamed("categorie", "CATEGORIE")
    )


def build_tro_emplois(df_final: DataFrame, emp: str) -> DataFrame:
    """
    - Calcul les TRO concernant les emplois
    - Pour la définition des termes comme TRO, RAF, DELTA voir la fonction build_tro
    - REMARQUE:
        Cette fonction peut etre optimisée afin de traiter les emplois et ressources
        au lieu de créer la fonction build_tro_ressources ci-après en passant par exemple
        la variable cible differenciant soldejourcr et soldejourdb
    """
    return (
        df_final.withColumn(
            f"OBJECTIF_PERIODE_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp, col(f"{emp}_OBJECTIF_CUMUL")
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_PERIODE_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    (col(f"OBJECTIF_PERIODE_{emp}") != 0)
                    & (col(f"OBJECTIF_PERIODE_{emp}").isNotNull()),
                    col("variation_annee_soldejourdb_devise")
                    / col(f"OBJECTIF_PERIODE_{emp}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_TRIMESTRE_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp, col(f"{emp}_OBJECTIF_TRIMESTRE")
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_TRIMESTRE_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    (col(f"OBJECTIF_TRIMESTRE_{emp}") != 0)
                    & (col(f"OBJECTIF_TRIMESTRE_{emp}").isNotNull()),
                    col("variation_trimestre_soldejourdb_devise")
                    / col(f"OBJECTIF_TRIMESTRE_{emp}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_TRIMESTRE_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    col(f"OBJECTIF_TRIMESTRE_{emp}").isNull()
                    & col("variation_trimestre_soldejourdb_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_{emp}").isNull()
                    & col("variation_trimestre_soldejourdb_devise").isNotNull(),
                    col("variation_trimestre_soldejourdb_devise"),
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_{emp}").isNotNull()
                    & col("variation_trimestre_soldejourdb_devise").isNull(),
                    col(f"OBJECTIF_TRIMESTRE_{emp}"),
                )
                .otherwise(
                    col(f"OBJECTIF_TRIMESTRE_{emp}")
                    - col("variation_trimestre_soldejourdb_devise")
                ),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_ANNUEL_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp, col(f"{emp}_OBJECTIF")
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_ANNUEL_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    col(f"OBJECTIF_ANNUEL_{emp}").isNull()
                    & col("variation_annee_soldejourdb_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_{emp}").isNull()
                    & col("variation_annee_soldejourdb_devise").isNotNull(),
                    col("variation_annee_soldejourdb_devise"),
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_{emp}").isNotNull()
                    & col("variation_annee_soldejourdb_devise").isNull(),
                    col(f"OBJECTIF_ANNUEL_{emp}"),
                )
                .otherwise(
                    col(f"OBJECTIF_ANNUEL_{emp}")
                    - col("variation_annee_soldejourdb_devise")
                ),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_PERIODE_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp, col(f"DELTA_{emp}_OBJECTIF_CUMUL")
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_PERIODE_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    (col(f"OBJECTIF_PERIODE_DELTA_{emp}") != 0)
                    & (col(f"OBJECTIF_PERIODE_DELTA_{emp}").isNotNull()),
                    col("variation_annee_delta_soldejourdb_devise")
                    / col(f"OBJECTIF_PERIODE_DELTA_{emp}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_TRIMESTRE_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                col(f"DELTA_{emp}_OBJECTIF_TRIMESTRE"),
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_TRIMESTRE_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    (col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}") != 0)
                    & (col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}").isNotNull()),
                    col("variation_trimestre_delta_soldejourdb_devise")
                    / col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_TRIMESTRE_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}").isNull()
                    & col("variation_trimestre_delta_soldejourdb_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}").isNull()
                    & col(
                        "variation_trimestre_delta_soldejourdb_devise"
                    ).isNotNull(),
                    col("variation_trimestre_delta_soldejourdb_devise"),
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}").isNotNull()
                    & col("variation_trimestre_delta_soldejourdb_devise").isNull(),
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}"),
                )
                .otherwise(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{emp}")
                    - col("variation_trimestre_delta_soldejourdb_devise")
                ),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_ANNUEL_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp, col(f"DELTA_{emp}_OBJECTIF")
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_ANNUEL_DELTA_{emp}",
            when(
                col("NOUVEAU_TYPE_EMPLOI") == emp,
                when(
                    col(f"OBJECTIF_ANNUEL_DELTA_{emp}").isNull()
                    & col("variation_annee_delta_soldejourdb_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_DELTA_{emp}").isNull()
                    & col("variation_annee_delta_soldejourdb_devise").isNotNull(),
                    col("variation_annee_delta_soldejourdb_devise"),
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_DELTA_{emp}").isNotNull()
                    & col("variation_annee_delta_soldejourdb_devise").isNull(),
                    col(f"OBJECTIF_ANNUEL_DELTA_{emp}"),
                )
                .otherwise(
                    col(f"OBJECTIF_ANNUEL_DELTA_{emp}")
                    - col("variation_annee_delta_soldejourdb_devise")
                ),
            ).otherwise(None),
        )
    )


def build_tro_ressources(df_final: DataFrame, ress: str) -> DataFrame:
    """
    - Calcul les TRO concernant les ressources
    - Pour la définition des termes comme TRO, RAF, DELTA voir la fonction build_tro
    """
    return (
        df_final.withColumn(
            f"OBJECTIF_PERIODE_{ress}",
            when(
                col("type_ressource_emploi") == ress, col(f"{ress}_OBJECTIF_CUMUL")
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_PERIODE_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    (col(f"OBJECTIF_PERIODE_{ress}") != 0)
                    & (col(f"OBJECTIF_PERIODE_{ress}").isNotNull()),
                    col("variation_annee_soldejourcr_devise")
                    / col(f"OBJECTIF_PERIODE_{ress}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_TRIMESTRE_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                col(f"{ress}_OBJECTIF_TRIMESTRE"),
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_TRIMESTRE_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    (col(f"OBJECTIF_TRIMESTRE_{ress}") != 0)
                    & (col(f"OBJECTIF_TRIMESTRE_{ress}").isNotNull()),
                    col("variation_trimestre_soldejourcr_devise")
                    / col(f"OBJECTIF_TRIMESTRE_{ress}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_TRIMESTRE_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    col(f"OBJECTIF_TRIMESTRE_{ress}").isNull()
                    & col("variation_trimestre_soldejourcr_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_{ress}").isNull()
                    & col("variation_trimestre_soldejourcr_devise").isNotNull(),
                    col("variation_trimestre_soldejourcr_devise"),
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_{ress}").isNotNull()
                    & col("variation_trimestre_soldejourcr_devise").isNull(),
                    col(f"OBJECTIF_TRIMESTRE_{ress}"),
                )
                .otherwise(
                    col(f"OBJECTIF_TRIMESTRE_{ress}")
                    - col("variation_trimestre_soldejourcr_devise")
                ),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_ANNUEL_{ress}",
            when(
                col("type_ressource_emploi") == ress, col(f"{ress}_OBJECTIF")
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_ANNUEL_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    col(f"OBJECTIF_ANNUEL_{ress}").isNull()
                    & col("variation_annee_soldejourcr_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_{ress}").isNull()
                    & col("variation_annee_soldejourcr_devise").isNotNull(),
                    col("variation_annee_soldejourcr_devise"),
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_{ress}").isNotNull()
                    & col("variation_annee_soldejourcr_devise").isNull(),
                    col(f"OBJECTIF_ANNUEL_{ress}"),
                )
                .otherwise(
                    col(f"OBJECTIF_ANNUEL_{ress}")
                    - col("variation_annee_soldejourcr_devise")
                ),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_PERIODE_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                col(f"DELTA_{ress}_OBJECTIF_CUMUL"),
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_PERIODE_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    (col(f"OBJECTIF_PERIODE_DELTA_{ress}") != 0)
                    & (col(f"OBJECTIF_PERIODE_DELTA_{ress}").isNotNull()),
                    col("variation_annee_delta_soldejourcr_devise")
                    / col(f"OBJECTIF_PERIODE_DELTA_{ress}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_TRIMESTRE_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                col(f"DELTA_{ress}_OBJECTIF_TRIMESTRE"),
            ).otherwise(None),
        )
        .withColumn(
            f"TRO_TRIMESTRE_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    (col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}") != 0)
                    & (col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}").isNotNull()),
                    col("variation_trimestre_delta_soldejourcr_devise")
                    / col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}"),
                ).otherwise(None),
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_TRIMESTRE_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}").isNull()
                    & col("variation_trimestre_delta_soldejourcr_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}").isNull()
                    & col(
                        "variation_trimestre_delta_soldejourcr_devise"
                    ).isNotNull(),
                    col("variation_trimestre_delta_soldejourcr_devise"),
                )
                .when(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}").isNotNull()
                    & col("variation_trimestre_delta_soldejourcr_devise").isNull(),
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}"),
                )
                .otherwise(
                    col(f"OBJECTIF_TRIMESTRE_DELTA_{ress}")
                    - col("variation_trimestre_delta_soldejourcr_devise")
                ),
            ).otherwise(None),
        )
        .withColumn(
            f"OBJECTIF_ANNUEL_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress, col(f"DELTA_{ress}_OBJECTIF")
            ).otherwise(None),
        )
        .withColumn(
            f"RAF_ANNUEL_DELTA_{ress}",
            when(
                col("type_ressource_emploi") == ress,
                when(
                    col(f"OBJECTIF_ANNUEL_DELTA_{ress}").isNull()
                    & col("variation_annee_delta_soldejourcr_devise").isNull(),
                    0,
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_DELTA_{ress}").isNull()
                    & col("variation_annee_delta_soldejourcr_devise").isNotNull(),
                    col("variation_annee_delta_soldejourcr_devise"),
                )
                .when(
                    col(f"OBJECTIF_ANNUEL_DELTA_{ress}").isNotNull()
                    & col("variation_annee_delta_soldejourcr_devise").isNull(),
                    col(f"OBJECTIF_ANNUEL_DELTA_{ress}"),
                )
                .otherwise(
                    col(f"OBJECTIF_ANNUEL_DELTA_{ress}")
                    - col("variation_annee_delta_soldejourcr_devise")
                ),
            ).otherwise(None),
        )
    )


def build_tro(
    df_kpi_final: DataFrame,
    objectifs_tro: DataFrame,
    saisonnalite: DataFrame,
    categorie: str,
    date_column: str = "DATE_TRAITEMENT",
):
    """
    INPUT:
        - df_kpi_final: contenant les KPIs calculés
        - objectifs_tro: contenant les objectifs annuels à réaliser.
        - saisonnalite: contenant les pourcentages de periodes (POURC_SEMAINE, POURC_TRIMESTRE, POURC_CUMUL)
        - categorie: permet de distinguer le calcul de TRO:
            - RESSOURCE_EMPLOI: pour le calcul de TRO pour les ressources et emplois
            - ASSURANCES: pour les TRO liés aux assurances
            - OPCVM: concerne les TRO d'OPCVM
        - date_column: colonne permettant de lier la table de KPIs à la saisonnalité

    - OUTPUT: La table des TRO

    # Définitions
    * DELTA_ENCOURS = SOLDEJOUR - LAST_YEAR_SOLDEJOUR
    * VARIATION_SEMAINE = SOLDEJOUR - LAST_5_DAY_SOLDEJOUR (J-7 sans les weekends)
        Ex: variation_semaine_soldejourcr_devise = soldejourcr_devise - last_5_days_soldejourcr_devise
    * VARIATION_TRIMESTRE = SOLDEJOUR - LAST_QUARTER_SOLDEJOUR
    * TRO (Taux de Réalisation d'Objectifs) = Réalisation (variation dans notre cas) / Objectif
        - Ex: TRO_TRIMESTRE = VARIATION_TRIMESTRE/OBJECTIF_TRIMESTRE
        - L'on peut multitplier par 100 pour obtenir le résultat en pourcentage
    * RAF (Reste A Faire) = Objectif - Réalisation
        -Ex: RAF_TRIMESTRE = OBJECTIF_TRIMESTRE - VARIATION_TRIMESTRE
    """

    df_final = (
        df_kpi_final.withColumn("NUMERO_SEMAINE", weekofyear(col(date_column)))
        .withColumn("ANNEE", year(col(date_column)))
        .join(saisonnalite, ["NUMERO_SEMAINE", "ANNEE"], how="left")
    )

    if categorie == "RESSOURCE_EMPLOI":
        """
        - Convertir les objectifs qui sont KDH en DH (c-à-d * 1000)
        - Remplacer les valeurs null par 0
        - Pour les objectifs non disponibles sans la table OBJECTIFS_TRO, les fixer à 0
        """
        objectifs_tro = (
            objectifs_tro.select(
                "CODE_AGENCE",
                col("CODE_FDC").alias("CODE_FDC_GEST"),
                expr("CREDIT_CONSO * 1000").alias("CONSO_OBJECTIF"),
                expr("DELTA_ENCOURS_CREDIT_CONSO_BRUT * 1000").alias(
                    "DELTA_CONSO_OBJECTIF"
                ),
                expr("CREDIT_HABITAT * 1000").alias("HABITAT_OBJECTIF"),
                expr("DELTA_ENCOURS_CREDIT_IMMO * 1000").alias(
                    "DELTA_HABITAT_OBJECTIF"
                ),
                expr("CREDIT_TRESORERIE * 1000").alias("TRESORERIE_OBJECTIF"),
                expr("CAV * 1000").alias("CAV_OBJECTIF"),
                expr("CREDIT_EQUIPEMENT * 1000").alias("EQUIPEMENT_OBJECTIF"),
                expr("RAV * 1000").alias("RAV_OBJECTIF"),
                expr("RAT * 1000").alias("RAT_OBJECTIF"),
                expr("REP * 1000").alias("REP_OBJECTIF"),
            )
            .na.fill(
                {
                    "CONSO_OBJECTIF": 0,
                    "DELTA_CONSO_OBJECTIF": 0,
                    "HABITAT_OBJECTIF": 0,
                    "DELTA_HABITAT_OBJECTIF": 0,
                    "TRESORERIE_OBJECTIF": 0,
                    "CAV_OBJECTIF": 0,
                    "EQUIPEMENT_OBJECTIF": 0,
                    "RAV_OBJECTIF": 0,
                    "RAT_OBJECTIF": 0,
                    "REP_OBJECTIF": 0,
                }
            )
            .withColumn("DELTA_TRESORERIE_OBJECTIF", lit(0))
            .withColumn("DELTA_CAV_OBJECTIF", lit(0))
            .withColumn("DELTA_EQUIPEMENT_OBJECTIF", lit(0))
            .withColumn("DELTA_RAV_OBJECTIF", lit(0))
            .withColumn("DELTA_RAT_OBJECTIF", lit(0))
            .withColumn("DELTA_REP_OBJECTIF", lit(0))
        )

        """
        - Lier la table contenant les KPIs et les saisonnalités à la table des OBJECTIFS
            avec la clé ("CODE_AGENCE", "CODE_FDC_GEST")
        - Calcul des objectifs (semaine, trimestre, cumul (periode)) pour chaque ressource/emploi
        - Création d'un nouveau type d'emploi facilitant les calculs de TRO
        - Calcul des indicateurs liés aux TRO
        """
        df_final = (
            df_final.join(
                objectifs_tro, ["CODE_AGENCE", "CODE_FDC_GEST"], how="left"
            )
            .withColumn(
                "CONSO_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("CONSO_OBJECTIF")),
            )
            .withColumn(
                "CONSO_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("CONSO_OBJECTIF")),
            )
            .withColumn(
                "CONSO_OBJECTIF_CUMUL", (col("POURC_CUMUL") * col("CONSO_OBJECTIF"))
            )
            .withColumn(
                "HABITAT_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("HABITAT_OBJECTIF")),
            )
            .withColumn(
                "HABITAT_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("HABITAT_OBJECTIF")),
            )
            .withColumn(
                "HABITAT_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("HABITAT_OBJECTIF")),
            )
            .withColumn(
                "TRESORERIE_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("TRESORERIE_OBJECTIF")),
            )
            .withColumn(
                "TRESORERIE_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("TRESORERIE_OBJECTIF")),
            )
            .withColumn(
                "TRESORERIE_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("TRESORERIE_OBJECTIF")),
            )
            .withColumn(
                "CAV_OBJECTIF_SEMAINE", (col("POURC_SEMAINE") * col("CAV_OBJECTIF"))
            )
            .withColumn(
                "CAV_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("CAV_OBJECTIF")),
            )
            .withColumn(
                "CAV_OBJECTIF_CUMUL", (col("POURC_CUMUL") * col("CAV_OBJECTIF"))
            )
            .withColumn(
                "EQUIPEMENT_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("EQUIPEMENT_OBJECTIF")),
            )
            .withColumn(
                "EQUIPEMENT_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("EQUIPEMENT_OBJECTIF")),
            )
            .withColumn(
                "EQUIPEMENT_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("EQUIPEMENT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_CONSO_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_CONSO_OBJECTIF")),
            )
            .withColumn(
                "DELTA_CONSO_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_CONSO_OBJECTIF")),
            )
            .withColumn(
                "DELTA_CONSO_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_CONSO_OBJECTIF")),
            )
            .withColumn(
                "DELTA_HABITAT_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_HABITAT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_HABITAT_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_HABITAT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_HABITAT_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_HABITAT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_TRESORERIE_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_TRESORERIE_OBJECTIF")),
            )
            .withColumn(
                "DELTA_TRESORERIE_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_TRESORERIE_OBJECTIF")),
            )
            .withColumn(
                "DELTA_TRESORERIE_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_TRESORERIE_OBJECTIF")),
            )
            .withColumn(
                "DELTA_CAV_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_CAV_OBJECTIF")),
            )
            .withColumn(
                "DELTA_CAV_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_CAV_OBJECTIF")),
            )
            .withColumn(
                "DELTA_CAV_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_CAV_OBJECTIF")),
            )
            .withColumn(
                "DELTA_EQUIPEMENT_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_EQUIPEMENT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_EQUIPEMENT_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_EQUIPEMENT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_EQUIPEMENT_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_EQUIPEMENT_OBJECTIF")),
            )
            .withColumn(
                "NOUVEAU_TYPE_EMPLOI",
                when(
                    col("CATEGORIE") == "EMPLOI",
                    when(col("type_ressource_emploi").isin(["DEC", "DEC_EC"]), "CAV")
                    .when(col("type_ressource_emploi").isin(["CMT"]), "EQUIPEMENT")
                    .when(
                        col("type_ressource_emploi").isin(
                            [
                                "AVAL",
                                "AVANCES",
                                "AVANCES DAT",
                                "ESCOMPTE",
                                "FED",
                                "IMPAYE",
                                "SPOT",
                                "MCNE",
                            ]
                        ),
                        "TRESORERIE",
                    )
                    .otherwise(col("type_ressource_emploi")),
                ).otherwise(None),
            )
            .withColumn(
                "RAV_OBJECTIF_SEMAINE", (col("POURC_SEMAINE") * col("RAV_OBJECTIF"))
            )
            .withColumn(
                "RAV_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("RAV_OBJECTIF")),
            )
            .withColumn(
                "RAV_OBJECTIF_CUMUL", (col("POURC_CUMUL") * col("RAV_OBJECTIF"))
            )
            .withColumn(
                "RAT_OBJECTIF_SEMAINE", (col("POURC_SEMAINE") * col("RAT_OBJECTIF"))
            )
            .withColumn(
                "RAT_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("RAT_OBJECTIF")),
            )
            .withColumn(
                "RAT_OBJECTIF_CUMUL", (col("POURC_CUMUL") * col("RAT_OBJECTIF"))
            )
            .withColumn(
                "REP_OBJECTIF_SEMAINE", (col("POURC_SEMAINE") * col("REP_OBJECTIF"))
            )
            .withColumn(
                "REP_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("REP_OBJECTIF")),
            )
            .withColumn(
                "REP_OBJECTIF_CUMUL", (col("POURC_CUMUL") * col("REP_OBJECTIF"))
            )
            .withColumn(
                "DELTA_RAV_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_RAV_OBJECTIF")),
            )
            .withColumn(
                "DELTA_RAV_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_RAV_OBJECTIF")),
            )
            .withColumn(
                "DELTA_RAV_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_RAV_OBJECTIF")),
            )
            .withColumn(
                "DELTA_RAT_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_RAT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_RAT_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_RAT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_RAT_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_RAT_OBJECTIF")),
            )
            .withColumn(
                "DELTA_REP_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_REP_OBJECTIF")),
            )
            .withColumn(
                "DELTA_REP_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_REP_OBJECTIF")),
            )
            .withColumn(
                "DELTA_REP_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_REP_OBJECTIF")),
            )
            .transform(build_tro_emplois, emp="CONSO")
            .transform(build_tro_emplois, emp="HABITAT")
            .transform(build_tro_emplois, emp="TRESORERIE")
            .transform(build_tro_emplois, emp="EQUIPEMENT")
            .transform(build_tro_emplois, emp="CAV")
            .transform(build_tro_ressources, ress="RAT")
            .transform(build_tro_ressources, ress="RAV")
            .transform(build_tro_ressources, ress="REP")
        )

        # Selection des colonnes representant les TRO
        partitions_by = [
            "DATE_TRAITEMENT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_FDC_GEST",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "NUMERO_COMPTE",
            "MARCHE",
            "CATEGORIE",
            "NOUVEAU_TYPE_EMPLOI",
            "type_ressource_emploi",
            # "ANCIEN_TYPE",
        ]
        tro_cols = [
            column
            for column in list(df_final.columns)
            if ("OBJECTIF_PERIODE_" in column)
            or ("OBJECTIF_TRIMESTRE_" in column)
            or ("OBJECTIF_ANNUEL_" in column)
            or ("TRO_" in column)
            or ("RAF_" in column)
        ]
        return df_final.select(*(partitions_by + tro_cols))

    if categorie == "ASSURANCES":
        objectifs_tro = objectifs_tro.select(
            "CODE_AGENCE",
            col("CODE_FDC").alias("CODE_FDC_GEST"),
            expr("ASSURANCES_VIE * 1000").alias("ASSURANCES_VIE_OBJECTIF"),
        ).na.fill({"ASSURANCES_VIE_OBJECTIF": 0})
        df_final = (
            df_final.join(
                objectifs_tro, ["CODE_AGENCE", "CODE_FDC_GEST"], how="left"
            )
            .withColumn(
                "ASSURANCES_VIE_OBJECTIF_SEMAINE",
                col("POURC_SEMAINE") * col("ASSURANCES_VIE_OBJECTIF"),
            )
            .withColumn(
                "ASSURANCES_VIE_OBJECTIF_TRIMESTRE",
                col("POURC_TRIMESTRE") * col("ASSURANCES_VIE_OBJECTIF"),
            )
            .withColumn(
                "ASSURANCES_VIE_OBJECTIF_CUMUL",
                col("POURC_CUMUL") * col("ASSURANCES_VIE_OBJECTIF"),
            )
            .withColumn(
                "OBJECTIF_PERIODE_ASSURANCES_VIE",
                col("ASSURANCES_VIE_OBJECTIF_CUMUL"),
            )
            .withColumn(
                "TRO_PERIODE_ASSURANCES_VIE",
                when(
                    col("ASSURANCES_VIE_OBJECTIF_CUMUL") != 0,
                    col("variation_annee_ENCOURS")
                    / col("ASSURANCES_VIE_OBJECTIF_CUMUL"),
                ).otherwise(None),
            )
            .withColumn(
                "OBJECTIF_TRIMESTRE_ASSURANCES_VIE",
                col("ASSURANCES_VIE_OBJECTIF_TRIMESTRE"),
            )
            .withColumn(
                "TRO_TRIMESTRE_ASSURANCES_VIE",
                when(
                    col("ASSURANCES_VIE_OBJECTIF_TRIMESTRE") != 0,
                    col("variation_trimestre_ENCOURS")
                    / col("ASSURANCES_VIE_OBJECTIF_TRIMESTRE"),
                ).otherwise(None),
            )
            .withColumn(
                "RAF_TRIMESTRE_ASSURANCES_VIE",
                col("ASSURANCES_VIE_OBJECTIF_TRIMESTRE")
                - col("variation_trimestre_ENCOURS"),
            )
            .withColumn(
                "OBJECTIF_ANNUEL_ASSURANCES_VIE", col("ASSURANCES_VIE_OBJECTIF")
            )
            .withColumn(
                "RAF_ANNUEL_ASSURANCES_VIE",
                col("ASSURANCES_VIE_OBJECTIF") - col("variation_annee_ENCOURS"),
            )
        )
        partitions_by = [
            "DATE_TRAITEMENT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_FDC_GEST",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "MARCHE",
        ]

        tro_cols = [
            column
            for column in list(df_final.columns)
            if ("OBJECTIF_PERIODE_" in column)
            or ("OBJECTIF_TRIMESTRE_" in column)
            or ("OBJECTIF_ANNUEL_" in column)
            or ("TRO_" in column)
            or ("RAF_" in column)
        ]
        return df_final.select(*(partitions_by + tro_cols))

    if categorie == "OPCVM":
        objectifs_tro = (
            objectifs_tro.select(
                "CODE_AGENCE",
                col("CODE_FDC").alias("CODE_FDC_GEST"),
                expr("OPCVM * 1000").alias("OPCVM_OBJECTIF"),
            )
            .withColumn("DELTA_OPCVM_OBJECTIF", lit(0))
            .na.fill({"OPCVM_OBJECTIF": 0})
        )
        df_final = (
            df_final.join(
                objectifs_tro, ["CODE_AGENCE", "CODE_FDC_GEST"], how="left"
            )
            .withColumn(
                "OPCVM_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("OPCVM_OBJECTIF")),
            )
            .withColumn(
                "OPCVM_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("OPCVM_OBJECTIF")),
            )
            .withColumn(
                "OPCVM_OBJECTIF_CUMUL", (col("POURC_CUMUL") * col("OPCVM_OBJECTIF"))
            )
            .withColumn(
                "DELTA_OPCVM_OBJECTIF_SEMAINE",
                (col("POURC_SEMAINE") * col("DELTA_OPCVM_OBJECTIF")),
            )
            .withColumn(
                "DELTA_OPCVM_OBJECTIF_TRIMESTRE",
                (col("POURC_TRIMESTRE") * col("DELTA_OPCVM_OBJECTIF")),
            )
            .withColumn(
                "DELTA_OPCVM_OBJECTIF_CUMUL",
                (col("POURC_CUMUL") * col("DELTA_OPCVM_OBJECTIF")),
            )
            .withColumn("OBJECTIF_PERIODE_OPCVM", col("OPCVM_OBJECTIF_CUMUL"))
            .withColumn(
                "TRO_PERIODE_OPCVM",
                when(
                    col("OPCVM_OBJECTIF_CUMUL") != 0,
                    col("variation_annee_ENCOURS") / col("OPCVM_OBJECTIF_CUMUL"),
                ).otherwise(None),
            )
            .withColumn("OBJECTIF_TRIMESTRE_OPCVM", col("OPCVM_OBJECTIF_TRIMESTRE"))
            .withColumn(
                "TRO_TRIMESTRE_OPCVM",
                when(
                    col("OPCVM_OBJECTIF_TRIMESTRE") != 0,
                    col("variation_trimestre_ENCOURS")
                    / col("OPCVM_OBJECTIF_TRIMESTRE"),
                ).otherwise(None),
            )
            .withColumn(
                "RAF_TRIMESTRE_OPCVM",
                col("OPCVM_OBJECTIF_TRIMESTRE") - col("variation_trimestre_ENCOURS"),
            )
            .withColumn("OBJECTIF_ANNUEL_OPCVM", col("OPCVM_OBJECTIF"))
            .withColumn(
                "RAF_ANNUEL_OPCVM",
                col("OPCVM_OBJECTIF") - col("variation_annee_ENCOURS"),
            )
            .withColumn(
                "OBJECTIF_PERIODE_DELTA_OPCVM", col("DELTA_OPCVM_OBJECTIF_CUMUL")
            )
            .withColumn(
                "TRO_PERIODE_DELTA_OPCVM",
                when(
                    col("DELTA_OPCVM_OBJECTIF_CUMUL") != 0,
                    col("variation_annee_delta_ENCOURS")
                    / col("DELTA_OPCVM_OBJECTIF_CUMUL"),
                ).otherwise(None),
            )
            .withColumn(
                "OBJECTIF_TRIMESTRE_DELTA_OPCVM",
                col("DELTA_OPCVM_OBJECTIF_TRIMESTRE"),
            )
            .withColumn(
                "TRO_TRIMESTRE_DELTA_OPCVM",
                when(
                    col("DELTA_OPCVM_OBJECTIF_TRIMESTRE") != 0,
                    col("variation_trimestre_delta_ENCOURS")
                    / col("DELTA_OPCVM_OBJECTIF_TRIMESTRE"),
                ).otherwise(None),
            )
            .withColumn(
                "RAF_TRIMESTRE_DELTA_OPCVM",
                col("DELTA_OPCVM_OBJECTIF_TRIMESTRE")
                - col("variation_trimestre_delta_ENCOURS"),
            )
            .withColumn("OBJECTIF_ANNUEL_DELTA_OPCVM", col("DELTA_OPCVM_OBJECTIF"))
            .withColumn(
                "RAF_ANNUEL_DELTA_OPCVM",
                col("DELTA_OPCVM_OBJECTIF") - col("variation_annee_delta_ENCOURS"),
            )
        )

        partitions_by = [
            "DATE_TRAITEMENT",
            "CODE_AGENCE",
            "NOM_AGENCE",
            "CODE_FDC_GEST",
            "CODE_GROUPE",
            "NOM_GROUPE",
            "CODE_REGION",
            "NOM_REGION",
            "CODE_BANQUE",
            "NOM_BANQUE",
            "MARCHE",
        ]

        tro_cols = [
            column
            for column in list(df_final.columns)
            if ("OBJECTIF_PERIODE_" in column)
            or ("OBJECTIF_TRIMESTRE_" in column)
            or ("OBJECTIF_ANNUEL_" in column)
            or ("TRO_" in column)
            or ("RAF_" in column)
        ]

        return df_final.select(*(partitions_by + tro_cols))
