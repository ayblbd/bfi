from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    add_months,
    coalesce,
    col,
    count,
    date_format,
    dayofmonth,
    first,
    last,
    last_day,
    lit,
    lpad,
    month,
    regexp_replace,
    round,
    row_number,
    rpad,
    substring,
    sum,
    to_date,
    when,
)
from pyspark.sql.types import DateType

from src.transformation.common import (
    get_last_recorded_sold_per_client_from,
    get_last_recorded_sold_per_client_from_last_month,
    get_last_recorded_sold_per_client_from_last_year,
    get_last_recorded_sold_per_client_from_same_day_last_year,
    map_account_balance,
)


def build_ancien_mapping_type_ressource_emploi(
    capjour_param2_oe: DataFrame,
) -> DataFrame:
    """
    Construit un mapping des types de ressources et d'emplois à partir des données en entrée.
    - Cette fonction traite un DataFrame pour catégoriser les lignes de crédit et de débit
      en fonction de règles de codification spécifiques.
    - - Les lignes de crédit sont associées à la catégorie "RESS" et les lignes de débit
      à la catégorie "EMP", avec des types spécifiques dérivés de la colonne concernée.
    Retourne un DataFrame combiné des deux catégories avec suppression des doublons.
    - La colonne "pci" est normalisée pour contenir exactement 6 caractères (complétés par "0" si nécessaire).
    - Deux sous-ensembles de données sont créés :
        - `part1` : Filtrage des lignes contenant "LIGNE_CREDIT" et application des règles de codification.
        - `part2` : Filtrage des lignes contenant "LIGNE_Debit" et application des mêmes règles.
    - Les deux sous-ensembles sont combinés à l'aide d'une opération UNION et dédoublés.

    Paramètres :
    - capjour_param2_oe : DataFrame d'entrée contenant les colonnes suivantes :
        - "pci" : Identifiant normalisé.
        - "NATURE" : Type de nature associé.
        - "LIGNE_CREDIT" : Colonne contenant les codes des lignes de crédit.
        - "LIGNE_Debit" : Colonne contenant les codes des lignes de débit.

    Retour :
    - DataFrame : Résultat avec les colonnes :
        - "PCI" : Identifiant normalisé.
        - "NATURE" : Type de nature associé.
        - "CODE_LIGNE" : Code ligne (de crédit ou de débit).
        - "CATEGORIE" : Catégorie ("RESS" pour ressources, "EMP" pour emplois).
        - "Type_RESS_EMPO" : Type associé au code ligne.
    """

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
    """
    Construit un DataFrame contenant les soldes clients et leur devise.

    Associe les soldes clients à des informations externes (nature externe et rubrique).
    Identifie le type de transaction (billet ou virement) en fonction de la rubrique.
    Convertit les soldes en types numériques pour une précision accrue.

    Les étapes :
    - Utilise une fenêtre pour sélectionner la première rubrique par compte dans `t601`.
    - Effectue plusieurs jointures pour enrichir les données :
        - Ajout des informations de nature externe à partir de `rp_gl_bct`.
        - Association des rubriques depuis `t601`.
    - Calcule les champs `PCI` (identifiant formaté) et `TYPE` (billet ou virement).
    - Sélectionne les colonnes nécessaires pour les analyses.

    Paramètres :
    - dm_sldcli : DataFrame contenant les soldes clients.
    - rp_gl_bct : DataFrame contenant les informations de nature externe.
    - t601 : DataFrame contenant les rubriques par compte.

    Retour :
    - DataFrame : Résultat avec les colonnes :
        - "DTTRTM" : Date de traitement.
        - "PCI" : Identifiant client formaté.
        - "NATURE_EXTERNE" : Nature externe associée.
        - "NUMERODECOMPTE" : Numéro de compte.
        - "DEVISE" : Devise associée.
        - "TYPE" : Type de transaction (billet ou virement).
        - "SOLDEJOURCR" : Solde créditeur au format décimal.
        - "SOLDEJOURDB" : Solde débiteur au format décimal.
    """
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
    """
    Calcule le taux de change moyen par unité pour les cours BBE.

    Les étapes :
    - Ajoute une colonne `moyen_par_unite` calculée comme la moyenne des cours
      de vente et d'achat divisée par l'unité de devise.
    - Sélectionne les colonnes nécessaires pour l'analyse des taux de change.

    Paramètres :
    - cours_bbe_bam : DataFrame contenant les cours de change BBE/BAM.

    Retour :
    - DataFrame : Résultat avec les colonnes :
        - "libDevise" : Libellé de la devise.
        - "venteClientele" : Cours de vente clientèle.
        - "achatClientele" : Cours d'achat clientèle.
        - "uniteDevise" : Unité de la devise.
        - "moyen_par_unite" : Taux moyen par unité.
        - "date" : Date du cours.
        - "partitiondate" : Date de partition.
    """

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
    """
    Calcule le taux de change moyen par unité pour les virements BAM.

    Les étapes :
    - Ajoute une colonne `moyen_par_unite` calculée comme le taux moyen divisé par
      l'unité de devise.
    - Sélectionne les colonnes pertinentes pour l'analyse des virements.

    Paramètres :
    - virement_bam : DataFrame contenant les données des virements BAM.

    Retour :
    - DataFrame : Résultat avec les colonnes :
        - "libDevise" : Libellé de la devise.
        - "moyen" : Taux moyen.
        - "uniteDevise" : Unité de la devise.
        - "moyen_par_unite" : Taux moyen par unité.
        - "date" : Date du cours.
        - "partitiondate" : Date de partition.
    """

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
    """
    Construit un DataFrame combiné des taux de change BAM (virement et BBE).

    Les étapes :
    - Calcule les taux moyens par unité pour les virements et les cours BBE.
    - Effectue des jointures pour associer les données enrichies aux références de devises.
    - Formate les colonnes finales pour inclure les informations pertinentes.

    Paramètres :
    - exploit_ref_devise : DataFrame contenant les références de devises.
    - virement_bam : DataFrame contenant les données des virements BAM.
    - cours_bbe_bam : DataFrame contenant les cours BBE/BAM.

    Retour :
    - DataFrame : Résultat avec les colonnes :
        - "CD_DEV_OPER" : Code opérationnel de la devise.
        - "CODE_DEVISE" : Code de la devise.
        - "DEVISE_LIBELLE" : Libellé de la devise.
        - "MIDBAM" : Taux moyen BAM.
        - "date_cours" : Date du cours.
        - "COURSMIN" : Cours minimum (achat).
        - "COURSMAX" : Cours maximum (vente).
        - "partitiondate" : Date de partition.
    """

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
    """
    Construit un DataFrame contenant les soldes clients ajustés par devise et taux de change.
    Cette fonction combine les soldes clients avec les taux de change pour calculer
      les soldes en devise d'origine et en devise ajustée.
    Prend en compte les transactions spécifiques en MAD (dirham marocain) pour lesquelles
      le taux de change est fixé à 1.
    Calcule les soldes créditeurs et débiteurs ajustés en fonction du type de transaction
      (virement ou autre).

    Les étapes :
    - Joint les soldes clients (`sldcli_nature`) avec les taux de change (`taux_change_bam`)
      sur le champ `DEVISE`.
    - Ajoute ou ajuste les colonnes suivantes :
        - `CODE_DEVISE` : Associe le code de devise, avec un traitement particulier pour le MAD.
        - `average_cours` : Moyenne des cours minimum et maximum pour les devises non-MAD.
        - `MIDBAM` : Ajuste le taux moyen BAM pour les devises MAD à 1.
        - `SOLDEJOURCR_DEVISE` et `SOLDEJOURDB_DEVISE` : Calcul des soldes ajustés par devise.
    - Sélectionne les colonnes finales nécessaires.

    Paramètres :
    - sldcli_nature : DataFrame contenant les soldes clients avec les colonnes suivantes :
        - `DEVISE` : Code devise.
        - `TYPE` : Type de transaction (ex., virement ou autre).
        - `SOLDEJOURCR` : Solde créditeur.
        - `SOLDEJOURDB` : Solde débiteur.
    - taux_change_bam : DataFrame contenant les taux de change BAM, avec les colonnes :
        - `CD_DEV_OPER` : Code opérationnel de la devise.
        - `CODE_DEVISE` : Code de devise standard.
        - `COURSMIN` : Cours minimum.
        - `COURSMAX` : Cours maximum.
        - `MIDBAM` : Taux moyen BAM.

    Retour :
    - DataFrame : Résultat enrichi avec les colonnes suivantes :
        - `DTTRTM` : Date de traitement.
        - `PCI` : Identifiant client formaté.
        - `NATURE_EXTERNE` : Nature externe de la transaction.
        - `NUMERODECOMPTE` : Numéro de compte.
        - `CODE_DEVISE` : Code de la devise ajustée.
        - `DEVISE` : Code de la devise d'origine.
        - `TYPE` : Type de transaction (virement ou autre).
        - `SOLDEJOURCR` : Solde créditeur d'origine.
        - `SOLDEJOURCR_DEVISE` : Solde créditeur ajusté en dirham.
        - `SOLDEJOURDB` : Solde débiteur d'origine.
        - `SOLDEJOURDB_DEVISE` : Solde débiteur ajusté en dirham.
    """

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
    """
    Applique un mappage des ressources et emplois en fonction du sens et des soldes.
    Transforme les colonnes de solde créditeur et débiteur (en devise et en valeur brute) en fonction de règles métier.
    Ajoute une colonne `categorie` pour classer les lignes comme "RESSOURCE" ou "EMPLOI".

    Les étapes :
    - Applique la transformation `map_account_balance` deux fois pour ajuster les colonnes `soldejourcr` et `soldejourdb`.
    - Détermine la `categorie` :
        - "EMPLOI" si `sens_mapping` est "AA".
        - "RESSOURCE" si `sens_mapping` est "PP".
        - Évalue les cas complexes lorsque `sens_mapping` est "AP".

    Paramètres :
    - df : DataFrame contenant les colonnes nécessaires (`sens_mapping`, `soldejourcr`, `soldejourdb`).

    Retour :
    - DataFrame : Résultat enrichi avec la colonne `categorie`.
    """

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
    """
    Prépare le mappage des ressources et emplois en normalisant les colonnes:
    - Normalise les valeurs de la colonne `nature` et renomme `sens` en `sens_mapping`.
    - Convertit les catégories en "RESSOURCE" ou "EMPLOI" selon la logique métier.

    Les étapes :
    - Ajoute une colonne `nature` formatée à trois caractères.
    - Renomme la colonne `sens` pour harmoniser la terminologie.
    - Reclassifie la colonne `categorie` en valeurs lisibles.

    Paramètres :
    - df : DataFrame contenant le mappage brut.

    Retour :
    - DataFrame : Résultat avec les colonnes normalisées.
    """
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
    """
    Construit le DataFrame des ressources en associant les soldes aux types de ressources :
    - Associe les soldes clients avec le mappage des ressources.
    - Filtre uniquement les lignes catégorisées comme "RESSOURCE".

    Les étapes :
    - Normalise et nettoie le mappage des ressources à l'aide de `selected_nouveau_mapping_ressource_emploi`.
    - Réalise des jointures :
        - Associe les soldes clients aux types de ressources par `pci` et `nature`.
        - Ajoute des détails supplémentaires sur les ressources (type, famille, libellé).
    - Filtre pour inclure uniquement les lignes de la catégorie "RESSOURCE".

    Paramètres :
    - sldcli_nature_daily_devises : DataFrame contenant les soldes clients.
    - nouveau_mapping_ressource_emploi : DataFrame contenant le mappage des ressources et emplois.

    Retour :
    - DataFrame : Résultat avec les colonnes enrichies pour les ressources.
    """

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
    """
    Construit le DataFrame des emplois en associant les soldes aux types d'emplois :
    - Associe les soldes clients avec le mappage des emplois.
    - Filtre uniquement les lignes catégorisées comme "EMPLOI".

    Les étapes :
    - Normalise et nettoie le mappage des emplois à l'aide de `selected_nouveau_mapping_ressource_emploi`.
    - Réalise des jointures :
        - Associe les soldes clients aux types d'emplois par `pci` et `nature`.
        - Ajoute des détails supplémentaires sur les emplois (type, famille, libellé).
    - Filtre pour inclure uniquement les lignes de la catégorie "EMPLOI".

    Paramètres :
    - sldcli_nature_daily_devises : DataFrame contenant les soldes clients.
    - nouveau_mapping_ressource_emploi : DataFrame contenant le mappage des ressources et emplois.

    Retour :
    - DataFrame : Résultat avec les colonnes enrichies pour les emplois.
    """
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
    """
    Construit un DataFrame pour identifier les comptes contentieux :
    - Identifie les comptes contentieux en combinant les informations des fonds de commerce
      et des comptes clients.
    - Associe les fonds de commerce à leur dernière modification.

    Les étapes :
    - Filtre les fonds de commerce actifs après une date donnée (`partition_date`) dans `t705`.
    - Utilise une fenêtre pour sélectionner la dernière modification par compte (`row_number`).
    - Filtre les comptes avec un type `TYPEFDCCOMPTE = 2` dans `fdcg_compte_ctx`.
    - Joint les deux ensembles pour associer les informations des gestionnaires et des agences.

    Paramètres :
    - t705 : DataFrame contenant les informations des fonds de commerce.
    - fdcg_compte_ctx : DataFrame contenant les comptes clients liés au contentieux.
    - niveau_regroupement : DataFrame pour enrichir les informations des agences.
    - partition_date : Date utilisée pour filtrer les fonds de commerce actifs.

    Retour :
    - DataFrame : Résultat avec les colonnes :
        - `numerocompteorigine` : Numéro de compte d'origine.
        - `codeagence_ctx` : Code de l'agence liée au compte.
        - `cdgest_ctx` : Code du gestionnaire lié au compte.
        - `etat` : Indique si le compte est "CONTENTIEUX".
    """

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
    """
    Construit un DataFrame combinant les informations des ressources et des emplois :
    - Combine les soldes des ressources et des emplois avec les informations des comptes clients.
    - Ajoute des informations enrichies des gestionnaires, agences, et niveaux de regroupement.
    - Identifie les comptes contentieux et marque leur état.

    Les étapes :
    - Regroupe les informations des comptes ouverts et des comptes clients (`unionByName`).
    - Joint les soldes avec les informations des comptes pour associer les agences et gestionnaires.
    - Ajoute des informations enrichies des tiers et utilisateurs :
        - Gestionnaires : Liés aux comptes et agences.
        - Niveaux de regroupement : Regroupements par agences, succursales et banques.
    - Identifie les comptes en contentieux et enrichit les colonnes liées au niveau des agences.

    Paramètres :
    - encours_ressource : DataFrame des ressources avec les colonnes nécessaires (`soldejourcr`, `soldejourdb`).
    - encours_emplois : DataFrame des emplois similaires à `encours_ressource`.
    - information_compte_tenu_par_client : DataFrame contenant les informations des comptes ouverts.
    - information_compte_client : DataFrame contenant les informations des comptes clients.
    - niveau_regroupement : DataFrame pour enrichir les regroupements (agences, banques).
    - contentieux : DataFrame des comptes contentieux.
    - information_tier : DataFrame contenant les informations des tiers clients.
    - utilisateur : DataFrame contenant les informations des utilisateurs (gestionnaires).

    Retour :
    - DataFrame : Résultat combiné et enrichi avec les colonnes :
        - Soldes : `soldejourcr`, `soldejourdb`, ajustés par devise.
        - Comptes : `numerodecompte`, `numerocompteorigine`.
        - Agences et gestionnaires : `codeagence`, `gestionnaire_gest`, `nomagence`.
        - Contentieux : Indication de l'état du compte (contentieux ou non).
    """
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
    """
    Construit un DataFrame combiné contenant les informations détaillées des tiers clients :
    - Cette fonction combine les informations des tiers clients avec des données enrichies,
      incluant les gestionnaires, agences, segments de marché, et classifications de qualité.
    - Associe plusieurs sources de données pour fournir une vue unifiée des clients.

    Les étapes :
    - Filtre et transforme les fonds de commerce (`fdc_tier`) pour séparer les gestionnaires
      et les opérateurs en fonction de leur type (`typefdctiers`).
    - Joint les données des gestionnaires et opérateurs avec les informations des clients (`client`).
    - Enrichit les données avec :
        - Les mappings des segments de marché et sous-segments (`mapping_marche_ge_pme` et `mapping_pro_retail`).
        - Les informations supplémentaires sur les clients, comme les qualités (`t706`), segments (`segmentation`),
          et sous-segments (`ref_sous_segment`).
        - Les informations statutaires des clients (`donnee_client`).
    - Utilise des colonnes calculées pour déterminer le marché, la gamme, et les libellés associés.

    Paramètres :
    - client : DataFrame contenant les informations de base des clients.
    - fdc_tier : DataFrame des fonds de commerce associés aux tiers.
    - t705 : DataFrame des rattachements et gestionnaires.
    - t706 : DataFrame des classifications de qualité.
    - mapping_marche_ge_pme : DataFrame pour mapper les agences gestionnaires aux segments de marché GE/PME.
    - mapping_pro_retail : DataFrame pour mapper les segments et sous-segments aux marchés PRO/RETAIL.
    - donnee_client : DataFrame contenant les statuts des clients.
    - segmentation : DataFrame pour mapper les codes de segment aux libellés.
    - ref_sous_segment : DataFrame pour mapper les codes de sous-segment aux libellés.
    - partition_date : Date utilisée pour filtrer les données actives.

    Retour :
    - DataFrame : Résultat enrichi avec les colonnes suivantes :
        - Identité : `numerotiers`, `prenomenscom`, `nomreduit`, `numeroidentite`, `typeidentite`, `datedenaissance`.
        - Localisation : `nationalite`, `zonederesidence`, `paysderesidence`.
        - Gestion : `agenceop`, `cd_fdc_op`, `gestionnaire_op`, `agencegest`, `cd_fdc_gest`, `gestionnaire_gest`.
        - Marché : `marche`, `libelle_marche`, `gamme`, `segment`, `libellesegment`, `new_seg`, `new_sous_seg`.
        - Statut : `statutclient`, `statut`, `cotation`, `cotationrisque`.
        - Sensibilité : `sensible`, `justsensible`, `kyceditee`, `lieukyc`, `vip`, `americain`.
    """

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
    """
    Construit un historique des soldes clients par devise :
    - Combine les soldes clients (`dm_sldcli`) avec les informations des comptes (`rp_gl_bct`)
      et les rubriques associées (`t601`).
    - Ajoute des informations enrichies comme le type de transaction (billet ou virement)
      et les partitions de date.

    Les étapes :
    - Filtre et traite les données de `t601` avec une fenêtre pour sélectionner
      la dernière rubrique active par compte et partition.
    - Joint les données des soldes clients avec les informations externes et enrichit
      les colonnes calculées (`PCI`, `TYPE`, `SOLDEJOURCR`, etc.).
    - Formate les colonnes pour un usage analytique.

    Paramètres :
    - dm_sldcli : DataFrame des soldes clients.
    - rp_gl_bct : DataFrame des comptes et informations externes.
    - t601 : DataFrame des rubriques associées aux comptes.

    Retour :
    - DataFrame : Historique des soldes enrichi.
    """

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
    """
    Construit un historique des taux de change BAM :
    - Combine les taux de change des virements (`virement_bam_history`)
      et des cours BBE (`cours_bbe_bam_history`).
    - Enrichit les données avec les informations des devises référencées.

    Les étapes :
    - Calcule les taux moyens par unité pour les virements et les cours BBE.
    - Joint les deux ensembles de données pour obtenir un historique consolidé.
    - Associe les données consolidées avec les références des devises pour ajouter
      des informations supplémentaires.

    Paramètres :
    - exploit_ref_devise : DataFrame des références de devises.
    - virement_bam_history : Historique des virements BAM.
    - cours_bbe_bam_history : Historique des cours BBE/BAM.

    Retour :
    - DataFrame : Historique enrichi des taux de change BAM.
    """

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
    """
    Construit un historique des soldes clients en devise :
    - Enrichit les soldes clients avec les taux de change pour convertir
      les soldes en devises spécifiques.
    - Ajoute des colonnes calculées pour les soldes créditeurs et débiteurs
      ajustés par devise.

    Les étapes :
    - Joint les données des soldes avec l'historique des taux de change sur les dates et devises.
    - Utilise des fenêtres pour combler les valeurs manquantes dans les colonnes de taux.
    - Calcule les soldes ajustés en utilisant les taux moyens ou les cours minimum/maximum.
    - Sélectionne les colonnes nécessaires pour l'analyse.

    Paramètres :
    - sldcli_nature : DataFrame des soldes clients.
    - taux_change_bam : DataFrame des taux de change BAM.

    Retour :
    - DataFrame : Historique des soldes enrichi avec les ajustements de taux.
    """

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
    """
    Construit un historique des soldes clients avec propagation temporelle :
    - Cette fonction applique des transformations successives pour récupérer les soldes des clients
      enregistrés à différentes périodes (jours, mois, années précédents).
    - La propagation inclut des ajustements pour combler les valeurs manquantes à partir des soldes antérieurs.

    Les étapes :
    - Utilise des transformations répétées pour calculer les soldes sur des périodes précises :
        - Derniers jours (`days` : 1 à 4).
        - Derniers mois (`months` : 1 à 4).
        - Dernières années (`years` : 1 à 2).
        - Même jour l'année précédente.
    - Les transformations utilisent des fonctions telles que `get_last_recorded_sold_per_client_from`.

    Paramètres :
    - dm_sldcli_history : DataFrame contenant l'historique des soldes clients.
    - partition_date : Date de partition utilisée comme référence pour calculer les périodes.

    Retour :
    - DataFrame enrichi avec des colonnes pour chaque période (jours, mois, années).
    """

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
        .select(
            "numerodecompte",
            "devise",
            "dttrtm",
            "last_day_soldejourcr_devise",
            "last_day_soldejourdb_devise",
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
        )
    )


def calculate_objectives_per_fdc(
    df: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Calcule les objectifs financiers par fonds de commerce (FDC) :
    - Cette fonction calcule les déviations des objectifs par rapport aux soldes des années et mois précédents.
    - Ajoute des colonnes calculées pour suivre les écarts entre objectifs et réalisations.

    Les étapes :
    - Multiplie les colonnes des objectifs (`objectif_cct`, `objectif_cmt`, etc.) par un facteur million.
    - Utilise des fenêtres pour agréger les soldes sur les périodes spécifiques :
        - Dernier mois (`last_month_soldejour*`).
        - Dernière année (`last_year_soldejour*`).
    - Calcule les écarts (`delta_year_*`) en soustrayant les objectifs des réalisations.

    Paramètres :
    - df : DataFrame contenant les soldes et objectifs financiers.
    - partition_date : Date de partition utilisée pour calculer les déviations temporelles.

    Retour :
    - DataFrame enrichi avec des colonnes pour les objectifs et écarts financiers.
    """

    window = Window.partitionBy("cdgest_ctx", "dttrtm").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    is_cct = col("objectif_cct").isNotNull()

    is_cmt = col("objectif_cmt").isNotNull()

    is_rav = col("objectif_rav").isNotNull()

    is_rat = col("objectif_rat").isNotNull()

    is_sf = col("objectif_sf").isNotNull()

    MILLION = lit(1_000_000)

    return (
        df.withColumn("objectif_cct", col("objectif_cct") * MILLION)
        .withColumn("objectif_cmt", col("objectif_cmt") * MILLION)
        .withColumn("objectif_sf", col("objectif_sf") * MILLION)
        .withColumn("objectif_rav", col("objectif_rav") * MILLION)
        .withColumn("objectif_rat", col("objectif_rat") * MILLION)
        .withColumn("is_tro", is_cct | is_cmt | is_rav | is_rat | is_sf)
        .withColumn(
            "last_month_soldejourdb_devise_per_fdc",
            sum(when(col("is_tro"), col("last_month_soldejourdb_devise"))).over(
                window
            ),
        )
        .withColumn(
            "last_month_soldejourcr_devise_per_fdc",
            sum(when(col("is_tro"), col("last_month_soldejourcr_devise"))).over(
                window
            ),
        )
        .withColumn(
            "last_year_soldejourdb_devise_per_fdc",
            sum(when(col("is_tro"), col("last_year_soldejourdb_devise"))).over(
                window
            ),
        )
        .withColumn(
            "last_year_soldejourcr_devise_per_fdc",
            sum(when(col("is_tro"), col("last_year_soldejourcr_devise"))).over(
                window
            ),
        )
        .withColumn(
            "delta_year_db",
            when(
                is_cct,
                col("objectif_cct") - col("last_year_soldejourdb_devise_per_fdc"),
            )
            .when(
                is_cmt,
                col("objectif_cmt") - col("last_year_soldejourdb_devise_per_fdc"),
            )
            .when(
                is_sf,
                col("objectif_sf") - col("last_year_soldejourdb_devise_per_fdc"),
            ),
        )
        .withColumn(
            "delta_year_cr",
            when(
                is_rav,
                col("objectif_rav") - col("last_year_soldejourcr_devise_per_fdc"),
            ).when(
                is_rat,
                col("objectif_rat") - col("last_year_soldejourcr_devise_per_fdc"),
            ),
        )
        .withColumn(
            "previous_month", month(add_months(to_date(lit(partition_date)), -1))
        )
    )


def build_fact_table(
    ressource_emplois: DataFrame,
    dm_sldcli_solde_history: DataFrame,
    nouveau_mapping_ressource_emploi: DataFrame,
    objectif_bfi_corpo: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une table de faits combinant les ressources et les emplois :
    - Combine les ressources et les emplois avec des historiques de soldes et des objectifs.
    - Ajoute des colonnes calculées pour analyser les soldes par période et type (ressources ou emplois).

    Les étapes :
    - Joint les historiques de soldes avec les ressources et emplois.
    - Sépare les types de données (`RESSOURCE`, `EMPLOI`) et effectue des transformations de netting sur les soldes.
    - Utilise les objectifs financiers pour enrichir les analyses.

    Paramètres :
    - ressource_emplois : DataFrame des ressources et emplois.
    - dm_sldcli_solde_history : Historique des soldes clients.
    - nouveau_mapping_ressource_emploi : Mapping des types de ressources et emplois.
    - objectif_bfi_corpo : Objectifs financiers associés aux gestionnaires.
    - partition_date : Date de partition utilisée pour calculer les analyses temporelles.

    Retour :
    - DataFrame enrichi avec les colonnes nécessaires pour une table de faits analytique.
    """

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
        .withColumn("last_month_soldejourdb_devise", lit(None))
        .withColumn("last_2_months_soldejourdb_devise", lit(None))
        .withColumn("last_3_months_soldejourdb_devise", lit(None))
        .withColumn("last_4_months_soldejourdb_devise", lit(None))
        .withColumn("last_year_soldejourdb_devise", lit(None))
        .withColumn("last_2_years_soldejourdb_devise", lit(None))
        .withColumn("same_day_last_year_soldejourdb_devise", lit(None))
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
        .withColumn("last_month_soldejourcr_devise", lit(None))
        .withColumn("last_2_months_soldejourcr_devise", lit(None))
        .withColumn("last_3_months_soldejourcr_devise", lit(None))
        .withColumn("last_4_months_soldejourcr_devise", lit(None))
        .withColumn("last_year_soldejourcr_devise", lit(None))
        .withColumn("last_2_years_soldejourcr_devise", lit(None))
        .withColumn("same_day_last_year_soldejourcr_devise", lit(None))
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

    objectif_bfi_corpo = objectif_bfi_corpo.withColumnRenamed(
        "cdfdc_gest", "cdgest_ctx"
    )

    fact_table_with_objectifs = fact_table.join(
        objectif_bfi_corpo, ["cdgest_ctx"], "left"
    ).transform(calculate_objectives_per_fdc, partition_date=partition_date)

    return fact_table_with_objectifs


def build_dataframe_with_expected_dates(
    df: DataFrame, partition_date: str
) -> DataFrame:
    """
    Construit un DataFrame avec des dates attendues et les remplit en cas de dates manquantes :
    - Génère une liste de dates quotidiennes entre le début de l'année et la date de partition donnée.
    - Compare ces dates attendues avec les dates réellement présentes dans le DataFrame d'entrée.
    - Remplit les dates manquantes avec la dernière date disponible (forward fill).

    Les étapes :
    - Crée une liste de dates quotidiennes (`expected_dates`) allant du 1er janvier à la `partition_date`.
    - Sélectionne les dates réelles (`actual_dates`) uniques du DataFrame d'entrée.
    - Utilise une fenêtre pour appliquer un "forward fill" sur les dates manquantes.
    - Retourne un DataFrame avec deux colonnes :
        - `dttrtm_expected` : Toutes les dates attendues.
        - `dttrtm_actual` : Les dates réelles ou la dernière date disponible remplie.

    Paramètres :
    - df : DataFrame contenant une colonne `dttrtm` avec les dates réelles.
    - partition_date : Dernière date attendue dans le format "YYYY-MM-DD".

    Retour :
    - DataFrame avec deux colonnes :
        - `dttrtm_expected` : Liste complète des dates attendues.
        - `dttrtm_actual` : Correspondance des dates réelles ou des dates remplies.

    Utilisation métier :
    - Permet d'analyser les données en comblant les trous dans les séries temporelles.
    - Facilite les calculs d'indicateurs temporels en s'assurant que toutes les dates attendues sont présentes.
    """

    spark = SparkSession.builder.getOrCreate()

    end_date = datetime.strptime(partition_date, "%Y-%m-%d").date()
    start_date = end_date.replace(month=1, day=1)

    list_of_dates = [
        start_date + timedelta(days=n)
        for n in range((end_date + timedelta(days=1) - start_date).days)
    ]

    expected_dates = spark.createDataFrame(
        list_of_dates, DateType()
    ).withColumnRenamed("value", "dttrtm_expected")
    actual_dates = (
        df.select("dttrtm")
        .drop_duplicates()
        .withColumnRenamed("dttrtm", "dttrtm_actual")
    )

    window_date = (
        Window.partitionBy("index")
        .orderBy(col("dttrtm_expected"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    forward_filled_dates = (
        expected_dates.join(
            actual_dates,
            expected_dates["dttrtm_expected"] == actual_dates["dttrtm_actual"],
            "left",
        )
        .withColumn("index", lit(1))
        .withColumn(
            "dttrtm_actual",
            last(col("dttrtm_actual"), ignorenulls=True).over(window_date),
        )
        .select("dttrtm_actual", "dttrtm_expected")
    )

    return forward_filled_dates


def build_bfi_monthly_legacy(
    solde_client_history: DataFrame,
    nouveau_mapping_ressource_emploi: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une table mensuelle des soldes clients avec propagation temporelle et classification :
    - Cette fonction génère une vue mensuelle des soldes des clients, classifiés par type de ressources et emplois.
    - Elle effectue un remplissage des dates manquantes pour garantir une continuité temporelle.
    - Les soldes créditeurs et débiteurs sont agrégés par compte, devise et mois.

    Les étapes :
    1. Préparation des mappings :
       - `nouveau_mapping_ressource_emploi` est transformé pour sélectionner les colonnes nécessaires à la classification.
       - Création des mappings nettoyés (`cleaned_mapping`) et enrichis (`type_ressource_emploi`).

    2. Ajout des informations de ressources/emplois :
       - Les soldes des clients (`solde_client_history`) sont joints avec les mappings pour ajouter les catégories et types.

    3. Propagation temporelle :
       - Les dates attendues sont générées pour l'année jusqu'à `partition_date` grâce à `build_dataframe_with_expected_dates`.
       - Les dates manquantes sont remplies par propagation à l'aide de jointures et transformations.

    4. Agrégation mensuelle :
       - Les soldes créditeurs et débiteurs (`soldejourcr`, `soldejourdb`) sont agrégés par :
         - Compte (`numerodecompte`), devise, mois (`dt_month`), PCI, nature, type et catégorie.
       - Une colonne supplémentaire `occurrence_mensuelle` est ajoutée pour indiquer le nombre de jours dans le mois.

    5. Formatage et arrondi :
       - Les soldes agrégés en devise sont arrondis à 4 décimales pour éviter des écarts minimes.

    Paramètres :
    - `solde_client_history` : DataFrame contenant l'historique des soldes des clients.
    - `nouveau_mapping_ressource_emploi` : Mapping des types de ressources et emplois.
    - `partition_date` : Date de partition pour définir la période d'analyse (au format `YYYY-MM-DD`).

    Retour :
    - DataFrame avec les colonnes suivantes :
      - `numerodecompte` : Identifiant unique du compte client.
      - `devise` : Code de la devise associée au compte.
      - `dt_month` : Mois de la période (au format `YYYYMM`).
      - `pci` : Code PCI (type de produit ou service).
      - `nature` : Nature de l'opération.
      - `sens_mapping` : Sens de l'opération (`RESSOURCE`, `EMPLOI`).
      - `categorie` : Catégorie (`RESSOURCE`, `EMPLOI`).
      - `soldejourcr_monthly` : Total créditeur du mois (brut).
      - `soldejourcr_devise_monthly` : Total créditeur du mois en dirham.
      - `soldejourdb_monthly` : Total débiteur du mois (brut).
      - `soldejourdb_devise_monthly` : Total débiteur du mois en dirham.
      - `occurrence_mensuelle` : Nombre de jours dans le mois.
    """

    nouveau_mapping_ressource_emploi = selected_nouveau_mapping_ressource_emploi(
        nouveau_mapping_ressource_emploi
    )

    window = (
        Window.partitionBy("numerodecompte", "devise")
        .orderBy("dttrtm")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
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

    solde_client_with_sens = (
        solde_client_history.withColumnRenamed("nature_externe", "nature")
        .withColumn("pci", last(col("pci"), ignorenulls=True).over(window))
        .withColumn("nature", last(col("nature"), ignorenulls=True).over(window))
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

    dataframe_with_expected_dates = build_dataframe_with_expected_dates(
        solde_client_with_sens, partition_date
    )

    sldcli_with_forward_filling = (
        solde_client_with_sens.join(
            dataframe_with_expected_dates,
            solde_client_with_sens["dttrtm"]
            == dataframe_with_expected_dates["dttrtm_actual"],
            "left",
        )
        .withColumn("dttrtm", col("dttrtm_expected"))
        .withColumn("dt_month", date_format("dttrtm", "yyyyMM"))
        .select(
            "numerodecompte",
            "devise",
            "dttrtm",
            "dt_month",
            "categorie",
            "soldejourcr",
            "soldejourcr_devise",
            "soldejourdb",
            "soldejourdb_devise",
            "pci",
            "nature",
            "sens_mapping",
            "type_ressource_emploi",
            "famille_tdb_banque",
            "libelle_type",
        )
    )

    balance_per_account_per_month = (
        sldcli_with_forward_filling.groupBy(
            "numerodecompte",
            "devise",
            "dt_month",
            "pci",
            "nature",
            "sens_mapping",
            "categorie",
        )
        .agg(
            sum("soldejourcr").alias("soldejourcr_monthly"),
            sum("soldejourcr_devise").alias("soldejourcr_devise_monthly"),
            sum("soldejourdb").alias("soldejourdb_monthly"),
            sum("soldejourdb_devise").alias("soldejourdb_devise_monthly"),
        )
        .withColumn("occurrence_mensuelle", dayofmonth(last_day(col("dt_month"))))
        .withColumn(
            "soldejourcr_devise_monthly", round(col("soldejourcr_devise_monthly"), 4)
        )
        .withColumn(
            "soldejourdb_devise_monthly", round(col("soldejourdb_devise_monthly"), 4)
        )
    )

    return balance_per_account_per_month.select(
        "numerodecompte",
        "devise",
        "dt_month",
        "pci",
        "nature",
        "sens_mapping",
        "categorie",
        "soldejourcr_monthly",
        "soldejourcr_devise_monthly",
        "soldejourdb_monthly",
        "soldejourdb_devise_monthly",
        "occurrence_mensuelle",
    )


def build_bfi_monthly(
    solde_client_history: DataFrame,
    nouveau_mapping_ressource_emploi: DataFrame,
) -> DataFrame:
    """
    Construit une vue mensuelle des soldes clients en agrégeant les données par compte, devise et type :
    - La fonction agrège les soldes mensuels créditeurs et débiteurs par client, devise et mois.
    - Elle utilise un mapping pour classer les soldes par type de ressources ou emplois.
    - Filtre les lignes inutiles (où les soldes créditeurs et débiteurs sont nuls ou manquants).

    Les étapes :
    1. Préparation des mappings :
       - Sélectionne les colonnes nécessaires pour le mapping des ressources/emplois (`cleaned_mapping`).
       - Ajoute des informations supplémentaires comme la catégorie et le type (`type_ressource_emploi`).

    2. Ajout des informations de classification :
       - Joint les données historiques des soldes (`solde_client_history`) avec le mapping pour enrichir les informations.
       - Utilise une fenêtre pour remplir les valeurs manquantes de PCI et nature.

    3. Calcul des occurrences mensuelles :
       - Compte le nombre de jours où des transactions sont enregistrées pour chaque compte et mois (`occurrence_per_account_per_month`).

    4. Agrégation mensuelle :
       - Calcule les totaux créditeurs et débiteurs (brut et en devise) par compte, devise, mois, PCI et catégorie.

    5. Filtrage :
       - Supprime les lignes où les soldes créditeurs et débiteurs sont simultanément nuls ou manquants.

    6. Jointure finale :
       - Ajoute les occurrences mensuelles aux données agrégées.

    Paramètres :
    - `solde_client_history` : DataFrame contenant l'historique des soldes clients.
    - `nouveau_mapping_ressource_emploi` : Mapping des types de ressources et emplois.

    Retour :
    - DataFrame avec les colonnes suivantes :
      - `numerodecompte` : Identifiant unique du compte client.
      - `devise` : Code de la devise associée au compte.
      - `dt_month` : Mois de la période (au format `yyyyMM`).
      - `pci` : Code PCI (type de produit ou service).
      - `nature` : Nature de l'opération.
      - `sens_mapping` : Sens de l'opération (`RESSOURCE`, `EMPLOI`).
      - `categorie` : Catégorie (`RESSOURCE`, `EMPLOI`).
      - `soldejourcr_monthly` : Total créditeur du mois (brut).
      - `soldejourcr_devise_monthly` : Total créditeur du mois en dirham.
      - `soldejourdb_monthly` : Total débiteur du mois (brut).
      - `soldejourdb_devise_monthly` : Total débiteur du mois en dirham.
      - `occurrence_mensuelle` : Nombre de jours actifs dans le mois.
    """
    nouveau_mapping_ressource_emploi = selected_nouveau_mapping_ressource_emploi(
        nouveau_mapping_ressource_emploi
    )

    window = (
        Window.partitionBy("numerodecompte", "devise")
        .orderBy("dttrtm")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
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

    solde_client_with_sens = (
        solde_client_history.withColumnRenamed("nature_externe", "nature")
        .withColumn("pci", last(col("pci"), ignorenulls=True).over(window))
        .withColumn("nature", last(col("nature"), ignorenulls=True).over(window))
        .join(
            cleaned_mapping,
            ["pci", "nature"],
            "left",
        )
        .transform(map_ressource_emploi)
        .withColumn("dt_month", date_format("dttrtm", "yyyyMM"))
        .join(
            type_ressource_emploi,
            ["pci", "nature", "sens_mapping", "categorie"],
            "left",
        )
    )

    occurrence_per_account_per_month = solde_client_with_sens.groupBy(
        "numerodecompte", "devise", "dt_month"
    ).agg(
        count("*").alias("occurrence_mensuelle"),
    )

    balance_per_account_per_month = (
        solde_client_with_sens.groupBy(
            "numerodecompte",
            "devise",
            "dt_month",
            "pci",
            "nature",
            "sens_mapping",
            "categorie",
        )
        .agg(
            sum("soldejourcr").alias("soldejourcr_monthly"),
            sum("soldejourcr_devise").alias("soldejourcr_devise_monthly"),
            sum("soldejourdb").alias("soldejourdb_monthly"),
            sum("soldejourdb_devise").alias("soldejourdb_devise_monthly"),
        )
        .filter(
            ~(
                (
                    col("soldejourcr_monthly").isNull()
                    | (col("soldejourcr_monthly") == lit("0"))
                )
                & (
                    col("soldejourdb_monthly").isNull()
                    | (col("soldejourdb_monthly") == lit("0"))
                )
            )
        )
    )

    return balance_per_account_per_month.join(
        occurrence_per_account_per_month,
        ["numerodecompte", "devise", "dt_month"],
        "left",
    ).select(
        "numerodecompte",
        "devise",
        "dt_month",
        "pci",
        "nature",
        "sens_mapping",
        "categorie",
        "soldejourcr_monthly",
        "soldejourcr_devise_monthly",
        "soldejourdb_monthly",
        "soldejourdb_devise_monthly",
        "occurrence_mensuelle",
    )
