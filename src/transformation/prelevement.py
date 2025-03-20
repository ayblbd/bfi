from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    to_date,
    when,
)


def build_prelevement_contrat(
    contrat: DataFrame,
    mouvement_position: DataFrame,
    exploit_typecontrat: DataFrame,
    exploit_souscompte: DataFrame,
    sage_decodage: DataFrame,
    titulaire: DataFrame,
    renouv_produit: DataFrame,
    categorie_assurance: DataFrame,
) -> DataFrame:
    """
    Cette fonction construit un DataFrame contenant les informations relatives aux prélèvements par contrat
    en combinant plusieurs sources de données.

    Principales étapes :
    1. Préparation des données :
    - Validation des contrats (colonne `is_contrat_valid`).
    - Filtrage des mouvements avec statut "OCI" et sens "1" (prélèvements).
    - Transformation et filtrage des données sur les types de contrats, sous-comptes, décodages Sage, titulaires,
        produits de renouvellement, et catégories d'assurance.
    2. Jointures :
    - Association des mouvements aux contrats via les références documentaire et client.
    - Jointure des informations des types de contrats, sous-comptes, et décodages Sage pour enrichir les données.
    - Intégration des informations sur les produits de renouvellement et catégories d'assurance.
    3. Calculs :
    - Détermination du type de facturation basé sur des règles spécifiques.
    - Identification de la catégorie produit ("bancaire" ou "non bancaire").
    4. Filtrage :
    - Exclusion des enregistrements avec `typefacturation` égal à "NP".
    5. Sélection des colonnes finales nécessaires pour les analyses.

    Retourne :
        Un DataFrame enrichi contenant les informations sur les contrats et leurs prélèvements.

    Paramètres :
    - contrat : DataFrame contenant les données des contrats.
    - mouvement_position : DataFrame des mouvements de compte.
    - exploit_typecontrat : DataFrame des types de contrats.
    - exploit_souscompte : DataFrame des sous-comptes.
    - sage_decodage : DataFrame pour les décodages Sage.
    - titulaire : DataFrame des titulaires de contrat.
    - renouv_produit : DataFrame des produits renouvelables.
    - categorie_assurance : DataFrame des catégories d'assurance.
    """
    contrat = contrat.withColumn(
        "is_contrat_valid",
        when(
            col("etatcontrat").isin(["3", "S", "T", "S", "X"]),
            "VALIDE",
        )
        .when(
            (col("etatcontrat") == "7")
            & (col("dateecheance") > to_date(col("partitiondate"))),
            "VALIDE",
        )
        .otherwise("NONVALIDE"),
    ).select(
        "numerocontratinterne",
        "numerocontratexterne",
        "typecontrat",
        "agencegestionnaire",
        "etatcontrat",
        "is_contrat_valid",
    )
    mouvement_position = (
        mouvement_position.filter((col("statut") == "OCI") & (col("sens") == "1"))
        .withColumn("montant", col("montant") / 100)
        .withColumn(
            "is_prelevee",
            col("motif").contains("Facture payee"),
        )
        .select(
            "numerodecompte",
            "dtvaleur",
            "montant",
            "refope",
            "motif",
            "is_prelevee",
            "refdocumentaire",
            "refcli",
            "dtope",
            "devise",
            "typeope",
        )
    )

    exploit_typecontrat = (
        exploit_typecontrat.filter(col("dfval") > col("partitiondate"))
        .select(
            "libcontrat",
            "typecontrat",
        )
        .drop_duplicates()
    )

    exploit_souscompte = exploit_souscompte.select(
        "numerodecompte",
        "etatcompte",
    )

    sage_decodage_etat_compte = (
        sage_decodage.filter(col("motcle") == "EtatCompte")
        .filter(col("dfval") > col("partitiondate"))
        .withColumn("SD_EtatCompte", col("valeurdecode"))
        .select(
            "valeurdecode",
            "valeurcode",
            "SD_EtatCompte",
        )
    ).drop_duplicates()

    sage_decodage_lib_etat_contrat = (
        sage_decodage.filter(col("motcle") == "EtatContrat")
        .filter(col("dfval") > col("partitiondate"))
        .withColumn("Lib_EtatContrat", col("valeurdecode"))
        .select(
            "valeurdecode",
            "valeurcode",
            "Lib_EtatContrat",
        )
    ).drop_duplicates()

    titulaire = titulaire.select(
        "numerotiers",
        "numerocontratinterne",
    )

    renouv_produit = (
        renouv_produit.filter(col("dfval") > col("partitiondate"))
        .filter(col("typecontrat") != "EX01")
        .select(
            "typefacturation",
            "typecontrat",
        )
        .drop_duplicates()
    )

    categorie_assurance = (
        categorie_assurance.withColumnRenamed("typecontrat", "typecontrat_check")
        .select("typecontrat_check")
        .drop_duplicates()
    )

    return (
        contrat.join(
            mouvement_position,
            (
                mouvement_position["refdocumentaire"]
                == contrat["numerocontratinterne"]
            )
            & (mouvement_position["refcli"] == contrat["numerocontratexterne"]),
        )
        .join(exploit_typecontrat, ["typecontrat"])
        .join(exploit_souscompte, ["numerodecompte"])
        .join(
            sage_decodage_etat_compte,
            sage_decodage_etat_compte["valeurcode"]
            == exploit_souscompte["etatcompte"],
        )
        .join(
            sage_decodage_lib_etat_contrat,
            sage_decodage_lib_etat_contrat["valeurcode"] == contrat["etatcontrat"],
        )
        .join(titulaire, ["numerocontratinterne"])
        .join(renouv_produit, ["typecontrat"], how="left")
        .join(
            categorie_assurance,
            contrat.typecontrat == categorie_assurance["typecontrat_check"],
            how="left",
        )
        .withColumn(
            "typefacturation",
            when((col("typecontrat") == "EX01") & (col("typeope") == "99S95"), "A")
            .when((col("typecontrat") == "EX01") & (col("typeope") == "99S93"), "M")
            .otherwise(col("typefacturation")),
        )
        .filter(col("typefacturation") != "NP")
        .withColumn(
            "categorie_produit",
            when(
                col("typecontrat_check").isNotNull(), "produit non bancaire"
            ).otherwise("produit bancaire"),
        )
        .select(
            "numerodecompte",
            "numerotiers",
            "numerocontratexterne",
            "numerocontratinterne",
            "agencegestionnaire",
            "typecontrat",
            "categorie_produit",
            "libcontrat",
            "is_contrat_valid",
            "SD_EtatCompte",
            "etatcontrat",
            "Lib_EtatContrat",
            "motif",
            "is_prelevee",
            "refope",
            "dtvaleur",
            "dtope",
            "typeope",
            "typefacturation",
            "montant",
            "devise",
        )
    )


def build_prelevement_contrat_approvisionne(
    mouvement_position: DataFrame,
    exp_contratprerenouv_banque: DataFrame,
    typecontrat: DataFrame,
    contrat: DataFrame,
    exploit_souscompte: DataFrame,
    sage_decodage: DataFrame,
) -> DataFrame:
    """
    Cette fonction construit un DataFrame contenant les prélèvements liés aux contrats approvisionnés
    en combinant plusieurs sources de données.

    Principales étapes :
    1. Préparation des données :
    - Filtrage des mouvements avec statut "OAP".
    - Transformation des colonnes dans `exp_contratprerenouv_banque` et renommage pour uniformité.
    - Sélection des colonnes pertinentes des DataFrames `typecontrat`, `contrat`, `exploit_souscompte`,
        et décodages Sage pour enrichissement.
    2. Jointures :
    - Association des mouvements avec les contrats approvisionnés via les références documentaire et dates de facturation.
    - Jointures avec les types de contrats, l'état des comptes, et les décodages Sage pour enrichir les informations.
    3. Sélection des colonnes nécessaires pour les analyses.

    Retourne :
        Un DataFrame contenant les informations suivantes :
        - numerodecompte : Numéro de compte.
        - numerotiers : Numéro du tiers.
        - SD_EtatCompte : Décodage de l'état du compte.
        - numerocontratexterne : Numéro externe du contrat.
        - numerocontratinterne : Numéro interne du contrat.
        - typecontrat : Type de contrat.
        - libcontrat : Libellé du contrat.
        - etatcontrat : État du contrat.
        - Lib_EtatContrat : Décodage de l'état du contrat.
        - code_lieu : Code du lieu.
        - refope : Référence opérationnelle.
        - typefacturation : Type de facturation.
        - datefacturation : Date de facturation.
        - montant_ttc : Montant TTC.
        - montant : Montant de l'opération.
        - position_compte : Position du compte.

    Paramètres :
    - mouvement_position : DataFrame contenant les mouvements des comptes.
    - exp_contratprerenouv_banque : DataFrame des contrats approvisionnés.
    - typecontrat : DataFrame des types de contrat.
    - contrat : DataFrame des contrats.
    - exploit_souscompte : DataFrame des sous-comptes.
    - sage_decodage : DataFrame pour les décodages Sage.
    """
    mouvement_position = (
        mouvement_position.filter(col("statut") == "OAP")
        .withColumn("montant", col("montant") / 100)
        .select(
            "refope",
            "montant",
            "refdocumentaire",
            "dtvaleur",
        )
    )

    exp_contratprerenouv_banque = (
        exp_contratprerenouv_banque.filter(col("etat") == "0")
        .withColumnRenamed("numcompte", "numerodecompte")
        .withColumnRenamed("numtiers", "numerotiers")
        .withColumnRenamed("numcontratexterne", "numerocontratexterne")
        .withColumnRenamed("numcontratinterne", "numerocontratinterne")
        .withColumnRenamed("lemontantttc", "montant_ttc")
        .withColumnRenamed("codelieu", "code_lieu")
        .withColumnRenamed("positionminute", "position_compte")
        .select(
            "numerodecompte",
            "numerotiers",
            "numerocontratexterne",
            "numerocontratinterne",
            "typecontrat",
            "typefacturation",
            "datefacturation",
            "montant_ttc",
            "code_lieu",
            "position_compte",
        )
    )

    typecontrat = typecontrat.select(
        "libcontrat",
        "typecontrat",
    )

    contrat = contrat.select(
        "etatcontrat",
        "numerocontratinterne",
    )

    exploit_souscompte = exploit_souscompte.select(
        "etatcompte",
        "numerodecompte",
    )

    sage_decodage_etat_compte = (
        sage_decodage.filter(col("motcle") == "EtatCompte")
        .filter(col("dfval") > col("partitiondate"))
        .withColumn("SD_EtatCompte", col("valeurdecode"))
        .select(
            "valeurdecode",
            "valeurcode",
            "SD_EtatCompte",
        )
    ).drop_duplicates()

    sage_decodage_lib_etat_contrat = (
        sage_decodage.filter(col("motcle") == "EtatContrat")
        .filter(col("dfval") > col("partitiondate"))
        .withColumn("Lib_EtatContrat", col("valeurdecode"))
        .select(
            "valeurdecode",
            "valeurcode",
            "Lib_EtatContrat",
        )
    ).drop_duplicates()

    return (
        mouvement_position.join(
            exp_contratprerenouv_banque,
            (
                mouvement_position["refdocumentaire"]
                == exp_contratprerenouv_banque["numerocontratinterne"]
            )
            & (
                mouvement_position["dtvaleur"]
                == exp_contratprerenouv_banque["datefacturation"]
            ),
        )
        .join(typecontrat, ["typecontrat"], how="left")
        .join(contrat, ["numerocontratinterne"], how="left")
        .join(exploit_souscompte, ["numerodecompte"], how="left")
        .join(
            sage_decodage_etat_compte,
            sage_decodage_etat_compte["valeurcode"]
            == exploit_souscompte["etatcompte"],
            how="left",
        )
        .join(
            sage_decodage_lib_etat_contrat,
            sage_decodage_lib_etat_contrat["valeurcode"] == contrat["etatcontrat"],
            how="left",
        )
        .select(
            "numerodecompte",
            "numerotiers",
            "SD_EtatCompte",
            "numerocontratexterne",
            "numerocontratinterne",
            "typecontrat",
            "libcontrat",
            "etatcontrat",
            "Lib_EtatContrat",
            "code_lieu",
            "refope",
            "typefacturation",
            "datefacturation",
            "montant_ttc",
            "montant",
            "position_compte",
        )
    )


def build_stock_contrat_banque(
    contrat_produit: DataFrame,
    produit: DataFrame,
    contrat: DataFrame,
) -> DataFrame:
    """
    Cette fonction construit un DataFrame représentant le stock des contrats bancaires en combinant
    les informations des produits, des contrats et des contrats produits.

    Principales étapes :
    1. Préparation des données :
    - Renommage et sélection des colonnes pertinentes dans `contrat_produit` pour uniformité.
    - Transformation et déduplication des colonnes liées aux produits (`produit`) pour inclure :
        - Les libellés des produits (via `codeproduit`).
        - Les types de contrats associés aux produits.
    - Sélection des colonnes nécessaires dans `contrat`.
    2. Jointures :
    - Association des contrats produits avec les produits via `codeproduit`.
    - Jointure des informations de type de contrat avec les produits.
    - Intégration des modifications des contrats à partir du DataFrame `contrat`.
    3. Sélection des colonnes finales :
    - Inclut des informations comme le numéro de compte, numéro de contrat interne/externe, date de souscription, etc.

    Retourne :
        Un DataFrame enrichi contenant les informations sur les contrats bancaires.

    Paramètres :
    - contrat_produit : DataFrame contenant les informations sur les contrats produits.
    - produit : DataFrame des produits bancaires.
    - contrat : DataFrame des contrats avec leurs modifications.
    """
    contrat_produit = contrat_produit.withColumnRenamed(
        "numerocarte", "numerocontratexterne"
    ).select(
        "numerodecompte",
        "datesouscription",
        "codeproduit",
        "numerocontratinterne",
        "numerocontratexterne",
        "numeropackage",
        "dateecheance",
        "etatproduit",
    )
    produit_with_codeproduit = (
        produit.withColumnRenamed("typedeproduit", "codeproduit")
        .select(
            "codeproduit",
            "libproduit",
        )
        .drop_duplicates()
    )

    produit_with_type_contrat = (
        produit.withColumnRenamed("libproduit", "libproduit_")
        .select(
            "typecontrat",
            "libproduit_",
        )
        .drop_duplicates()
    )

    contrat = contrat.select(
        "numerocontratinterne",
        "dtmodif",
    )

    return (
        contrat_produit.join(produit_with_codeproduit, ["codeproduit"], "left")
        .join(
            produit_with_type_contrat,
            contrat_produit["codeproduit"]
            == produit_with_type_contrat["typecontrat"],
        )
        .join(contrat, ["numerocontratinterne"], "left")
        .select(
            "numerodecompte",
            "numerocontratinterne",
            "numerocontratexterne",
            "datesouscription",
            "codeproduit",
            "numeropackage",
            coalesce(
                produit_with_codeproduit["libproduit"],
                produit_with_type_contrat["libproduit_"],
            ).alias("libproduit"),
            "dateecheance",
            "etatproduit",
            "dtmodif",
        )
    )
