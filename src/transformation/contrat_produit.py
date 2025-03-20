from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_replace, substring, upper, when


def build_contrat_produit(
    sous_compte: DataFrame,
    fonctionne_avec: DataFrame,
    elt_contrat: DataFrame,
    contrat: DataFrame,
    titulaire: DataFrame,
    type_contrat: DataFrame,
    type_elt_contrat: DataFrame,
    compte: DataFrame,
) -> DataFrame:
    """
    Construit une vue consolidée des contrats produits et comptes associés.

    Cette fonction combine plusieurs tables pour créer une vue globale des contrats produits
    associés aux clients, en incluant les comptes, les éléments spécifiques des contrats,
    et les informations des titulaires.

    Utilisation métier:
    - Suivi des contrats produits par client
    - Analyse et reporting des informations contractuelles
    - Consolidation des données contractuelles et comptes pour des tableaux de bord
    """

    # Préparation des données sources
    sous_compte = sous_compte.select(
        "numeroelementinterne",
        "numerodecompte",
    )
    fonctionne_avec = fonctionne_avec.select(
        "elementcontrat1",
        "elementcontrat2",
    )

    elt_contrat = elt_contrat.select(
        "numeroelementinterne",
        "numerocontratinterne",
        "ordre",
        "typedeproduit",
        "partiespecifique",
    )

    contrat = contrat.select(
        "numerocontratinterne",
        "typecontrat",
        "etatcontrat",
        "numerocontratexterne",
        "datesouscription",
        "dateecheance",
        "numeropackage",
        "etatpackage",
    )

    titulaire = titulaire.select(
        "numerotiers", "titulaireprincipal", "numerocontratinterne"
    )

    type_contrat = type_contrat.filter(col("dfval") > col("partitiondate")).select(
        "typecontrat"
    )

    type_elt_contrat = type_elt_contrat.filter(
        col("dfval") > col("partitiondate")
    ).select("typedeproduit")

    # Filtrage et transformation des comptes
    compte = (
        compte.filter(col("typecompte") == lit("origine"))
        .filter(substring(col("numerodecompte"), 4, 3).isin("010", "014"))
        .withColumn(
            "codeproduit",
            when(substring(col("numerodecompte"), 4, 3) == "010", "CSC1").when(
                substring(col("numerodecompte"), 4, 3) == "014", "CSC2"
            ),
        )
        .withColumn("datesouscription", lit(None).cast("string"))
        .withColumn("dateecheance", lit(None).cast("string"))
        .withColumnRenamed("code_etatcompte", "etatproduit")
        .withColumn("numerocarte", col("numerodecompte"))
        .withColumn("numeropackage", lit(None).cast("string"))
        .withColumn("numerocontratinterne", lit(None).cast("string"))
        .select(
            "numerotiers",
            "numerodecompte",
            "etatproduit",
            "numerocontratinterne",
            "numerocarte",
            "datesouscription",
            "dateecheance",
            "codeproduit",
            "numeropackage",
        )
    )

    # Construction des bases de contrats et comptes
    base = (
        sous_compte.join(
            fonctionne_avec,
            sous_compte["numeroelementinterne"]
            == fonctionne_avec["elementcontrat1"],
        )
        .join(
            elt_contrat,
            fonctionne_avec["elementcontrat2"]
            == elt_contrat["numeroelementinterne"],
        )
        .join(contrat, ["numerocontratinterne"])
        .join(titulaire, ["numerocontratinterne"])
        .filter((titulaire["titulaireprincipal"] == 1))
        .select(
            "numerotiers",
            "numerodecompte",
            "numerocontratinterne",
            "typecontrat",
            "numerocontratexterne",
            "partiespecifique",
            "ordre",
            "etatcontrat",
            "numeropackage",
            "datesouscription",
            "dateecheance",
            "typedeproduit",
        )
        .drop_duplicates()
    )

    # Transformation et filtrage des contrats
    contracts = base.join(type_contrat, ["typecontrat"])

    contracts = (
        contracts.filter(~col("typecontrat").isin("0001", "0003", "0005"))
        .filter(col("ordre") == 1)
        .withColumnRenamed("etatcontrat", "etatproduit")
        .withColumnRenamed("numerocontratexterne", "numerocarte")
        .withColumnRenamed("typecontrat", "codeproduit")
        .select(
            "numerotiers",
            "numerodecompte",
            "etatproduit",
            "numerocontratinterne",
            "numerocarte",
            "datesouscription",
            "codeproduit",
            "numeropackage",
            "dateecheance",
        )
    )

    # Transformation des éléments spécifiques des contrats
    contract_elements = base.join(type_elt_contrat, ["typedeproduit"])

    contract_elements = (
        contract_elements.filter(col("typecontrat").isin("0001", "0003", "0005"))
        .withColumnRenamed("etatcontrat", "etatproduit")
        .withColumn(
            "codeproduit",
            when(
                col("typedeproduit") == "0013",
                when(substring(col("partiespecifique"), 40, 4) == "0606", "9913")
                .when(substring(col("partiespecifique"), 40, 4) == "1010", "8813")
                .otherwise("0013"),
            ).otherwise(col("typedeproduit")),
        )
        .withColumn("numerocarte", substring(col("numerocontratexterne"), 1, 19))
        .select(
            "numerotiers",
            "numerodecompte",
            "etatproduit",
            "numerocontratinterne",
            "numerocarte",
            "datesouscription",
            "dateecheance",
            "codeproduit",
            "numeropackage",
        )
    )

    # Union des résultats pour construire la vue finale
    return (
        contracts.unionByName(contract_elements)
        .unionByName(compte)
        .drop_duplicates()
    )


def build_etat_contrat(etat_contrat: DataFrame) -> DataFrame:
    """
    Prépare les données des états des contrats pour une meilleure lisibilité et exploitation.

    Cette fonction standardise les colonnes des états des contrats et génère une nouvelle colonne
    formatée pour faciliter les analyses.

    Utilisation:
    - Normalisation des libellés des états pour des rapports clairs
    - Gestion simplifiée des états dans les bases de données
    """

    return (
        etat_contrat.withColumnRenamed("code", "code_etat_contrat")
        .withColumnRenamed("libelle", "libelle_etat_contrat")
        .withColumn(
            "etat_contrat", regexp_replace(upper("valide_marches"), r"\s", "_")
        )
        .select(
            "code_etat_contrat",
            "libelle_etat_contrat",
            "etat_contrat",
        )
    )
