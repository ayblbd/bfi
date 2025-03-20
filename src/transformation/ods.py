from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    lit,
    substring,
    to_date,
    when,
)


def build_information_compte_tenu_par_client(
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
    ref_devise: DataFrame,
) -> DataFrame:
    """
    Construit un tableau récapitulatif des informations liées aux comptes tenus par client.

    Cette fonction permet de rassembler, enrichir et transformer plusieurs sources de données
    liées aux comptes commerciaux, clients, contrats, et fonds de commerce.
    Elle produit une vue consolidée des comptes avec les informations pertinentes pour
    le suivi et la gestion des clients.

    Étapes principales :
    1. Joindre et enrichir les données des comptes commerciaux avec leurs caractéristiques principales.
    2. Intégrer les informations liées aux sous-comptes, contrats, et clients pour contextualiser chaque compte.
    3. Ajouter les états de gestion et opération des fonds de commerce pour fournir une vue complète.
    4. Filtrer et formater les données pour produire un tableau final prêt à l'analyse.

    """

    cpte_tenu_par_client = (
        cpte_tenu_par_client.join(
            compte_commercial,
            cpte_tenu_par_client["numerocompteorigine"]
            == compte_commercial["numerodecompte"],
            "left",
        )
        .select(
            cpte_tenu_par_client["numerocompteorigine"],
            cpte_tenu_par_client["numerodecompte"],
            cpte_tenu_par_client["devisecompteorigine"],
            compte_commercial["naturecompte"],
            compte_commercial["intitule"],
            compte_commercial["representantetranger"],
        )
        .withColumn(
            "devisecompteorigine",
            when(col("devisecompteorigine") == lit("000"), lit("MAD")).otherwise(
                col("devisecompteorigine")
            ),
        )
        .drop_duplicates()
    )

    compte_commercial = compte_commercial.select(
        "numerodecompte",
        "intitule",
        "naturecompte",
    )

    sous_compte = sous_compte.select(
        "numeroelementinterne",
        "etatcompte",
        "numerodecompte",
    )

    elt_contrat = elt_contrat.select(
        "numeroelementinterne",
        "numerocontratinterne",
        "typedeproduit",
        "dateeffet",
        "dateecheance",
    )

    titulaire = titulaire.select(
        "numerocontratinterne",
        "titulaireprincipal",
        "numerotiers",
    )

    client = client.select(
        "numerotiers",
        "devise",
    )

    contrat = contrat.select(
        "numerocontratinterne",
    )

    ref_devise = ref_devise.withColumnRenamed("cd_dev_oper", "devise").select(
        "cd_devise", "devise"
    )

    compte_tenu_par_client = (
        cpte_tenu_par_client.join(
            sous_compte,
            cpte_tenu_par_client["numerocompteorigine"]
            == sous_compte["numerodecompte"],
        )
        .join(elt_contrat, ["numeroelementinterne"])
        .join(titulaire, ["numerocontratinterne"])
        .join(contrat, ["numerocontratinterne"])
        .join(client, ["numerotiers"])
        .filter(
            (titulaire["titulaireprincipal"] == 1)
            & (elt_contrat["typedeproduit"] == "CA03")
        )
        .withColumn(
            "etat_compte",
            when(sous_compte["etatcompte"] == lit("0"), "OUVERT")
            .when(sous_compte["etatcompte"] == lit("1"), "OUVERT")
            .when(sous_compte["etatcompte"] == lit("3"), "CLOTURE")
            .when(sous_compte["etatcompte"] == lit("5"), "ACLORE")
            .otherwise("DEMCLOT"),
        )
        .join(
            ref_devise,
            cpte_tenu_par_client["devisecompteorigine"] == ref_devise["cd_devise"],
            "left",
        )
        .select(
            client["numerotiers"],
            cpte_tenu_par_client["numerodecompte"],
            cpte_tenu_par_client["numerocompteorigine"],
            ref_devise["devise"],
            cpte_tenu_par_client["naturecompte"],
            cpte_tenu_par_client["intitule"],
            cpte_tenu_par_client["representantetranger"],
            "etat_compte",
            elt_contrat["dateeffet"],
            elt_contrat["dateecheance"],
        )
    )

    fdc_compte_gest = fdc_compte.filter(col("typefdccompte") == 2).select(
        "numerodecompte",
        "lieufdccompte",
        "codefondsdecommerce",
    )

    t705_filtered = (
        t705.filter(col("dfval") > col("partitiondate"))
        .withColumn("lieufdccompte", col("rattache"))
        .select(
            "rattache",
            "lieufdccompte",
            "codefondsdecommerce",
            "cdgest",
            "gestionnaire",
            "nomgest",
            "qualiteptf",
        )
    )

    compte_tenu_par_client_with_gest = (
        compte_tenu_par_client.join(
            fdc_compte_gest,
            fdc_compte_gest["numerodecompte"]
            == compte_tenu_par_client["numerocompteorigine"],
            "left",
        )
        .join(
            t705_filtered,
            ["lieufdccompte", "codefondsdecommerce"],
            "left",
        )
        .withColumn("agencegest", col("rattache"))
        .withColumnRenamed("cdgest", "cdfdc_gest")
        .withColumnRenamed("gestionnaire", "gestionnaire_gest")
        .withColumnRenamed("nomgest", "nomgest_gest")
        .select(
            "numerotiers",
            compte_tenu_par_client["numerodecompte"],
            "numerocompteorigine",
            "devise",
            "naturecompte",
            "intitule",
            "representantetranger",
            "etat_compte",
            "dateeffet",
            "dateecheance",
            "agencegest",
            "cdfdc_gest",
            "gestionnaire_gest",
            "nomgest_gest",
            "qualiteptf",
        )
    )

    fdc_compte_op = fdc_compte.filter(col("typefdccompte") == 1).select(
        "numerodecompte",
        "lieufdccompte",
        "codefondsdecommerce",
    )

    compte_tenu_par_client_with_gest_and_op = (
        compte_tenu_par_client_with_gest.join(
            fdc_compte_op,
            fdc_compte_op["numerodecompte"]
            == compte_tenu_par_client_with_gest["numerocompteorigine"],
            "left",
        )
        .join(
            t705_filtered,
            ["lieufdccompte", "codefondsdecommerce"],
            "left",
        )
        .withColumn("agenceop", col("rattache"))
        .withColumnRenamed("cdgest", "cdfdc_op")
        .withColumnRenamed("gestionnaire", "gestionnaire_op")
        .withColumnRenamed("nomgest", "nomgest_op")
        .select(
            "numerotiers",
            compte_tenu_par_client_with_gest["numerodecompte"],
            "numerocompteorigine",
            "devise",
            "naturecompte",
            "intitule",
            "representantetranger",
            "etat_compte",
            "dateeffet",
            "dateecheance",
            "agencegest",
            "cdfdc_gest",
            "gestionnaire_gest",
            "nomgest_gest",
            compte_tenu_par_client_with_gest["qualiteptf"],
            "agenceop",
            "cdfdc_op",
            "gestionnaire_op",
            "nomgest_op",
        )
    )

    return compte_tenu_par_client_with_gest_and_op


def build_information_compte_client(
    compte_commercial: DataFrame,
    client: DataFrame,
    titulaire: DataFrame,
    sous_compte: DataFrame,
    elt_contrat: DataFrame,
    contrat: DataFrame,
    fdc_compte: DataFrame,
    t705: DataFrame,
    ref_devise: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit les informations consolidées des comptes clients.

    Cette fonction regroupe et transforme plusieurs ensembles de données liées aux comptes
    clients, sous-comptes, contrats, fonds de commerce et données de gestion pour produire
    une vue détaillée et structurée des comptes clients.

    Étapes principales :
    1. Sélection et jointure des données essentielles pour les comptes clients.
    2. Ajout des informations sur l'état des comptes et les devises associées.
    3. Enrichissement des données avec les fonds de commerce (gestion et opération).
    4. Formatage des données finales pour inclure les informations clés comme :
    - La nature des comptes, les responsables de gestion, les échéances des contrats.
    - Les agences de gestion et d'opération.
    """

    compte_commercial = compte_commercial.select(
        "numerodecompte",
        "intitule",
        "naturecompte",
    )

    sous_compte = sous_compte.select(
        "numeroelementinterne",
        "etatcompte",
        "numerodecompte",
        "devise",
    )

    elt_contrat = elt_contrat.select(
        "numeroelementinterne",
        "numerocontratinterne",
        "typedeproduit",
        "dateeffet",
    )

    titulaire = titulaire.select(
        "numerocontratinterne",
        "titulaireprincipal",
        "numerotiers",
    )

    client = client.select(
        "numerotiers",
        "devise",
    )

    contrat = contrat.select("numerocontratinterne", "dateecheance")

    ref_devise = ref_devise.withColumnRenamed("cd_dev_oper", "devise").select(
        "cd_devise", "devise"
    )

    compte_client = (
        compte_commercial.join(
            sous_compte,
            ["numerodecompte"],
        )
        .join(elt_contrat, ["numeroelementinterne"])
        .join(
            titulaire,
            ["numerocontratinterne"],
        )
        .join(
            client,
            ["numerotiers"],
        )
        .join(contrat, ["numerocontratinterne"])
        .filter(
            (titulaire["titulaireprincipal"] == 1)
            & (elt_contrat["typedeproduit"] == "CA03")
        )
        .withColumn(
            "etat_compte",
            when(sous_compte["etatcompte"] == lit("0"), "OUVERT")
            .when(sous_compte["etatcompte"] == lit("1"), "OUVERT")
            .when(sous_compte["etatcompte"] == lit("3"), "CLOTURE")
            .when(sous_compte["etatcompte"] == lit("5"), "ACLORE")
            .otherwise("DEMCLOT"),
        )
        .join(ref_devise, sous_compte["devise"] == ref_devise["cd_devise"], "left")
        .select(
            client["numerotiers"],
            compte_commercial["numerodecompte"],
            ref_devise["devise"],
            compte_commercial["intitule"],
            col("etat_compte"),
            elt_contrat["dateeffet"],
            contrat["dateecheance"],
            compte_commercial["naturecompte"],
        )
    )

    fdc_compte_gest = fdc_compte.filter(col("typefdccompte") == 2)

    compte_client_with_fdc = compte_client.join(
        fdc_compte_gest, ["numerodecompte"], "left"
    ).select(
        compte_client["numerotiers"],
        compte_client["numerodecompte"],
        compte_client["devise"],
        compte_client["intitule"],
        compte_client["etat_compte"],
        compte_client["dateeffet"],
        compte_client["dateecheance"],
        compte_client["naturecompte"],
        fdc_compte_gest["lieufdccompte"],
        fdc_compte_gest["codefondsdecommerce"],
    )

    t705_filtered = t705.filter(col("dfval") > col("partitiondate")).withColumn(
        "lieufdccompte", col("rattache")
    )

    compte_client_with_fdc_gest = (
        compte_client_with_fdc.join(
            t705_filtered, ["lieufdccompte", "codefondsdecommerce"], "left"
        )
        .withColumn("agencegest", col("rattache"))
        .withColumnRenamed("cdgest", "cd_fdc_gest")
        .withColumnRenamed("gestionnaire", "gestionnaire_gest")
        .select(
            "numerotiers",
            "numerodecompte",
            "naturecompte",
            "intitule",
            "etat_compte",
            "devise",
            "agencegest",
            "cd_fdc_gest",
            "gestionnaire_gest",
            "dateeffet",
            "dateecheance",
        )
    )

    fdc_compte_op = fdc_compte.filter(col("typefdccompte") == 1)

    compte_client_with_fdc_gest_and_fdc_op = compte_client_with_fdc_gest.join(
        fdc_compte_op, ["numerodecompte"], "left"
    ).select(
        "numerodecompte",
        "numerotiers",
        "naturecompte",
        "intitule",
        "etat_compte",
        "devise",
        "agencegest",
        "cd_fdc_gest",
        "gestionnaire_gest",
        "lieufdccompte",
        "codefondsdecommerce",
        "dateeffet",
        "dateecheance",
    )

    information_compte_client = (
        compte_client_with_fdc_gest_and_fdc_op.join(
            t705_filtered, ["lieufdccompte", "codefondsdecommerce"], "left"
        )
        .withColumn("partitiondate", to_date(lit(partition_date)))
        .withColumn("agenceop", col("rattache"))
        .withColumnRenamed("cdgest", "cd_fdc_op")
        .withColumnRenamed("gestionnaire", "gestionnaire_op")
        .select(
            "numerotiers",
            "numerodecompte",
            "naturecompte",
            "intitule",
            "etat_compte",
            "devise",
            "agencegest",
            "cd_fdc_gest",
            "gestionnaire_gest",
            "agenceop",
            "cd_fdc_op",
            "gestionnaire_op",
            "dateeffet",
            "dateecheance",
            "partitiondate",
        )
    )

    return information_compte_client


def build_niveau_regroupement(
    t608: DataFrame,
    t705: DataFrame,
    utilisateur: DataFrame,
) -> DataFrame:
    """
    Construit le niveau de regroupement des agences, régions et banques.

    Cette fonction consolide et enrichit les données des agences en les regroupant
    par région, banque et succursale tout en intégrant les informations des
    gestionnaires associées.

    Utilisation métier :
    - Suivi hiérarchique des agences dans la structure organisationnelle.
    - Analyse des regroupements par région et banque pour le pilotage stratégique.
    - Identification et association des gestionnaires aux agences pour un suivi précis.

    Étapes principales :
    1. Filtrer les données pour conserver uniquement les enregistrements valides
    après une date de partition donnée.
    2. Regrouper et enrichir les données des agences (par entités et gestionnaires).
    3. Intégrer les informations des régions, banques et succursales pour compléter
    le niveau hiérarchique.
    4. Ajouter les noms des gestionnaires associés à chaque agence.
    """

    t608 = t608.filter(col("dfval") > col("partitiondate"))

    t705 = t705.filter(col("dfval") > col("partitiondate"))

    t608_for_agence_1 = (
        t608.filter(col("typedelieusysen").isin("30", "40"))
        .join(t705, t705["rattache"] == t608["entite"])
        .filter(t705["rattache"].isNotNull())
        .select(
            "rattache",
            "nomentit",
            "grreg",
            "dirzone",
            "cdplateforme",
            "cdgest",
            "codefondsdecommerce",
            "gestionnaire",
            "succursale",
        )
    )

    t608_for_agence_2 = (
        t608.join(t705, t705["rattache"] == t608["entite"])
        .filter(t705["rattache"].isNotNull())
        .filter(t705["rattache"] < "507")
        .filter(~t705["rattache"].isin("480", "485", "486"))
        .select(
            "rattache",
            "nomentit",
            "grreg",
            "dirzone",
            "cdplateforme",
            "cdgest",
            "codefondsdecommerce",
            "gestionnaire",
            "succursale",
        )
    )

    agence = (
        t608_for_agence_1.unionByName(t608_for_agence_2)
        .withColumnRenamed("rattache", "codeagence")
        .withColumnRenamed("nomentit", "nomagence")
        .withColumnRenamed("grreg", "coderegion")
        .withColumnRenamed("dirzone", "codebanque")
        .withColumnRenamed("cdgest", "codefdcencentrale")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "codebanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    t608_with_nomentit = t608.select("entite", "nomentit")

    agence_region = (
        agence.join(
            t608_with_nomentit, agence["coderegion"] == t608_with_nomentit["entite"]
        )
        .withColumnRenamed("nomentit", "nomregion")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    agence_region_banque = (
        agence_region.join(
            t608_with_nomentit,
            agence_region["codebanque"] == t608_with_nomentit["entite"],
        )
        .withColumnRenamed("nomentit", "nombanque")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    agence_region_banque_succ = (
        agence_region_banque.join(
            t608_with_nomentit,
            agence_region_banque["succursale"] == t608_with_nomentit["entite"],
            "left",
        )
        .withColumn("succursale", coalesce(col("succursale"), col("codeagence")))
        .withColumn("nomsuccursale", coalesce(col("nomentit"), col("nomagence")))
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    utilisateur = (
        utilisateur.filter(col("dfval") > col("partitiondate"))
        .withColumnRenamed("identifiant", "gestionnaire")
        .withColumn(
            "nomgestionnaire", concat(col("prenom"), lit(" "), col("nomuti"))
        )
        .select(
            "gestionnaire",
            "nomgestionnaire",
        )
    )

    return agence_region_banque_succ.join(
        utilisateur, ["gestionnaire"], "left"
    ).select(
        "codeagence",
        "nomagence",
        "succursale",
        "nomsuccursale",
        "coderegion",
        "nomregion",
        "codebanque",
        "nombanque",
        "cdplateforme",
        "codefdcencentrale",
        "codefondsdecommerce",
        "gestionnaire",
        "nomgestionnaire",
    )


def build_contrat_produit_carts(
    exploit_fonctionneavec: DataFrame,
    exploit_elt_contrat: DataFrame,
    exploit_souscompte: DataFrame,
    exploit_titulaire: DataFrame,
    contrat: DataFrame,
    type_elt_contrat: DataFrame,
) -> DataFrame:
    """
    Construit une vue des contrats et produits liés aux cartes.

    Cette fonction traite et fusionne plusieurs sources de données pour créer une vue
    détaillée des contrats, produits, et cartes associés aux clients.

    Utilisation métier :
    - Suivi des produits spécifiques liés aux cartes.
    - Gestion des états des produits (valide, résilié, échu, non valide).
    - Création de rapports sur les contrats et produits pour les clients.

    Étapes principales :
    1. Préparation des données :
    - Filtrage des produits d'intérêt selon leur type (par exemple, "0034", "0029").
    - Sélection des colonnes essentielles pour chaque table source.

    2. Enrichissement des données :
    - Association des éléments de contrat avec leurs sous-comptes correspondants.
    - Intégration des informations sur les titulaires et les contrats.

    3. Ajout des informations des produits :
    - Calcul de l'état des produits (valide, résilié, échu, non valide).
    - Enrichissement des données avec les libellés des produits.

    4. Consolidation finale :
    - Création d'un DataFrame contenant les informations consolidées des produits
        et des cartes associées.

    Résultat :
    Un DataFrame contenant :
    - Les informations des produits : type, code produit, état, et libellé.
    - Les informations des contrats : numéro, dates (souscription, échéance).
    - Les cartes associées aux produits et contrats.
    """

    exploit_fonctionneavec = exploit_fonctionneavec.select(
        "elementcontrat2",
        "elementcontrat1",
    )

    exploit_elt_contrat = exploit_elt_contrat.filter(
        col("typedeproduit").isin(["0034", "0029", "0033", "CEN2"])
    ).select(
        "numerocontratinterne",
        "numeroelementinterne",
        "typedeproduit",
    )

    exploit_souscompte = exploit_souscompte.select(
        "numerodecompte",
        "numeroelementinterne",
    )

    exploit_titulaire = exploit_titulaire.select(
        "numerotiers",
        "numerocontratinterne",
    )

    contrat = (
        contrat.withColumnRenamed("numerocontratexterne", "numerocarte")
        .withColumn(
            "etatproduit",
            when(
                col("etatcontrat").isin(["3", "7", "8", "D", "E", "S", "X"]),
                "VALIDE",
            )
            .when(col("etatcontrat") == "5", "RESILIE")
            .when(col("etatcontrat") == "2", "ECHUE")
            .when(col("etatcontrat").isin(["1", "A", "C"]), "NONVALIDE"),
        )
        .select(
            "dateecheance",
            "numerocontratinterne",
            "etatproduit",
            "numerocarte",
            "numeropackage",
            "datesouscription",
        )
    )

    type_elt_contrat = type_elt_contrat.filter(
        (col("dfval") > col("partitiondate"))
        & (col("typedeproduit").isin(["0034", "0029", "0033", "CEN2"]))
    ).select(
        "typedeproduit",
        "libproduit",
    )

    return (
        exploit_elt_contrat.join(
            exploit_fonctionneavec,
            exploit_elt_contrat.numeroelementinterne
            == exploit_fonctionneavec.elementcontrat2,
        )
        .join(
            exploit_souscompte,
            exploit_fonctionneavec.elementcontrat1
            == exploit_souscompte.numeroelementinterne,
        )
        .join(exploit_titulaire, ["numerocontratinterne"])
        .join(contrat, ["numerocontratinterne"])
        .join(type_elt_contrat, ["typedeproduit"])
        .withColumnRenamed("typedeproduit", "codeproduit")
        .select(
            "numerodecompte",
            "numerotiers",
            "numerocontratinterne",
            "etatproduit",
            "codeproduit",
            "numerocarte",
            "numeropackage",
            "datesouscription",
            "dateecheance",
        )
    )


def build_contrat_produit(
    sous_compte: DataFrame,
    fonctionne_avec: DataFrame,
    elt_contrat: DataFrame,
    contrat: DataFrame,
    titulaire: DataFrame,
    type_contrat: DataFrame,
    type_elt_contrat: DataFrame,
    information_compte_client: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue consolidée des contrats et produits.

    Cette fonction rassemble et traite les données contractuelles, produits et comptes
    clients pour produire une vue unique et détaillée des contrats et produits associés.

    Utilisation métier :
    - Analyse et suivi des contrats et produits par client.
    - Gestion des états des produits (valide, résilié, échu).
    - Association des comptes clients spécifiques à des produits et contrats.
    - Création de rapports consolidés pour le pilotage des activités commerciales.

    Étapes principales :
    1. Préparation des données d'entrée :
    - Sélection et renommage des colonnes pertinentes dans chaque table source.
    - Filtrage des données sur des critères spécifiques (types de contrats, états).

    2. Enrichissement des données :
    - Jointure des tables pour relier les sous-comptes, les contrats et les titulaires.
    - Ajout des informations de produits et des états des contrats.

    3. Filtrage des données :
    - Suppression des types de contrats non pertinents.
    - Filtrage des produits et contrats selon leurs états et types.

    4. Consolidation finale :
    - Création de colonnes pour les codes produits et états des produits.
    - Union des différentes sources de données pour produire un tableau unique.

    Résultat :
    Un DataFrame consolidé contenant :
    - Les informations sur les contrats : numéros, dates (souscription, échéance), état.
    - Les produits associés : code produit, état produit, carte, et package.
    - Les données des comptes clients spécifiques, y compris leurs états et produits associés.
    """

    sous_compte = sous_compte.withColumnRenamed(
        "NUMEROELEMENTINTERNE", "ELEMENTCONTRAT1"
    ).select("ELEMENTCONTRAT1", "NUMERODECOMPTE", "partitiondate")

    fonctionne_avec = fonctionne_avec.withColumnRenamed(
        "ELEMENTCONTRAT2", "NUMEROELEMENTINTERNE"
    ).select("ELEMENTCONTRAT1", "NUMEROELEMENTINTERNE", "partitiondate")

    elt_contrat = elt_contrat.select(
        "NUMEROELEMENTINTERNE",
        "NUMEROCONTRATINTERNE",
        "ORDRE",
        "TYPEDEPRODUIT",
        "PARTIESPECIFIQUE",
        "partitiondate",
    )

    contrat = contrat.select(
        "NUMEROCONTRATINTERNE",
        "TYPECONTRAT",
        "ETATCONTRAT",
        "NUMEROCONTRATEXTERNE",
        "DATESOUSCRIPTION",
        "DATEECHEANCE",
        "NUMEROPACKAGE",
        "ETATPACKAGE",
        "partitiondate",
    )

    titulaire = titulaire.select(
        "NUMEROTIERS", "TITULAIREPRINCIPAL", "NUMEROCONTRATINTERNE", "partitiondate"
    )

    type_contrat = type_contrat.select("TYPECONTRAT", "DFVAL", "partitiondate")

    type_elt_contrat = type_elt_contrat.select(
        "TYPEDEPRODUIT", "DFVAL", "partitiondate"
    )

    information_compte_client = (
        information_compte_client.filter(
            substring(col("numerodecompte"), 4, 3).isin("010", "014")
        )
        .withColumn(
            "codeproduit",
            when(substring(col("numerodecompte"), 4, 3) == "010", "CSC1").when(
                substring(col("numerodecompte"), 4, 3) == "014", "CSC2"
            ),
        )
        .withColumnRenamed("dateeffet", "datesouscription")
        .withColumn(
            "etatproduit",
            when(
                substring(col("etat_compte"), 1, 6) == "OUVERT", "VALIDE"
            ).otherwise("RESILIE"),
        )
        .withColumn("numerocarte", lit(None).cast("string"))
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
            "partitiondate",
        )
    )

    df_final = (
        sous_compte.join(
            fonctionne_avec, ["ELEMENTCONTRAT1", "partitiondate"], how="inner"
        )
        .join(elt_contrat, ["NUMEROELEMENTINTERNE", "partitiondate"], how="inner")
        .join(contrat, ["NUMEROCONTRATINTERNE", "partitiondate"], how="inner")
        .join(titulaire, ["NUMEROCONTRATINTERNE", "partitiondate"], how="inner")
        .filter((titulaire["TITULAIREPRINCIPAL"] == 1))
    )

    df_final_1 = df_final.join(
        type_contrat, ["TYPECONTRAT", "partitiondate"], how="inner"
    )

    df_final_1 = (
        df_final_1.filter(
            (to_date(df_final_1.DFVAL) > to_date(lit(partition_date)))
            & (~type_contrat["TYPECONTRAT"].isin(["0001", "0003", "0005"]))
            & (df_final_1.ORDRE == 1)
        )
        .select(
            "NUMEROTIERS",
            "NUMERODECOMPTE",
            "ETATCONTRAT",
            "NUMEROCONTRATINTERNE",
            "NUMEROCONTRATEXTERNE",
            "DATESOUSCRIPTION",
            "TYPECONTRAT",
            "NUMEROPACKAGE",
            "DATEECHEANCE",
            "partitiondate",
        )
        .withColumn(
            "ETATPRODUIT",
            when(
                col("ETATCONTRAT").isin(["3", "7", "8", "D", "E", "S", "X"]),
                "VALIDE",
            )
            .when(col("ETATCONTRAT") == "5", "RESILIE")
            .when(col("ETATCONTRAT") == "2", "ECHUE")
            .when(col("ETATCONTRAT").isin(["1", "A", "C"]), "NONVALIDE"),
        )
        .withColumnRenamed("NUMEROCONTRATEXTERNE", "NUMEROCARTE")
        .withColumnRenamed("TYPECONTRAT", "CODEPRODUIT")
        .filter(col("etatproduit").isNotNull())
        .select(
            "NUMEROTIERS",
            "NUMERODECOMPTE",
            "ETATPRODUIT",
            "NUMEROCONTRATINTERNE",
            "NUMEROCARTE",
            "DATESOUSCRIPTION",
            "CODEPRODUIT",
            "NUMEROPACKAGE",
            "DATEECHEANCE",
            "partitiondate",
        )
    )

    df_final_2 = df_final.join(
        type_elt_contrat, ["TYPEDEPRODUIT", "partitiondate"], how="inner"
    )

    df_final_2 = (
        df_final_2.filter(
            (to_date(df_final_2.DFVAL) > to_date(lit(partition_date)))
            & (
                df_final_2.TYPEDEPRODUIT.isin(
                    [
                        "0001",
                        "0006",
                        "0007",
                        "0008",
                        "0009",
                        "0010",
                        "0011",
                        "0012",
                        "0013",
                        "VIS1",
                        "0014",
                        "SIL1",
                        "CEN1",
                        "TITA",
                        "PLT1",
                        "0027",
                        "TAWF",
                        "TAKH",
                        "0030",
                        "TAMW",
                        "0031",
                        "0032",
                        "CVI1",
                        "PME1",
                    ]
                )
            )
            & (df_final_2.ETATCONTRAT.isin(["3", "5", "2", "7", "8", "D", "E", "S"]))
        )
        .select(
            "NUMEROTIERS",
            "NUMERODECOMPTE",
            "ETATCONTRAT",
            "NUMEROCONTRATINTERNE",
            "NUMEROCONTRATEXTERNE",
            "DATESOUSCRIPTION",
            "TYPEDEPRODUIT",
            "PARTIESPECIFIQUE",
            "DATEECHEANCE",
            "NUMEROPACKAGE",
            "partitiondate",
        )
        .withColumn(
            "ETATPRODUIT",
            when(col("ETATCONTRAT").isin(["3", "7", "8", "D", "E", "S"]), "VALIDE")
            .when(col("ETATCONTRAT") == "5", "RESILIE")
            .when(col("ETATCONTRAT") == "2", "ECHUE")
            .when(
                (
                    col("TYPEDEPRODUIT").isin(
                        [
                            "0006",
                            "0007",
                            "0008",
                            "0009",
                            "0010",
                            "0011",
                            "0012",
                            "0013",
                            "VIS1",
                            "0014",
                            "SIL1",
                            "CEN1",
                            "TITA",
                            "PLT1",
                            "0027",
                            "TAWF",
                            "TAKH",
                            "0030",
                            "TAMW",
                        ]
                    )
                )
                & (to_date(col("DATEECHEANCE")) <= to_date(lit(partition_date))),
                "ECHUE",
            )
            .otherwise(""),
        )
        .withColumn(
            "CODEPRODUIT",
            when(
                col("TYPEDEPRODUIT") == "0013",
                when(substring(col("PARTIESPECIFIQUE"), 40, 4) == "0606", "9913")
                .when(substring(col("PARTIESPECIFIQUE"), 40, 4) == "1010", "8813")
                .otherwise("0013"),
            ).otherwise(col("TYPEDEPRODUIT")),
        )
        .withColumn("NUMEROCARTE", substring(col("NUMEROCONTRATEXTERNE"), 1, 19))
        .select(
            "NUMEROTIERS",
            "NUMERODECOMPTE",
            "ETATPRODUIT",
            "NUMEROCONTRATINTERNE",
            "NUMEROCARTE",
            "DATESOUSCRIPTION",
            "DATEECHEANCE",
            "CODEPRODUIT",
            "NUMEROPACKAGE",
            "partitiondate",
        )
    )

    df_final = (
        df_final_1.unionByName(df_final_2)
        .unionByName(information_compte_client)
        .distinct()
    )

    return df_final


def build_contrat_produit_with_carts(
    exploit_souscompte: DataFrame,
    exploit_fonctionneavec: DataFrame,
    exploit_elt_contrat: DataFrame,
    contrat: DataFrame,
    exploit_titulaire: DataFrame,
    type_contrat: DataFrame,
    type_elt_contrat: DataFrame,
    information_compte_client: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue consolidée des contrats et produits, incluant les cartes associées.

    Ces fonctions rassemblent, enrichissent et filtrent des données contractuelles,
    produits et comptes pour produire une vue complète et uniforme des contrats
    et des produits, en y intégrant les informations sur les cartes associées.

    Utilisation métier :
    - Analyse et suivi des contrats et produits des clients.
    - Gestion des états des produits (valide, résilié, échu, non valide).
    - Identification et traitement des cartes et packages associés aux contrats.
    - Consolidation des informations pour les rapports clients et audits.

    Étapes principales :
    1. **build_contrat_produit_carts** :
    - Traite les données relatives aux cartes et produits pour créer une vue spécifique.
    - Filtre les types de produits et les états des contrats pertinents.
    - Associe les informations des cartes, produits et comptes.

    2. **build_contrat_produit** :
    - Rassemble les données des sous-comptes, des contrats et des titulaires.
    - Filtre les données par type de produit, contrat, et état.
    - Ajoute les informations des produits et des états (valide, résilié, etc.).
    - Intègre les comptes clients avec des règles spécifiques (produits CSC1, CSC2).

    3. **build_contrat_produit_with_carts** :
    - Combine les résultats de `build_contrat_produit_carts` et `build_contrat_produit`.
    - Produit une vue globale contenant les informations sur les contrats, produits
        et cartes associées.

    Résultat :
    Un DataFrame consolidé contenant :
    - Les informations des contrats : numéro, dates (souscription, échéance), état.
    - Les informations des produits : code produit, état produit.
    - Les cartes et packages associés.
    - Les données des comptes clients associées pour une vue complète.
    """

    contrat_produit_carts = build_contrat_produit_carts(
        exploit_fonctionneavec,
        exploit_elt_contrat,
        exploit_souscompte,
        exploit_titulaire,
        contrat,
        type_elt_contrat,
    )

    contrat_produit = build_contrat_produit(
        exploit_souscompte,
        exploit_fonctionneavec,
        exploit_elt_contrat,
        contrat,
        exploit_titulaire,
        type_contrat,
        type_elt_contrat,
        information_compte_client,
        partition_date,
    )
    contrat_produit = contrat_produit.drop("partitiondate")
    return contrat_produit.unionByName(contrat_produit_carts)
