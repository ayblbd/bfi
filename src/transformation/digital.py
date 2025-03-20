from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    count_distinct,
    lit,
    max,
    month,
    substring,
    to_date,
    weekofyear,
    when,
    year,
)


def select_information_tier(information_tier: DataFrame) -> DataFrame:
    """
    Sélectionne les informations relatives aux tiers.

    Cette fonction extrait les colonnes principales concernant les tiers, telles que le numéro de tiers,
    le code gestionnaire, l'agence gestionnaire, le statut client, et le marché. Elle élimine également
    les doublons pour garantir l'unicité des informations.
    """
    return information_tier.select(
        "numerotiers", "cd_fdc_gest", "agencegest", "statutclient", "marche"
    ).drop_duplicates()


def select_niveau_regroupement(niveau_regroupement: DataFrame) -> DataFrame:
    """
    Sélectionne les informations de niveau de regroupement.

    Cette fonction extrait les colonnes pertinentes liées aux niveaux de regroupement,
    telles que les codes d'agence, de région et de banque, ainsi que les noms des régions et des banques,
    tout en éliminant les doublons.
    """
    return niveau_regroupement.select(
        "codeagence",
        "coderegion",
        "codebanque",
        "nomregion",
        "nombanque",
    ).drop_duplicates()


def select_relation_commerciale(relation_commerciale: DataFrame) -> DataFrame:
    """
    Sélectionne les relations commerciales.

    Cette fonction renomme certaines colonnes des relations commerciales, ajoute un statut par défaut
    pour les relations commerciales, et filtre les colonnes nécessaires tout en éliminant les doublons.
    """
    return (
        relation_commerciale.withColumnRenamed("identifiantrc", "numerotiers")
        .withColumnRenamed("id", "relation_commerciale_id")
        .withColumn("statut_relation_commerciale", lit(True))
        .select(
            "numerotiers",
            "relation_commerciale_id",
            "statut_relation_commerciale",
        )
        .drop_duplicates()
    )


def select_audit_log_event(audit_log_event: DataFrame) -> DataFrame:
    """
    Sélectionne les événements des journaux d'audit.

    Cette fonction extrait les colonnes principales des journaux d'audit liées aux événements,
    comme l'acteur, la date de création, le type d'événement et la date de partition,
    tout en supprimant les doublons.
    """
    return audit_log_event.select(
        "actor", "datecreated", "eventname", "partitiondate"
    ).drop_duplicates()


def aggregate_audit_log_event(audit_log_event: DataFrame) -> DataFrame:
    """
    Agrège les événements des journaux d'audit.

    Cette fonction filtre les événements liés aux connexions réussies ("ConnexionOK") et calcule
    les métriques suivantes par acteur et date de partition : la dernière connexion, le nombre
    de connexions par jour, par semaine et par mois.
    """
    filtered_events = select_audit_log_event(audit_log_event).filter(
        col("eventname") == "ConnexionOK"
    )

    return filtered_events.groupBy("actor", "partitiondate").agg(
        max("datecreated").alias("derniere_connexion"),
        count_distinct(
            when(
                to_date(col("datecreated")) == col("partitiondate"),
                col("datecreated"),
            )
        ).alias("nb_connexion_jour"),
        count_distinct(
            when(
                (weekofyear(col("datecreated")) == weekofyear(col("partitiondate")))
                & (year(col("datecreated")) == year(col("partitiondate"))),
                col("datecreated"),
            )
        ).alias("nb_connexion_semaine"),
        count_distinct(
            when(
                (month(col("datecreated")) == month(col("partitiondate")))
                & (year(col("datecreated")) == year(col("partitiondate"))),
                col("datecreated"),
            )
        ).alias("nb_connexion_mois"),
    )


def join_audit_log_event(
    audit_log_event: DataFrame,
    utilisateur: DataFrame,
    rattachement_abonne_contrat: DataFrame,
    contrat_abonnement: DataFrame,
    relation_commerciale: DataFrame,
) -> DataFrame:
    """
    Joint les événements des journaux d'audit avec les informations utilisateur et contractuelles.

    Cette fonction combine les données des journaux d'audit, des utilisateurs, des rattachements
    des abonnés, des contrats d'abonnement et des relations commerciales pour produire un DataFrame
    contenant des indicateurs agrégés des connexions par tiers.

    Arguments :
        audit_log_event : DataFrame contenant les journaux d'audit.
        utilisateur : DataFrame contenant les informations des utilisateurs.
        rattachement_abonne_contrat : DataFrame reliant les abonnés aux contrats.
        contrat_abonnement : DataFrame contenant les contrats d'abonnement.
        relation_commerciale : DataFrame contenant les relations commerciales.

    Retourne :
        Un DataFrame avec les informations consolidées des connexions des clients.
    """
    relation_commerciale = relation_commerciale.select(
        "numerotiers", "relation_commerciale_id"
    )

    log_results = aggregate_audit_log_event(audit_log_event)

    utilisateur = utilisateur.withColumnRenamed("id", "abonne_id").select(
        "abonne_id", "username"
    )

    rattachement_abonne_contrat = rattachement_abonne_contrat.select(
        "abonne_id", "contrat_id"
    )

    contrat_abonnement = contrat_abonnement.withColumnRenamed(
        "id", "contrat_id"
    ).select("contrat_id", "relation_commerciale_id")

    return (
        log_results.join(
            utilisateur, utilisateur["username"] == log_results["actor"]
        )
        .join(rattachement_abonne_contrat, ["abonne_id"])
        .join(contrat_abonnement, ["contrat_id"])
        .join(relation_commerciale, ["relation_commerciale_id"])
        .select(
            "actor",
            "numerotiers",
            "derniere_connexion",
            "nb_connexion_jour",
            "nb_connexion_semaine",
            "nb_connexion_mois",
            "partitiondate",
        )
    )


def build_client_e_corpo(
    contrat_produit: DataFrame,
    elt_contrat: DataFrame,
    relation_commerciale: DataFrame,
) -> DataFrame:
    """
    Construit les données clients pour Ecorpo.

    Cette fonction filtre et enrichit les données des contrats et des relations commerciales
    pour identifier les clients e-corpo et leurs statuts en fonction des produits et des options
    associés.

    Arguments :
        contrat_produit : DataFrame contenant les données des contrats produits.
        elt_contrat : DataFrame contenant les éléments des contrats.
        relation_commerciale : DataFrame contenant les relations commerciales.

    Retourne :
        Un DataFrame contenant les informations des clients e-corpo avec leurs statuts et options.
    """
    elt_contrat = elt_contrat.select(
        "numerocontratinterne",
        "partiespecifique",
    ).drop_duplicates()

    contrat_produit = (
        contrat_produit.filter(col("codeproduit").isin("EBAY", "CN01"))
        .select(
            "numerotiers",
            "codeproduit",
            "etatproduit",
            "numerocontratinterne",
            "datesouscription",
        )
        .drop_duplicates()
    )

    relation_commerciale = relation_commerciale.select(
        "numerotiers",
        "statut_relation_commerciale",
    )

    is_ecorpo = (
        (col("codeproduit").isNotNull())
        & (col("codeproduit") == lit("CN01"))
        & (col("option").isNotNull())
        & (col("option").isin("7", "8"))
    )

    is_ebay = (col("codeproduit").isNotNull()) & (col("codeproduit") == lit("EBAY"))

    is_relation_commerciale = (
        (col("statut_relation_commerciale")) & (~is_ebay) & (~is_ecorpo)
    )

    return (
        contrat_produit.join(relation_commerciale, ["numerotiers"], "full")
        .join(elt_contrat, ["numerocontratinterne"], "left")
        .withColumn("option", substring(col("partiespecifique"), 143, 1))
        .filter(is_ebay | is_ecorpo | is_relation_commerciale)
        .withColumn("statut_e_corpo", lit(True))
        .select(
            "numerotiers",
            "etatproduit",
            "datesouscription",
            "statut_e_corpo",
            "option",
        )
        .replace("", None, subset=["option"])
        .drop_duplicates()
    )


def build_digital_e_corpo(
    contrat_produit: DataFrame,
    information_tier: DataFrame,
    niveau_regroupement: DataFrame,
    elt_contrat: DataFrame,
    audit_log_event: DataFrame,
    relation_commerciale: DataFrame,
    contrat_abonnement: DataFrame,
    rattachement_abonne_contrat: DataFrame,
    utilisateur: DataFrame,
) -> DataFrame:
    """
    Construit les données digitales pour Ecorpo.

    Cette fonction combine les données des tiers, des niveaux de regroupement, des contrats,
    des journaux d'audit et des relations commerciales pour produire un DataFrame complet
    avec les indicateurs digitaux et e-corpo.

    Arguments :
        contrat_produit : DataFrame contenant les données des contrats produits.
        information_tier : DataFrame contenant les informations des tiers.
        niveau_regroupement : DataFrame contenant les niveaux de regroupement.
        elt_contrat : DataFrame contenant les éléments des contrats.
        audit_log_event : DataFrame contenant les journaux d'audit.
        relation_commerciale : DataFrame contenant les relations commerciales.
        contrat_abonnement : DataFrame contenant les contrats d'abonnement.
        rattachement_abonne_contrat : DataFrame reliant les abonnés aux contrats.
        utilisateur : DataFrame contenant les informations des utilisateurs.

    Retourne :
        Un DataFrame avec les indicateurs digitaux et les statuts e-corpo des clients.
    """
    information_tier = select_information_tier(information_tier)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    relation_commerciale = select_relation_commerciale(relation_commerciale)

    joined_log = join_audit_log_event(
        audit_log_event,
        utilisateur,
        rattachement_abonne_contrat,
        contrat_abonnement,
        relation_commerciale,
    )

    client_e_corpo = build_client_e_corpo(
        contrat_produit, elt_contrat, relation_commerciale
    )

    filtered_contrat_produit = (
        contrat_produit.filter(col("codeproduit").isin("EBAY", "CN01"))
        .withColumn("statut_digital", lit(True))
        .select(
            "numerotiers",
            "statut_digital",
        )
        .drop_duplicates()
    )

    return (
        information_tier.join(
            niveau_regroupement,
            information_tier["agencegest"] == niveau_regroupement["codeagence"],
            "left",
        )
        .join(client_e_corpo, ["numerotiers"], "left")
        .join(joined_log, ["numerotiers"], "left")
        .join(filtered_contrat_produit, ["numerotiers"], "left")
        .withColumn(
            "libelle_option",
            when(col("option").isin("1", "3"), lit("Consultation"))
            .when(col("option").isin("7", "8"), lit("Transactionnel Ecorpo"))
            .when(col("option").isin("4", "5", "6"), lit("Transactionnel MY CDM")),
        )
        .withColumn("is_e_corpo", coalesce(col("statut_e_corpo"), lit(False)))
        .withColumn(
            "is_digital",
            coalesce(col("statut_digital") | col("statut_e_corpo"), lit(False)),
        )
        .withColumn("date_souscription", col("datesouscription"))
        .withColumn(
            "is_transactionnel", col("option").isin("7", "8").cast("integer")
        )
        .withColumn("is_consultation", col("option").isin("1", "3").cast("integer"))
        .withColumn("etat_contrat", col("etatproduit"))
        .select(
            "numerotiers",
            "cd_fdc_gest",
            "agencegest",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "statutclient",
            "option",
            "libelle_option",
            "is_digital",
            "is_e_corpo",
            "date_souscription",
            "is_transactionnel",
            "is_consultation",
            "etat_contrat",
            "actor",
            "derniere_connexion",
            "nb_connexion_jour",
            "nb_connexion_semaine",
            "nb_connexion_mois",
            "partitiondate",
        )
        .fillna(
            0,
            subset=[
                "is_transactionnel",
                "is_consultation",
                "nb_connexion_jour",
                "nb_connexion_semaine",
                "nb_connexion_mois",
            ],
        )
    )


def select_audit(audit: DataFrame) -> DataFrame:
    """
    Sélectionne et renomme les colonnes principales des journaux d'audit.

    Cette fonction extrait et renomme les colonnes principales des journaux d'audit
    pour une meilleure lisibilité.
    """
    return audit.withColumnRenamed("id_tiers", "numerotiers").select(
        "numerotiers", "utilisateur", "date", "action", "partitiondate"
    )


def aggregate_audit(audit: DataFrame) -> DataFrame:
    """
    Agrège les données des journaux d'audit.

    Cette fonction filtre les événements d'authentification et calcule des indicateurs
    agrégés comme la dernière connexion, les connexions journalières, hebdomadaires et mensuelles.
    """
    filtered_events = select_audit(audit).filter(
        col("action") == "Authentification d'un client"
    )

    return filtered_events.groupBy(
        "numerotiers", "utilisateur", "partitiondate"
    ).agg(
        max("date").alias("derniere_connexion"),
        count_distinct(
            when(
                to_date(col("date")) == col("partitiondate"),
                col("date"),
            )
        ).alias("nb_connexion_jour"),
        count_distinct(
            when(
                (weekofyear(col("date")) == weekofyear(col("partitiondate")))
                & (year(col("date")) == year(col("partitiondate"))),
                col("date"),
            )
        ).alias("nb_connexion_semaine"),
        count_distinct(
            when(
                (month(col("date")) == month(col("partitiondate")))
                & (year(col("date")) == year(col("partitiondate"))),
                col("date"),
            )
        ).alias("nb_connexion_mois"),
    )


def build_digital_my_cdm_web(
    contrat_produit: DataFrame,
    information_tier: DataFrame,
    niveau_regroupement: DataFrame,
    elt_contrat: DataFrame,
    audit: DataFrame,
) -> DataFrame:
    """
    Construit les données digitales pour le portail MY CDM Web.

    Cette fonction combine les données des tiers, des niveaux de regroupement, des contrats,
    des journaux d'audit et des éléments contractuels pour produire un DataFrame complet
    avec les indicateurs digitaux MY CDM Web.

    Arguments :
        contrat_produit : DataFrame contenant les données des contrats produits.
        information_tier : DataFrame contenant les informations des tiers.
        niveau_regroupement : DataFrame contenant les niveaux de regroupement.
        elt_contrat : DataFrame contenant les éléments des contrats.
        audit : DataFrame contenant les journaux d'audit.

    Retourne :
        Un DataFrame avec les indicateurs digitaux pour MY CDM Web.
    """
    information_tier = select_information_tier(information_tier)

    niveau_regroupement = select_niveau_regroupement(niveau_regroupement)

    joined_log = aggregate_audit(audit)

    contrat_produit = (
        contrat_produit.filter(col("codeproduit").isin("EBAY", "CN01"))
        .withColumn("statut_digital", lit(True))
        .select(
            "numerotiers",
            "statut_digital",
            "numerocontratinterne",
            "etatproduit",
            "datesouscription",
        )
        .drop_duplicates()
    )

    elt_contrat = elt_contrat.select(
        "numerocontratinterne",
        "partiespecifique",
    ).drop_duplicates()

    return (
        information_tier.join(
            niveau_regroupement,
            information_tier["agencegest"] == niveau_regroupement["codeagence"],
            "left",
        )
        .join(joined_log, ["numerotiers"], "left")
        .join(contrat_produit, ["numerotiers"], "left")
        .join(elt_contrat, ["numerocontratinterne"], "left")
        .withColumn("option", substring(col("partiespecifique"), 143, 1))
        .withColumn(
            "libelle_option",
            when(col("option").isin("1", "3"), lit("Consultation"))
            .when(col("option").isin("7", "8"), lit("Transactionnel Ecorpo"))
            .when(col("option").isin("4", "5", "6"), lit("Transactionnel MY CDM")),
        )
        .withColumn("is_my_cdm_web", coalesce(col("statut_digital"), lit(False)))
        .withColumn("is_digital", coalesce(col("statut_digital"), lit(False)))
        .withColumn("date_souscription", col("datesouscription"))
        .withColumn(
            "is_transactionnel", col("option").isin("4", "5", "6").cast("integer")
        )
        .withColumn("is_consultation", col("option").isin("1", "3").cast("integer"))
        .withColumn("etat_contrat", col("etatproduit"))
        .select(
            "numerotiers",
            "cd_fdc_gest",
            "agencegest",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "statutclient",
            "option",
            "libelle_option",
            "is_digital",
            "is_my_cdm_web",
            "date_souscription",
            "is_transactionnel",
            "is_consultation",
            "etat_contrat",
            "utilisateur",
            "derniere_connexion",
            "nb_connexion_jour",
            "nb_connexion_semaine",
            "nb_connexion_mois",
            "partitiondate",
        )
        .drop_duplicates()
        .fillna(
            0,
            subset=[
                "is_transactionnel",
                "is_consultation",
                "nb_connexion_jour",
                "nb_connexion_semaine",
                "nb_connexion_mois",
            ],
        )
        .replace("", None, subset=["option"])
    )
