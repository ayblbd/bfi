from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    lit,
    max,
    months_between,
    sum,
    to_date,
    trim,
    when,
    year,
)


def base_de_calcul(
    tier: DataFrame,
    marche: DataFrame,
) -> DataFrame:
    """
    Préparer une base de calcul contenant des informations sur les clients, leur marché,
    et la date d'ouverture de leur premier compte.
    """

    marche = marche.select("marche", "code_marche")
    return tier.join(marche, ["code_marche"], how="left").select(
        "numerotiers",
        "marche",
        "date_ouverture_premier_compte",
        "partitiondate",
    )


def select_ressource_emploi(
    ressource_emploi: DataFrame,
) -> DataFrame:
    """
    Extraire les colonnes nécessaires liées aux ressources et emplois.

    Colonnes sélectionnées :
    - `numerodecompte` : Numéro du compte.
    - `soldejourcr_devise` : Solde créditeur en dh.
    - `partitiondate` : Date de partition.
    - `fk_compte` : Clé étrangère du compte.
    """
    return ressource_emploi.select(
        "numerodecompte",
        "soldejourcr_devise",
        "partitiondate",
        "fk_compte",
    )


def select_eer(
    mvt_comptable: DataFrame,
    taux_change_bam: DataFrame,
) -> DataFrame:
    """
    Calculer les flux créditeurs pour chaque compte à partir des mouvements comptables
    et des taux de change BAM.

    Étapes :
    1. Préparer les colonnes de `taux_change_bam` :
       - Renommer et sélectionner : `devise`, `midbam`, `datetraitement`.
    2. Filtrer et nettoyer `mvt_comptable` :
       - Filtrer uniquement les flux créditeurs (`sens == "C"`).
       - Sélectionner les colonnes nécessaires.
    3. Effectuer une jointure entre les mouvements comptables et les taux de change.
    4. Calculer :
       - `montant_convert` : Montant converti en fonction des taux de change.
       - Agréger les données pour obtenir la somme des montants par compte.
    """

    taux_change_bam = (
        taux_change_bam.withColumn("devise", trim(col("cd_dev_oper")))
        .withColumnRenamed("partitiondate", "datetraitement")
        .select(
            "devise",
            "midbam",
            "datetraitement",
        )
    )

    mvt_comptable = (
        mvt_comptable.withColumn("devise", trim("devise"))
        .filter(col("sens") == "C")
        .withColumn("datetraitement", to_date("datetraitement"))
        .select("numerodecompte", "montantecriture", "datetraitement", "devise")
    )

    return (
        mvt_comptable.join(
            taux_change_bam,
            ["datetraitement", "devise"],
            "left",
        )
        .withColumn(
            "midbam", when(col("devise") == "000C", 1).otherwise(col("midbam"))
        )
        .withColumn(
            "montant_convert",
            coalesce((col("montantecriture") * col("midbam")) / 100, lit(0)),
        )
        .groupBy("numerodecompte")
        .agg(
            sum("montant_convert").alias("sum_montant_ecriture"),
        )
        .select("numerodecompte", "sum_montant_ecriture")
        .drop_duplicates()
    )


def eer_en_cour_numerotiers(
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    tiers: DataFrame,
    compte: DataFrame,
    marche: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Identifier les clients dans un état d'Entrée en Relation (EER) en fonction de leur
    historique de comptes et de flux financiers.

    Étapes :
    1. Filtrer les données pour les segments de marché `RETAIL` et `PRO`.
    2. Définir la période d'EER comme étant inférieure ou égale à 36 mois depuis
       l'ouverture du premier compte.
    3. Agréger les données par `numerotiers` pour déterminer :
       - Si le client est en EER (`is_entrer_en_relation_date`).
       - Le flux créditeur total (`flux_crediteur`).
    4. Ajouter des colonnes calculées pour :
       - Identifier si le client est actuellement en EER (`is_eer_en_cour`).
       - Confirmer l'état d'EER (`is_entrer_en_relation`).
    """

    eer = select_eer(mouvement_comptable, taux_change_bam)

    compte = compte.select("numerotiers", "numerodecompte")

    base = base_de_calcul(tiers, marche)

    base = base.filter((col("marche") == "RETAIL") | (col("marche") == "PRO"))

    return (
        base.join(compte, ["numerotiers"], how="left")
        .join(eer, ["numerodecompte"], how="left")
        .withColumn(
            "is_eer",
            (
                (
                    months_between(
                        to_date(lit(partition_date)),
                        to_date(col("date_ouverture_premier_compte")),
                    )
                    <= 36
                )
                & (
                    months_between(
                        to_date(lit(partition_date)),
                        to_date(col("date_ouverture_premier_compte")),
                    )
                    > 0
                )
            ).cast("integer"),
        )
        .groupBy("numerotiers")
        .agg(
            max("is_eer").alias("is_entrer_en_relation_date"),
            sum("sum_montant_ecriture").alias("flux_crediteur"),
        )
        .withColumn(
            "is_eer_en_cour",
            (
                (col("is_entrer_en_relation_date") == 1)
                & (col("flux_crediteur") < 2000)
            ).cast("integer"),
        )
        .withColumn(
            "is_entrer_en_relation",
            (
                (col("is_entrer_en_relation_date") == 1)
                & (col("is_eer_en_cour") == 1)
            ).cast("integer"),
        )
        .select(
            "numerotiers",
            "is_entrer_en_relation",
            "flux_crediteur",
        )
    )


def select_num_compte_retail_pro(
    tier: DataFrame,
    compte: DataFrame,
    marche: DataFrame,
    ressource_emploi: DataFrame,
) -> DataFrame:
    """
    Sélectionner les comptes liés aux segments RETAIL et PRO ayant un solde créditeur minimum requis.

    Étapes :
    1. Crée une base de calcul à partir des tiers et des marchés filtrés pour les segments RETAIL et PRO.
    2. Filtre les comptes avec un type "origine" et joint avec la table `ressource_emploi`.
    3. Identifie si les comptes respectent le seuil minimum de solde créditeur :
       - 500 pour RETAIL.
       - 2500 pour PRO.
    4. Retourne les comptes avec les informations associées.
    """

    base = base_de_calcul(tier, marche)

    base = base.filter((col("marche") == "RETAIL") | (col("marche") == "PRO"))

    compte = compte.filter(col("typecompte") == "origine").select(
        "numerotiers", "naturecompte", "pk_compte"
    )

    ressource_emploi = ressource_emploi.transform(select_ressource_emploi)

    compte_retail_pro_sup_500 = (
        base.join(compte, ["numerotiers"], "left")
        .join(
            ressource_emploi,
            ressource_emploi["fk_compte"] == compte["pk_compte"],
            how="left",
        )
        .withColumn(
            "is_encour",
            when((col("marche") == "RETAIL") & (col("soldejourcr_devise") >= 500), 1)
            .when((col("marche") == "PRO") & (col("soldejourcr_devise") >= 2500), 1)
            .otherwise(0),
        )
        .select(
            "numerotiers",
            "numerodecompte",
            "naturecompte",
            "soldejourcr_devise",
            "is_encour",
            "marche",
        )
        .na.fill(
            {
                "soldejourcr_devise": 0,
                "is_encour": 0,
            }
        )
    )

    return compte_retail_pro_sup_500.dropDuplicates()


def select_mouvement_comptable(
    mouvement_comptable: DataFrame,
    operation_initiative_client: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Sélectionner les mouvements comptables pour les 6 derniers mois.

    Étapes :
    1. Filtre les mouvements comptables pour les 6 derniers mois par rapport à la date de partition.
    2. Effectue une jointure avec les opérations liées à l'initiative client (`operation_initiative_client`).
    3. Identifie les comptes avec des opérations dans les 6 derniers mois.

    Colonnes retournées :
    - `numerodecompte`
    - `max_operation_6` : Indique s'il y a eu une opération dans les 6 derniers mois.
    """

    mouvement_comptable = mouvement_comptable.filter(
        months_between(to_date(lit(partition_date)), to_date(col("datetraitement")))
        <= 6
    )

    operation_initiative_client = operation_initiative_client.select("typoper")

    mvt_comptable_6_mois_operation = (
        mouvement_comptable.join(
            operation_initiative_client,
            operation_initiative_client["typoper"]
            == mouvement_comptable["codeoperation"],
            how="inner",
        )
        .withColumn(
            "is_operation_6",
            (
                (
                    months_between(
                        to_date(lit(partition_date)), to_date(col("datetraitement"))
                    )
                    <= 6
                )
                & (
                    months_between(
                        to_date(lit(partition_date)), to_date(col("datetraitement"))
                    )
                    >= 0
                )
            ).cast("integer"),
        )
        .groupBy("numerodecompte")
        .agg(
            max("is_operation_6").alias("max_operation_6"),
        )
        .select(
            "numerodecompte",
            "max_operation_6",
        )
    )
    return mvt_comptable_6_mois_operation


def build_is_actif_regle_2(
    tier: DataFrame,
    compte: DataFrame,
    marche: DataFrame,
    ressource_emploi: DataFrame,
    mouvement_comptable: DataFrame,
    operation_initiative_client: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Déterminer si un client est actif selon la règle 2.

    Étapes :
    1. Identifie les comptes avec des soldes créditeurs dépassant le seuil pour RETAIL et PRO.
    2. Sélectionne les mouvements comptables des 6 derniers mois.
    3. Agrège les données pour déterminer si un client a des comptes actifs selon la règle 2.

    Colonnes retournées :
    - `numerotiers`
    - `is_actif_regle2` : Indique si le client est actif selon la règle 2.
    - `max_is_encour` : Indique si un compte du client est en cours.
    """

    compte_retail_pro_sup_500 = select_num_compte_retail_pro(
        tier, compte, marche, ressource_emploi
    )
    mvt_comptable_6_mois_operation = select_mouvement_comptable(
        mouvement_comptable, operation_initiative_client, partition_date
    )
    grouped_numerotiers = (
        compte_retail_pro_sup_500.join(
            mvt_comptable_6_mois_operation, ["numerodecompte"], how="left"
        )
        .withColumn(
            "is_actif_6_mois", when((col("max_operation_6") == 1), 1).otherwise(0)
        )
        .groupBy("numerotiers")
        .agg(
            max("is_actif_6_mois").alias("is_actif_regle2"),
            max("is_encour").alias("max_is_encour"),
        )
        .select(
            "numerotiers",
            "is_actif_regle2",
            "max_is_encour",
        )
        .na.fill(
            {
                "is_actif_regle2": 0,
            }
        )
    )

    return grouped_numerotiers


def build_engagement(
    ressource_emploi: DataFrame,
    mapping_ressource_emploi: DataFrame,
) -> DataFrame:
    """
    Construire l'engagement des clients en fonction de leurs ressources et emplois.

    Étapes :
    1. Sélectionne les colonnes pertinentes de `ressource_emploi`.
    2. Joint les informations de mapping pour obtenir les libellés des types de ressources/emplois.
    3. Calcule :
       - `is_engaged` : Indique si un client est engagé (selon les types d'emplois spécifiés).
       - `has_ressource` : Indique si un client a des ressources significatives.
    """

    select_ressource_emploi = ressource_emploi.select(
        "numerotiers",
        "numerodecompte",
        "soldejourdb_devise",
        "soldejourcr_devise",
        "fk_mapressourceemploi",
    ).drop_duplicates()

    mapping_ressource_emploi = mapping_ressource_emploi.select(
        "pk_mapressourceemploi", "libelle_type", "type_ressource_emploi"
    )

    type_emploi = [
        "AVANCES",
        "CAUTIONS",
        "CMT",
        "CONSO",
        "CREDOC",
        "DEC",
        "ESCOMPTE",
        "FED",
        "HABITAT",
        "MOURABAHA IMMO",
        "MOURABAHA MOB",
        "PIM",
    ]

    type_ressource = ["REP", "RAT"]

    return (
        select_ressource_emploi.join(
            mapping_ressource_emploi,
            select_ressource_emploi["fk_mapressourceemploi"]
            == mapping_ressource_emploi["pk_mapressourceemploi"],
        )
        .withColumn(
            "is_engaged",
            when(
                (col("type_ressource_emploi").isin(type_emploi))
                & (col("soldejourdb_devise") >= 100),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "has_ressource",
            when(
                (col("type_ressource_emploi").isin(type_ressource))
                & (col("soldejourcr_devise") >= 100),
                1,
            ).otherwise(0),
        )
        .select(
            "numerotiers",
            "numerodecompte",
            "soldejourdb_devise",
            "pk_mapressourceemploi",
            "libelle_type",
            "is_engaged",
            "has_ressource",
        )
    )


def buils_is_conquete(
    tiers: DataFrame,
    marche: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Détermine si un client est une conquête pour une année donnée.

    Étapes :
    1. Crée une base de calcul à partir des tiers et des marchés filtrés pour les segments RETAIL et PRO.
    2. Ajoute une colonne `is_conquete` qui indique si la date d'ouverture du premier compte correspond
       à l'année en cours (`partition_date`).
    """

    base = base_de_calcul(tiers, marche)
    base = base.filter((col("marche") == "PRO") | (col("marche") == "RETAIL"))
    return base.withColumn(
        "is_conquete",
        (
            year(to_date(lit(partition_date)))
            == year(to_date(col("date_ouverture_premier_compte")))
        ).cast("integer"),
    ).select(
        "numerotiers",
        "date_ouverture_premier_compte",
        "is_conquete",
        "partitiondate",
    )


def build_activite_client(
    tier: DataFrame,
    compte: DataFrame,
    marche: DataFrame,
    ressource_emploi: DataFrame,
    mouvement_comptable: DataFrame,
    operation_initiative_client: DataFrame,
    mapping_ressource_emploi: DataFrame,
    taux_change_bam: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit l'activité des clients en combinant plusieurs critères.

    Étapes :
    1. Calcule les clients en cours d'entrée en relation (EER).
    2. Évalue si un client est actif en fonction de règles spécifiques (Règle 2).
    3. Identifie les engagements des clients.
    4. Détermine si un client est une conquête.
    5. Agrège les données pour calculer les indicateurs `is_actif`, `is_inactif`,
       `is_eer_encours`, et `is_conquete_active`.

    Colonnes retournées :
    - `numerotiers`: Identifiant du client.
    - `is_actif`: Indique si le client est actif (1 = Oui, 0 = Non).
    - `is_inactif`: Indique si le client est inactif (1 = Oui, 0 = Non).
    - `is_eer_encours`: Indique si le client est en cours d'entrée en relation.
    - `flux_crediteur`: Montant du flux créditeur.
    - `is_conquete_active`: Indique si le client est une conquête active.
    """
    entrer_enrelation = eer_en_cour_numerotiers(
        mouvement_comptable, taux_change_bam, tier, compte, marche, partition_date
    )
    base_de_calcul = build_is_actif_regle_2(
        tier,
        compte,
        marche,
        ressource_emploi,
        mouvement_comptable,
        operation_initiative_client,
        partition_date,
    )

    engagement = build_engagement(ressource_emploi, mapping_ressource_emploi)

    conquete = buils_is_conquete(tier, marche, partition_date)

    return (
        base_de_calcul.join(engagement, ["numerotiers"], how="left")
        .withColumn(
            "is_actif",
            (
                (col("is_actif_regle2") == 1)
                | (col("is_engaged") == 1)
                | (col("max_is_encour") == 1)
                | (col("has_ressource") == 1)
            ).cast("integer"),
        )
        .groupBy("numerotiers")
        .agg(
            max("is_actif").alias("is_actif"),
        )
        .join(entrer_enrelation, ["numerotiers"], how="left")
        .na.fill(0, ["is_entrer_en_relation"])
        .withColumn(
            "is_actif",
            when(col("is_entrer_en_relation") != 1, col("is_actif")).otherwise(0),
        )
        .withColumn(
            "is_inactif",
            ((col("is_entrer_en_relation") == 0) & (col("is_actif") == 0)).cast(
                "integer"
            ),
        )
        .join(conquete, ["numerotiers"], how="left")
        .withColumn(
            "is_conquete_actif",
            ((col("is_conquete") == 1) & (col("is_actif") == 1)).cast("integer"),
        )
        .na.fill(0, ["is_actif", "is_inactif", "is_conquete_actif"])
        .withColumnRenamed("is_entrer_en_relation", "is_eer_encours")
        .withColumnRenamed("is_conquete_actif", "is_conquete_active")
        .select(
            "numerotiers",
            "is_actif",
            "is_inactif",
            "is_eer_encours",
            "flux_crediteur",
            "is_conquete_active",
        )
    )
