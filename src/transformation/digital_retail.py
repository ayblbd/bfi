from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    add_months,
    col,
    concat_ws,
    date_add,
    date_trunc,
    expr,
    from_json,
    last_day,
    lit,
    max,
    substring,
    to_date,
)
from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.common import (
    get_is_during_a_period_for_equipement,
    get_last_day_of_week,
    is_three_months_ago,
)


def get_open_account(df):
    """
    Identifie les comptes non clôturés.
    - Filtre les comptes ayant un statut différent de "CLOTURE".
    - Retourne un DataFrame contenant uniquement les comptes ouverts.
    """
    return df.filter(col("statutcompte") != "CLOTURE")


def select_audit_log_event(audit_log_event: DataFrame) -> DataFrame:
    """
    Sélectionne les événements clés du journal d'audit.
    - Conserve les colonnes : "actor", "datecreated", et "eventname".
    - Supprime les doublons pour éviter les répétitions dans les données.
    """
    return audit_log_event.select(
        "actor", "datecreated", "eventname"
    ).drop_duplicates()


def select_utilisateur(df: DataFrame) -> DataFrame:
    """
    Récupère les informations essentielles des utilisateurs.
    - Sélectionne les colonnes "typeprofile" et "username".
    - Élimine les doublons pour réduire la taille des données.
    """
    return df.select("typeprofile", "username").drop_duplicates()


def select_audit(df: DataFrame) -> DataFrame:
    """
    Transforme et sélectionne les données du journal d'audit.
    - Parse la colonne "description" en un format JSON pour extraire des champs spécifiques.
    - Renomme les colonnes et reformate les dates pour une analyse simplifiée des connexions des tiers.
    """

    schema = StructType(
        [
            StructField("heure de connexion", StringType(), True),
            StructField("id Tiers", StringType(), True),
            StructField("canal", StringType(), True),
            StructField("Date de connexion", StringType(), True),
        ]
    )

    return (
        df.withColumn("description", from_json(col("description"), schema=schema))
        .select("user", "action", col("description.*"))
        .withColumnRenamed("id Tiers", "numerotiers")
        .withColumn(
            "date_connexion",
            concat_ws(
                " ",
                to_date(col("Date de connexion"), "dd-MM-yyyy"),
                col("heure de connexion"),
            ),
        )
        .select("numerotiers", "user", "action", "date_connexion")
    )


def select_mouvement_comptable(df: DataFrame) -> DataFrame:
    """
    Extrait les informations des mouvements comptables.
    - Conserve les colonnes essentielles : "codeoperation", "numerodecompte", "dateoperation".
    """
    return df.select("codeoperation", "numerodecompte", "dateoperation")


def select_digital_operations(df: DataFrame) -> DataFrame:
    """
    Identifie les opérations numériques.
    - Filtre les opérations appartenant au canal "Digital" et à la famille "Operation".
    - Retourne les codes d'opérations numériques pour une analyse spécifique.
    """
    is_digital = col("canal") == "Digital"
    is_operation = col("famille") == "Operation"
    return (
        df.filter(is_digital & is_operation)
        .withColumnRenamed("cod", "codeoperation")
        .select("codeoperation")
    )


def get_contract_type(
    contrat_produit: DataFrame, elt_contrat: DataFrame
) -> DataFrame:
    """
    Détermine le type de contrat à partir des données contractuelles.
    - Identifie si le contrat est une "consultation" ou une "transaction" selon des options spécifiques.
    - Joint les contrats avec leurs caractéristiques détaillées.
    """
    elt_contrat = (
        elt_contrat.withColumnRenamed(
            "numerocontratinterne", "numero_contrat_interne"
        )
        .select(
            "numero_contrat_interne",
            "partiespecifique",
        )
        .drop_duplicates()
    )
    is_consultation = col("option").isin(["1", "3"])
    is_transaction = col("option").isin(["4", "5", "6"])
    return (
        contrat_produit.join(elt_contrat, ["numero_contrat_interne"], "left")
        .withColumn("option", substring(col("partiespecifique"), 143, 1))
        .withColumn("is_consultation", is_consultation)
        .withColumn("is_transaction", is_transaction)
        .select(
            "numero_contrat_interne",
            "numero_contrat",
            "is_consultation",
            "is_transaction",
        )
    )


def get_digital_transaction(
    mouvementcomptable: DataFrame,
    compte_client: DataFrame,
    ref_operation: DataFrame,
    partition_date: str,
    name: str = "digital_transaction",
) -> DataFrame:
    """
    Analyse les transactions numériques des clients.
    - Récupère les opérations numériques et les mouvements comptables.
    - Identifie les transactions numériques réalisées par les clients.
    - Calcule des indicateurs pour suivre les transactions numériques sur différentes périodes
      (semaine, mois, trimestre, année) pour les 3 derniers mois.
    - Retourne un DataFrame consolidé avec les indicateurs pour chaque client.
    """
    digital_operations = select_digital_operations(ref_operation)
    mouvementcomptable = select_mouvement_comptable(mouvementcomptable)

    compte_client = (
        compte_client.transform(get_open_account)
        .withColumnRenamed("numerotiers", "numero_tiers")
        .select("numero_tiers", "numerodecompte")
    )
    realised_digital_operations = (
        mouvementcomptable.join(digital_operations, ["codeoperation"])
        .join(compte_client, ["numerodecompte"])
        .select("numero_tiers", "dateoperation")
        .drop_duplicates()
    )

    dt = to_date(lit(partition_date))
    a_1_week_ago = dt - expr("INTERVAL 1 WEEK")
    a_2_weeks_ago = dt - expr("INTERVAL 2 WEEK")
    a_1_month_ago = add_months(dt, -1)
    a_2_months_ago = add_months(dt, -2)
    a_quarter_ago = date_add(date_trunc("quarter", dt), -1)
    a_year_ago = date_add(date_trunc("year", dt), -1)

    return (
        realised_digital_operations.withColumn(
            f"is_{name}_s", is_three_months_ago(dt, col("dateoperation"))
        )
        .withColumn(
            f"is_{name}_s_1",
            is_three_months_ago(
                get_last_day_of_week(a_1_week_ago), col("dateoperation")
            ),
        )
        .withColumn(
            f"is_{name}_s_2",
            is_three_months_ago(
                get_last_day_of_week(a_2_weeks_ago), col("dateoperation")
            ),
        )
        .withColumn(f"is_{name}_m", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_m_1",
            is_three_months_ago(last_day(a_1_month_ago), col("dateoperation")),
        )
        .withColumn(
            f"is_{name}_m_2",
            is_three_months_ago(last_day(a_2_months_ago), col("dateoperation")),
        )
        .withColumn(f"is_{name}_t", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_t_1",
            is_three_months_ago(a_quarter_ago, col("dateoperation")),
        )
        .withColumn(f"is_{name}_y", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_y_1",
            is_three_months_ago(a_year_ago, col("dateoperation")),
        )
        .groupBy("numero_tiers")
        .agg(
            max(f"is_{name}_s").alias(f"is_{name}_s"),
            max(f"is_{name}_s_1").alias(f"is_{name}_s_1"),
            max(f"is_{name}_s_2").alias(f"is_{name}_s_2"),
            max(f"is_{name}_m").alias(f"is_{name}_m"),
            max(f"is_{name}_m_1").alias(f"is_{name}_m_1"),
            max(f"is_{name}_m_2").alias(f"is_{name}_m_2"),
            max(f"is_{name}_t").alias(f"is_{name}_t"),
            max(f"is_{name}_t_1").alias(f"is_{name}_t_1"),
            max(f"is_{name}_y").alias(f"is_{name}_y"),
            max(f"is_{name}_y_1").alias(f"is_{name}_y_1"),
        )
        .select(
            "numero_tiers",
            f"is_{name}_s",
            f"is_{name}_s_1",
            f"is_{name}_s_2",
            f"is_{name}_m",
            f"is_{name}_m_1",
            f"is_{name}_m_2",
            f"is_{name}_t",
            f"is_{name}_t_1",
            f"is_{name}_y",
            f"is_{name}_y_1",
        )
    )


def get_digital_consultation(
    audit_log_event: DataFrame,
    utilisateur: DataFrame,
    audit: DataFrame,
    partition_date: str,
    name: str = "digital_consultation",
) -> DataFrame:
    """
    Identifie les consultations digitales des clients.

    Cette fonction récupère et traite les données des connexions digitales
    (mobile et web) à partir des journaux d'audit, des informations utilisateurs
    et des événements d'audit. Elle calcule également des indicateurs de présence
    sur plusieurs périodes temporelles (semaine, mois, trimestre, année) pour
    les 3 trois derniers mois pour chaque client.

    Arguments :
        audit_log_event : DataFrame contenant les événements de journal d'audit.
        utilisateur : DataFrame contenant les informations des utilisateurs.
        audit : DataFrame contenant les données d'audit.
        partition_date : Date de partition utilisée pour les calculs temporels.
        name : Nom utilisé pour personnaliser les colonnes de sortie (par défaut : "digital_consultation").

    Retourne :
        Un DataFrame contenant les indicateurs de consultations digitales pour chaque client.
    """
    mobile_events = select_audit_log_event(audit_log_event).filter(
        col("eventname") == "ConnexionOK"
    )
    filtered_users = select_utilisateur(utilisateur).filter(
        col("typeprofile") == "CL"
    )

    web_consultation = (
        select_audit(audit)
        .filter(col("action") == "Authentification d'un client")
        .withColumnRenamed("numerotiers", "numero_tiers")
        .select("numero_tiers", "date_connexion")
        .drop_duplicates()
    )

    mobile_consultation = (
        mobile_events.join(
            filtered_users, mobile_events["actor"] == filtered_users["username"]
        )
        .withColumnRenamed("datecreated", "date_connexion")
        .withColumnRenamed("actor", "numero_tiers")
        .select("numero_tiers", "date_connexion")
        .drop_duplicates()
    )

    digital_consultation = mobile_consultation.unionByName(
        web_consultation
    ).drop_duplicates()

    dt = to_date(lit(partition_date))
    a_1_week_ago = dt - expr("INTERVAL 1 WEEK")
    a_2_weeks_ago = dt - expr("INTERVAL 2 WEEK")
    a_1_month_ago = add_months(dt, -1)
    a_2_months_ago = add_months(dt, -2)
    a_quarter_ago = date_add(date_trunc("quarter", dt), -1)
    a_year_ago = date_add(date_trunc("year", dt), -1)

    return (
        digital_consultation.withColumn(
            f"is_{name}_s", is_three_months_ago(dt, col("date_connexion"))
        )
        .withColumn(
            f"is_{name}_s_1",
            is_three_months_ago(
                get_last_day_of_week(a_1_week_ago), col("date_connexion")
            ),
        )
        .withColumn(
            f"is_{name}_s_2",
            is_three_months_ago(
                get_last_day_of_week(a_2_weeks_ago), col("date_connexion")
            ),
        )
        .withColumn(f"is_{name}_m", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_m_1",
            is_three_months_ago(last_day(a_1_month_ago), col("date_connexion")),
        )
        .withColumn(
            f"is_{name}_m_2",
            is_three_months_ago(last_day(a_2_months_ago), col("date_connexion")),
        )
        .withColumn(f"is_{name}_t", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_t_1",
            is_three_months_ago(a_quarter_ago, col("date_connexion")),
        )
        .withColumn(f"is_{name}_y", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_y_1",
            is_three_months_ago(a_year_ago, col("date_connexion")),
        )
        .groupBy("numero_tiers")
        .agg(
            max(f"is_{name}_s").alias(f"is_{name}_s"),
            max(f"is_{name}_s_1").alias(f"is_{name}_s_1"),
            max(f"is_{name}_s_2").alias(f"is_{name}_s_2"),
            max(f"is_{name}_m").alias(f"is_{name}_m"),
            max(f"is_{name}_m_1").alias(f"is_{name}_m_1"),
            max(f"is_{name}_m_2").alias(f"is_{name}_m_2"),
            max(f"is_{name}_t").alias(f"is_{name}_t"),
            max(f"is_{name}_t_1").alias(f"is_{name}_t_1"),
            max(f"is_{name}_y").alias(f"is_{name}_y"),
            max(f"is_{name}_y_1").alias(f"is_{name}_y_1"),
        )
        .select(
            "numero_tiers",
            f"is_{name}_s",
            f"is_{name}_s_1",
            f"is_{name}_s_2",
            f"is_{name}_m",
            f"is_{name}_m_1",
            f"is_{name}_m_2",
            f"is_{name}_t",
            f"is_{name}_t_1",
            f"is_{name}_y",
            f"is_{name}_y_1",
        )
    )


def get_contract_status_from_history(
    contrat_produit: DataFrame, partition_date: str, name: str = "valid"
) -> DataFrame:
    """
    Extrait le statut des contrats valides à partir de l'historique.

    Cette fonction filtre les contrats ayant l'état "VALIDE" et calcule des indicateurs
    de validité pour différentes périodes (semaine, mois, trimestre, année) en fonction
    de la date de partition.

    Arguments :
        contrat_produit : DataFrame contenant les données des contrats produits.
        partition_date : Date de partition utilisée pour les calculs temporels.
        name : Nom utilisé pour personnaliser les colonnes de sortie (par défaut : "valid").

    Retourne :
        Un DataFrame avec les indicateurs de validité des contrats sur différentes périodes.
    """
    return (
        contrat_produit.select(
            "numero_contrat",
            "numero_contrat_interne",
            "etat_produit",
            "partitiondate",
        )
        .filter(col("etat_produit") == "VALIDE")
        .transform(
            get_is_during_a_period_for_equipement,
            by="partitiondate",
            name=name,
            partition_date=partition_date,
        )
        .groupBy("numero_contrat_interne", "numero_contrat")
        .agg(
            max(f"is_{name}_s").alias(f"is_{name}_s"),
            max(f"is_{name}_s_1").alias(f"is_{name}_s_1"),
            max(f"is_{name}_s_2").alias(f"is_{name}_s_2"),
            max(f"is_{name}_m").alias(f"is_{name}_m"),
            max(f"is_{name}_m_1").alias(f"is_{name}_m_1"),
            max(f"is_{name}_m_2").alias(f"is_{name}_m_2"),
            max(f"is_{name}_t").alias(f"is_{name}_t"),
            max(f"is_{name}_t_1").alias(f"is_{name}_t_1"),
            max(f"is_{name}_y").alias(f"is_{name}_y"),
            max(f"is_{name}_y_1").alias(f"is_{name}_y_1"),
        )
        .select(
            "numero_contrat",
            "numero_contrat_interne",
            f"is_{name}_s",
            f"is_{name}_s_1",
            f"is_{name}_s_2",
            f"is_{name}_m",
            f"is_{name}_m_1",
            f"is_{name}_m_2",
            f"is_{name}_t",
            f"is_{name}_t_1",
            f"is_{name}_y",
            f"is_{name}_y_1",
        )
    )


def build_digital(
    contrat_produit: DataFrame,
    contrat: DataFrame,
    elt_contrat: DataFrame,
    etat_contrat: DataFrame,
    mouvementcomptable: DataFrame,
    audit_log_event: DataFrame,
    utilisateur: DataFrame,
    audit: DataFrame,
    compte_client: DataFrame,
    ref_operation: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit les données de consultation digitale des contrats.

    Cette fonction combine et enrichit les données des contrats en y ajoutant des indicateurs
    de statut de souscription, de validité, et de transactions digitales. Elle utilise des
    informations provenant de plusieurs sources (contrats, transactions, consultations digitales, etc.)
    et produit un DataFrame consolidé avec des indicateurs calculés sur plusieurs périodes.

    Arguments :
        contrat_produit : DataFrame contenant les données des contrats produits.
        contrat : DataFrame contenant les informations des contrats.
        elt_contrat : DataFrame contenant les éléments des contrats.
        etat_contrat : DataFrame contenant les états des contrats.
        mouvementcomptable : DataFrame des mouvements comptables.
        audit_log_event : DataFrame des événements du journal d'audit.
        utilisateur : DataFrame contenant les informations des utilisateurs.
        audit : DataFrame contenant les données d'audit.
        compte_client : DataFrame contenant les comptes des clients.
        ref_operation : DataFrame contenant les références des opérations.
        partition_date : Date de partition utilisée pour les calculs temporels.

    Retourne :
        Un DataFrame consolidé avec les indicateurs de consultation digitale et des contrats.
    """
    only_digital_contrat = (
        contrat.filter(col("typecontrat") == "CN01")
        .withColumnRenamed("numerocontratinterne", "numero_contrat_interne")
        .select("numero_contrat_interne", "partitiondate")
        .drop_duplicates()
    )

    etat_contrat = (
        etat_contrat.withColumnRenamed("code_etat_contrat", "code_etat_produit")
        .withColumnRenamed("etat_contrat", "etat_produit")
        .select("code_etat_produit", "etat_produit")
    )

    contrat_produit = (
        contrat_produit.withColumnRenamed("numerotiers", "numero_tiers")
        .withColumnRenamed("numerodecompte", "numero_compte")
        .withColumnRenamed("numerocarte", "numero_contrat")
        .withColumnRenamed("numerocontratinterne", "numero_contrat_interne")
        .withColumnRenamed("codeproduit", "code_produit")
        .withColumnRenamed("datesouscription", "date_souscription")
        .withColumnRenamed("dateecheance", "date_echeance")
        .withColumnRenamed("etatproduit", "code_etat_produit")
        .join(etat_contrat, ["code_etat_produit"], "left")
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_contrat",
            "numero_contrat_interne",
            "code_produit",
            "date_souscription",
            "date_echeance",
            "etat_produit",
            "partitiondate",
        )
    )

    valid_contract_from_history = get_contract_status_from_history(
        contrat_produit, partition_date
    )
    contract_type = get_contract_type(contrat_produit, elt_contrat)
    digital_transaction = get_digital_transaction(
        mouvementcomptable, compte_client, ref_operation, partition_date
    )
    digital_consultation = get_digital_consultation(
        audit_log_event, utilisateur, audit, partition_date
    )

    kpis = [
        "is_subscribed_s",
        "is_subscribed_s_1",
        "is_subscribed_s_2",
        "is_subscribed_m",
        "is_subscribed_m_1",
        "is_subscribed_m_2",
        "is_subscribed_t",
        "is_subscribed_t_1",
        "is_subscribed_y",
        "is_subscribed_y_1",
        "is_valid_s",
        "is_valid_s_1",
        "is_valid_s_2",
        "is_valid_m",
        "is_valid_m_1",
        "is_valid_m_2",
        "is_valid_t",
        "is_valid_t_1",
        "is_valid_y",
        "is_valid_y_1",
        "is_consultation",
        "is_transaction",
        "is_digital_transaction_s",
        "is_digital_transaction_s_1",
        "is_digital_transaction_s_2",
        "is_digital_transaction_m",
        "is_digital_transaction_m_1",
        "is_digital_transaction_m_2",
        "is_digital_transaction_t",
        "is_digital_transaction_t_1",
        "is_digital_transaction_y",
        "is_digital_transaction_y_1",
        "is_digital_consultation_s",
        "is_digital_consultation_s_1",
        "is_digital_consultation_s_2",
        "is_digital_consultation_m",
        "is_digital_consultation_m_1",
        "is_digital_consultation_m_2",
        "is_digital_consultation_t",
        "is_digital_consultation_t_1",
        "is_digital_consultation_y",
        "is_digital_consultation_y_1",
    ]

    return (
        contrat_produit.join(
            only_digital_contrat, ["numero_contrat_interne", "partitiondate"]
        )
        .transform(
            get_is_during_a_period_for_equipement,
            by="date_souscription",
            name="subscribed",
            partition_date=partition_date,
        )
        .join(
            valid_contract_from_history,
            ["numero_contrat", "numero_contrat_interne"],
            "left",
        )
        .join(contract_type, ["numero_contrat", "numero_contrat_interne"], "left")
        .join(digital_transaction, ["numero_tiers"], "left")
        .join(digital_consultation, ["numero_tiers"], "left")
        .na.fill(False, kpis)
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_contrat",
            "numero_contrat_interne",
            "code_produit",
            "date_souscription",
            "date_echeance",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
        .drop_duplicates()
    )
