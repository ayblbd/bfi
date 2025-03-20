from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    add_months,
    col,
    date_add,
    date_trunc,
    expr,
    last_day,
    lit,
    lpad,
    max,
    min,
    to_date,
    trim,
)

from src.transformation.common import (
    get_is_during_a_period_for_equipement,
    get_last_day_of_week,
    is_three_months_ago,
    get_status_relative_to_last_day_of_period,
)


def build_activated_card(authorization: DataFrame) -> DataFrame:
    """
    Identifie les cartes activées à partir des données d'autorisation bancaire.

    Cette fonction:
    - Analyse les transactions bancaires pour détecter la première utilisation d'une carte
    - Filtre les transactions selon les critères suivants:
        * Transaction non annulée (aut_reve_stat différent de 'F')
        * Transaction réussie (code réponse '000')
        * Transaction sur les cartes CDM uniquement (bin_type = '3')
    - Conserve uniquement la date de première utilisation de chaque carte

    Utilisation métier:
    - Permet de suivre le taux d'activation des cartes
    - Identifie les cartes jamais utilisées
    - Mesure le délai entre émission et première utilisation
    """

    is_reve_stat_diff_f = col("aut_reve_stat") != "F"
    is_success_response = col("aut_resp_code_f039") == "000"
    is_cdm_bin = col("aut_bin_type") == "3"

    return (
        authorization.filter(is_reve_stat_diff_f & is_success_response & is_cdm_bin)
        .withColumn("numero_carte", lpad(col("aut_prim_acct_numb_f002"), 19, "0"))
        .withColumnRenamed("aut_requ_syst_time", "date_transaction")
        .select("numero_carte", "date_transaction")
        .drop_duplicates()
        .groupBy("numero_carte")
        .agg(
            min("date_transaction").alias("first_date_transaction"),
        )
    )


def get_contract_status_from_history(
    contrat_produit: DataFrame, partition_date: str, name: str = "valid"
) -> DataFrame:
    """
    Analyse l'historique des contrats pour déterminer leur statut sur différentes périodes temporelles.

    Cette fonction:
    - Se concentre uniquement sur les contrats à l'état 'VALIDE'
    - Établit des indicateurs de validité pour plusieurs horizons temporels:
        * Hebdomadaire: semaine courante (s) et 2 semaines précédentes (s_1, s_2)
        * Mensuel: mois courant (m) et 2 mois précédents (m_1, m_2)
        * Trimestriel: trimestre courant (t) et précédent (t_1)
        * Annuel: année courante (y) et précédente (y_1)

    Utilisation métier:
    - Suivi de l'évolution du portefeuille de contrats
    - Analyse des tendances de résiliation
    - Détection des variations saisonnières
    - Production d'indicateurs de performance commerciale
    """

    return (
        contrat_produit.select(
            "numero_carte", "numero_contrat_interne", "etat_produit", "partitiondate"
        )
        .filter(col("etat_produit") == "VALIDE")
        .transform(
            get_status_relative_to_last_day_of_period,
            by="partitiondate",
            partition_date=partition_date,
            name=name,
        )
        .groupBy("numero_contrat_interne", "numero_carte")
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
            "numero_carte",
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


def get_card_status_from_history(
    elt_stock: DataFrame,
    partition_date: str,
    etat: str,
    name: str,
) -> DataFrame:
    """
    Analyse l'historique des états des cartes bancaires sur différentes périodes.

    Cette fonction:
    - Se concentre sur les éléments de type carte (typeeltstock = 2)
    - Suit l'évolution d'un état spécifique (ex: VALIDE=2, RETIRE=8)
    - Calcule des indicateurs temporels multiples:
        * Court terme: suivi hebdomadaire sur 3 semaines
        * Moyen terme: suivi mensuel sur 3 mois
        * Long terme: suivi trimestriel et annuel

    Utilisation métier:
    - Monitoring du cycle de vie des cartes
    - Détection des anomalies dans les changements d'état
    - Analyse des motifs de retrait des cartes
    - Optimisation de la gestion du parc de cartes
    """
    return (
        elt_stock.select(
            "idtexterne",
            "typeeltstock",
            "dtretrait",
            "etateltstock",
            "partitiondate",
        )
        .filter(col("typeeltstock") == 2)
        .filter(
            trim(col("etateltstock")) == lit(etat)
        )  # 2 is 'VALIDE', 8 is 'RETIRE'
        .withColumn("dtretrait", to_date(col("dtretrait")))
        .withColumnRenamed("idtexterne", "numero_carte")
        .transform(
            get_status_relative_to_last_day_of_period,
            by="dtretrait",
            partition_date=partition_date,
            name=name,
        )
        .groupBy("numero_carte")
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
            "numero_carte",
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


def get_card_activity_from_history(
    authorization: DataFrame, partition_date: str, name: str = "active"
) -> DataFrame:
    """
    Analyse l'activité transactionnelle des cartes sur plusieurs périodes.

    Cette fonction:
    - Filtre les transactions valides:
        * Non annulées (aut_reve_stat != 'F')
        * Autorisées (code réponse '000')
        * Sur le périmètre CDM (bin_type = '3')

    - Calcule l'activité sur une fenêtre glissante de 3 mois pour chaque point temporel:
        * Base hebdomadaire:
            - S: activité sur les 3 derniers mois jusqu'à aujourd'hui
            - S-1: activité sur les 3 mois précédant la fin de S-1
            - S-2: activité sur les 3 mois précédant la fin de S-2
        * Base mensuelle:
            - M: même période que S
            - M-1: activité sur les 3 mois précédant la fin de M-1
            - M-2: activité sur les 3 mois précédant la fin de M-2
        * Base trimestrielle:
            - T: même période que S
            - T-1: activité sur les 3 mois précédant la fin du trimestre précédent
        * Base annuelle:
            - A: même période que S
            - A-1: activité sur les 3 mois précédant la fin de l'année précédente

    Utilisation métier:
    - Mesure du taux d'utilisation récent des cartes (3 derniers mois)
    - Identification des cartes devenues inactives
    - Analyse des tendances d'utilisation au fil du temps
    - Support aux campagnes de réactivation
    """

    is_reve_stat_diff_f = col("aut_reve_stat") != "F"
    is_success_response = col("aut_resp_code_f039") == "000"
    is_cdm_bin = col("aut_bin_type") == "3"

    authorization = (
        authorization.filter(is_reve_stat_diff_f & is_success_response & is_cdm_bin)
        .withColumn("numero_carte", lpad(col("aut_prim_acct_numb_f002"), 19, "0"))
        .withColumnRenamed("aut_requ_syst_time", "date_transaction")
        .select("numero_carte", "date_transaction")
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
        authorization.withColumn(
            f"is_{name}_s", is_three_months_ago(dt, col("date_transaction"))
        )
        .withColumn(
            f"is_{name}_s_1",
            is_three_months_ago(
                get_last_day_of_week(a_1_week_ago), col("date_transaction")
            ),
        )
        .withColumn(
            f"is_{name}_s_2",
            is_three_months_ago(
                get_last_day_of_week(a_2_weeks_ago), col("date_transaction")
            ),
        )
        .withColumn(f"is_{name}_m", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_m_1",
            is_three_months_ago(last_day(a_1_month_ago), col("date_transaction")),
        )
        .withColumn(
            f"is_{name}_m_2",
            is_three_months_ago(last_day(a_2_months_ago), col("date_transaction")),
        )
        .withColumn(f"is_{name}_t", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_t_1",
            is_three_months_ago(a_quarter_ago, col("date_transaction")),
        )
        .withColumn(f"is_{name}_y", col(f"is_{name}_s"))
        .withColumn(
            f"is_{name}_y_1",
            is_three_months_ago(a_year_ago, col("date_transaction")),
        )
        .groupBy("numero_carte")
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
            "numero_carte",
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


def get_activated_card_from_history(
    authorization: DataFrame, partition_date: str
) -> DataFrame:
    """
    Construit une vue complète et unifiée du parc de cartes bancaires.

    Cette fonction:
    - Consolide les informations provenant de multiples sources:
        * Contrats produits
        * Stock de cartes
        * Autorisations bancaires
        * États des contrats
    - Enrichit avec des indicateurs multidimensionnels:
        * Statut contractuel (souscription, validité)
        * État physique de la carte (retrait)
        * Usage (activation, activité)

    Indicateurs produits (sur différentes périodes):
    - Souscription (is_souscrit_*)
    - Validité contractuelle (is_valid_*)
    - Retrait de la carte (is_withdrawn_*)
    - Activité transactionnelle (is_active_*)
    - Activation (is_activated_*)

    Utilisation métier:
    - Vision à 360° du parc de cartes
    - Support au pilotage commercial
    - Analyse de la performance produit
    - Détection des anomalies
    - Production de reportings réglementaires
    """

    activated_card = build_activated_card(authorization)

    return activated_card.transform(
        get_status_relative_to_last_day_of_period,
        by="first_date_transaction",
        partition_date=partition_date,
        name="activated",
    ).select(
        "numero_carte",
        "is_activated_s",
        "is_activated_s_1",
        "is_activated_s_2",
        "is_activated_m",
        "is_activated_m_1",
        "is_activated_m_2",
        "is_activated_t",
        "is_activated_t_1",
        "is_activated_y",
        "is_activated_y_1",
    )


def build_carte(
    contrat_produit: DataFrame,
    elt_stock: DataFrame,
    authorization: DataFrame,
    contrat: DataFrame,
    etat_contrat: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit une vue complète et unifiée du parc de cartes bancaires.

    Cette fonction:
    - Consolide les informations provenant de multiples sources:
        * Contrats produits
        * Stock de cartes
        * Autorisations bancaires
        * États des contrats
    - Enrichit avec des indicateurs multidimensionnels:
        * Statut contractuel (souscription, validité)
        * État physique de la carte (retrait)
        * Usage (activation, activité)

    Indicateurs produits (sur différentes périodes):
    - Souscription (is_souscrit_*)
    - Validité contractuelle (is_valid_*)
    - Retrait de la carte (is_withdrawn_*)
    - Activité transactionnelle (is_active_*)
    - Activation (is_activated_*)

    Utilisation métier:
    - Vision à 360° du parc de cartes
    - Support au pilotage commercial
    - Analyse de la performance produit
    - Détection des anomalies
    - Production de reportings réglementaires
    """

    only_card_contrat = (
        contrat.filter(col("typecontrat").isin("0003", "0005"))
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
        .withColumnRenamed("numerocarte", "numero_carte")
        .withColumnRenamed("numerocontratinterne", "numero_contrat_interne")
        .withColumnRenamed("codeproduit", "code_produit")
        .withColumnRenamed("datesouscription", "date_souscription")
        .withColumnRenamed("dateecheance", "date_echeance")
        .withColumnRenamed("etatproduit", "code_etat_produit")
        .join(etat_contrat, ["code_etat_produit"], "left")
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_carte",
            "numero_contrat_interne",
            "code_produit",
            "date_souscription",
            "date_echeance",
            "etat_produit",
            "partitiondate",
        )
    )

    valid_contract_from_history = get_contract_status_from_history(
        contrat_produit, partition_date=partition_date
    )

    withdrawn_card_status_from_history = get_card_status_from_history(
        elt_stock, partition_date=partition_date, etat="8", name="withdrawn"
    )

    card_activity_from_history = get_card_activity_from_history(
        authorization, partition_date=partition_date, name="active"
    )

    activated_card_from_history = get_activated_card_from_history(
        authorization, partition_date=partition_date
    )

    kpis = [
        "is_souscrit_s",
        "is_souscrit_s_1",
        "is_souscrit_s_2",
        "is_souscrit_m",
        "is_souscrit_m_1",
        "is_souscrit_m_2",
        "is_souscrit_t",
        "is_souscrit_t_1",
        "is_souscrit_y",
        "is_souscrit_y_1",
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
        "is_withdrawn_s",
        "is_withdrawn_s_1",
        "is_withdrawn_s_2",
        "is_withdrawn_m",
        "is_withdrawn_m_1",
        "is_withdrawn_m_2",
        "is_withdrawn_t",
        "is_withdrawn_t_1",
        "is_withdrawn_y",
        "is_withdrawn_y_1",
        "is_active_s",
        "is_active_s_1",
        "is_active_s_2",
        "is_active_m",
        "is_active_m_1",
        "is_active_m_2",
        "is_active_t",
        "is_active_t_1",
        "is_active_y",
        "is_active_y_1",
        "is_activated_s",
        "is_activated_s_1",
        "is_activated_s_2",
        "is_activated_m",
        "is_activated_m_1",
        "is_activated_m_2",
        "is_activated_t",
        "is_activated_t_1",
        "is_activated_y",
        "is_activated_y_1",
    ]

    return (
        contrat_produit.join(
            only_card_contrat, ["numero_contrat_interne", "partitiondate"]
        )
        .transform(
            get_is_during_a_period_for_equipement,
            by="date_souscription",
            name="souscrit",
            partition_date=partition_date,
        )
        .join(
            valid_contract_from_history,
            ["numero_carte", "numero_contrat_interne"],
            "left",
        )
        .join(withdrawn_card_status_from_history, ["numero_carte"], "left")
        .join(card_activity_from_history, ["numero_carte"], "left")
        .join(activated_card_from_history, ["numero_carte"], "left")
        .na.fill(False, kpis)
        .select(
            "numero_tiers",
            "numero_compte",
            "numero_carte",
            "numero_contrat_interne",
            "code_produit",
            "date_souscription",
            "date_echeance",
            *[col(kpi).cast("integer") for kpi in kpis],
        )
    )
