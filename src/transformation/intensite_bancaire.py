from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    add_months,
    coalesce,
    col,
    concat_ws,
    count,
    count_distinct,
    date_trunc,
    from_json,
    least,
    lit,
    lower,
    make_date,
    max,
    min,
    months_between,
    regexp_replace,
    sum,
    to_date,
    trim,
    when,
)
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.table_loader import get_history_from


# Palier 1
def is_csc_sec(
    compte: DataFrame,
) -> DataFrame:
    """
    Identifie les comptes CSC (Compte sur Carnet) secondaires.

    Cette fonction vérifie si les comptes appartiennent à des natures spécifiques
    ("010", "011", "014") et ajoute une colonne indiquant si un client a uniquement un compte
    CSC secondaire.

    Arguments :
        compte : DataFrame contenant les informations des comptes.

    Retourne :
        Un DataFrame avec une colonne supplémentaire "is_csc_sec".
    """
    window = Window.partitionBy("numerotiers")
    is_csc_natures = col("naturecompte").isin(["010", "011", "014"])
    return compte.withColumn(
        "is_csc_sec", when(is_csc_natures, 1).otherwise(0)
    ).withColumn("is_csc_sec", min(col("is_csc_sec")).over(window))


def select_mouvement_comptable(
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Sélectionne et agrège les mouvements comptables.

    Cette fonction traite les données des mouvements comptables en les filtrant sur
    une période de deux mois précédant la date de partition. Elle applique les taux
    de change BAM et calcule des indicateurs comme les montants totaux et les flags
    associés.

    Arguments :
        mouvement_comptable : DataFrame des mouvements comptables.
        taux_change_bam : DataFrame des taux de change BAM.
        partition_date : Date de partition utilisée pour les calculs.

    Retourne :
        Un DataFrame avec les mouvements comptables agrégés par compte.
    """
    partition_date_2_month_eralier_start = date_trunc(
        "MM", add_months(to_date(lit(partition_date)), -2)
    )

    taux_change_bam = (
        taux_change_bam.withColumn("cd_dev_oper", trim(col("cd_dev_oper")))
        .withColumnRenamed("date_cours", "datetraitement")
        .withColumn("devise", col("cd_dev_oper"))
        .select(
            "devise",
            "midbam",
            "datetraitement",
            # "partitiondate",
        )
    )

    mouvement_comptable = (
        mouvement_comptable.withColumn(
            "date_op",
            make_date(col("annee_traitement"), col("mois_traitement"), lit(1)),
        )
        .filter(
            (to_date(col("date_op")) >= partition_date_2_month_eralier_start)
            & (to_date(col("date_op")) <= to_date(lit(partition_date)))
        )
        .filter(col("sens") == "C")
        .withColumn("devise", trim("devise"))
        .withColumn("datetraitement", to_date(col("datetraitement")))
        .join(
            taux_change_bam,
            ["datetraitement", "devise"],
            how="left",
        )
        .withColumn(
            "midbam", when(col("devise") == lit("000C"), 1).otherwise(col("midbam"))
        )
        .withColumn(
            "month_m",
            col("date_op")
            == date_trunc("MM", to_date(lit(partition_date))).cast("date"),
        )
        .withColumn(
            "month_m_1",
            col("date_op")
            == date_trunc("MM", add_months(to_date(lit(partition_date)), -1)).cast(
                "date"
            ),
        )
        .withColumn(
            "month_m_2",
            col("date_op")
            == date_trunc("MM", add_months(to_date(lit(partition_date)), -2)).cast(
                "date"
            ),
        )
        .withColumn("montant", (col("midbam") * col("montantecriture")) / 100)
        .withColumn("flag_2000", when(col("montant") >= 2000, 1).otherwise(0))
        .groupBy("numerodecompte")
        .agg(
            max(when(col("month_m"), col("flag_2000")).otherwise(0)).alias(
                "m_flag_2000"
            ),
            max(when(col("month_m_1"), col("flag_2000")).otherwise(0)).alias(
                "m_1_flag_2000"
            ),
            max(when(col("month_m_2"), col("flag_2000")).otherwise(0)).alias(
                "m_2_flag_2000"
            ),
            sum(
                when(col("month_m"), coalesce(col("montant"), lit(0))).otherwise(0)
            ).alias("total_montant"),
        )
        .withColumn("date_op", to_date(date_trunc("MM", lit(partition_date))))
        .select(
            "numerodecompte",
            "date_op",
            # "dateoperation",
            "total_montant",
            "m_flag_2000",
            "m_1_flag_2000",
            "m_2_flag_2000",
            # "codeoperation",
            # "libellecomplementaire",
        )
    )

    return mouvement_comptable


def build_monthly_totals(
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    compte: DataFrame,
    tiers: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit les indicateurs mensuels agrégés par tiers.

    Cette fonction combine les informations des comptes, des mouvements comptables,
    et des tiers pour calculer des indicateurs mensuels comme les montants totaux,
    les flags, et les scores associés. Elle tient compte de l'ancienneté des tiers
    et de la nature des comptes.

    Arguments :
        mouvement_comptable : DataFrame des mouvements comptables.
        taux_change_bam : DataFrame des taux de change BAM.
        compte : DataFrame contenant les informations des comptes.
        tiers : DataFrame contenant les informations des tiers.
        partition_date : Date de partition utilisée pour les calculs.

    Retourne :
        Un DataFrame avec les indicateurs mensuels agrégés par tiers.
    """
    is_compte_non_cloture = col("statutcompte") != "CLOTURE"
    compte = (
        compte.filter(is_compte_non_cloture)
        .transform(is_csc_sec)
        .filter(
            col("naturecompte").isin(
                ["001", "002", "003", "004", "005", "006", "010", "011", "014"]
            )
        )
        .select(
            "numerodecompte",
            "numerotiers",
            "naturecompte",
            "is_csc_sec",
        )
    )

    tiers = (
        tiers.withColumn(
            "anciennete",
            months_between(
                to_date(lit(partition_date)),
                add_months(
                    date_trunc("MM", col("date_ouverture_premier_compte")), 1
                ),
            ).cast("integer"),
        )
        .select(
            "date_creation",
            "anciennete",
            "numerotiers",
            "is_mre",
            "code_csp",
        )
        .drop_duplicates()
    )

    virements_anciennete = select_mouvement_comptable(
        mouvement_comptable,
        taux_change_bam,
        partition_date,
    )

    older_than_three_months = (col("anciennete") >= 0) & (col("anciennete") >= 3)
    two_months_old = (col("anciennete") >= 0) & (col("anciennete") == 2)
    one_month_old = (col("anciennete") >= 0) & (col("anciennete") == 1)

    sum_flag_m_and_m_1_and_m_2 = (
        col("m_flag_2000") + col("m_1_flag_2000") + col("m_2_flag_2000")
    )
    sum_flag_m_and_m_1 = col("m_flag_2000") + col("m_1_flag_2000")

    return (
        compte.join(virements_anciennete, ["numerodecompte"], how="left")
        .groupBy("numerotiers")
        .agg(
            max("m_flag_2000").alias("m_flag_2000"),
            max("m_1_flag_2000").alias("m_1_flag_2000"),
            max("m_2_flag_2000").alias("m_2_flag_2000"),
            sum(coalesce(col("total_montant"), lit(0))).alias("total_montant"),
            min("is_csc_sec").alias("is_csc_sec"),
        )
        .join(tiers, ["numerotiers"])
        .withColumn(
            "3_flags_sum",
            (col("m_flag_2000") + col("m_1_flag_2000") + col("m_2_flag_2000")),
        )
        .withColumn(
            "score_flag_2000_m",
            when(older_than_three_months & (sum_flag_m_and_m_1_and_m_2 <= 1), 0)
            .when(older_than_three_months & (sum_flag_m_and_m_1_and_m_2 >= 2), 1)
            .when(two_months_old & (sum_flag_m_and_m_1 == 0), 0)
            .when(two_months_old & (sum_flag_m_and_m_1 >= 1), 1)
            .when(one_month_old, col("m_flag_2000"))
            .otherwise(0),
        )
        .withColumn("date_op", to_date(date_trunc("MM", lit(partition_date))))
        .na.fill(
            value=0,
            subset=[
                "total_montant",
                "m_flag_2000",
                "m_1_flag_2000",
                "m_2_flag_2000",
                "3_flags_sum",
                "score_flag_2000_m",
                "is_csc_sec",
            ],
        )
        .select(
            "numerotiers",
            "date_op",
            "anciennete",
            "total_montant",
            # "m_1_total_montant",
            # "m_2_total_montant",
            # "montant_rolling_avg",
            "m_flag_2000",
            "m_1_flag_2000",
            "m_2_flag_2000",
            "3_flags_sum",
            "score_flag_2000_m",
            "is_csc_sec",
            "is_mre",
            "code_csp",
        )
    )


# Palier 2
def build_transaction_score(
    compte: DataFrame,
    mouvement_comptable: DataFrame,
    ref_operation: DataFrame,
    transaction_monetique: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit les scores de transaction pour les clients.

    Cette fonction calcule divers scores de transaction (virement, mise à disposition, retrait, chèque, carte)
    pour chaque client, en se basant sur les données des comptes, des mouvements comptables, des références d'opérations,
    et des transactions monétiques. Les scores sont agrégés par tiers et un score total de transaction est calculé.

    Arguments :
        compte : DataFrame contenant les informations des comptes.
        mouvement_comptable : DataFrame des mouvements comptables.
        ref_operation : DataFrame contenant les références d'opérations.
        transaction_monetique : DataFrame des transactions monétiques.
        partition_date : Date de partition utilisée pour les calculs.

    Retourne :
        Un DataFrame avec les scores de transaction par tiers, incluant un score global.
    """
    is_tpe_internet = col("tra_type_terminal").isin(["TPE", "INTERNET"])
    is_compte_non_cloture = col("statutcompte") != "CLOTURE"

    mouvement_comptable = get_history_from(
        mouvement_comptable, partition_date, months=0, full_period=True
    )

    dt = to_date(lit(partition_date))
    first_day_month = date_trunc("MM", dt)
    transaction_monetique = transaction_monetique.filter(
        col("tra_date_transaction").between(first_day_month, dt)
    ).select(
        "numerodecompte",
        "tra_type_terminal",
    )

    compte = (
        compte.filter(
            col("naturecompte").isin(
                ["001", "002", "003", "004", "005", "006", "010", "011", "014"]
            )
            & is_compte_non_cloture
        )
        .join(transaction_monetique, ["numerodecompte"], how="left")
        .withColumn("score_carte", is_tpe_internet)
        .select(
            "numerotiers",
            "numerodecompte",
            "score_carte",
        )
    )

    ref_operation = ref_operation.withColumnRenamed("cod", "codeoperation").select(
        "codeoperation",
        "canal",
        "sous_categorie_id",
        "famille",
    )

    is_agence = col("canal") == "Agence"
    is_virement = col("sous_categorie_id").isin(
        "TRA_VIR_RTGS",
        "TRA_VIR_PER",
        "TRA_VIR_STD",
        "TRA_VIR_SIMT",
        "TRA_VIR_COLL",
        "TRA_VIR_INS",
        "TRA_VIR_M",
        "TRA_VIR_INT",
        "TRA_VIR_SLR",
    )
    is_mise_a_disposition = col("sous_categorie_id") == "MOY_MAD"
    is_operation = col("famille") == "Operation"
    is_agence_gab = col("canal").isin(["Agence", "Agence/GAB", "GAB"])
    is_retrait = col("sous_categorie_id") == "MOY_RET"
    is_cheque_lcn = col("sous_categorie_id") == "MOY_CHE"

    mouvement_comptable = (
        mouvement_comptable.select(
            "numerodecompte",
            "codeoperation",
        )
        .join(ref_operation, ["codeoperation"], how="left")
        .withColumn("score_virement", (is_agence & is_virement))
        .withColumn("score_mad", (is_agence & is_mise_a_disposition & is_operation))
        .withColumn("score_retrait", (is_agence_gab & is_retrait & is_operation))
        .withColumn("score_cheque", (is_agence & is_cheque_lcn & is_operation))
        .groupBy("numerodecompte")
        .agg(
            max("score_virement").alias("score_virement"),
            max("score_mad").alias("score_mad"),
            max("score_retrait").alias("score_retrait"),
            max("score_cheque").alias("score_cheque"),
        )
        .select(
            "numerodecompte",
            "score_virement",
            "score_mad",
            "score_retrait",
            "score_cheque",
        )
    )

    return (
        compte.join(mouvement_comptable, ["numerodecompte"], how="left")
        .groupBy("numerotiers")
        .agg(
            max("score_virement").alias("score_virement"),
            max("score_mad").alias("score_mad"),
            max("score_retrait").alias("score_retrait"),
            max("score_cheque").alias("score_cheque"),
            max("score_carte").alias("score_carte"),
        )
        .withColumn("score_virement", col("score_virement").cast(IntegerType()) / 2)
        .withColumn("score_mad", col("score_mad").cast(IntegerType()) / 2)
        .withColumn("score_retrait", col("score_retrait").cast(IntegerType()) / 2)
        .withColumn("score_cheque", col("score_cheque").cast(IntegerType()) / 2)
        .withColumn("score_carte", col("score_carte").cast(IntegerType()))
        .na.fill(
            value=0,
            subset=[
                "score_virement",
                "score_mad",
                "score_retrait",
                "score_cheque",
                "score_carte",
            ],
        )
        .withColumn(
            "scores_sum",
            col("score_virement")
            + col("score_mad")
            + col("score_retrait")
            + col("score_cheque")
            + col("score_carte"),
        )
        .withColumn("score_transaction", least(col("scores_sum"), lit(1.5)))
    )


# Palier 3
def build_open_account(df):
    """
    Construit les comptes ouverts.

    Cette fonction filtre les comptes pour exclure ceux ayant le statut "CLOTURE".
    """
    return df.filter(col("statutcompte") != "CLOTURE")


def build_base_client(compte: DataFrame) -> DataFrame:
    """
    Construit la base des clients actifs.

    Cette fonction identifie les clients ayant des comptes actifs parmi des natures
    spécifiques et élimine les doublons.
    """
    is_base_nature = col("naturecompte").isin(
        ["001", "002", "003", "004", "005", "006", "010", "011", "014"]
    )
    return (
        compte.filter(is_base_nature)
        .select("numerotiers", "partitiondate")
        .drop_duplicates()
        .cache()
    )


def select_audit_log_event(audit_log_event: DataFrame) -> DataFrame:
    """
    Sélectionne les événements des journaux d'audit.

    Cette fonction extrait les colonnes principales des journaux d'audit, incluant
    l'utilisateur, la date et le type d'événement.
    """
    return audit_log_event.select(
        "actor", "datecreated", "eventname"
    ).drop_duplicates()


def select_utilisateur(df: DataFrame) -> DataFrame:
    """
    Sélectionne les utilisateurs actifs.

    Cette fonction extrait les colonnes principales des utilisateurs, comme le type de
    profil et le nom d'utilisateur.
    """
    return df.select("typeprofile", "username").drop_duplicates()


def select_audit(df: DataFrame) -> DataFrame:
    """
    Sélectionne et formate les événements d'audit.

    Cette fonction décode les descriptions des événements d'audit, extrait les informations
    clés, et les reformate pour une meilleure lisibilité.
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
                to_date(col("Date de connexion"), "dd-Mm_yyyy"),
                col("heure de connexion"),
            ),
        )
        .select("numerotiers", "user", "action", "date_connexion")
    )


def select_mouvement_comptable_operation(df: DataFrame) -> DataFrame:
    """
    Sélectionne les opérations comptables.

    Cette fonction extrait les colonnes principales des mouvements comptables,
    notamment le code opération, le numéro de compte, et la date de l'opération.
    """
    return df.select("codeoperation", "numerodecompte", "dateoperation")


def select_digital_operations(df: DataFrame) -> DataFrame:
    """
    Sélectionne les opérations digitales.

    Cette fonction identifie les opérations digitales spécifiques, comme les virements,
    paiements de factures, mises à disposition, et dotations, et les classe par type d'opération.
    """
    is_digital = col("canal") == "Digital"
    is_operation = col("famille") == "Operation"
    is_digital_operation = col("sous_categorie_id").isin(
        "MOY_MAD",
        "PRO_FAT",
        "TRA_DOT",
        "TRA_VIR_DIFF",
        "TRA_VIR_INS",
        "TRA_VIR_MUL",
        "TRA_VIR_PER",
        "TRA_VIR_RTGS",
        "TRA_VIR_STD",
    )
    return (
        df.filter(is_digital & is_operation & is_digital_operation)
        .withColumn(
            "type_operation",
            when(col("sous_categorie_id") == "TRA_DOT", "activation_dotation")
            .when(col("sous_categorie_id") == "PRO_FAT", "paiement_facture")
            .when(col("sous_categorie_id") == "MOY_MAD", "mise_a_disposition")
            .when(
                col("sous_categorie_id").isin(
                    [
                        "TRA_VIR_DIFF",
                        "TRA_VIR_INS",
                        "TRA_VIR_MUL",
                        "TRA_VIR_PER",
                        "TRA_VIR_RTGS",
                        "TRA_VIR_STD",
                    ]
                ),
                "virement",
            ),
        )
        .withColumnRenamed("cod", "codeoperation")
        .select("codeoperation", "type_operation")
    )


def get_connection_details(df: DataFrame) -> DataFrame:
    """
    Calcule les détails de connexion pour les clients.

    Cette fonction agrège les connexions des clients sur le mois en cours, incluant la
    dernière connexion et le nombre de connexions distinctes.
    """
    is_in_month = to_date(col("connectiondate")).between(
        col("first_day_month"), col("partitiondate")
    )
    return (
        df.withColumn("first_day_month", date_trunc("MM", col("partitiondate")))
        .groupBy("numerotiers")
        .agg(
            max(when(is_in_month, col("connectiondate"))).alias(
                "derniere_connexion"
            ),
            count_distinct(when(is_in_month, col("connectiondate"))).alias(
                "nb_connexion_mois"
            ),
        )
        .na.fill(value=0, subset=["nb_connexion_mois"])
    )


def get_operation_details(df: DataFrame) -> DataFrame:
    """
    Calcule les détails des opérations pour les clients.

    Cette fonction agrège les opérations digitales des clients sur le mois en cours,
    par type d'opération, incluant le nombre d'opérations et la dernière opération.
    """
    is_in_month = to_date(col("dateoperation")).between(
        col("first_day_month"), col("partitiondate")
    )
    return (
        df.withColumn("first_day_month", date_trunc("MM", col("partitiondate")))
        .groupBy("numerotiers")
        .pivot(
            "type_operation",
            values=[
                "activation_dotation",
                "mise_a_disposition",
                "paiement_facture",
                "virement",
            ],
        )
        .agg(
            max(when(is_in_month, col("dateoperation"))).alias("derniere_operation"),
            count(when(is_in_month, col("dateoperation"))).alias(
                "nb_operation_mois"
            ),
        )
        .na.fill(
            value=0,
            subset=[
                "activation_dotation_nb_operation_mois",
                "mise_a_disposition_nb_operation_mois",
                "paiement_facture_nb_operation_mois",
                "virement_nb_operation_mois",
            ],
        )
    )


def build_mycdm_mobile(
    audit_log_event: DataFrame, utilisateur: DataFrame, base_client: DataFrame
) -> DataFrame:
    """
    Construit les connexions MY CDM Mobile.

    Cette fonction agrège les connexions réussies depuis des terminaux mobiles MY CDM
    et calcule des métriques comme la dernière connexion et le nombre de connexions mensuelles.

    Arguments :
        audit_log_event : DataFrame contenant les événements des journaux d'audit.
        utilisateur : DataFrame contenant les informations des utilisateurs.
        base_client : DataFrame contenant les clients actifs.

    Retourne :
        Un DataFrame avec les connexions MY CDM Mobile des clients.
    """
    filtered_events = select_audit_log_event(audit_log_event).filter(
        col("eventname") == "ConnexionOK"
    )
    filtered_users = select_utilisateur(utilisateur).filter(
        col("typeprofile") == "CL"
    )

    connected_users = filtered_events.join(
        filtered_users, filtered_events["actor"] == filtered_users["username"]
    )

    return (
        base_client.join(
            connected_users,
            connected_users["actor"] == base_client["numerotiers"],
            "left",
        )
        .withColumnRenamed("datecreated", "connectiondate")
        .select("numerotiers", "connectiondate", "partitiondate")
        .transform(get_connection_details)
        .withColumnRenamed("derniere_connexion", "derniere_connexion_mycdm_mobile")
        .withColumnRenamed("nb_connexion_mois", "nb_connexion_mois_mycdm_mobile")
        .select(
            "numerotiers",
            "derniere_connexion_mycdm_mobile",
            "nb_connexion_mois_mycdm_mobile",
        )
    )


def build_mycdm_web(audit: DataFrame, base_client: DataFrame) -> DataFrame:
    """
    Construit les connexions MY CDM Web.

    Cette fonction calcule des métriques de connexion pour les clients via le portail MY CDM Web,
    comme la dernière connexion et le nombre de connexions mensuelles.

    Arguments :
        audit : DataFrame contenant les données d'audit.
        base_client : DataFrame contenant les clients actifs.

    Retourne :
        Un DataFrame avec les connexions MY CDM Web des clients.
    """
    filtered_events = select_audit(audit).filter(
        col("action") == "Authentification d'un client"
    )

    return (
        base_client.join(filtered_events, ["numerotiers"], "left")
        .withColumnRenamed("date_connexion", "connectiondate")
        .select("numerotiers", "connectiondate", "partitiondate")
        .transform(get_connection_details)
        .withColumnRenamed("derniere_connexion", "derniere_connexion_mycdm_web")
        .withColumnRenamed("nb_connexion_mois", "nb_connexion_mois_mycdm_web")
        .select(
            "numerotiers",
            "derniere_connexion_mycdm_web",
            "nb_connexion_mois_mycdm_web",
        )
    )


def build_mycdm_operation(
    mouvementcomptable: DataFrame,
    ref_operation: DataFrame,
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit les opérations digitales MY CDM.

    Cette fonction calcule des métriques relatives aux opérations digitales des clients,
    telles que le nombre et la dernière opération pour chaque type (virement, paiement de facture, etc.).

    Arguments :
        mouvementcomptable : DataFrame contenant les mouvements comptables.
        ref_operation : DataFrame contenant les références d'opérations.
        compte_client : DataFrame contenant les informations des comptes des clients.
        base_client : DataFrame contenant les clients actifs.

    Retourne :
        Un DataFrame avec les opérations digitales MY CDM des clients.
    """
    digital_operations = select_digital_operations(ref_operation)
    mouvementcomptable = select_mouvement_comptable_operation(mouvementcomptable)

    compte_client_selected = compte_client.select("numerotiers", "numerodecompte")
    realised_digital_operations = (
        mouvementcomptable.join(digital_operations, ["codeoperation"])
        .join(compte_client_selected, ["numerodecompte"])
        .select("numerotiers", "type_operation", "dateoperation")
    )
    return (
        base_client.join(realised_digital_operations, ["numerotiers"], "left")
        .select("numerotiers", "type_operation", "dateoperation", "partitiondate")
        .transform(get_operation_details)
        .select(
            "numerotiers",
            "activation_dotation_derniere_operation",
            "activation_dotation_nb_operation_mois",
            "mise_a_disposition_derniere_operation",
            "mise_a_disposition_nb_operation_mois",
            "paiement_facture_derniere_operation",
            "paiement_facture_nb_operation_mois",
            "virement_derniere_operation",
            "virement_nb_operation_mois",
        )
    )


def build_mycdm_score(
    audit_log_event: DataFrame,
    utilisateur: DataFrame,
    audit: DataFrame,
    mouvementcomptable: DataFrame,
    ref_operation: DataFrame,
    compte_client: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Construit les scores MY CDM.

    Cette fonction calcule un score global MY CDM basé sur les connexions (Mobile et Web)
    et les transactions digitales (opérations MY CDM) des clients.

    Arguments :
        audit_log_event : DataFrame contenant les événements des journaux d'audit.
        utilisateur : DataFrame contenant les informations des utilisateurs.
        audit : DataFrame contenant les données d'audit.
        mouvementcomptable : DataFrame contenant les mouvements comptables.
        ref_operation : DataFrame contenant les références d'opérations.
        compte_client : DataFrame contenant les informations des comptes des clients.
        partition_date : Date de partition utilisée pour les calculs.

    Retourne :
        Un DataFrame avec les scores MY CDM pour chaque client.
    """
    base_client = build_base_client(compte_client)
    compte_client = build_open_account(compte_client)
    mycdm_mobile = build_mycdm_mobile(audit_log_event, utilisateur, base_client)
    mycdm_web = build_mycdm_web(audit, base_client)
    mouvementcomptable = get_history_from(
        mouvementcomptable, partition_date, months=0, full_period=True
    )
    mycdm_operation = build_mycdm_operation(
        mouvementcomptable, ref_operation, compte_client, base_client
    )

    has_mobile_connection = col("nb_connexion_mois_mycdm_mobile") > 0
    has_web_connection = col("nb_connexion_mois_mycdm_web") > 0
    has_dotation = col("activation_dotation_nb_operation_mois") > 0
    has_disposition = col("mise_a_disposition_nb_operation_mois") > 0
    has_facture = col("paiement_facture_nb_operation_mois") > 0
    has_virement = col("virement_nb_operation_mois") > 0

    return (
        mycdm_mobile.join(mycdm_web, ["numerotiers"])
        .join(mycdm_operation, ["numerotiers"])
        .withColumn(
            "score_connexion",
            when(has_mobile_connection | has_web_connection, 0.5).otherwise(0),
        )
        .withColumn(
            "score_transaction_digital",
            when(
                has_dotation | has_disposition | has_facture | has_virement, 0.5
            ).otherwise(0),
        )
        .withColumn(
            "score_mycdm", col("score_connexion") + col("score_transaction_digital")
        )
        .select(
            "numerotiers",
            "derniere_connexion_mycdm_mobile",
            "nb_connexion_mois_mycdm_mobile",
            "derniere_connexion_mycdm_web",
            "nb_connexion_mois_mycdm_web",
            "activation_dotation_derniere_operation",
            "activation_dotation_nb_operation_mois",
            "mise_a_disposition_derniere_operation",
            "mise_a_disposition_nb_operation_mois",
            "paiement_facture_derniere_operation",
            "paiement_facture_nb_operation_mois",
            "virement_derniere_operation",
            "virement_nb_operation_mois",
            "score_connexion",
            "score_transaction_digital",
            "score_mycdm",
        )
    )


# Palier 4
def select_ressource_emploi(ressource_emploi: DataFrame) -> DataFrame:
    """
    Sélectionne et renomme les colonnes des ressources et emplois.

    Cette fonction extrait les colonnes principales des ressources et emplois, telles que le numéro de tiers,
    le numéro de compte, le solde en devise, et le clé primaire des ressources et emplois.
    """
    return ressource_emploi.withColumnRenamed(
        "fk_mapressourceemploi", "pk_mapressourceemploi"
    ).select(
        "numerotiers",
        "numerodecompte",
        "soldejourdb_devise",
        "pk_mapressourceemploi",
    )


def select_map_ressource_emploi(df: DataFrame) -> DataFrame:
    return df.select(
        "pk_mapressourceemploi", "famille_tdb_banque", "type_ressource_emploi"
    )


def select_contrat_produit(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("etatproduit", "code_etat_contrat").select(
        "numerotiers", "numerocarte", "codeproduit", "code_etat_contrat"
    )


def select_etat_contrat(df: DataFrame) -> DataFrame:
    return df.select("code_etat_contrat", "etat_contrat")


def build_valid_contract(
    contrat_produit: DataFrame, etat_contrat: DataFrame
) -> DataFrame:
    """
    Construit les contrats valides.

    Cette fonction filtre les contrats produits pour inclure uniquement ceux ayant un état valide ("VALIDE"),
    en se basant sur le mapping des états des contrats.
    """
    contrat_produit = select_contrat_produit(contrat_produit)

    is_valid_state = col("etat_contrat") == "VALIDE"
    etat_contrat = select_etat_contrat(etat_contrat)
    valid_state = etat_contrat.filter(is_valid_state)
    return contrat_produit.join(valid_state, ["code_etat_contrat"]).select(
        "numerotiers", "numerocarte", "codeproduit"
    )


def select_gamme_assurance(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "gamme",
            when(col("gamme").contains("Prévoyance"), "prevoyance").otherwise(
                lower(col("gamme"))
            ),
        )
        .withColumnRenamed("code_produit", "codeproduit")
        .select("gamme", "codeproduit")
    )


def select_opcvm(df):
    return df.select("numerodecompte", "codevaleur", "encours_s")


def build_base_ressource_emploi(
    ressource_emploi: DataFrame,
    map_ressource_emploi: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit la base des ressources et emplois.

    Cette fonction identifie les produits des catégories consommation, immobilier, RAT,
    et REP dans les ressources et emplois, et calcule le nombre de produits par catégorie pour chaque client.

    Arguments :
        ressource_emploi : DataFrame contenant les ressources et emplois.
        map_ressource_emploi : DataFrame mappant les types de ressources et emplois.
        base_client : DataFrame contenant les clients actifs.

    Retourne :
        Un DataFrame avec les catégories et le nombre de produits par client.
    """
    has_credit_greater_than_100 = col("soldejourdb_devise") >= 100
    is_conso = col("famille_tdb_banque") == "Consommation"
    is_immo = col("famille_tdb_banque") == "Immobilier"
    is_rat = col("type_ressource_emploi") == "RAT"
    is_rep = col("type_ressource_emploi") == "REP"

    ressource_emploi = select_ressource_emploi(ressource_emploi)

    map_ressource_emploi = select_map_ressource_emploi(map_ressource_emploi)
    filtered_map_ressource_emploi = map_ressource_emploi.filter(
        is_conso | is_immo | is_rat | is_rep
    )
    filtered_map_ressource_emploi = filtered_map_ressource_emploi.withColumn(
        "categorie",
        when(is_conso, "conso")
        .when(is_immo, "immo")
        .when(is_rat, "rat")
        .when(is_rep, "rep"),
    )

    ressource_emploi_type = (
        ressource_emploi.join(
            filtered_map_ressource_emploi, ["pk_mapressourceemploi"]
        )
        .filter(
            (has_credit_greater_than_100 & (is_conso | is_immo)) | is_rat | is_rep
        )
        .select("numerotiers", "type_ressource_emploi", "categorie")
    )
    return (
        base_client.join(ressource_emploi_type, ["numerotiers"], "left")
        .groupBy("numerotiers")
        .pivot("categorie", values=["conso", "immo", "rat", "rep"])
        .agg(count(col("type_ressource_emploi")).alias("nb_produit"))
        .na.fill(value=0, subset=["conso", "immo", "rat", "rep"])
        .withColumnRenamed("conso", "nb_produit_conso")
        .withColumnRenamed("immo", "nb_produit_immo")
        .withColumnRenamed("rat", "nb_produit_rat")
        .withColumnRenamed("rep", "nb_produit_rep")
        .select(
            "numerotiers",
            "nb_produit_conso",
            "nb_produit_immo",
            "nb_produit_rat",
            "nb_produit_rep",
        )
    )


def build_score_consommation(
    base_ressource_emploi: DataFrame,
) -> DataFrame:
    """
    Construit le score de consommation.

    Cette fonction attribue un score basé sur la présence d'au moins un produit de consommation
    dans les ressources et emplois du client.

    Arguments :
        base_ressource_emploi : DataFrame contenant les informations des ressources et emplois.

    Retourne :
        Un DataFrame avec le score de consommation pour chaque client.
    """
    has_at_least_one_product = col("nb_produit_conso") > 0
    return base_ressource_emploi.withColumn(
        "score_conso", when(has_at_least_one_product, 1).otherwise(0)
    ).select("numerotiers", "nb_produit_conso", "score_conso")


def build_score_immobilier(base_ressource_emploi: DataFrame) -> DataFrame:
    """
    Construit le score immobilier.

    Cette fonction attribue un score basé sur la présence d'au moins un produit immobilier
    dans les ressources et emplois du client.

    Arguments :
        base_ressource_emploi : DataFrame contenant les informations des ressources et emplois.

    Retourne :
        Un DataFrame avec le score immobilier pour chaque client.
    """
    has_at_least_one_product = col("nb_produit_immo") > 0
    return base_ressource_emploi.withColumn(
        "score_immo", when(has_at_least_one_product, 1).otherwise(0)
    ).select("numerotiers", "nb_produit_immo", "score_immo")


def build_score_assurance(
    valid_contrat_produit: DataFrame,
    gamme_assurance: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit le score d'assurance.

    Cette fonction calcule un score d'assurance basé sur le nombre de produits dans les gammes d'assurance
    ("dommage", "prévoyance", "assistance"). Le score est attribué selon le nombre total de produits détenus.

    Arguments :
        valid_contrat_produit : DataFrame contenant les contrats valides.
        gamme_assurance : DataFrame contenant les informations des gammes d'assurance.
        base_client : DataFrame contenant la base des clients actifs.

    Retourne :
        Un DataFrame avec le score d'assurance pour chaque client.
    """
    is_assurance_gamme = col("gamme").isin(["dommage", "prevoyance", "assistance"])
    gamme_assurance = select_gamme_assurance(gamme_assurance)
    filtered_assurance = gamme_assurance.filter(is_assurance_gamme)

    has_more_than_one_product = col("total_produit_assurance") > 1
    has_one_product = col("total_produit_assurance") == 1

    produit_assurance = valid_contrat_produit.join(
        filtered_assurance, ["codeproduit"]
    ).select("numerotiers", "numerocarte", "codeproduit", "gamme")

    return (
        base_client.join(produit_assurance, ["numerotiers"], "left")
        .groupBy("numerotiers", "gamme", "codeproduit")
        .agg(count_distinct(col("numerocarte")).alias("nb_contrat"))
        .groupBy("numerotiers")
        .pivot("gamme", values=["assistance", "dommage", "prevoyance"])
        .agg(sum(col("nb_contrat")))
        .na.fill(value=0, subset=["assistance", "dommage", "prevoyance"])
        .withColumn(
            "total_produit_assurance",
            col("dommage") + col("prevoyance") + col("assistance"),
        )
        .withColumn(
            "score_assurance",
            when(has_more_than_one_product, 1)
            .when(has_one_product, 0.5)
            .otherwise(0),
        )
        .withColumnRenamed("dommage", "nb_produit_dommage")
        .withColumnRenamed("prevoyance", "nb_produit_prevoyance")
        .withColumnRenamed("assistance", "nb_produit_assistance")
        .select(
            "numerotiers",
            "nb_produit_dommage",
            "nb_produit_prevoyance",
            "nb_produit_assistance",
            "total_produit_assurance",
            "score_assurance",
        )
    )


def select_encours_assurance(df: DataFrame) -> DataFrame:
    """
    Sélectionne et reformate les encours d'assurance.

    Cette fonction extrait les données des encours d'assurance et les reformate pour inclure
    les types d'assurance et les encours associés.

    Arguments :
        df : DataFrame contenant les encours d'assurance.

    Retourne :
        Un DataFrame avec les colonnes sélectionnées et formatées.
    """
    return (
        df.withColumnRenamed("numero_adhesion", "numerocarte")
        .withColumn(
            "type_assurance",
            when(col("entreprise") == "AXA ASSURANCE", "axa")
            .when(col("entreprise") == "RMA WATANYA", "rma")
            .when(col("entreprise") == "SAHAM ASSURANCE", "saham")
            .when(col("entreprise") == "WAFA ASSURANCE", "wafa"),
        )
        .select("type_assurance", "numerocarte", "code_produit", "encours_s")
    )


def build_base_stock_assurance(
    encours_assurance: DataFrame,
    valid_contrat_produit: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit la base des stocks d'assurance.

    Cette fonction filtre les encours d'assurance pour inclure uniquement ceux avec des montants supérieurs à zéro,
    et calcule le nombre total de contrats d'assurance par type pour chaque client.

    Arguments :
        encours_assurance : DataFrame des encours d'assurance.
        valid_contrat_produit : DataFrame contenant les contrats valides.
        base_client : DataFrame contenant la base des clients actifs.

    Retourne :
        Un DataFrame avec les informations des stocks d'assurance pour chaque client.
    """
    is_encours_greater_than_zero = col("encours_s") > 0
    encours_assurance = select_encours_assurance(encours_assurance).filter(
        is_encours_greater_than_zero
    )

    valid_assurance_contrat = valid_contrat_produit.join(
        encours_assurance, ["numerocarte"]
    ).select("numerotiers", "numerocarte", "code_produit", "type_assurance")

    return (
        base_client.join(valid_assurance_contrat, ["numerotiers"], "left")
        .groupBy("numerotiers", "type_assurance", "code_produit")
        .agg(count_distinct(col("numerocarte")).alias("nb_contrat"))
        .groupBy("numerotiers")
        .pivot("type_assurance", values=["axa", "wafa", "rma", "saham"])
        .agg(sum(col("nb_contrat")))
        .na.fill(value=0, subset=["axa", "wafa", "rma", "saham"])
        .withColumn(
            "stock_assurance",
            col("axa") + col("wafa") + col("rma") + col("saham"),
        )
        .withColumnRenamed("axa", "nb_produit_axa")
        .withColumnRenamed("wafa", "nb_produit_wafa")
        .withColumnRenamed("rma", "nb_produit_rma")
        .withColumnRenamed("saham", "nb_produit_saham")
        .select(
            "numerotiers",
            "nb_produit_axa",
            "nb_produit_wafa",
            "nb_produit_rma",
            "nb_produit_saham",
            "stock_assurance",
        )
    )


def build_base_liberis_epargne(
    valid_contrat_produit: DataFrame,
    gamme_assurance: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit la base des produits Liberis épargne.

    Cette fonction filtre les produits d'épargne Liberis à partir des contrats valides,
    et calcule le nombre total de produits d'assurance épargne pour chaque client.

    Arguments :
        valid_contrat_produit : DataFrame contenant les contrats valides.
        gamme_assurance : DataFrame contenant les informations des gammes d'assurance.
        base_client : DataFrame contenant la base des clients actifs.

    Retourne :
        Un DataFrame avec les informations des produits d'assurance épargne pour chaque client.
    """
    is_produit_epargne = col("gamme") == "epargne"
    gamme_assurance = select_gamme_assurance(gamme_assurance)
    gamme_assurance = gamme_assurance.filter(is_produit_epargne)

    # is_liberis_epargne = col("codeproduit").isin(
    #     ["LER1", "LEE1", "LEP1", "LPR1", "LPP1"]
    # )

    valid_contrat_liberis_epargne = valid_contrat_produit.join(
        gamme_assurance, ["codeproduit"]
    )

    return (
        base_client.join(valid_contrat_liberis_epargne, ["numerotiers"], "left")
        .groupBy("numerotiers", "codeproduit")
        .agg(count_distinct(col("numerocarte")).alias("nb_contrat"))
        .groupBy("numerotiers")
        .agg(sum(col("nb_contrat")).alias("nb_produit_assurance_epargne"))
        .na.fill(value=0, subset=["nb_produit_assurance_epargne"])
        .select(
            "numerotiers",
            "nb_produit_assurance_epargne",
        )
    )


def build_base_csc(
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit la base des comptes CSC.

    Cette fonction identifie les comptes CSC à partir des comptes clients actifs,
    et calcule le nombre total de produits CSC pour chaque client.

    Arguments :
        compte_client : DataFrame des comptes clients.
        base_client : DataFrame contenant la base des clients actifs.

    Retourne :
        Un DataFrame avec les informations des produits CSC pour chaque client.
    """
    is_csc_nature = col("naturecompte").isin(["010", "011", "014"])
    compte_client_selected = compte_client.filter(is_csc_nature).select(
        "numerotiers", "naturecompte"
    )
    return (
        base_client.join(compte_client_selected, ["numerotiers"], "left")
        .withColumn("is_csc", when(is_csc_nature, 1).otherwise(0))
        .groupBy("numerotiers")
        .agg(sum(col("is_csc")).alias("nb_produit_csc"))
        .select(
            "numerotiers",
            "nb_produit_csc",
        )
    )


def build_base_opcvm(
    opcvm: DataFrame,
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit la base des OPCVM.

    Cette fonction calcule le nombre total de produits OPCVM pour chaque client, à partir des encours supérieurs à zéro.

    Arguments :
        opcvm : DataFrame des OPCVM.
        compte_client : DataFrame des comptes clients.
        base_client : DataFrame contenant la base des clients actifs.

    Retourne :
        Un DataFrame avec les informations des produits OPCVM pour chaque client.
    """
    is_encours_greater_than_zero = col("encours_s") > 0
    opcvm = select_opcvm(opcvm)

    compte_client_selected = compte_client.select("numerotiers", "numerodecompte")

    nb_opcvm_per_tiers = (
        opcvm.filter(is_encours_greater_than_zero)
        .groupBy("numerodecompte")
        .agg(count_distinct(col("codevaleur")).alias("nb_produit_opcvm"))
        .join(compte_client_selected, ["numerodecompte"])
        .select("numerotiers", "nb_produit_opcvm")
        .groupBy("numerotiers")
        .agg(sum(col("nb_produit_opcvm")).alias("nb_produit_opcvm"))
    )

    return (
        base_client.join(nb_opcvm_per_tiers, ["numerotiers"], "left")
        .na.fill(0, subset=["nb_produit_opcvm"])
        .select("numerotiers", "nb_produit_opcvm")
    )


def build_score_epargne(
    valid_contrat_produit: DataFrame,
    opcvm: DataFrame,
    encours_assurance: DataFrame,
    base_ressource_emploi: DataFrame,
    gamme_assurance: DataFrame,
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    """
    Construit le score d'épargne.

    Cette fonction agrège plusieurs types de produits d'épargne (CSC, Liberis, OPCVM, assurance vie)
    et calcule un score global d'épargne basé sur le nombre total de produits détenus.

    Arguments :
        valid_contrat_produit : DataFrame contenant les contrats valides.
        opcvm : DataFrame des OPCVM.
        encours_assurance : DataFrame des encours d'assurance.
        base_ressource_emploi : DataFrame des ressources et emplois.
        gamme_assurance : DataFrame des gammes d'assurance.
        compte_client : DataFrame des comptes clients.
        base_client : DataFrame contenant la base des clients actifs.

    Retourne :
        Un DataFrame avec le score d'épargne pour chaque client.
    """
    liberis_epargne = build_base_liberis_epargne(
        valid_contrat_produit, gamme_assurance, base_client
    )
    csc = build_base_csc(compte_client, base_client)
    opcvm = build_base_opcvm(opcvm, compte_client, base_client)
    assurance_vie = build_base_stock_assurance(
        encours_assurance,
        valid_contrat_produit,
        base_client,
    )
    base_ressource_emploi = base_ressource_emploi.select(
        "numerotiers", "nb_produit_rat", "nb_produit_rep"
    )

    has_more_than_one_product = col("total_produit_epargne") > 1
    has_one_product = col("total_produit_epargne") == 1

    return (
        liberis_epargne.join(csc, ["numerotiers"])
        .join(opcvm, ["numerotiers"])
        .join(assurance_vie, ["numerotiers"])
        .join(base_ressource_emploi, ["numerotiers"])
        .withColumn(
            "nb_produit_ressource", col("nb_produit_rat") + col("nb_produit_rep")
        )
        .withColumn(
            "total_produit_epargne",
            col("nb_produit_csc")
            + col("nb_produit_assurance_epargne")
            + col("nb_produit_opcvm")
            + col("nb_produit_ressource"),
        )
        .withColumn(
            "score_epargne",
            when(has_more_than_one_product, 1)
            .when(has_one_product, 0.5)
            .otherwise(0),
        )
    )


def build_equipement_score(
    contrat_produit: DataFrame,
    etat_contrat: DataFrame,
    gamme_assurance: DataFrame,
    opcvm: DataFrame,
    encours_assurance: DataFrame,
    ressource_emploi: DataFrame,
    map_ressource_emploi: DataFrame,
    compte_client: DataFrame,
) -> DataFrame:
    """
    Construit le score d'équipement.

    Cette fonction combine plusieurs sous-scores (consommation, immobilier, épargne, assurance)
    pour produire un score global d'équipement bancaire pour chaque client.

    Arguments :
        contrat_produit : DataFrame contenant les contrats produits.
        etat_contrat : DataFrame contenant les états des contrats.
        gamme_assurance : DataFrame des gammes d'assurance.
        opcvm : DataFrame des OPCVM.
        encours_assurance : DataFrame des encours d'assurance.
        ressource_emploi : DataFrame des ressources et emplois.
        map_ressource_emploi : DataFrame de mappage des ressources et emplois.
        compte_client : DataFrame des comptes clients.

    Retourne :
        Un DataFrame avec le score d'équipement pour chaque client.
    """
    compte_client = build_open_account(compte_client)
    base_client = build_base_client(compte_client)
    base_ressource_emploi = build_base_ressource_emploi(
        ressource_emploi, map_ressource_emploi, base_client
    )
    valid_contrat_produit = build_valid_contract(contrat_produit, etat_contrat)

    score_epargne = build_score_epargne(
        valid_contrat_produit,
        opcvm,
        encours_assurance,
        base_ressource_emploi,
        gamme_assurance,
        compte_client,
        base_client,
    )
    score_assurance = build_score_assurance(
        valid_contrat_produit, gamme_assurance, base_client
    )
    score_conso = build_score_consommation(base_ressource_emploi)
    score_immo = build_score_immobilier(base_ressource_emploi)
    return (
        score_conso.join(score_immo, ["numerotiers"])
        .join(score_assurance, ["numerotiers"])
        .join(score_epargne, ["numerotiers"])
        .withColumn(
            "score_equipement",
            least(
                lit(1.5),
                col("score_conso")
                + col("score_immo")
                + col("score_assurance")
                + col("score_epargne"),
            ),
        )
        .select(
            "numerotiers",
            "nb_produit_conso",
            "score_conso",
            "nb_produit_immo",
            "score_immo",
            "nb_produit_dommage",
            "nb_produit_prevoyance",
            "nb_produit_assistance",
            "total_produit_assurance",
            "score_assurance",
            "nb_produit_assurance_epargne",
            "nb_produit_csc",
            "nb_produit_opcvm",
            "nb_produit_axa",
            "nb_produit_wafa",
            "nb_produit_rma",
            "nb_produit_saham",
            "stock_assurance",
            "nb_produit_rep",
            "nb_produit_rat",
            "nb_produit_ressource",
            "total_produit_epargne",
            "score_epargne",
            "score_equipement",
        )
    )


def select_niveau_activite_client(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("score_flux_entrants", "score_flag_2000_m")
        .withColumn(
            "score_flag_2000_m", col("score_flag_2000_m").cast(IntegerType())
        )
        .withColumn(
            "score_transaction",
            regexp_replace(col("score_transaction"), ",", ".").cast(FloatType()),
        )
        .withColumn(
            "score_mycdm",
            regexp_replace(col("score_mycdm"), ",", ".").cast(FloatType()),
        )
        .withColumn(
            "score_equipement",
            regexp_replace(col("score_equipement"), ",", ".").cast(FloatType()),
        )
        .withColumn(
            "total", regexp_replace(col("total"), ",", ".").cast(FloatType())
        )
        .withColumn("niveau_activite", col("niveau_activite").cast(IntegerType()))
        .select(
            "score_flag_2000_m",
            "score_transaction",
            "score_mycdm",
            "score_equipement",
            "total",
            "niveau_activite",
            "reco_concat",
        )
    )


def build_intensite_bancaire_score(
    monthly_totals: DataFrame,
    transaction_score: DataFrame,
    mydcm_score: DataFrame,
    equipement_score: DataFrame,
    niveau_activite_client: DataFrame,
) -> DataFrame:
    """
    Construit le score d'intensité bancaire.

    Cette fonction agrège les scores provenant de différentes sources (totaux mensuels, transactions, scores MyCDM,
    scores d'équipement) et les combine avec le niveau d'activité du client pour calculer l'intensité bancaire.

    Arguments :
        monthly_totals : DataFrame contenant les totaux mensuels par client.
        transaction_score : DataFrame contenant les scores de transaction par client.
        mydcm_score : DataFrame contenant les scores liés à MyCDM par client.
        equipement_score : DataFrame contenant les scores d'équipement par client.
        niveau_activite_client : DataFrame contenant le niveau d'activité et les scores consolidés du client.

    Retourne :
        Un DataFrame combiné avec les informations d'intensité bancaire pour chaque client.
    """
    niveau_activite_client = select_niveau_activite_client(niveau_activite_client)
    return (
        monthly_totals.join(transaction_score, ["numerotiers"], "left")
        .join(mydcm_score, ["numerotiers"], "left")
        .join(equipement_score, ["numerotiers"], "left")
        .join(
            niveau_activite_client,
            [
                "score_flag_2000_m",
                "score_transaction",
                "score_mycdm",
                "score_equipement",
            ],
            "left",
        )
    )
