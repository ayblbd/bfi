from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    count_distinct,
    date_trunc,
    from_json,
    max,
    to_date,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType


def build_base_client(compte: DataFrame) -> DataFrame:
    is_base_nature = col("naturecompte").isin(
        ["001", "002", "003", "004", "005", "006", "010", "011", "014"]
    )
    return (
        compte.filter(is_base_nature)
        .select("numerotiers", "partitiondate")
        .drop_duplicates()
        .cache()
    )


def build_open_account(df):
    return df.filter(col("statutcompte") != "CLOTURE")


def select_audit_log_event(audit_log_event: DataFrame) -> DataFrame:
    return audit_log_event.select(
        "actor", "datecreated", "eventname"
    ).drop_duplicates()


def select_utilisateur(df: DataFrame) -> DataFrame:
    return df.select("typeprofile", "username").drop_duplicates()


def select_audit(df: DataFrame) -> DataFrame:
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
    return df.select("codeoperation", "numerodecompte", "dateoperation")


def select_digital_operations(df: DataFrame) -> DataFrame:
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
    digital_operations = select_digital_operations(ref_operation)
    mouvementcomptable = select_mouvement_comptable(mouvementcomptable)

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


def build_mycdm(
    audit_log_event: DataFrame,
    utilisateur: DataFrame,
    audit: DataFrame,
    mouvementcomptable: DataFrame,
    ref_operation: DataFrame,
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    mycdm_mobile = build_mycdm_mobile(audit_log_event, utilisateur, base_client)
    mycdm_web = build_mycdm_web(audit, base_client)
    mycdm_operation = build_mycdm_operation(
        mouvementcomptable, ref_operation, compte_client, base_client
    )

    return (
        mycdm_mobile.join(mycdm_web, ["numerotiers"])
        .join(mycdm_operation, ["numerotiers"])
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
        )
    )


def calculate_score_mycdm(df: DataFrame) -> DataFrame:
    has_mobile_connection = col("nb_connexion_mois_mycdm_mobile") > 0
    has_web_connection = col("nb_connexion_mois_mycdm_web") > 0
    has_dotation = col("activation_dotation_nb_operation_mois") > 0
    has_disposition = col("mise_a_disposition_nb_operation_mois") > 0
    has_facture = col("paiement_facture_nb_operation_mois") > 0
    has_virement = col("virement_nb_operation_mois") > 0

    return (
        df.withColumn(
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
