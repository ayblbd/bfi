import pandas as pd
from pyspark.sql.session import SparkSession

from src.transformation.digital import (
    aggregate_audit,
    aggregate_audit_log_event,
    build_client_e_corpo,
    build_digital_e_corpo,
    build_digital_my_cdm_web,
    join_audit_log_event,
    select_audit,
    select_information_tier,
    select_niveau_regroupement,
    select_relation_commerciale,
)
from tests.conftest import compare_dataframes


def test_select_information_tier(spark):
    information_tier = spark.createDataFrame(
        [
            ("ID1", "fdc1", "agence1", "actif", "marché1", "M", 2020),
            ("ID2", "fdc2", "agence2", "inactif", "marché2", "M", 2019),
            ("ID3", "fdc3", "agence3", "actif", "marché3", "M", 2021),
            ("ID4", "fdc4", "agence4", "inactif", "marché4", "M", 2018),
        ],
        [
            "numerotiers",
            "cd_fdc_gest",
            "agencegest",
            "statutclient",
            "marche",
            "nationalite",
            "anneedecreation",
        ],
    )
    actual = select_information_tier(information_tier)

    expected = pd.DataFrame(
        [
            ("ID1", "fdc1", "agence1", "actif", "marché1"),
            ("ID2", "fdc2", "agence2", "inactif", "marché2"),
            ("ID3", "fdc3", "agence3", "actif", "marché3"),
            ("ID4", "fdc4", "agence4", "inactif", "marché4"),
        ],
        columns=[
            "numerotiers",
            "cd_fdc_gest",
            "agencegest",
            "statutclient",
            "marche",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_select_niveau_regroupement(spark):
    niveau_regroupement = spark.createDataFrame(
        [
            ("agence1", "region1", "banque1", "Region A", "Banque A", "gest1"),
            ("agence2", "region2", "banque2", "Region B", "Banque B", "gest2"),
            ("agence3", "region3", "banque1", "Region C", "Banque A", "gest2"),
            ("agence4", "region4", "banque2", "Region D", "Banque B", "gest1"),
        ],
        [
            "codeagence",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "gestionnaire",
        ],
    )
    actual = select_niveau_regroupement(niveau_regroupement)

    expected = pd.DataFrame(
        [
            ("agence1", "region1", "banque1", "Region A", "Banque A"),
            ("agence2", "region2", "banque2", "Region B", "Banque B"),
            ("agence3", "region3", "banque1", "Region C", "Banque A"),
            ("agence4", "region4", "banque2", "Region D", "Banque B"),
        ],
        columns=["codeagence", "coderegion", "codebanque", "nomregion", "nombanque"],
    )
    compare_dataframes(actual, expected, sort_by=["codeagence"])


def test_select_relation_commerciale(spark):
    relation_commerciale = spark.createDataFrame(
        [
            (1, "RC001", "2024-01-01"),
            (2, "RC002", "2024-01-02"),
            (3, "RC003", "2024-01-03"),
        ],
        ["id", "identifiantrc", "date_entree_relation"],
    )

    actual = select_relation_commerciale(relation_commerciale)

    expected = pd.DataFrame(
        [
            ("RC001", 1, True),
            ("RC002", 2, True),
            ("RC003", 3, True),
        ],
        columns=[
            "numerotiers",
            "relation_commerciale_id",
            "statut_relation_commerciale",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_aggregate_audit_log_event(spark):
    df = spark.createDataFrame(
        [
            ("user1", "2024-10-01 08:00:00", "ConnexionOK", "2024-10-02"),
            ("user1", "2024-10-02 09:00:00", "ConnexionOK", "2024-10-02"),
            ("user1", "2024-11-02 09:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2024-10-01 10:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2024-10-03 11:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2022-10-03 11:00:00", "ConnexionOK", "2024-10-02"),
            ("user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        ["actor", "datecreated", "eventname", "partitiondate"],
    )

    actual = aggregate_audit_log_event(df)

    expected = pd.DataFrame(
        [
            ("user1", "2024-11-02 09:00:00", 1, 2, 2, "2024-10-02"),
            ("user2", "2024-10-03 11:00:00", 0, 2, 2, "2024-10-02"),
        ],
        columns=[
            "actor",
            "derniere_connexion",
            "nb_connexion_jour",
            "nb_connexion_semaine",
            "nb_connexion_mois",
            "partitiondate",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["actor"])


def test_join_audit_log_event(spark):
    audit_log_event = spark.createDataFrame(
        [
            ("user1", "2024-10-01 08:00:00", "ConnexionOK", "2024-10-02"),
            ("user1", "2024-10-02 09:00:00", "ConnexionOK", "2024-10-02"),
            ("user1", "2024-11-02 09:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2024-10-01 10:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2024-10-03 11:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2025-10-03 11:00:00", "ConnexionOK", "2024-10-02"),
            ("user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        ["actor", "datecreated", "eventname", "partitiondate"],
    )
    utilisateur = spark.createDataFrame(
        [
            (1, "user1"),
            (2, "user2"),
            (3, "user3"),
        ],
        ["id", "username"],
    )
    rattachement_abonne_contrat = spark.createDataFrame(
        [
            (1, 100),
            (2, 200),
            (3, 300),
        ],
        ["abonne_id", "contrat_id"],
    )

    contrat_abonnement = spark.createDataFrame(
        [
            (100, 1),
            (200, 2),
            (300, 3),
        ],
        ["contrat_id", "relation_commerciale_id"],
    )

    relation_commerciale = spark.createDataFrame(
        [
            (1, "ID1"),
            (2, "ID2"),
            (3, "ID3"),
        ],
        ["relation_commerciale_id", "numerotiers"],
    )

    actual = join_audit_log_event(
        audit_log_event,
        utilisateur,
        rattachement_abonne_contrat,
        contrat_abonnement,
        relation_commerciale,
    )

    expected = pd.DataFrame(
        [
            ("user1", "ID1", "2024-11-02 09:00:00", 1, 2, 2, "2024-10-02"),
            ("user2", "ID2", "2025-10-03 11:00:00", 0, 2, 2, "2024-10-02"),
        ],
        columns=[
            "actor",
            "numerotiers",
            "derniere_connexion",
            "nb_connexion_jour",
            "nb_connexion_semaine",
            "nb_connexion_mois",
            "partitiondate",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["actor"])


def test_build_client_e_corpo(spark: SparkSession):
    contrat_produit = spark.createDataFrame(
        [
            ("ID1", "EBAY", "12345", "actif", "2024-01-01"),
            ("ID1", "LM1", "12345", "actif", "2024-01-05"),
            ("ID2", "CN01", "12346", "inactif", "2024-01-02"),
            ("ID2", "MA1", "12346", "inactif", "2024-01-06"),
            ("ID3", "M01", "12345", "actif", "2024-01-03"),
        ],
        [
            "numerotiers",
            "codeproduit",
            "numerocontratinterne",
            "etatproduit",
            "datesouscription",
        ],
    )

    elt_contrat = spark.createDataFrame(
        [
            (
                12345,
                "1" * 150,
            ),
            (
                12346,
                "7" * 150,
            ),
        ],
        ["numerocontratinterne", "partiespecifique"],
    )

    relation_commerciale = spark.createDataFrame(
        [
            (1, "ID1", "2024-01-01"),
            (2, "ID2", "2024-01-02"),
            (3, "ID4", "2024-01-03"),
        ],
        ["id", "identifiantrc", "date_entree_relation"],
    )
    relation_commerciale = select_relation_commerciale(relation_commerciale)

    actual = build_client_e_corpo(
        contrat_produit,
        elt_contrat,
        relation_commerciale,
    )

    expected = pd.DataFrame(
        [
            ("ID1", "actif", "2024-01-01", True, "1"),
            ("ID2", "inactif", "2024-01-02", True, "7"),
            ("ID4", None, None, True, None),
        ],
        columns=[
            "numerotiers",
            "etatproduit",
            "datesouscription",
            "statut_e_corpo",
            "option",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_build_digital_e_corpo(spark: SparkSession):
    contrat_produit = spark.createDataFrame(
        [
            ("ID1", "EBAY", "12345", "actif", "2024-01-01"),
            ("ID1", "LM1", "12345", "actif", "2024-01-05"),
            ("ID2", "CN01", "12346", "inactif", "2024-01-02"),
            ("ID2", "MA1", "12346", "inactif", "2024-01-06"),
            ("ID3", "M01", "12345", "actif", "2024-01-03"),
        ],
        [
            "numerotiers",
            "codeproduit",
            "numerocontratinterne",
            "etatproduit",
            "datesouscription",
        ],
    )
    information_tier = spark.createDataFrame(
        [
            ("ID1", "fdc1", "agence1", "actif", "marché1", "M", 2020),
            ("ID2", "fdc2", "agence2", "inactif", "marché2", "M", 2019),
            ("ID3", "fdc3", "agence3", "actif", "marché3", "M", 2021),
            ("ID4", "fdc4", "agence4", "inactif", "marché4", "M", 2018),
        ],
        [
            "numerotiers",
            "cd_fdc_gest",
            "agencegest",
            "statutclient",
            "marche",
            "nationalite",
            "anneedecreation",
        ],
    )

    elt_contrat = spark.createDataFrame(
        [
            (
                12345,
                "1" * 150,
            ),
            (
                12346,
                "7" * 150,
            ),
        ],
        ["numerocontratinterne", "partiespecifique"],
    )

    niveau_regroupement = spark.createDataFrame(
        [
            ("agence1", "region1", "banque1", "Region A", "Banque A", "gest1"),
            ("agence2", "region2", "banque2", "Region B", "Banque B", "gest2"),
            ("agence3", "region3", "banque1", "Region C", "Banque A", "gest2"),
            ("agence4", "region4", "banque2", "Region D", "Banque B", "gest1"),
        ],
        [
            "codeagence",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "gestionnaire",
        ],
    )

    audit_log_event = spark.createDataFrame(
        [
            ("user1", "2024-10-01 08:00:00", "ConnexionOK", "2024-10-02"),
            ("user1", "2024-10-02 09:00:00", "ConnexionOK", "2024-10-02"),
            ("user1", "2024-11-02 09:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2024-10-01 10:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2024-10-03 11:00:00", "ConnexionOK", "2024-10-02"),
            ("user2", "2025-10-03 11:00:00", "ConnexionOK", "2024-10-02"),
            ("user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        ["actor", "datecreated", "eventname", "partitiondate"],
    )
    utilisateur = spark.createDataFrame(
        [
            (1, "user1"),
            (2, "user2"),
            (3, "user3"),
        ],
        ["id", "username"],
    )
    rattachement_abonne_contrat = spark.createDataFrame(
        [
            (1, 100),
            (2, 200),
            (3, 300),
        ],
        ["abonne_id", "contrat_id"],
    )

    contrat_abonnement = spark.createDataFrame(
        [
            (100, 1),
            (200, 2),
            (300, 3),
        ],
        ["contrat_id", "relation_commerciale_id"],
    )

    relation_commerciale = spark.createDataFrame(
        [
            (1, "ID1", "2024-01-01"),
            (2, "ID2", "2024-01-02"),
            (3, "ID3", "2024-01-03"),
        ],
        ["id", "identifiantrc", "date_entree_relation"],
    )

    actual = build_digital_e_corpo(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        elt_contrat,
        audit_log_event,
        relation_commerciale,
        contrat_abonnement,
        rattachement_abonne_contrat,
        utilisateur,
    )

    expected = pd.DataFrame(
        [
            (
                "ID3",
                "fdc3",
                "agence3",
                "region3",
                "banque1",
                "Region C",
                "Banque A",
                "actif",
                None,
                None,
                True,
                True,
                None,
                0,
                0,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
            ),
            (
                "ID2",
                "fdc2",
                "agence2",
                "region2",
                "banque2",
                "Region B",
                "Banque B",
                "inactif",
                "7",
                "Transactionnel Ecorpo",
                True,
                True,
                "2024-01-02",
                1,
                0,
                "inactif",
                "user2",
                "2025-10-03 11:00:00",
                0,
                2,
                2,
                "2024-10-02",
            ),
            (
                "ID1",
                "fdc1",
                "agence1",
                "region1",
                "banque1",
                "Region A",
                "Banque A",
                "actif",
                "1",
                "Consultation",
                True,
                True,
                "2024-01-01",
                0,
                1,
                "actif",
                "user1",
                "2024-11-02 09:00:00",
                1,
                2,
                2,
                "2024-10-02",
            ),
            (
                "ID4",
                "fdc4",
                "agence4",
                "region4",
                "banque2",
                "Region D",
                "Banque B",
                "inactif",
                None,
                None,
                False,
                False,
                None,
                0,
                0,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
            ),
        ],
        columns=[
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
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_select_audit(spark):
    audit = spark.createDataFrame(
        [
            (
                "ID1",
                "user1",
                "2024-10-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID1",
                "user1",
                "2024-11-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID2",
                "user2",
                "2024-10-01 10:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            ("ID3", "user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        ["id_tiers", "utilisateur", "date", "action", "partitiondate"],
    )

    actual = select_audit(audit)

    expected = pd.DataFrame(
        [
            (
                "ID1",
                "user1",
                "2024-10-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID1",
                "user1",
                "2024-11-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID2",
                "user2",
                "2024-10-01 10:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            ("ID3", "user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        columns=["numerotiers", "utilisateur", "date", "action", "partitiondate"],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_aggregate_audit(spark):
    df = spark.createDataFrame(
        [
            (
                "ID1",
                "user1",
                "2024-10-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID1",
                "user1",
                "2024-11-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID3",
                "user2",
                "2024-10-01 10:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID4",
                "user1",
                "2024-10-01 08:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID5",
                "user2",
                "2024-10-03 11:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID6",
                "user2",
                "2022-10-03 11:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            ("ID7", "user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        ["numerotiers", "utilisateur", "date", "action", "partitiondate"],
    )

    actual = aggregate_audit(df)

    expected = pd.DataFrame(
        [
            ("ID1", "user1", "2024-11-02 09:00:00", 1, 1, 1, "2024-10-02"),
            ("ID3", "user2", "2024-10-01 10:00:00", 0, 1, 1, "2024-10-02"),
            ("ID4", "user1", "2024-10-01 08:00:00", 0, 1, 1, "2024-10-02"),
            ("ID5", "user2", "2024-10-03 11:00:00", 0, 1, 1, "2024-10-02"),
            ("ID6", "user2", "2022-10-03 11:00:00", 0, 0, 0, "2024-10-02"),
        ],
        columns=[
            "numerotiers",
            "utilisateur",
            "derniere_connexion",
            "nb_connexion_jour",
            "nb_connexion_semaine",
            "nb_connexion_mois",
            "partitiondate",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_build_digital_my_cdm_web(spark: SparkSession):
    contrat_produit = spark.createDataFrame(
        [
            ("ID1", "EBAY", "12345", "actif", "2024-01-01"),
            ("ID1", "LM1", "12345", "actif", "2024-02-01"),
            ("ID2", "CN01", "12346", "inactif", "2022-02-01"),
            ("ID2", "MA1", "12346", "inactif", "2019-01-06"),
            ("ID3", "MA1", "12347", "actif", "2021-05-01"),
        ],
        [
            "numerotiers",
            "codeproduit",
            "numerocontratinterne",
            "etatproduit",
            "datesouscription",
        ],
    )
    information_tier = spark.createDataFrame(
        [
            ("ID1", "fdc1", "agence1", "actif", "marché1", "M", 2020),
            ("ID2", "fdc2", "agence2", "inactif", "marché2", "M", 2019),
            ("ID3", "fdc3", "agence3", "actif", "marché3", "M", 2021),
            ("ID4", "fdc4", "agence4", "inactif", "marché4", "M", 2018),
        ],
        [
            "numerotiers",
            "cd_fdc_gest",
            "agencegest",
            "statutclient",
            "marche",
            "nationalite",
            "anneedecreation",
        ],
    )

    elt_contrat = spark.createDataFrame(
        [
            (
                12345,
                "1" * 150,
            ),
            (
                12346,
                "5" * 150,
            ),
        ],
        ["numerocontratinterne", "partiespecifique"],
    )

    niveau_regroupement = spark.createDataFrame(
        [
            ("agence1", "region1", "banque1", "Region A", "Banque A", "gest1"),
            ("agence2", "region2", "banque2", "Region B", "Banque B", "gest2"),
            ("agence3", "region3", "banque1", "Region C", "Banque A", "gest2"),
            ("agence4", "region4", "banque2", "Region D", "Banque B", "gest1"),
        ],
        [
            "codeagence",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "gestionnaire",
        ],
    )

    audit = spark.createDataFrame(
        [
            (
                "ID1",
                "user1",
                "2024-10-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID1",
                "user1",
                "2024-11-02 09:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            (
                "ID2",
                "user2",
                "2024-10-01 10:00:00",
                "Authentification d'un client",
                "2024-10-02",
            ),
            ("ID3", "user3", "2024-10-03 11:00:00", "ConnexionFailed", "2024-10-02"),
        ],
        ["numerotiers", "utilisateur", "date", "action", "partitiondate"],
    )

    actual = build_digital_my_cdm_web(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        elt_contrat,
        audit,
    )

    expected = pd.DataFrame(
        [
            (
                "ID3",
                "fdc3",
                "agence3",
                "region3",
                "banque1",
                "Region C",
                "Banque A",
                "actif",
                None,
                None,
                False,
                False,
                None,
                0,
                0,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
            ),
            (
                "ID2",
                "fdc2",
                "agence2",
                "region2",
                "banque2",
                "Region B",
                "Banque B",
                "inactif",
                "5",
                "Transactionnel MY CDM",
                True,
                True,
                "2022-02-01",
                1,
                0,
                "inactif",
                "user2",
                "2024-10-01 10:00:00",
                0,
                1,
                1,
                "2024-10-02",
            ),
            (
                "ID1",
                "fdc1",
                "agence1",
                "region1",
                "banque1",
                "Region A",
                "Banque A",
                "actif",
                "1",
                "Consultation",
                True,
                True,
                "2024-01-01",
                0,
                1,
                "actif",
                "user1",
                "2024-11-02 09:00:00",
                1,
                1,
                1,
                "2024-10-02",
            ),
            (
                "ID4",
                "fdc4",
                "agence4",
                "region4",
                "banque2",
                "Region D",
                "Banque B",
                "inactif",
                None,
                None,
                False,
                False,
                None,
                0,
                0,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
            ),
        ],
        columns=[
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
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])
