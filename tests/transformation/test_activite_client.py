import pandas as pd

from src.transformation.activite_client import (
    build_engagement,
    build_is_actif_regle_2,
    eer_en_cour_numerotiers,
    select_eer,
    select_mouvement_comptable,
    select_num_compte_retail_pro,
)
from tests.conftest import compare_dataframes


def test_select_eer(spark):
    mvt_comptable = spark.createDataFrame(
        [
            ("123456", "000C", "2000", "C", "2024-10-10", "2024-10-10"),
            ("123456", "000C", "15000", "D", "2021-10-10", "2024-10-10"),
            ("123456", "000C", "15000", "C", "2021-10-10", "2024-10-10"),
            ("654123", "000A", "15000", "C", "2024-10-10", "2024-10-10"),
            ("654123", "000A", "15000", "C", "2024-10-10", "2024-10-10"),
            ("654321", "000C", "15000", "D", "2024-10-10", "2024-10-10"),
        ],
        [
            "numerodecompte",
            "devise",
            "montantecriture",
            "sens",
            "datetraitement",
            "partitiondate",
        ],
    )

    taux_change = spark.createDataFrame(
        [
            ("000C", 1, "2024-10-10"),
            ("000A", 10, "2024-10-10"),
        ],
        ["cd_dev_oper", "midbam", "partitiondate"],
    )

    actual = select_eer(mvt_comptable, taux_change)

    expected = pd.DataFrame(
        [
            ("123456", 20),
            ("654123", 3000),
        ],
        columns=["numerodecompte", "sum_montant_ecriture"],
    )
    compare_dataframes(actual, expected, sort_by=["numerodecompte"])


def test_eer_en_cour_numerotiers(spark):
    mvt_comptable = spark.createDataFrame(
        [
            ("123456", "000C", "2000", "C", "2024-10-10", "2024-10-10"),
            ("123456", "000C", "15000", "D", "2021-10-10", "2024-10-10"),
            ("123456", "000C", "15000", "C", "2021-10-10", "2024-10-10"),
            ("654123", "000A", "15000", "C", "2024-10-10", "2024-10-10"),
            ("654123", "000A", "15000", "C", "2024-10-10", "2024-10-10"),
            ("784596", "000C", "1000", "C", "2024-10-10", "2024-10-10"),
            ("784596", "000C", "1000", "C", "2024-10-10", "2024-10-10"),
            ("146352", "000C", "15000", "C", "2024-10-10", "2024-10-10"),
            ("146352", "000C", "15000", "C", "2024-10-10", "2024-10-10"),
            ("489657", "000C", "18000", "D", "2024-10-10", "2024-10-10"),
            ("963258", "000C", "30000", "C", "2024-10-10", "2024-10-10"),
        ],
        [
            "numerodecompte",
            "devise",
            "montantecriture",
            "sens",
            "datetraitement",
            "partitiondate",
        ],
    )

    taux_change = spark.createDataFrame(
        [
            ("000C", 1, "2024-10-10"),
            ("000A", 10, "2024-10-10"),
        ],
        ["cd_dev_oper", "midbam", "partitiondate"],
    )

    tiers = spark.createDataFrame(
        [
            ("000123", "R", "2022-10-15", "2024-10-10"),
            ("000124", "P", "2020-10-15", "2024-10-10"),
            ("000125", "R", "2024-10-15", "2024-10-10"),
            ("000126", "R", "2022-01-01", "2024-10-10"),
            ("000127", "P", None, "2024-10-10"),
            ("000128", "C", "2022-01-01", "2024-10-10"),
        ],
        [
            "numerotiers",
            "code_marche",
            "date_ouverture_premier_compte",
            "partitiondate",
        ],
    )
    marche = spark.createDataFrame(
        [("P", "PRO"), ("R", "RETAIL"), ("C", "CA")], ["code_marche", "marche"]
    )

    compte = spark.createDataFrame(
        [
            ("000123", "123456"),
            ("000123", "654123"),
            ("000124", "784596"),
            ("000125", "146352"),
            ("000126", "147852"),
            ("000127", "489657"),
            ("000128", "963258"),
        ],
        ["numerotiers", "numerodecompte"],
    )

    actual = eer_en_cour_numerotiers(
        mvt_comptable, taux_change, tiers, compte, marche
    )

    expected = pd.DataFrame(
        [
            ("000123", 0, 3020),
            ("000124", 0, 20),
            ("000125", 0, 300),
            ("000126", 0, None),
            ("000127", 0, None),
        ],
        columns=["numerotiers", "is_entrer_en_relation", "flux_crediteur"],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_select_num_compte_retail_pro(spark):
    tiers = spark.createDataFrame(
        [
            ("000123", "R", "2022-10-15", "2024-10-10"),
            ("000124", "P", "2020-10-15", "2024-10-10"),
            ("000125", "R", "2024-10-15", "2024-10-10"),
            ("000126", "R", "2022-01-01", "2024-10-10"),
            ("000127", "P", None, "2024-10-10"),
            ("000128", "C", "2022-01-01", "2024-10-10"),
        ],
        [
            "numerotiers",
            "code_marche",
            "date_ouverture_premier_compte",
            "partitiondate",
        ],
    )
    marche = spark.createDataFrame(
        [("P", "PRO"), ("R", "RETAIL"), ("C", "CA")], ["code_marche", "marche"]
    )

    compte = spark.createDataFrame(
        [
            ("000123", "123156", "156"),
            ("000123", "654123", "123"),
            ("000124", "784296", "296"),
            ("000125", "146352", "352"),
            ("000126", "147852", "852"),
            ("000127", "489257", "257"),
            ("000128", "963258", "258"),
        ],
        ["numerotiers", "numerodecompte", "naturecompte"],
    )

    ressource_emploi = spark.createDataFrame(
        [
            ("123156", "500", "2024-10-10"),
            ("654123", "3500", "2024-10-10"),
            ("784296", "300", "2024-10-10"),
            ("146352", "500", "2024-10-10"),
            ("147852", "500", "2024-10-10"),
            ("489257", None, "2024-10-10"),
        ],
        ["numerodecompte", "soldejourcr_devise", "partitiondate"],
    )

    actual = select_num_compte_retail_pro(tiers, compte, marche, ressource_emploi)

    expected = pd.DataFrame(
        [
            ("000123", "123156", "156", "500", 1, "RETAIL"),
            ("000123", "654123", "123", "3500", 1, "RETAIL"),
            ("000124", "784296", "296", "300", 0, "PRO"),
            ("000125", "146352", "352", "0", 0, "RETAIL"),
            ("000126", "147852", "852", "0", 0, "RETAIL"),
            ("000127", "489257", "257", "0", 0, "PRO"),
        ],
        columns=[
            "numerotiers",
            "numerodecompte",
            "naturecompte",
            "soldejourcr_devise",
            "is_encour",
            "marche",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_select_mouvement_comptable(spark):
    mvt_comptable = spark.createDataFrame(
        [
            (
                "123456",
                "000C",
                "2000",
                "C",
                "2024-10-10",
                "2024-05-10",
                "2024-10-10",
                "OP1",
            ),
            (
                "123456",
                "000C",
                "15000",
                "D",
                "2024-10-10",
                "2024-09-15",
                "2024-10-10",
                "OP1",
            ),
            (
                "563456",
                "000C",
                "15000",
                "C",
                "2024-10-10",
                "2024-09-10",
                "2024-10-10",
                "OP1",
            ),
            (
                "654123",
                "000A",
                "15000",
                "C",
                "2024-10-10",
                "2024-06-10",
                "2024-10-10",
                "OP2",
            ),
            (
                "654123",
                "000A",
                "15000",
                "C",
                "2024-10-10",
                "2024-03-10",
                "2024-10-10",
                "OP2",
            ),
            (
                "754321",
                "000C",
                "15000",
                "D",
                "2024-10-10",
                None,
                "2024-10-10",
                "OP3",
            ),
        ],
        [
            "numerodecompte",
            "devise",
            "montantecriture",
            "sens",
            "datetraitement",
            "dateoperation",
            "partitiondate",
            "codeoperation",
        ],
    )

    t602 = spark.createDataFrame(
        [
            ("OP1", "operation1"),
            ("OP2", "operation2"),
        ],
        ["typoper", "libelleoper"],
    )

    actual = select_mouvement_comptable(mvt_comptable, t602)

    expected = pd.DataFrame(
        [
            ("123456", 1),
            ("563456", 1),
            ("654123", 1),
        ],
        columns=["numerodecompte", "max_operation_6"],
    )

    compare_dataframes(actual, expected, sort_by=["numerodecompte"])


def test_build_is_actif_regle_2(spark):
    tiers = spark.createDataFrame(
        [
            ("000123", "R", "2022-10-15", "2024-10-10"),
            ("000124", "P", "2020-10-15", "2024-10-10"),
            ("000125", "R", "2024-10-15", "2024-10-10"),
            ("000126", "R", "2022-01-01", "2024-10-10"),
            ("000127", "P", None, "2024-10-10"),
            ("000128", "C", "2022-01-01", "2024-10-10"),
        ],
        [
            "numerotiers",
            "code_marche",
            "date_ouverture_premier_compte",
            "partitiondate",
        ],
    )
    marche = spark.createDataFrame(
        [("P", "PRO"), ("R", "RETAIL"), ("C", "CA")], ["code_marche", "marche"]
    )

    compte = spark.createDataFrame(
        [
            ("000123", "123156", "156"),
            ("000123", "654123", "123"),
            ("000124", "784296", "296"),
            ("000125", "146352", "352"),
            ("000126", "147852", "852"),
            ("000127", "489257", "257"),
            ("000128", "963258", "258"),
        ],
        ["numerotiers", "numerodecompte", "naturecompte"],
    )

    ressource_emploi = spark.createDataFrame(
        [
            ("123156", "500", "2024-10-10"),
            ("654123", "3500", "2024-10-10"),
            ("784296", "300", "2024-10-10"),
            ("146352", "500", "2024-10-10"),
            ("147852", "500", "2024-10-10"),
            ("489257", None, "2024-10-10"),
            ("963258", None, "2024-10-10"),
        ],
        ["numerodecompte", "soldejourcr_devise", "partitiondate"],
    )

    mvt_comptable = spark.createDataFrame(
        [
            (
                "123156",
                "000C",
                "2000",
                "C",
                "2024-10-10",
                "2024-05-10",
                "2024-10-10",
                "OP1",
            ),
            (
                "123156",
                "000C",
                "15000",
                "D",
                "2024-10-10",
                "2024-09-15",
                "2024-10-10",
                "OP1",
            ),
            (
                "784296",
                "000C",
                "15000",
                "C",
                "2024-10-10",
                "2024-09-10",
                "2024-10-10",
                "OP1",
            ),
            (
                "146352",
                "000A",
                "15000",
                "C",
                "2024-10-10",
                "2024-06-10",
                "2024-10-10",
                "OP2",
            ),
            (
                "654103",
                "000A",
                "15000",
                "C",
                "2024-10-10",
                "2024-03-10",
                "2024-10-10",
                "OP2",
            ),
            (
                "489257",
                "000C",
                "15000",
                "D",
                "2024-10-10",
                None,
                "2024-10-10",
                "OP3",
            ),
        ],
        [
            "numerodecompte",
            "devise",
            "montantecriture",
            "sens",
            "datetraitement",
            "dateoperation",
            "partitiondate",
            "codeoperation",
        ],
    )

    t602 = spark.createDataFrame(
        [
            ("OP1", "operation1"),
            ("OP2", "operation2"),
        ],
        ["typoper", "libelleoper"],
    )

    actual = build_is_actif_regle_2(
        tiers, compte, marche, ressource_emploi, mvt_comptable, t602
    )

    expected = pd.DataFrame(
        [
            ("000123", 1, 1),
            ("000124", 1, 0),
            ("000125", 1, 0),
            ("000126", 0, 0),
            ("000127", 0, 0),
        ],
        columns=["numerotiers", "is_actif_regle2", "max_is_encour"],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_build_engagement(spark):
    ressource_emploi = spark.createDataFrame(
        [
            ("000123", "123156", "500", "resMAP4"),
            ("000123", "654123", "3500", "resMAP"),
            ("000124", "784296", "300", "resMAP1"),
            ("000125", "146352", "500", "resMAP2"),
            ("000126", "147852", "500", "resMAP1"),
            ("000127", "489257", None, "resMAP2"),
            ("000128", "963258", None, "resMAP3"),
        ],
        [
            "numerotiers",
            "numerodecompte",
            "soldejourdb_devise",
            "fk_mapressourceemploi",
        ],
    )

    mapping_ressource_emploi = spark.createDataFrame(
        [
            ("resMAP", "AV", "AVANCES"),
            ("resMAP1", "CA", "CAUTIONS"),
            ("resMAP2", "C", "CMT"),
            ("resMAP3", "RE", "PE"),
        ],
        ["pk_mapressourceemploi", "libelle_type", "type_ressource_emploi"],
    )

    actual = build_engagement(ressource_emploi, mapping_ressource_emploi)

    expected = pd.DataFrame(
        [
            ("000123", "654123", "3500", "resMAP", "AV", 1, 0),
            ("000124", "784296", "300", "resMAP1", "CA", 1, 0),
            ("000125", "146352", "500", "resMAP2", "C", 1, 0),
            ("000126", "147852", "500", "resMAP1", "CA", 1, 0),
            ("000127", "489257", None, "resMAP2", "C", 0, 0),
            ("000128", "963258", None, "resMAP3", "RE", 0, 0),
        ],
        columns=[
            "numerotiers",
            "numerodecompte",
            "soldejourdb_devise",
            "pk_mapressourceemploi",
            "libelle_type",
            "is_engaged",
            "has_ressource",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])
