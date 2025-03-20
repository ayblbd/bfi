import pandas as pd

from src.transformation.csc import (
    build_csc,
    is_csc_sec_from_history,
)
from tests.conftest import compare_dataframes


def test_kpi_csc(spark):
    donnees_compte = spark.createDataFrame(
        [
            ("compte_1", "ACTIF", "2024-03-10"),
            ("compte_1", "ACTIF", "2024-02-10"),
            ("compte_1", "ACTIF", "2024-01-10"),
            ("compte_1", "ACTIF", "2023-01-10"),
            ("compte_2", "ACTIF", "2024-03-04"),
            ("compte_3", "ACTIF", "2024-02-29"),
            ("compte_4", "ACTIF", "2024-02-22"),
        ],
        ["numerodecompte", "statutcompte", "partitiondate"],
    )

    compte = spark.createDataFrame(
        [
            ("compte_1", "010", "2024-03-01"),
            ("compte_2", "010", "2024-03-04"),
            ("compte_3", "010", "2024-02-29"),
            ("compte_4", "010", "2024-02-22"),
        ],
        ["numerodecompte", "naturecompte", "date_ouverture"],
    )

    expected = pd.DataFrame(
        [
            (
                "compte_1",
                "010",
                "2024-03-01",
                False,
                True,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                False,
                True,
                True,
                True,
                True,
                False,
                True,
                True,
            ),
            (
                "compte_2",
                "010",
                "2024-03-04",
                True,
                False,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
            ),
            (
                "compte_3",
                "010",
                "2024-02-29",
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                False,
                True,
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ),
            (
                "compte_4",
                "010",
                "2024-02-22",
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                False,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
                True,
                False,
            ),
        ],
        columns=[
            "numerodecompte",
            "naturecompte",
            "date_ouverture",
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
            "is_actif_s",
            "is_actif_s_1",
            "is_actif_s_2",
            "is_actif_m",
            "is_actif_m_1",
            "is_actif_m_2",
            "is_actif_t",
            "is_actif_t_1",
            "is_actif_y",
            "is_actif_y_1",
        ],
    )

    actual = build_csc(
        compte, donnees_compte, ressource_emploi=compte, partition_date="2024-03-10"
    )
    actual.orderBy("numerodecompte").show()

    compare_dataframes(actual, expected, sort_by=["numerodecompte"])


def test_is_csc_sec(spark):
    compte = spark.createDataFrame(
        [
            ("numCompte1", "Tier_1", "001", "CLOTURE"),
            ("numCompte2", "Tier_2", "010", "CLOTURE"),
            ("numCompte3", "Tier_3", "011", "CLOTURE"),
            ("numCompte4", "Tier_4", "001", "CLOTURE"),
            ("numCompte5", "Tier_4", "011", "ACTIFFF"),
            ("numCompte6", "Tier_4", "010", "CLOTURE"),
            ("numCompte7", "Tier_8", "014", "CLOTURE"),
            ("numCompte8", "Tier_8", "011", "CLOTURE"),
            ("numCompte9", "Tier_9", "001", "CLOTURE"),
            ("numCompteV", "Tier_5", "001", "CLOTURE"),
        ],
        ["numerodecompte", "numerotiers", "naturecompte", "statutcompte"],
    )

    actual = is_csc_sec_from_history(compte, "2024-10-10")
    actual.orderBy("numerodecompte").show()

    expected = pd.DataFrame(
        [
            ("numCompte1", "Tier_1", "001", "statutcompte", 0),
            ("numCompte2", "Tier_2", "010", "statutcompte", 1),
            ("numCompte3", "Tier_3", "011", "statutcompte", 1),
            ("numCompte4", "Tier_4", "001", "statutcompte", 0),
            ("numCompte5", "Tier_4", "011", "ACTIFFF", 0),
            ("numCompte6", "Tier_4", "010", "statutcompte", 0),
            ("numCompte7", "Tier_8", "014", "statutcompte", 1),
            ("numCompte8", "Tier_8", "011", "statutcompte", 1),
            ("numCompte9", "Tier_9", "001", "statutcompte", 0),
            ("numCompteV", "Tier_5", "001", "statutcompte", 0),
        ],
        columns=["numerodecompte", "numerotiers", "naturecompte", "is_csc_sec"],
    )

    compare_dataframes(actual, expected, sort_by=["numerodecompte"])
