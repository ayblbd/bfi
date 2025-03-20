import pandas as pd

from src.transformation.equipement import (
    build_digital_activity,
    build_equipement_corporate,
    get_card_activity,
    get_contrat_history,
    get_last_card_transaction_date,
    get_last_connection_date,
    get_last_month_contrat_etat_produit,
    get_last_month_csc_etat_produit,
    get_last_week_contrat_etat_produit,
    get_last_week_csc_etat_produit,
    get_last_year_contrat_etat_produit,
    get_last_year_csc_etat_produit,
    get_monthy_corporate,
    get_weekly_corporate,
    get_yearly_corporate,
    select_audit_log_event,
    select_information_compte_client,
    select_information_tier,
    select_niveau_regroupement,
    select_utilisateur,
)
from tests.conftest import compare_dataframes


def test_build_equipement_corporate(spark):
    contrat_produit = spark.createDataFrame(
        [
            (
                "05200002975",
                "04703085728",
                "VALIDE",
                "888SNT16186692",
                "888SNT16186692",
                "2022-01-19",
                "SNT1",
                "2999-12-20",
                "2024-07-17",
            ),
            (
                "13000003327",
                "13003006379",
                "RESILIE",
                "130SNT10095351",
                "130SNT10095351",
                "2019-05-17",
                "SNT1",
                "2021-01-15",
                "2024-07-17",
            ),
            (
                "19700003602",
                "19703303733",
                "NONVALIDE",
                "197SNT10875961",
                "197SNT10875961",
                "2024-02-20",
                "SNT1",
                "2999-12-20",
                "2024-07-17",
            ),
            (
                "19700006472",
                "19703307807",
                "VALIDE",
                "197SNT10859148",
                "197SNT10859148",
                "2023-03-13",
                "SNT1",
                "2999-12-20",
                "2024-07-17",
            ),
        ],
        [
            "numerotiers",
            "numerodecompte",
            "etatproduit",
            "numerocontratinterne",
            "numerocarte",
            "datesouscription",
            "codeproduit",
            "dateecheance",
            "partitiondate",
        ],
    )

    contrat_history = spark.createDataFrame(
        [
            (
                "888CN016186692",
                "3",
                "2024-07-17",
            ),
            (
                "130CN010095351",
                "1",
                "2024-07-17",
            ),
            (
                "197EBAY0875961",
                "7",
                "2024-07-17",
            ),
            (
                "197EBAY0859148",
                "3",
                "2024-07-17",
            ),
        ],
        ["numerocontratinterne", "etatcontrat", "partitiondate"],
    )

    information_tier = spark.createDataFrame(
        [
            (
                "05200002975",
                "PME",
                "422",
                "422E",
                None,
                None,
                None,
                None,
                "Petite et Moyenne Entreprise",
                "PME",
                "ACTIF",
            ),
            (
                "13000003327",
                "PME",
                "423",
                "423P",
                None,
                None,
                None,
                None,
                "Petite et Moyenne Entreprise",
                "PME",
                "ACTIF",
            ),
            (
                "19700003602",
                "PRO",
                "197",
                "197N",
                "PE",
                "TPE",
                "PE17",
                "TPE - AUTRES",
                "Professionnel",
                "TPE",
                "INACTIF",
            ),
            (
                "19700006472",
                "PRO",
                "198",
                "198N",
                "PE",
                "TPE",
                "PE6",
                "TPE - INDUSTRIE_Bon_pot.",
                "Professionnel",
                "TPE",
                "INACTIF",
            ),
        ],
        [
            "numerotiers",
            "marche",
            "agencegest",
            "cd_fdc_gest",
            "new_seg",
            "libellesegment",
            "new_sous_seg",
            "libellesoussegment",
            "libelle_marche",
            "gamme",
            "statutclient",
        ],
    )

    niveau_regroupement = spark.createDataFrame(
        [
            (
                "422",
                "CENTRE AFFAIRES EXPANSION",
                "422",
                "CENTRE AFFAIRES EXPANSION",
                "501",
                "828",
                "DIR CASA NORD, RABAT,NORD",
                "CORPORATE BANKING",
            ),
            (
                "423",
                "CENTRE AFFAIRES CENTRE VILLE",
                "423",
                "CENTRE AFFAIRES CENTRE VILLE",
                "502",
                "828",
                "DIR CASA SUD-CENTRE ORIENTAL",
                "CORPORATE BANKING",
            ),
            (
                "197",
                "TANGER ATLANTIQUE",
                "197",
                "TANGER   ATLANTIQUE",
                "541",
                "826",
                "NORD MEDITERRANEE",
                "DIRECTION DU RESEAU",
            ),
            (
                "198",
                "TEMARA",
                "198",
                "TEMARA",
                "531",
                "826",
                "RABAT KENITRA",
                "DIRECTION DU RESEAU",
            ),
        ],
        [
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
        ],
    )
    relation_commercial = spark.createDataFrame(
        [
            ("888CN016186692",),
            ("130CN010095351",),
            ("197EBAY0875961",),
            ("197EBAY0859148",),
        ],
        [
            "identifiantrc",
        ],
    )
    elt_contrat = spark.createDataFrame(
        [
            (
                "888CN016186692",
                "03200176319    MRANI ALAOUI                    HACHEM                          SALAM 2 N¿185 SIDI SAID         MEKNES                                                          50032MEKNES                    1                                                                                                                                                                               1                               000000000000010000",
            ),
            (
                "130CN010095351",
                "127600000396BENSOUNA FOUZIA           24 3 LOT BENKHIRANE BD ABOU BAKR     EL KADIRI NO 69                                                 60020OUJDA",
            ),
        ],
        [
            "numerocontratinterne",
            "partiespecifique",
        ],
    )

    actual = build_equipement_corporate(
        contrat_produit,
        information_tier,
        niveau_regroupement,
        contrat_history,
        relation_commercial,
        "SNT1",
        "E-Swift",
        elt_contrat,
    )

    expected = pd.DataFrame(
        [
            (
                "05200002975",
                "04703085728",
                "888SNT16186692",
                "2022-01-19",
                "2999-12-20",
                "2024-07-17",
                "E-Swift",
                "PME",
                "Petite et Moyenne Entreprise",
                "422E",
                "422",
                "CENTRE AFFAIRES EXPANSION",
                "501",
                "828",
                "DIR CASA NORD, RABAT,NORD",
                "CORPORATE BANKING",
                "422",
                "CENTRE AFFAIRES EXPANSION",
                1,
                1,
                "SNT1",
                "VALIDE",
                True,
                False,
                False,
                False,
                "E-Swift",
                True,
                None,
                None,
                None,
                None,
                "PME",
                None,
                None,
                None,
            ),
            (
                "13000003327",
                "13003006379",
                "130SNT10095351",
                "2019-05-17",
                "2021-01-15",
                "2024-07-17",
                "E-Swift",
                "PME",
                "Petite et Moyenne Entreprise",
                "423P",
                "423",
                "CENTRE AFFAIRES CENTRE VILLE",
                "502",
                "828",
                "DIR CASA SUD-CENTRE ORIENTAL",
                "CORPORATE BANKING",
                "423",
                "CENTRE AFFAIRES CENTRE VILLE",
                1,
                1,
                "SNT1",
                "RESILIE",
                False,
                False,
                False,
                False,
                "E-Swift",
                True,
                None,
                None,
                None,
                None,
                "PME",
                None,
                None,
                None,
            ),
            (
                "19700003602",
                "19703303733",
                "197SNT10875961",
                "2024-02-20",
                "2999-12-20",
                "2024-07-17",
                "E-Swift",
                "PRO",
                "Professionnel",
                "197N",
                "197",
                "TANGER ATLANTIQUE",
                "541",
                "826",
                "NORD MEDITERRANEE",
                "DIRECTION DU RESEAU",
                "197",
                "TANGER   ATLANTIQUE",
                0,
                1,
                "SNT1",
                "NONVALIDE",
                False,
                False,
                False,
                True,
                "E-Swift",
                True,
                "PE",
                "PE17",
                "TPE",
                "TPE - AUTRES",
                "TPE",
                None,
                None,
                None,
            ),
            (
                "19700006472",
                "19703307807",
                "197SNT10859148",
                "2023-03-13",
                "2999-12-20",
                "2024-07-17",
                "E-Swift",
                "PRO",
                "Professionnel",
                "198N",
                "198",
                "TEMARA",
                "531",
                "826",
                "RABAT KENITRA",
                "DIRECTION DU RESEAU",
                "198",
                "TEMARA",
                0,
                1,
                "SNT1",
                "VALIDE",
                True,
                False,
                False,
                False,
                "E-Swift",
                True,
                "PE",
                "PE6",
                "TPE",
                "TPE - INDUSTRIE_Bon_pot.",
                "TPE",
                None,
                None,
                None,
            ),
        ],
        columns=[
            "numerotiers",
            "numerodecompte",
            "numerocontrat",
            "datesouscription",
            "dateecheance",
            "partitiondate",
            "libproduit",
            "marche",
            "libelle_marche",
            "fdc_gest",
            "codeagence",
            "nomagence",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
            "succursale",
            "nomsuccursale",
            "total_tier_actif_par_fdc",
            "total_tier_par_fdc",
            "codeproduit",
            "etatproduit",
            "is_valid",
            "this_week",
            "this_month",
            "this_year",
            "categorie",
            "is_corporate",
            "new_seg",
            "new_sous_seg",
            "libellesegment",
            "libellesoussegment",
            "gamme",
            "etatproduit_semaine_prec",
            "etatproduit_mois_prec",
            "etatproduit_annee_prec",
        ],
    )

    compare_dataframes(
        actual,
        expected,
        sort_by=["numerotiers"],
    )


def test_get_weekly_corpo(spark):
    df = spark.createDataFrame(
        [
            (True, True, True),
            (True, True, True),
            (False, True, True),
            (True, False, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
            (True, False, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
        ],
        ["is_valid", "this_week", "is_corporate"],
    )

    actual = get_weekly_corporate(df)

    expected = pd.DataFrame(
        [
            (True, True, True, 1),
            (True, True, True, 1),
            (False, True, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
        ],
        columns=["is_valid", "this_week", "is_corporate", "is_weekly"],
    )
    compare_dataframes(
        actual, expected, sort_by=["is_valid", "this_week", "is_corporate"]
    )


def test_get_monthly_corpo(spark):
    df = spark.createDataFrame(
        [
            (True, True, True),
            (True, True, True),
            (False, True, True),
            (True, False, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
            (True, False, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
        ],
        ["is_valid", "this_month", "is_corporate"],
    )

    actual = get_monthy_corporate(df)

    expected = pd.DataFrame(
        [
            (True, True, True, 1),
            (True, True, True, 1),
            (False, True, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
        ],
        columns=["is_valid", "this_month", "is_corporate", "is_monthly"],
    )

    compare_dataframes(
        actual, expected, sort_by=["is_valid", "this_month", "is_corporate"]
    )


def test_get_yearly_corpo(spark):
    df = spark.createDataFrame(
        [
            (True, True, True),
            (True, True, True),
            (False, True, True),
            (True, False, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
            (True, False, True),
            (True, False, True),
            (True, False, True),
            (True, True, True),
        ],
        ["is_valid", "this_year", "is_corporate"],
    )

    actual = get_yearly_corporate(df)

    expected = pd.DataFrame(
        [
            (True, True, True, 1),
            (True, True, True, 1),
            (False, True, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, False, True, 0),
            (True, True, True, 1),
        ],
        columns=["is_valid", "this_year", "is_corporate", "is_yearly"],
    )

    compare_dataframes(
        actual, expected, sort_by=["is_valid", "this_year", "is_corporate"]
    )


def test_select_information_tier(spark):
    information_tier = spark.createDataFrame(
        [
            (
                "00D",
                "ACTIF",
                "000G",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00A",
                "RUPTURE DE RELATION",
                "0001",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00B",
                "ACTIF",
                "0004",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00A",
                "RUPTURE DE RELATION",
                "0002",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00B",
                "ACTIF",
                "0005",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "ACTIF",
                "000HHH",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "ACTIF",
                "000D",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "INACTIF",
                "000X",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00A",
                "INACTIF",
                "0003",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00B",
                "ACTIF",
                "0006",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "ACTIF",
                "000D",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "ACTIF",
                "000HH",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "ACTIF",
                "000E",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "ACTIF",
                "000F",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "INACTIF",
                "000H",
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "INACTIF",
                None,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
        ],
        [
            "cd_fdc_gest",
            "statutclient",
            "numerotiers",
            "marche",
            "agencegest",
            "new_seg",
            "libellesegment",
            "new_sous_seg",
            "libellesoussegment",
            "libelle_marche",
            "gamme",
        ],
    )

    expected = pd.DataFrame(
        [
            (
                "00D",
                "000G",
                5,
                4,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00A",
                "0001",
                3,
                0,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00B",
                "0004",
                3,
                3,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00A",
                "0002",
                3,
                0,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00B",
                "0005",
                3,
                3,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "000HHH",
                5,
                4,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "000D",
                3,
                2,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "000X",
                3,
                2,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00A",
                "0003",
                3,
                0,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00B",
                "0006",
                3,
                3,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "000D",
                3,
                2,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "000HH",
                5,
                4,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00C",
                "000E",
                3,
                2,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "000F",
                5,
                4,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                "000H",
                5,
                4,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
            (
                "00D",
                None,
                5,
                4,
                "marhce_1",
                "agence_1",
                "new_seg_1",
                "libseg1",
                "new_souseg_1",
                "libsouseg_1",
                "lib_marche",
                "game_1",
            ),
        ],
        columns=[
            "fdc_gest",
            "numerotiers",
            "total_tier_par_fdc",
            "total_tier_actif_par_fdc",
            "marche",
            "codeagence",
            "new_seg",
            "libellesegment",
            "new_sous_seg",
            "libellesoussegment",
            "libelle_marche",
            "gamme",
        ],
    )

    actual = select_information_tier(information_tier)

    compare_dataframes(
        actual, expected, sort_by=["fdc_gest", "numerotiers", "total_tier_par_fdc"]
    )


def test_select_information_compte_client(spark):
    df = spark.createDataFrame(
        [
            ("1", "15", "af1"),
            ("1", "12", "af1"),
            ("2", "00", "af5"),
            ("2", "001", "af1"),
            ("3", "101", "bc"),
            ("3", "1011", "dd4"),
            ("4", "078", "af5"),
            ("4", "155", "cd0"),
            ("5", "135", "cc"),
            ("5", "055", "bc"),
        ],
        ["numerotiers", "numerodecompte", "cd_fdc_gest"],
    )

    actual = select_information_compte_client(df)

    expected = pd.DataFrame(
        [
            ("1", "15", "af1"),
            ("1", "12", "af1"),
            ("2", "00", "af5"),
            ("2", "001", "af1"),
            ("3", "101", "bc"),
            ("3", "1011", "dd4"),
            ("4", "078", "af5"),
            ("4", "155", "cd0"),
            ("5", "135", "cc"),
            ("5", "055", "bc"),
        ],
        columns=["numerotiers", "numerodecompte", "fdc_gest_compte"],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_select_niveau_regroupement(spark):
    df = spark.createDataFrame(
        [
            ("1", "noma1", "123", "A", "cc1", "L1", "fr", "nomb1"),
            ("2", "noma1", "124", "B", "cc1", "L2", "rr", "nomb2"),
            ("3", "noma1", "125", "C", "cc3", "L3", "gr", "nomb1"),
            ("4", "noma1", "126", "D", "cc3", "L4", "rf", "nomb1"),
            ("5", "noma1", "123", "E", "cc1", "L1", "cdd", "nomb1"),
            ("5", "noma1", "123", "E", "cc1", "L1", "cdd", "nomb1"),
            ("7", "noma1", "128", "F", "cc5", "L6", "cdd", "nomb3"),
            ("8", "noma2", "129", "G", "cc8", "L7", "cd", "nomb1"),
            ("9", "noma2", "125", "C", "cc1", "L3", "cd", "nomb3"),
            ("10", "noma1", "130", "H", "cc3", "L8", "cd", "nomb2"),
        ],
        [
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
        ],
    )

    actual = select_niveau_regroupement(df)

    expected = pd.DataFrame(
        [
            ("1", "noma1", "123", "A", "cc1", "L1", "fr", "nomb1"),
            ("2", "noma1", "124", "B", "cc1", "L2", "rr", "nomb2"),
            ("3", "noma1", "125", "C", "cc3", "L3", "gr", "nomb1"),
            ("4", "noma1", "126", "D", "cc3", "L4", "rf", "nomb1"),
            ("5", "noma1", "123", "E", "cc1", "L1", "cdd", "nomb1"),
            ("7", "noma1", "128", "F", "cc5", "L6", "cdd", "nomb3"),
            ("8", "noma2", "129", "G", "cc8", "L7", "cd", "nomb1"),
            ("9", "noma2", "125", "C", "cc1", "L3", "cd", "nomb3"),
            ("10", "noma1", "130", "H", "cc3", "L8", "cd", "nomb2"),
        ],
        columns=[
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "codebanque",
            "nomregion",
            "nombanque",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["codeagence"])


def test_get_last_year_csc_etat_produit(spark):
    df = spark.createDataFrame(
        [
            ("1", "2023-01-01", "0"),
            ("1", "2024-01-01", "1"),
            ("2", "2023-01-01", "0"),
            ("2", "2024-01-01", "0"),
            ("3", "2023-01-01", "1"),
            ("3", "2024-01-01", "1"),
            ("4", "2023-01-01", "0"),
            ("4", "2024-01-01", "1"),
            ("5", "2023-01-01", "1"),
            ("5", "2024-01-01", "0"),
        ],
        ["numerodecompte", "partitiondate", "etatcompte"],
    )

    actual = get_last_year_csc_etat_produit(df)

    expected = pd.DataFrame(
        [
            ("1", "2023-01-01", "0", None),
            ("1", "2024-01-01", "1", "VALIDE"),
            ("2", "2023-01-01", "0", None),
            ("2", "2024-01-01", "0", "VALIDE"),
            ("3", "2023-01-01", "1", None),
            ("3", "2024-01-01", "1", "VALIDE"),
            ("4", "2023-01-01", "0", None),
            ("4", "2024-01-01", "1", "VALIDE"),
            ("5", "2023-01-01", "1", None),
            ("5", "2024-01-01", "0", "VALIDE"),
        ],
        columns=[
            "numerodecompte",
            "partitiondate",
            "etatcompte",
            "etatproduit_annee_prec",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerodecompte"])


def test_get_last_week_csc_etat_produit(spark):
    df = spark.createDataFrame(
        [
            ("1", "2023-01-01", "0"),
            ("1", "2024-01-01", "1"),
            ("2", "2023-01-01", "0"),
            ("2", "2024-01-01", "0"),
            ("3", "2023-01-01", "1"),
            ("3", "2024-01-01", "1"),
            ("4", "2023-01-01", "0"),
            ("4", "2024-01-01", "1"),
            ("5", "2023-01-01", "1"),
            ("5", "2024-01-01", "0"),
        ],
        ["numerodecompte", "partitiondate", "etatcompte"],
    )

    actual = get_last_week_csc_etat_produit(df)

    expected = pd.DataFrame(
        [
            ("1", "2023-01-01", "0", None),
            ("1", "2024-01-01", "1", "VALIDE"),
            ("2", "2023-01-01", "0", None),
            ("2", "2024-01-01", "0", "VALIDE"),
            ("3", "2023-01-01", "1", None),
            ("3", "2024-01-01", "1", "VALIDE"),
            ("4", "2023-01-01", "0", None),
            ("4", "2024-01-01", "1", "VALIDE"),
            ("5", "2023-01-01", "1", None),
            ("5", "2024-01-01", "0", "VALIDE"),
        ],
        columns=[
            "numerodecompte",
            "partitiondate",
            "etatcompte",
            "etatproduit_semaine_prec",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerodecompte"])


def test_get_last_month_csc_etat_produit(spark):
    df = spark.createDataFrame(
        [
            ("1", "2023-01-01", "0"),
            ("1", "2024-01-01", "1"),
            ("2", "2023-01-01", "0"),
            ("2", "2024-01-01", "0"),
            ("3", "2023-01-01", "1"),
            ("3", "2024-01-01", "1"),
            ("4", "2023-01-01", "0"),
            ("4", "2024-01-01", "1"),
            ("5", "2023-01-01", "1"),
            ("5", "2024-01-01", "0"),
        ],
        ["numerodecompte", "partitiondate", "etatcompte"],
    )
    actual = get_last_month_csc_etat_produit(df)
    expected = pd.DataFrame(
        [
            ("1", "2023-01-01", "0", None),
            ("1", "2024-01-01", "1", "VALIDE"),
            ("2", "2023-01-01", "0", None),
            ("2", "2024-01-01", "0", "VALIDE"),
            ("3", "2023-01-01", "1", None),
            ("3", "2024-01-01", "1", "VALIDE"),
            ("4", "2023-01-01", "0", None),
            ("4", "2024-01-01", "1", "VALIDE"),
            ("5", "2023-01-01", "1", None),
            ("5", "2024-01-01", "0", "VALIDE"),
        ],
        columns=[
            "numerodecompte",
            "partitiondate",
            "etatcompte",
            "etatproduit_mois_prec",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerodecompte"])


def test_get_last_year_contrat_etat_produit(spark):
    df = spark.createDataFrame(
        [
            ("1", "2021-01-15", "1"),
            ("1", "2021-06-20", "3"),
            ("1", "2022-01-10", "5"),
            ("2", "2022-03-01", "2"),
            ("2", "2023-01-05", "A"),
            ("3", "2021-07-30", "6"),
            ("3", "2022-02-25", "1"),
            ("3", "2022-08-15", "5"),
            ("4", "2021-11-11", "C"),
            ("4", "2022-04-04", "1"),
        ],
        ["numerocontratinterne", "partitiondate", "etatcontrat"],
    )

    actual = get_last_year_contrat_etat_produit(df)

    expected = pd.DataFrame(
        [
            ("1", "2021-01-15", "1", None),
            ("1", "2021-06-20", "3", None),
            ("1", "2022-01-10", "5", "VALIDE"),
            ("2", "2022-03-01", "2", None),
            ("2", "2023-01-05", "A", "ECHUE"),
            ("3", "2021-07-30", "6", None),
            ("3", "2022-02-25", "1", None),
            ("3", "2022-08-15", "5", None),
            ("4", "2021-11-11", "C", None),
            ("4", "2022-04-04", "1", "NONVALIDE"),
        ],
        columns=[
            "numerocontratinterne",
            "partitiondate",
            "etatcontrat",
            "etatproduit_annee_prec",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerocontratinterne"])


def test_get_last_month_contrat_etat_produit(spark):
    df = spark.createDataFrame(
        [
            ("1", "2021-01-15", "1"),
            ("1", "2021-06-20", "3"),
            ("1", "2022-01-10", "5"),
            ("2", "2022-03-01", "2"),
            ("2", "2023-01-05", "A"),
            ("3", "2021-07-30", "6"),
            ("3", "2022-02-25", "1"),
            ("3", "2022-08-15", "5"),
            ("4", "2021-11-11", "C"),
            ("4", "2022-04-04", "1"),
        ],
        ["numerocontratinterne", "partitiondate", "etatcontrat"],
    )

    actual = get_last_month_contrat_etat_produit(df)

    expected = pd.DataFrame(
        [
            ("1", "2021-01-15", "1", None),
            ("1", "2021-06-20", "3", "NONVALIDE"),
            ("1", "2022-01-10", "5", "VALIDE"),
            ("2", "2022-03-01", "2", None),
            ("2", "2023-01-05", "A", "ECHUE"),
            ("3", "2021-07-30", "6", None),
            ("3", "2022-02-25", "1", None),
            ("3", "2022-08-15", "5", "NONVALIDE"),
            ("4", "2021-11-11", "C", None),
            ("4", "2022-04-04", "1", "NONVALIDE"),
        ],
        columns=[
            "numerocontratinterne",
            "partitiondate",
            "etatcontrat",
            "etatproduit_mois_prec",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerocontratinterne"])


def test_get_last_week_contrat_etat_produit(spark):
    df = spark.createDataFrame(
        [
            ("1", "2021-01-15", "1"),
            ("1", "2021-06-20", "3"),
            ("1", "2022-01-10", "5"),
            ("2", "2022-03-01", "2"),
            ("2", "2023-01-05", "A"),
            ("3", "2021-07-30", "6"),
            ("3", "2022-02-25", "1"),
            ("3", "2022-08-15", "5"),
            ("4", "2021-11-11", "C"),
            ("4", "2022-04-04", "1"),
        ],
        ["numerocontratinterne", "partitiondate", "etatcontrat"],
    )

    actual = get_last_week_contrat_etat_produit(df)

    expected = pd.DataFrame(
        [
            ("1", "2021-01-15", "1", None),
            ("1", "2021-06-20", "3", "NONVALIDE"),
            ("1", "2022-01-10", "5", "VALIDE"),
            ("2", "2022-03-01", "2", None),
            ("2", "2023-01-05", "A", "ECHUE"),
            ("3", "2021-07-30", "6", None),
            ("3", "2022-02-25", "1", None),
            ("3", "2022-08-15", "5", "NONVALIDE"),
            ("4", "2021-11-11", "C", None),
            ("4", "2022-04-04", "1", "NONVALIDE"),
        ],
        columns=[
            "numerocontratinterne",
            "partitiondate",
            "etatcontrat",
            "etatproduit_semaine_prec",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerocontratinterne"])


def test_get_contrat_history(spark):
    df = spark.createDataFrame(
        [
            ("1", "2021-01-15", "1"),
            ("1", "2021-06-20", "3"),
            ("1", "2022-01-10", "5"),
            ("2", "2022-03-01", "2"),
            ("2", "2023-01-05", "A"),
            ("3", "2021-07-30", "6"),
            ("3", "2022-02-25", "1"),
            ("3", "2022-08-15", "5"),
            ("4", "2021-11-11", "C"),
            ("4", "2022-04-04", "1"),
        ],
        ["numerocontratinterne", "partitiondate", "etatcontrat"],
    )

    actual = get_contrat_history(df)

    expected = pd.DataFrame(
        [
            ("1", "2021-01-15", "1", None, None, None),
            ("1", "2021-06-20", "3", None, "NONVALIDE", "NONVALIDE"),
            ("1", "2022-01-10", "5", "VALIDE", "VALIDE", "VALIDE"),
            ("2", "2022-03-01", "2", None, None, None),
            ("2", "2023-01-05", "A", "ECHUE", "ECHUE", "ECHUE"),
            ("3", "2021-07-30", "6", None, None, None),
            ("3", "2022-02-25", "1", None, None, None),
            ("3", "2022-08-15", "5", None, "NONVALIDE", "NONVALIDE"),
            ("4", "2021-11-11", "C", None, None, None),
            ("4", "2022-04-04", "1", "NONVALIDE", "NONVALIDE", "NONVALIDE"),
        ],
        columns=[
            "numerocontratinterne",
            "partitiondate",
            "etatcontrat",
            "etatproduit_annee_prec",
            "etatproduit_mois_prec",
            "etatproduit_semaine_prec",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["numerocontratinterne"])


def test_select_utilisateur(spark):
    utilisateur = spark.createDataFrame(
        [
            ("U1", "CL"),
            ("U2", "CV"),
            ("U3", "DL"),
            ("U4", "CL"),
            ("U5", "CL"),
        ],
        ["username", "typeprofile"],
    )
    actual = select_utilisateur(utilisateur)
    expected = pd.DataFrame(
        [
            ("U1", "CL"),
            ("U4", "CL"),
            ("U5", "CL"),
        ],
        columns=["username", "typeprofile"],
    )
    compare_dataframes(actual, expected, sort_by=["username"])


def test_select_audit_log_event(spark):
    audit_log_event = spark.createDataFrame(
        [
            ("U1", "021", "ConnexionOK", "2024-10-10"),
            ("U2", "033", "ConnexionOK", "2024-09-01"),
            ("U3", "021", "ConnexionOK", "2022-05-15"),
            ("U4", "021", "ConnexionError", "2024-10-20"),
            ("U5", "054", "ConnexionOK", "2021-02-22"),
            ("U6", "021", "ConnexionOK", "2019-05-15"),
        ],
        ["actor", "codebanqueassocie", "eventname", "datecreated"],
    )
    actual = select_audit_log_event(audit_log_event)
    expected = pd.DataFrame(
        [
            ("U1", "2024-10-10"),
            ("U3", "2022-05-15"),
            ("U6", "2019-05-15"),
        ],
        columns=["numerotiers", "datecreated"],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_get_last_connection_date(spark):
    audit_log_event = spark.createDataFrame(
        [
            ("U1", "2024-10-10"),
            ("U1", "2022-05-15"),
            ("U1", "2024-09-10"),
            ("U2", "2024-09-15"),
            ("U2", "2024-10-21"),
            ("U3", "2024-10-15"),
            ("U4", "2024-08-10"),
            ("U4", "2021-10-15"),
            ("U7", "2024-10-22"),
        ],
        ["numerotiers", "datecreated"],
    )

    utilisateur = spark.createDataFrame(
        [
            ("U1", "CL"),
            ("U2", "CL"),
            ("U3", "CL"),
            ("U4", "CL"),
            ("U5", "CL"),
        ],
        ["username", "typeprofile"],
    )
    actual = get_last_connection_date(audit_log_event, utilisateur)

    expected = pd.DataFrame(
        [
            ("U1", "2024-10-10"),
            ("U2", "2024-10-21"),
            ("U3", "2024-10-15"),
            ("U4", "2024-08-10"),
        ],
        columns=["numerotiers", "date_derniere_connexion"],
    )
    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_build_digital_activity(spark):
    digital = spark.createDataFrame(
        [
            ("U1", "2024-10-22", "2024-10-10"),
            ("U2", "2024-10-22", None),
            ("U3", "2024-10-22", "2024-09-20"),
            ("U4", "2024-10-22", "2023-09-21"),
            ("U5", "2024-10-22", "2024-10-22"),
        ],
        ["numerotiers", "partitiondate", "date_derniere_connexion"],
    )
    actual = digital.transform(build_digital_activity, 30).transform(
        build_digital_activity, 90
    )

    expected = pd.DataFrame(
        [
            ("U1", "2024-10-22", "2024-10-10", True, True),
            ("U2", "2024-10-22", None, False, False),
            ("U3", "2024-10-22", "2024-09-20", False, True),
            ("U4", "2024-10-22", "2023-09-21", False, False),
            ("U5", "2024-10-22", "2024-10-22", True, True),
        ],
        columns=[
            "numerotiers",
            "partitiondate",
            "date_derniere_connexion",
            "is_digital_active_30j",
            "is_digital_active_90j",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerotiers"])


def test_get_last_card_transaction_date(spark):
    authorization_df = spark.createDataFrame(
        [
            ("N", "1", None, "ATM", "4000123456789010", "2024-10-10", "000"),
            ("N", "2", "07000", None, "4000123456789010", "2023-10-10", "000"),
            ("N", "3", "07000", "ATM", "4000123456789011", "2024-06-30", "000"),
            ("N", "1", "07000", None, "4000123456789011", "2024-08-30", "000"),
            ("N", "3", None, "ATM", "4000123456789012", "2024-10-15", "111"),
            ("W", "3", None, None, "4000123456789012", "2024-10-17", "000"),
        ],
        [
            "aut_reve_stat",
            "aut_bin_type",
            "aut_tran_code",
            "aut_user",
            "aut_prim_acct_numb_f002",
            "aut_requ_syst_time",
            "aut_resp_code_f039",
        ],
    )

    actual = get_last_card_transaction_date(authorization_df)

    expected = pd.DataFrame(
        [
            ("4000123456789011", "2024-06-30"),
            ("4000123456789012", "2024-10-17"),
        ],
        columns=["tra_num_porteur", "date_derniere_transaction"],
    )

    compare_dataframes(
        actual,
        expected,
        sort_by=["tra_num_porteur"],
    )


def test_card_activity(spark):
    df = spark.createDataFrame(
        [
            ("4000123456789010", "2024-10-17", "2024-10-10"),
            ("4000123456789011", "2024-10-17", "2024-09-01"),
            ("4000123456789012", "2024-10-17", "2024-10-15"),
            ("4000123456789013", "2024-10-17", "2024-06-17"),
            ("4000123456789014", "2024-10-17", "2024-08-10"),
            ("4000123456789015", "2024-10-17", None),
        ],
        ["numerocarte", "partitiondate", "date_derniere_transaction"],
    )

    actual = (
        df.transform(get_card_activity, 30)
        .transform(get_card_activity, 90)
        .select(
            "numerocarte",
            "partitiondate",
            "date_derniere_transaction",
            "is_card_active_30j",
            "is_card_active_90j",
        )
    )

    expected = pd.DataFrame(
        [
            ("4000123456789010", "2024-10-17", "2024-10-10", True, True),
            ("4000123456789011", "2024-10-17", "2024-09-01", False, True),
            ("4000123456789012", "2024-10-17", "2024-10-15", True, True),
            ("4000123456789013", "2024-10-17", "2024-06-17", False, False),
            ("4000123456789014", "2024-10-17", "2024-08-10", False, True),
            ("4000123456789015", "2024-10-17", None, False, False),
        ],
        columns=[
            "numerocarte",
            "partitiondate",
            "date_derniere_transaction",
            "is_card_active_30j",
            "is_card_active_90j",
        ],
    )

    compare_dataframes(
        actual,
        expected,
        sort_by=["numerocarte"],
    )
