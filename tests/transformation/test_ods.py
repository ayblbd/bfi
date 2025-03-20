import pandas as pd

from src.transformation.ods import (
    build_contrat_produit,
    build_contrat_produit_carts,
)
from tests.conftest import compare_dataframes


def test_build_contrat_produit(spark):
    sous_compte = spark.createDataFrame(
        [
            ("0980100970", "elem1_value1", "2023-01-01"),
            ("0980100970", "elem1_value2", "2023-01-01"),
            ("0980140977", "elem1_value3", "2023-01-01"),
        ],
        [
            "NUMERODECOMPTE",
            "NUMEROELEMENTINTERNE",
            "partitiondate",
        ],
    )
    fonctionne_avec = spark.createDataFrame(
        [
            ("e_intern1", "elem1_value1", "2023-01-01"),
            ("e_intern2", "elem1_value2", "2023-01-01"),
            ("e_intern3", "elem1_value3", "2023-01-01"),
            ("e_intern4", "elem1_value4", "2023-01-01"),
        ],
        [
            "ELEMENTCONTRAT2",
            "ELEMENTCONTRAT1",
            "partitiondate",
        ],
    )
    elt_contrat = spark.createDataFrame(
        [
            ("num1", "e_intern1", "0034", "ordre1", "ps_spec1", "2023-01-01"),
            ("num2", "e_intern2", "0029", "ordre2", "ps_spec2", "2023-01-01"),
            ("num3", "e_intern3", "0033", "ordre3", "ps_spec3", "2023-01-01"),
            ("num4", "e_intern4", "CEN2", "ordre3", "ps_spec4", "2023-01-01"),
            ("num5", "e_intern5", "05", "ordre4", "ps_spec5", "2023-01-01"),
        ],
        [
            "NUMEROCONTRATINTERNE",
            "NUMEROELEMENTINTERNE",
            "TYPEDEPRODUIT",
            "ORDRE",
            "PARTIESPECIFIQUE",
            "partitiondate",
        ],
    )

    contrat = spark.createDataFrame(
        [
            (
                "3",
                "0001",
                "num_p1",
                "2023-01-01",
                "num1",
                "date1",
                "numero_Ex1",
                "etat_p1",
                "2023-01-01",
            ),
            (
                "1",
                "0033",
                "num_p2",
                "2023-01-02",
                "num2",
                "date2",
                "numero_Ex2",
                "etat_p2",
                "2023-01-01",
            ),
            (
                "8",
                "CEN2",
                "num_p3",
                "2023-01-03",
                "num3",
                "date3",
                "numero_Ex3",
                "etat_p3",
                "2023-01-01",
            ),
        ],
        [
            "ETATCONTRAT",
            "TYPECONTRAT",
            "NUMEROPACKAGE",
            "DATESOUSCRIPTION",
            "NUMEROCONTRATINTERNE",
            "DATEECHEANCE",
            "NUMEROCONTRATEXTERNE",
            "ETATPACKAGE",
            "partitiondate",
        ],
    )

    titulaire = spark.createDataFrame(
        [
            ("num_t1", "num1", "titulaire_sp1", "2023-01-01"),
            ("num_t2", "num2", "titulaire_sp2", "2023-01-01"),
            ("num_t3", "num3", "titulaire_sp3", "2023-01-01"),
            ("num_t4", "num4", "titulaire_sp4", "2023-01-01"),
        ],
        [
            "NUMEROTIERS",
            "NUMEROCONTRATINTERNE",
            "TITULAIREPRINCIPAL",
            "partitiondate",
        ],
    )

    type_contrat = spark.createDataFrame(
        [
            ("0001", "2023-01-01", "2023-01-01"),
            ("0001", "2023-01-02", "2023-01-01"),
            ("0033", "2023-01-03", "2023-01-01"),
            ("CEN2", "2023-01-04", "2023-01-01"),
        ],
        ["TYPECONTRAT", "DFVAL", "partitiondate"],
    )
    type_elt_contrat = spark.createDataFrame(
        [
            ("0001", "2023-01-01", "2023-01-01"),
            ("0001", "2023-01-02", "2023-01-01"),
            ("0033", "2023-01-03", "2023-01-01"),
            ("CEN2", "2023-01-04", "2023-01-01"),
            ("05", "2023-01-05", "2023-01-01"),
        ],
        ["TYPEDEPRODUIT", "DFVAL", "partitiondate"],
    )
    information_compte_client = spark.createDataFrame(
        [
            ("num_t1", "0980100970", "OUVERT", "2023-01-01", "date1", "2023-01-01"),
            ("num_t2", "0980100970", "OUVERT", "2023-01-02", "date2", "2023-01-01"),
            ("num_t3", "0980140977", "CLOTURE", "2023-01-03", "date3", "2023-01-01"),
        ],
        [
            "numerotiers",
            "numerodecompte",
            "etat_compte",
            "dateeffet",
            "dateecheance",
            "partitiondate",
        ],
    )
    partition_date = "2023-01-01"
    actual = build_contrat_produit(
        sous_compte,
        fonctionne_avec,
        elt_contrat,
        contrat,
        titulaire,
        type_contrat,
        type_elt_contrat,
        information_compte_client,
        partition_date,
    )
    expected = pd.DataFrame(
        [
            (
                "num_t3",
                "0980140977",
                "RESILIE",
                None,
                None,
                "2023-01-03",
                "CSC2",
                None,
                "date3",
                "2023-01-01",
            ),
            (
                "num_t1",
                "0980100970",
                "VALIDE",
                None,
                None,
                "2023-01-01",
                "CSC1",
                None,
                "date1",
                "2023-01-01",
            ),
            (
                "num_t2",
                "0980100970",
                "VALIDE",
                None,
                None,
                "2023-01-02",
                "CSC1",
                None,
                "date2",
                "2023-01-01",
            ),
        ],
        columns=[
            "NUMEROTIERS",
            "NUMERODECOMPTE",
            "ETATPRODUIT",
            "NUMEROCONTRATINTERNE",
            "NUMEROCARTE",
            "DATESOUSCRIPTION",
            "CODEPRODUIT",
            "NUMEROPACKAGE",
            "DATEECHEANCE",
            "partitiondate",
        ],
    )
    compare_dataframes(actual, expected, sort_by=["NUMERODECOMPTE"])


def test_build_contrat_produit_carts(spark):
    exploit_fonctionneavec = spark.createDataFrame(
        [
            ("e_intern1", "elem1_value1"),
            ("e_intern2", "elem1_value2"),
            ("e_intern3", "elem1_value3"),
            ("e_intern4", "elem1_value4"),
        ],
        [
            "elementcontrat2",
            "elementcontrat1",
        ],
    )
    exploit_elt_contrat = spark.createDataFrame(
        [
            ("num1", "e_intern1", "0034"),
            ("num2", "e_intern2", "0029"),
            ("num3", "e_intern3", "0033"),
            ("num4", "e_intern4", "CEN2"),
            ("num5", "e_intern5", "05"),
        ],
        [
            "numerocontratinterne",
            "numeroelementinterne",
            "typedeproduit",
        ],
    )
    exploit_souscompte = spark.createDataFrame(
        [
            ("num_c1", "elem1_value1"),
            ("num_c2", "elem1_value2"),
            ("num_c3", "elem1_value3"),
        ],
        [
            "numerodecompte",
            "numeroelementinterne",
        ],
    )
    exploit_titulaire = spark.createDataFrame(
        [
            ("num_t1", "num1"),
            ("num_t2", "num2"),
            ("num_t3", "num3"),
        ],
        [
            "numerotiers",
            "numerocontratinterne",
        ],
    )
    contrat = spark.createDataFrame(
        [
            ("3", "num_p1", "2023-01-01", "num1", "date1", "numero_Ex1"),
            ("1", "num_p2", "2023-01-02", "num2", "date2", "numero_Ex2"),
            ("8", "num_p3", "2023-01-03", "num3", "date3", "numero_Ex3"),
        ],
        [
            "etatcontrat",
            "numeropackage",
            "datesouscription",
            "numerocontratinterne",
            "dateecheance",
            "numerocontratexterne",
        ],
    )
    exploit_typeeltcontrat = spark.createDataFrame(
        [
            ("plib1", "0034", "2023-01-01", "2023-01-01"),
            ("plib2", "0029", "2023-01-02", "2023-01-01"),
            ("plib3", "0033", "2023-01-03", "2023-01-02"),
            ("plib4", "CEN2", "2023-01-04", "2023-01-03"),
            ("plib5", "05", "2023-01-05", "2023-01-04"),
        ],
        ["libproduit", "typedeproduit", "dfval", "partitiondate"],
    )

    actual = build_contrat_produit_carts(
        exploit_fonctionneavec,
        exploit_elt_contrat,
        exploit_souscompte,
        exploit_titulaire,
        contrat,
        exploit_typeeltcontrat,
    )

    expected = pd.DataFrame(
        [
            (
                "num_c2",
                "num_t2",
                "num2",
                "NONVALIDE",
                "0029",
                "numero_Ex2",
                "num_p2",
                "2023-01-02",
                "date2",
            ),
            (
                "num_c3",
                "num_t3",
                "num3",
                "VALIDE",
                "0033",
                "numero_Ex3",
                "num_p3",
                "2023-01-03",
                "date3",
            ),
        ],
        columns=[
            "numerodecompte",
            "numerotiers",
            "numerocontratinterne",
            "etatproduit",
            "codeproduit",
            "numerocarte",
            "numeropackage",
            "datesouscription",
            "dateecheance",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["numerodecompte"])
