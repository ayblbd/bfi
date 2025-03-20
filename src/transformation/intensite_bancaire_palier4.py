from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    count_distinct,
    least,
    lit,
    lower,
    substring,
    sum,
    trim,
    when,
)
from pyspark.sql.types import FloatType


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


def select_ressource_emploi(ressource_emploi: DataFrame) -> DataFrame:
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


def select_v_solde_clt(df):
    return df.select(
        "titulaire_code_cdm", "code_valeur_cdm", "sd_maj", "sd_cr", "sd_db"
    )


def build_base_ressource_emploi(
    ressource_emploi: DataFrame,
    map_ressource_emploi: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    has_credit_greater_than_100 = col("soldejourdb_devise") >= 100
    is_conso = col("famille_tdb_banque") == "Consommation"
    is_immo = col("famille_tdb_banque") == "Immobilier"
    is_dat = col("type_ressource_emploi").contains("DAT")

    ressource_emploi = select_ressource_emploi(ressource_emploi)
    ressource_emploi = ressource_emploi.filter(has_credit_greater_than_100)

    map_ressource_emploi = select_map_ressource_emploi(map_ressource_emploi)
    filtered_map_ressource_emploi = map_ressource_emploi.filter(
        is_conso | is_immo | is_dat
    )
    filtered_map_ressource_emploi = filtered_map_ressource_emploi.withColumn(
        "categorie",
        when(is_conso, "conso").when(is_immo, "immo").when(is_dat, "dat"),
    )

    ressource_emploi_type = ressource_emploi.join(
        filtered_map_ressource_emploi, ["pk_mapressourceemploi"]
    ).select("numerotiers", "type_ressource_emploi", "categorie")
    return (
        base_client.join(ressource_emploi_type, ["numerotiers"], "left")
        .groupBy("numerotiers")
        .pivot("categorie", values=["conso", "immo", "dat"])
        .agg(count(col("type_ressource_emploi")).alias("nb_produit"))
        .na.fill(value=0, subset=["conso", "immo", "dat"])
        .withColumnRenamed("conso", "nb_produit_conso")
        .withColumnRenamed("immo", "nb_produit_immo")
        .withColumnRenamed("dat", "nb_produit_dat")
        .select(
            "numerotiers", "nb_produit_conso", "nb_produit_immo", "nb_produit_dat"
        )
    )


def build_score_consommation(
    base_ressource_emploi: DataFrame,
) -> DataFrame:
    has_at_least_one_product = col("nb_produit_conso") > 0
    return base_ressource_emploi.withColumn(
        "score_conso", when(has_at_least_one_product, 0.5).otherwise(0)
    ).select("numerotiers", "nb_produit_conso", "score_conso")


def build_score_immobilier(base_ressource_emploi: DataFrame) -> DataFrame:
    has_at_least_one_product = col("nb_produit_immo") > 0
    return base_ressource_emploi.withColumn(
        "score_immo", when(has_at_least_one_product, 0.5).otherwise(0)
    ).select("numerotiers", "nb_produit_immo", "score_immo")


def build_score_assurance(
    valid_contrat_produit: DataFrame,
    gamme_assurance: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
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


def select_axa_assurance(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "numerocarte", concat_ws("", lit("0"), col("field2"))
    ).select("numerocarte", "encours")


def select_wafa_assurance(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("numero_adhesion", "numerocarte").select(
        "numerocarte", "type_d", "epargne"
    )


def select_rma_watanya(df: DataFrame) -> DataFrame:
    return df.withColumn("numerocarte", trim(col("numero_contrat"))).select(
        "numerocarte", "encours"
    )


def select_saham_assurance(df: DataFrame) -> DataFrame:
    return df.withColumn("numerocarte", trim(col("contrat"))).select(
        "numerocarte", "encours_net"
    )


def build_assurance_vie(
    axa_assurance: DataFrame,
    wafa_assurance: DataFrame,
    rma_watanya: DataFrame,
    saham_assurance: DataFrame,
) -> DataFrame:
    axa_assurance = select_axa_assurance(axa_assurance)
    axa_assurance = axa_assurance.withColumn("type_assurance", lit("axa")).select(
        "numerocarte", "type_assurance"
    )

    is_wafa_retrait = col("type_d") != "Z"
    wafa_assurance = select_wafa_assurance(wafa_assurance)
    wafa_assurance = (
        wafa_assurance.filter(is_wafa_retrait)
        .withColumn("type_assurance", lit("wafa"))
        .select("numerocarte", "type_assurance")
    )

    rma_watanya = select_rma_watanya(rma_watanya)
    rma_watanya = rma_watanya.withColumn("type_assurance", lit("rma")).select(
        "numerocarte", "type_assurance"
    )

    saham_assurance = select_saham_assurance(saham_assurance)
    saham_assurance = saham_assurance.withColumn(
        "type_assurance", lit("saham")
    ).select("numerocarte", "type_assurance")
    return (
        axa_assurance.unionByName(wafa_assurance)
        .unionByName(rma_watanya)
        .unionByName(saham_assurance)
    )


def build_base_assurance_vie(
    axa_assurance: DataFrame,
    wafa_assurance: DataFrame,
    rma_watanya: DataFrame,
    saham_assurance: DataFrame,
    valid_contrat_produit: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    assurance_vie = build_assurance_vie(
        axa_assurance, wafa_assurance, rma_watanya, saham_assurance
    )
    valid_axa_contrat = valid_contrat_produit.join(
        assurance_vie, ["numerocarte"]
    ).select("numerotiers", "numerocarte", "codeproduit", "type_assurance")
    valid_axa_contrat = valid_contrat_produit.join(
        assurance_vie, ["numerocarte"]
    ).select("numerotiers", "numerocarte", "codeproduit", "type_assurance")
    return (
        base_client.join(valid_axa_contrat, ["numerotiers"], "left")
        .groupBy("numerotiers", "type_assurance", "codeproduit")
        .agg(count_distinct(col("numerocarte")).alias("nb_contrat"))
        .groupBy("numerotiers")
        .pivot("type_assurance", values=["axa", "wafa", "rma", "saham"])
        .agg(sum(col("nb_contrat")))
        .na.fill(value=0, subset=["axa", "wafa", "rma", "saham"])
        .withColumn(
            "nb_produit_assurance_vie",
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
            "nb_produit_assurance_vie",
        )
    )


def build_base_liberis_epargne(
    valid_contrat_produit: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    is_liberis_epargne = col("codeproduit").isin(
        ["LER1", "LEE1", "LEP1", "LPR1", "LPP1"]
    )

    valid_contrat_liberis_epargne = valid_contrat_produit.filter(is_liberis_epargne)
    valid_contrat_liberis_epargne = valid_contrat_produit.filter(is_liberis_epargne)

    return (
        base_client.join(valid_contrat_liberis_epargne, ["numerotiers"], "left")
        .groupBy("numerotiers", "codeproduit")
        .agg(count_distinct(col("numerocarte")).alias("nb_contrat"))
        .groupBy("numerotiers")
        .agg(sum(col("nb_contrat")).alias("nb_produit_liberis_epargne"))
        .na.fill(value=0, subset=["nb_produit_liberis_epargne"])
        .select(
            "numerotiers",
            "nb_produit_liberis_epargne",
        )
    )


def build_base_csc(
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
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
    v_solde_clt: DataFrame,
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    is_encours_greater_than_zero = col("encours") > 0
    v_solde_clt = select_v_solde_clt(v_solde_clt)

    compte_client_selected = compte_client.select("numerotiers", "numerodecompte")

    nb_opcvm_per_tiers = (
        v_solde_clt.withColumn(
            "numerodecompte", substring(col("titulaire_code_cdm"), 2, 11)
        )
        .groupBy("numerodecompte", "code_valeur_cdm", "sd_maj")
        .agg(
            sum(
                col("sd_cr").cast(FloatType()) - col("sd_db").cast(FloatType())
            ).alias("encours")
        )
        .filter(is_encours_greater_than_zero)
        .groupBy("numerodecompte")
        .agg(count_distinct(col("code_valeur_cdm")).alias("nb_produit_opcvm"))
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
    v_solde_clt: DataFrame,
    axa_assurance: DataFrame,
    wafa_assurance: DataFrame,
    rma_watanya: DataFrame,
    saham_assurance: DataFrame,
    base_ressource_emploi: DataFrame,
    compte_client: DataFrame,
    base_client: DataFrame,
) -> DataFrame:
    liberis_epargne = build_base_liberis_epargne(valid_contrat_produit, base_client)
    csc = build_base_csc(compte_client, base_client)
    opcvm = build_base_opcvm(v_solde_clt, compte_client, base_client)
    assurance_vie = build_base_assurance_vie(
        axa_assurance,
        wafa_assurance,
        rma_watanya,
        saham_assurance,
        valid_contrat_produit,
        base_client,
    )
    base_ressource_emploi = base_ressource_emploi.select(
        "numerotiers", "nb_produit_dat"
    )

    has_more_than_one_product = col("total_produit_epargne") > 1
    has_one_product = col("total_produit_epargne") == 1

    return (
        liberis_epargne.join(csc, ["numerotiers"])
        .join(opcvm, ["numerotiers"])
        .join(assurance_vie, ["numerotiers"])
        .join(base_ressource_emploi, ["numerotiers"])
        .withColumn(
            "total_produit_epargne",
            col("nb_produit_csc")
            + col("nb_produit_liberis_epargne")
            + col("nb_produit_opcvm")
            + col("nb_produit_assurance_vie")
            + col("nb_produit_dat"),
        )
        .withColumn(
            "score_epargne",
            when(has_more_than_one_product, 1)
            .when(has_one_product, 0.5)
            .otherwise(0),
        )
    )


def calculate_score_equipement(
    score_conso: DataFrame,
    score_immo: DataFrame,
    score_assurance: DataFrame,
    score_epargne: DataFrame,
) -> DataFrame:
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
    )
