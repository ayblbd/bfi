from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    concat,
    concat_ws,
    crc32,
    lit,
    weekofyear,
    max,
)


def fdc_transformation(fdc: DataFrame, partition_date) -> DataFrame:
    window = Window.partitionBy(col("codefondsdecommerce"), col("rattache"))
    fdc_processed = (
        fdc.withColumn("filter_dfval", max("dfval").over(window))
        .withColumn("filter_ddval", max("ddval").over(window))
        .filter(
            (col("dfval") == col("filter_dfval"))
            & (col("ddval") == col("filter_ddval"))
        )
        .withColumnRenamed("rattache", "lieufdc")
        .withColumn(
            "pk_fondsdecommerce",
            crc32(concat_ws("/", col("codefondsdecommerce"), col("lieufdc"))),
        )
        .withColumnRenamed("cdgest", "code_fdc")
        .select(
            col("codefondsdecommerce"),
            col("lieufdc"),
            col("nomgest"),
            col("gestionnaire"),
            col("ddval"),
            col("dfval"),
            col("pk_fondsdecommerce"),
            col("code_fdc"),
        )
        .dropDuplicates()
        .na.fill(-1)
    )
    return fdc_processed


def build_niveau_regroupement(
    t608: DataFrame,
    t705: DataFrame,
    utilisateur: DataFrame,
) -> DataFrame:
    t608 = t608.filter(col("dfval") > col("partitiondate"))

    t705 = t705.filter(col("dfval") > col("partitiondate"))

    t608_for_agence_1 = (
        t608.filter(col("typedelieusysen").isin("30", "40"))
        .join(t705, t705["rattache"] == t608["entite"])
        .filter(t705["rattache"].isNotNull())
        .select(
            "rattache",
            "nomentit",
            "grreg",
            "dirzone",
            "cdplateforme",
            "cdgest",
            "codefondsdecommerce",
            "gestionnaire",
            "succursale",
        )
    )

    t608_for_agence_2 = (
        t608.join(t705, t705["rattache"] == t608["entite"])
        .filter(t705["rattache"].isNotNull())
        .filter(t705["rattache"] < "507")
        .filter(~t705["rattache"].isin("480", "485", "486"))
        .select(
            "rattache",
            "nomentit",
            "grreg",
            "dirzone",
            "cdplateforme",
            "cdgest",
            "codefondsdecommerce",
            "gestionnaire",
            "succursale",
        )
    )

    agence = (
        t608_for_agence_1.unionByName(t608_for_agence_2)
        .withColumnRenamed("rattache", "codeagence")
        .withColumnRenamed("nomentit", "nomagence")
        .withColumnRenamed("grreg", "coderegion")
        .withColumnRenamed("dirzone", "codebanque")
        .withColumnRenamed("cdgest", "codefdcencentrale")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "codebanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    t608_with_nomentit = t608.select("entite", "nomentit")

    agence_region = (
        agence.join(
            t608_with_nomentit, agence["coderegion"] == t608_with_nomentit["entite"]
        )
        .withColumnRenamed("nomentit", "nomregion")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    agence_region_banque = (
        agence_region.join(
            t608_with_nomentit,
            agence_region["codebanque"] == t608_with_nomentit["entite"],
        )
        .withColumnRenamed("nomentit", "nombanque")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    agence_region_banque_succ = (
        agence_region_banque.join(
            t608_with_nomentit,
            agence_region_banque["succursale"] == t608_with_nomentit["entite"],
            "left",
        )
        .withColumn("succursale", coalesce(col("succursale"), col("codeagence")))
        .withColumn("nomsuccursale", coalesce(col("nomentit"), col("nomagence")))
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
            "cdplateforme",
            "codefdcencentrale",
            "codefondsdecommerce",
            "gestionnaire",
        )
    )

    utilisateur = (
        utilisateur.filter(col("dfval") > col("partitiondate"))
        .withColumnRenamed("identifiant", "gestionnaire")
        .withColumn(
            "nomgestionnaire", concat(col("prenom"), lit(" "), col("nomuti"))
        )
        .select(
            "gestionnaire",
            "nomgestionnaire",
        )
    )

    return (
        agence_region_banque_succ.join(utilisateur, ["gestionnaire"], "left")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
            "cdplateforme",
        )
        .dropDuplicates()
        .na.fill(-1)
        .persist()
    )


def niveau_regroupement_transformation(t608: DataFrame) -> DataFrame:
    t608 = t608.filter(col("dfval") > col("partitiondate"))

    agence = (
        t608.withColumnRenamed("entite", "codeagence")
        .withColumnRenamed("nomentit", "nomagence")
        .withColumnRenamed("grreg", "coderegion")
        .withColumnRenamed("dirzone", "codebanque")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "codebanque",
        )
        .dropDuplicates()
    )
    t608_with_nomentit = t608.select("entite", "nomentit")

    agence_region = (
        agence.join(
            t608_with_nomentit, agence["coderegion"] == t608_with_nomentit["entite"]
        )
        .withColumnRenamed("nomentit", "nomregion")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "nomregion",
            "codebanque",
        )
    )

    agence_region_banque = (
        agence_region.join(
            t608_with_nomentit,
            agence_region["codebanque"] == t608_with_nomentit["entite"],
        )
        .withColumnRenamed("nomentit", "nombanque")
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
        )
    )

    agence_region_banque_succ = (
        agence_region_banque.join(
            t608_with_nomentit,
            agence_region_banque["succursale"] == t608_with_nomentit["entite"],
            "left",
        )
        .withColumn("succursale", coalesce(col("succursale"), col("codeagence")))
        .withColumn("nomsuccursale", coalesce(col("nomentit"), col("nomagence")))
        .select(
            "codeagence",
            "nomagence",
            "succursale",
            "nomsuccursale",
            "coderegion",
            "nomregion",
            "codebanque",
            "nombanque",
        )
    )
    return agence_region_banque_succ


def objectif_transformation(
    objectif: DataFrame, saisonalite: DataFrame
) -> DataFrame:
    join_clause = ["numero_semaine"]

    saisonalite = saisonalite.select(
        col("numero_semaine"), col("pourc_semaine"), col("pourc_cumul")
    )

    objectif_processed = (
        objectif.withColumn("numero_semaine", weekofyear(col("partitiondate")))
        .join(broadcast(saisonalite), join_clause, "left")
        .withColumn(
            "taux_objectif_cct_sem", col("objectif_cct") * col("pourc_semaine")
        )
        .withColumn(
            "taux_objectif_cmt_sem", col("objectif_cmt") * col("pourc_semaine")
        )
        .withColumn(
            "taux_objectif_rav_sem", col("objectif_rav") * col("pourc_semaine")
        )
        .withColumn(
            "taux_objectif_rat_sem", col("objectif_rat") * col("pourc_semaine")
        )
        .withColumn(
            "taux_objectif_sf_sem", col("objectif_sf") * col("pourc_semaine")
        )
        .withColumn(
            "taux_objectif_cct_cum", col("objectif_cct") * col("pourc_cumul")
        )
        .withColumn(
            "taux_objectif_cmt_cum", col("objectif_cmt") * col("pourc_cumul")
        )
        .withColumn(
            "taux_objectif_rav_cum", col("objectif_rav") * col("pourc_cumul")
        )
        .withColumn(
            "taux_objectif_rat_cum", col("objectif_rat") * col("pourc_cumul")
        )
        .withColumn("taux_objectif_sf_cum", col("objectif_sf") * col("pourc_cumul"))
    ).fillna(-1)

    return objectif_processed


def utilisateur_transformation(
    iam_perssones_poste: DataFrame, habilitation: DataFrame
) -> DataFrame:
    join_clause = ["lib_emploi"]
    utilisateur_processed = iam_perssones_poste.join(
        broadcast(habilitation), join_clause, "left"
    ).select(
        col("idu"),
        col("givenname"),
        col("nomjeunefille"),
        col("personaltitle"),
        col("email"),
        col("lib_unite"),
        col("lib_emploi"),
        col("code_ops"),
        col("role"),
    )
    return utilisateur_processed
