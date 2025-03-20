from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    add_months,
    col,
    concat,
    length,
    lit,
    substring,
    to_date,
    trim,
    when,
)


def build_wafasalaf_pret(df: DataFrame, partition_date: str) -> DataFrame:
    return (
        df.withColumn("nomemprunteur", trim(substring(df.value, 1, 32)))
        .withColumn("prenomemprunteur", trim(substring(df.value, 33, 32)))
        .withColumn("typeidentite", trim(substring(df.value, 65, 2)))
        .withColumn("numidentite", trim(substring(df.value, 67, 10)))
        .withColumn("numtelephone", trim(substring(df.value, 77, 10)))
        .withColumn("adresse", trim(substring(df.value, 87, 96)))
        .withColumn("devise", trim(substring(df.value, 183, 3)))
        .withColumn("cdbanque", trim(substring(df.value, 186, 3)))
        .withColumn("cdville", trim(substring(df.value, 189, 3)))
        .withColumn("aux", trim(substring(df.value, 192, 4)))
        .withColumn("numerodecompte", trim(substring(df.value, 196, 11)))
        .withColumn("code_agence", substring(col("numerodecompte"), 1, 3))
        .withColumn("cle1", trim(substring(df.value, 207, 1)))
        .withColumn("clerib", trim(substring(df.value, 208, 2)))
        .withColumn("ind_ctx", trim(substring(df.value, 210, 1)))
        .withColumn("categoriedeprets", trim(substring(df.value, 211, 4)))
        .withColumn("num_doss", trim(substring(df.value, 215, 8)))
        .withColumn(
            "numerodeprets", concat(lit("0"), substring(col("num_doss"), 1, 7))
        )
        .withColumn("field45", trim(substring(df.value, 223, 8)))
        .withColumn("doti", trim(substring(df.value, 231, 1)))
        .withColumn("etat", trim(substring(df.value, 232, 1)))
        .withColumn("lieutraitement", lit("CdmS@rii"))
        .withColumn(
            "nominal", trim(substring(df.value, 233, 10)).cast("float") / 100
        )
        .withColumn("restdu", trim(substring(df.value, 243, 10)).cast("float") / 100)
        .withColumn("duree", trim(substring(df.value, 253, 3)).cast("int"))
        .withColumn(
            "dt_realisation", to_date(trim(substring(df.value, 256, 8)), "yyyyMMdd")
        )
        .withColumn("datedeblocage", col("dt_realisation"))
        .withColumn("datesaisie", col("dt_realisation"))
        .withColumn(
            "dateecheance",
            when(col("dt_realisation").isNull(), None).otherwise(
                add_months(col("dt_realisation"), col("duree"))
            ),
        )
        .withColumn("ech1", to_date(trim(substring(df.value, 264, 9)), "yyyyMMdd"))
        .withColumn("numjourech", trim(substring(df.value, 273, 2)).cast("int"))
        .withColumn(
            "dt_proposition", to_date(trim(substring(df.value, 275, 8)), "yyyyMMdd")
        )
        .withColumn("field41", trim(substring(df.value, 283, 6)))
        .withColumn(
            "dt_accord", to_date(trim(substring(df.value, 289, 8)), "yyyyMMdd")
        )
        .withColumn("field42", trim(substring(df.value, 297, 6)))
        .withColumn(
            "dt_dernmodif", to_date(trim(substring(df.value, 303, 8)), "yyyyMMdd")
        )
        .withColumn("field43", trim(substring(df.value, 311, 6)))
        .withColumn(
            "dt_signature", to_date(trim(substring(df.value, 317, 8)), "yyyyMMdd")
        )
        .withColumn("taux_eff", trim(substring(df.value, 325, 9)))
        .withColumn(
            "taux", trim(substring(df.value, 334, 9)).cast("float") / 1000000
        )
        .withColumn("code_org", trim(substring(df.value, 343, 5)))
        .withColumn(
            "mtt_assur", trim(substring(df.value, 348, 10)).cast("float") / 100
        )
        .withColumn(
            "mtt_fr_dos", trim(substring(df.value, 358, 10)).cast("float") / 100
        )
        .withColumn(
            "mtt_tva", trim(substring(df.value, 368, 10)).cast("float") / 100
        )
        .withColumn("nombreimpaye", trim(substring(df.value, 378, 4)).cast("int"))
        .withColumn(
            "mnt_globimpay", trim(substring(df.value, 382, 10)).cast("float") / 100
        )
        .withColumn(
            "dt_premechimp", to_date(trim(substring(df.value, 392, 8)), "yyyyMMdd")
        )
        .withColumn(
            "mnt_interretard", trim(substring(df.value, 400, 10)).cast("float") / 100
        )
        .withColumn(
            "mnt_interpenalite",
            trim(substring(df.value, 410, 10)).cast("float") / 100,
        )
        .withColumn("field44", trim(substring(df.value, 420, 8)))
        .withColumn(
            "montantecheance", trim(substring(df.value, 428, 10)).cast("float") / 100
        )
        .withColumn("situationpret", lit("ACCORDE"))
        .withColumn("initiateur", trim(substring(df.value, 438, 32)))
        .withColumn("field47", trim(substring(df.value, 470, 28)))
        .withColumn(
            "matricule",
            when(length(trim(substring(df.value, 498, 4))) == 0, None).otherwise(
                concat(lit("cdm-u"), trim(substring(df.value, 498, 4)))
            ),
        )
        .withColumn("code_age_app", trim(substring(df.value, 502, 10)))
        .withColumn("code_regl", trim(substring(df.value, 512, 10)))
        .withColumn(
            "date_cpt_cli", to_date(trim(substring(df.value, 522, 12)), "yyyyMMdd")
        )
        .withColumn(
            "date_ctx", to_date(trim(substring(df.value, 534, 8)), "yyyyMMdd")
        )
        .withColumn(
            "date_ech_imp", to_date(trim(substring(df.value, 542, 10)), "yyyyMMdd")
        )
        .withColumn(
            "date_rpat", to_date(trim(substring(df.value, 552, 10)), "yyyyMMdd")
        )
        .withColumn("nom_cli_empl", trim(substring(df.value, 562, 50)))
        .withColumn("forcage", trim(substring(df.value, 612, 1)))
        .withColumn("field57", trim(substring(df.value, 613, 4)))
        .withColumn(
            "frais_dos_ttc", trim(substring(df.value, 617, 22)).cast("float")
        )
        .withColumn("frais_imm", trim(substring(df.value, 639, 22)).cast("float"))
        .withColumn("frais_recouv", trim(substring(df.value, 661, 22)).cast("float"))
        .withColumn("heure_accord", trim(substring(df.value, 683, 10)))
        .withColumn("heure_debloc", trim(substring(df.value, 693, 10)))
        .withColumn("libelle_bar", trim(substring(df.value, 703, 100)))
        .withColumn("mt_crd_ctx", trim(substring(df.value, 803, 22)).cast("float"))
        .withColumn("mt_int_diff", trim(substring(df.value, 825, 22)).cast("float"))
        .withColumn(
            "mt_cred_autre", trim(substring(df.value, 847, 22)).cast("float")
        )
        .withColumn("mt_cred_brut", trim(substring(df.value, 869, 22)).cast("float"))
        .withColumn("mt_rpat", trim(substring(df.value, 891, 22)).cast("float"))
        .withColumn("mt_cred_net", trim(substring(df.value, 913, 22)).cast("float"))
        .withColumn("nature_credit", trim(substring(df.value, 935, 5)))
        .withColumn("nbr_aff_sub", trim(substring(df.value, 940, 3)))
        .withColumn("nom_agence", trim(substring(df.value, 943, 100)))
        .withColumn("rev_cli", trim(substring(df.value, 1043, 22)).cast("float"))
        .withColumn("type_ra", trim(substring(df.value, 1065, 10)))
        .withColumn("user_debl", trim(substring(df.value, 1075, 30)))
        .withColumn("mt_autre", trim(substring(df.value, 1105, 22)).cast("float"))
        .withColumn("heure_instr", trim(substring(df.value, 1127, 10)))
        .withColumn("mt_rees", trim(substring(df.value, 1137, 22)).cast("float"))
        .withColumn(
            "rib",
            concat(
                col("cdbanque"),
                col("cdville"),
                col("aux"),
                col("numerodecompte"),
                col("cle1"),
                col("clerib"),
            ),
        )
        .withColumn("rib_prelev", trim(substring(df.value, 1159, 24)))
        .withColumn("date_traitement", to_date(lit(partition_date)))
        .withColumn("partitiondate", col("date_traitement"))
        .drop("value")
        .select(
            "code_agence",
            "numerodecompte",
            "numerodeprets",
            "categoriedeprets",
            "nominal",
            "restdu",
            "datedeblocage",
            "dateecheance",
            "duree",
            "montantecheance",
            "taux",
            "nombreimpaye",
            "situationpret",
            "etat",
            "lieutraitement",
            "nomemprunteur",
            "prenomemprunteur",
            "typeidentite",
            "numidentite",
            "numtelephone",
            "adresse",
            "doti",
            "numjourech",
            "dt_premechimp",
            "dt_proposition",
            "dt_accord",
            "dt_dernmodif",
            "dt_signature",
            "mnt_globimpay",
            "mnt_interretard",
            "mnt_interpenalite",
            "initiateur",
            "datesaisie",
            "matricule",
            "code_age_app",
            "code_regl",
            "date_cpt_cli",
            "date_ctx",
            "date_ech_imp",
            "date_rpat",
            "nom_cli_empl",
            "forcage",
            "frais_dos_ttc",
            "frais_imm",
            "frais_recouv",
            "heure_accord",
            "heure_debloc",
            "libelle_bar",
            "mt_crd_ctx",
            "mt_int_diff",
            "mt_cred_autre",
            "mt_cred_brut",
            "mt_rpat",
            "mt_cred_net",
            "nature_credit",
            "nbr_aff_sub",
            "nom_agence",
            "rev_cli",
            "type_ra",
            "user_debl",
            "mt_autre",
            "heure_instr",
            "mt_rees",
            "devise",
            "rib",
            "ind_ctx",
            "ech1",
            "code_org",
            "mtt_assur",
            "mtt_fr_dos",
            "mtt_tva",
            "rib_prelev",
            "partitiondate",
        )
    )
