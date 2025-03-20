from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    first,
    lit,
    max,
    round,
    rtrim,
    substring,
    sum,
    when,
)
from pyspark.sql.types import FloatType

from src.transformation.common import (
    get_last_recorded_sold_per_client_from_last_month,
    get_last_recorded_sold_per_client_from_last_quarter,
    get_last_recorded_sold_per_client_from_last_week,
    get_last_recorded_sold_per_client_from_last_year,
)


def select_v_solde_clt(df: DataFrame) -> DataFrame:
    """
    Sélectionne et transforme les données de solde client.

    Cette fonction filtre les enregistrements avec un code de valeur non nul, extrait et
    formate les colonnes principales comme le numéro de compte et le code de valeur. Elle
    calcule également la quantité de valeurs disponibles ("nombrevaleur") en soustrayant
    les soldes débit et crédit.

    Retourne :
        Un DataFrame contenant les informations transformées sur les soldes des clients.
    """
    return (
        df.filter(col("code_valeur_cdm").isNotNull())
        .withColumn("numerodecompte", substring(col("titulaire_code_cdm"), 2, 11))
        .withColumn("codevaleur", rtrim(col("code_valeur_cdm")))
        .withColumnRenamed("sd_maj", "daterealisation")
        .withColumn("cours", lit(0))
        .withColumn(
            "nombrevaleur",
            col("sd_cr").cast(FloatType()) - col("sd_db").cast(FloatType()),
        )
        .select(
            "numerodecompte",
            "codevaleur",
            "nombrevaleur",
            "cours",
            "daterealisation",
            "partitiondate",
        )
    )


def select_v_corres_val(df: DataFrame) -> DataFrame:
    """
    Cette fonction sélectionne et transforme les colonnes pertinentes pour la correspondance des valeurs.
    Elle effectue les actions suivantes :
    - Nettoie la colonne "code_valeur_cdm" en supprimant les espaces à droite.
    - Renomme "val_isin_code" en "cours_ref_isin".
    - Sélectionne uniquement les colonnes nécessaires pour le traitement.

    Retourne :
        Un DataFrame contenant les correspondances de valeurs transformées.
    """
    return (
        df.withColumn("codevaleur", rtrim(col("code_valeur_cdm")))
        .withColumnRenamed("val_isin_code", "cours_ref_isin")
        .select("codevaleur", "val_lib_court", "cours_ref_isin", "partitiondate")
    )


def select_v_cours_ref(df: DataFrame) -> DataFrame:
    """
    Cette fonction sélectionne les colonnes pertinentes pour les cours de référence.
    Elle extrait uniquement les informations nécessaires pour le traitement.

    Retourne :
        Un DataFrame contenant les cours de référence sélectionnés.
    """
    return df.select(
        "cours_ref_date", "cours_ref_isin", "cours_ref", "partitiondate"
    )


def build_encours_opcvm(
    v_solde_clt: DataFrame,
    v_corres_val: DataFrame,
    v_cours_ref: DataFrame,
    code_valeur_opcvm: DataFrame,
) -> DataFrame:
    """
    Cette fonction construit un DataFrame contenant les encours OPCVM à partir des soldes clients,
    des correspondances des valeurs et des cours de référence.

    Principales étapes :
    1. Sélection des codes OPCVM depuis le DataFrame `code_valeur_opcvm`.
    2. Transformation des données des soldes clients avec la fonction `select_v_solde_clt`.
    3. Agrégation des soldes clients pour calculer le total des "nombrevaleur" et des "cours" par compte.
    4. Transformation des correspondances de valeurs et des cours de référence avec les fonctions
    `select_v_corres_val` et `select_v_cours_ref`.
    5. Construction des cours de référence par valeur en considérant le dernier cours disponible.
    6. Jointure des soldes clients avec les cours de référence et les codes OPCVM.
    7. Ajustement de la colonne "cours" en utilisant les cours de référence lorsque nécessaire.

    Retourne :
        Un DataFrame contenant les informations suivantes :
        - numerodecompte : Numéro de compte client.
        - codevaleur : Code de la valeur OPCVM.
        - nombrevaleur : Nombre total de valeurs.
        - cours : Cours utilisé pour calculer les encours.
        - daterealisation : Date de réalisation.
        - datetraitement : Date de traitement (partition).
    """
    code_valeur_opcvm = code_valeur_opcvm.select("codevaleur")
    v_solde_clt = select_v_solde_clt(v_solde_clt)
    solde_client = v_solde_clt.groupBy(
        "numerodecompte",
        "codevaleur",
        "daterealisation",
        "partitiondate",
    ).agg(
        sum("nombrevaleur").alias("nombrevaleur"),
        sum("cours").alias("cours"),
    )

    v_corres_val = select_v_corres_val(v_corres_val)
    v_cours_ref = select_v_cours_ref(v_cours_ref)
    cours_val_ref = (
        v_corres_val.join(v_cours_ref, ["cours_ref_isin", "partitiondate"])
        .groupBy("codevaleur", "cours_ref_isin", "partitiondate")
        .agg(max("cours_ref_date").alias("cours_ref_date"))
    )

    cours_val_ref = (
        cours_val_ref.join(
            v_cours_ref,
            ["cours_ref_date", "cours_ref_isin", "partitiondate"],
            "left",
        )
        .withColumn(
            "cours_ref",
            when(col("cours_ref_isin") == "BE0009525883", 0).otherwise(
                col("cours_ref")
            ),
        )
        .groupBy("codevaleur", "partitiondate")
        .agg(max("cours_ref").alias("cours_ref"))
        .select("codevaleur", "cours_ref", "partitiondate")
    )

    return (
        solde_client.join(cours_val_ref, ["codevaleur", "partitiondate"], "left")
        .join(code_valeur_opcvm, ["codevaleur"])
        .withColumn(
            "cours",
            when(col("numerodecompte").isNull(), col("cours"))
            .otherwise(col("cours_ref"))
            .cast("double"),
        )
        .withColumnRenamed("partitiondate", "datetraitement")
        .select(
            "numerodecompte",
            "codevaleur",
            "nombrevaleur",
            "cours",
            "daterealisation",
            "datetraitement",
        )
    )


def build_detail_opcvm(
    v_solde_clt: DataFrame,
    v_corres_val: DataFrame,
    v_cours_ref: DataFrame,
    code_valeur_opcvm: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Cette fonction filtre les encours OPCVM pour une date de partition spécifique.

    Retourne :
        Un DataFrame contenant les détails des encours OPCVM pour la date donnée.
    """
    return build_encours_opcvm(
        v_solde_clt, v_corres_val, v_cours_ref, code_valeur_opcvm
    ).filter(col("datetraitement") == partition_date)


def build_opcvm(
    v_solde_clt: DataFrame,
    v_corres_val: DataFrame,
    v_cours_ref: DataFrame,
    code_valeur_opcvm: DataFrame,
    compte: DataFrame,
    partition_date: str,
) -> DataFrame:
    """
    Cette fonction construit les indicateurs OPCVM pour une date de partition spécifique
    et inclut les historiques des encours sur différentes périodes (semaine, mois, trimestre, année).

    Principales étapes :
    1. Filtrage des données pour conserver uniquement les enregistrements avec des valeurs positives.
    2. Utilisation d'une fenêtre pour déterminer le cours maximum pour chaque groupe de données (compte, codevaleur, datetraitement).
    3. Calcul des encours à partir du nombre de valeurs et du cours.
    4. Application de transformations pour récupérer les encours historiques sur différentes périodes :
    - Dernière semaine, 2 dernières semaines.
    - Dernier mois, 2 derniers mois.
    - Dernier trimestre, dernière année.
    5. Jointure des encours actuels et historiques avec les comptes clients.
    6. Remplissage des valeurs manquantes avec 0 pour les indicateurs clés (KPI).

    Retourne :
        Un DataFrame contenant les indicateurs OPCVM avec les encours actuels et historiques.

    Paramètres :
    - v_solde_clt : DataFrame contenant les soldes des clients.
    - v_corres_val : DataFrame des correspondances des valeurs.
    - v_cours_ref : DataFrame des cours de référence.
    - code_valeur_opcvm : DataFrame contenant les codes OPCVM.
    - compte : DataFrame des informations de compte client.
    - partition_date : Date de partition pour filtrer les encours actuels.
    """
    has_positive_valeur = col("nombrevaleur") > 0
    has_positive_cours = col("cours") > 0
    window_spec = Window.partitionBy(
        "numerodecompte", "codevaleur", "datetraitement"
    ).orderBy(col("daterealisation").desc())

    opcvm_history = build_encours_opcvm(
        v_solde_clt, v_corres_val, v_cours_ref, code_valeur_opcvm
    )
    opcvm_history = (
        opcvm_history.filter(has_positive_valeur & has_positive_cours)
        .withColumn("max_cours", first("cours").over(window_spec))
        .groupBy("numerodecompte", "codevaleur", "datetraitement")
        .agg(
            sum("nombrevaleur").alias("nombrevaleur"),
            max("max_cours").alias("cours"),
        )
        .withColumn("encours", round(col("nombrevaleur") * col("cours"), 2))
        .select(
            "numerodecompte",
            "codevaleur",
            "nombrevaleur",
            "cours",
            "encours",
            "datetraitement",
        )
    )

    compte = compte.select("numerodecompte", "numerotiers")

    encours_opcvm = opcvm_history.filter(col("datetraitement") == partition_date)

    encours_opcvm_history = (
        opcvm_history.transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="encours",
            partition_date=partition_date,
            weeks=1,
            partition_columns=[
                "numerodecompte",
                "codevaleur",
            ],
            order_column="datetraitement",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_week,
            by="encours",
            partition_date=partition_date,
            weeks=2,
            partition_columns=[
                "numerodecompte",
                "codevaleur",
            ],
            order_column="datetraitement",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="encours",
            partition_date=partition_date,
            months=1,
            partition_columns=[
                "numerodecompte",
                "codevaleur",
            ],
            order_column="datetraitement",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_month,
            by="encours",
            partition_date=partition_date,
            months=2,
            partition_columns=[
                "numerodecompte",
                "codevaleur",
            ],
            order_column="datetraitement",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_quarter,
            by="encours",
            partition_columns=[
                "numerodecompte",
                "codevaleur",
            ],
            order_column="datetraitement",
        )
        .transform(
            get_last_recorded_sold_per_client_from_last_year,
            by="encours",
            partition_date=partition_date,
            years=1,
            partition_columns=[
                "numerodecompte",
                "codevaleur",
            ],
            order_column="datetraitement",
        )
        .select(
            "numerodecompte",
            "codevaleur",
            "datetraitement",
            "last_week_encours",
            "last_2_weeks_encours",
            "last_month_encours",
            "last_2_months_encours",
            "last_quarter_encours",
            "last_year_encours",
        )
    )

    kpis = [
        "nombrevaleur",
        "cours",
        "encours_s",
        "encours_s_1",
        "encours_s_2",
        "encours_m_1",
        "encours_m_2",
        "encours_t_1",
        "encours_y_1",
    ]

    return (
        encours_opcvm.join(compte, ["numerodecompte"], "left")
        .join(
            encours_opcvm_history,
            [
                "numerodecompte",
                "codevaleur",
                "datetraitement",
            ],
            "left_outer",
        )
        .withColumnRenamed("encours", "encours_s")
        .withColumnRenamed("last_week_encours", "encours_s_1")
        .withColumnRenamed("last_2_weeks_encours", "encours_s_2")
        .withColumnRenamed("last_month_encours", "encours_m_1")
        .withColumnRenamed("last_2_months_encours", "encours_m_2")
        .withColumnRenamed("last_quarter_encours", "encours_t_1")
        .withColumnRenamed("last_year_encours", "encours_y_1")
        .na.fill(0, kpis)
        .select("numerotiers", "numerodecompte", "codevaleur", *kpis)
    )
