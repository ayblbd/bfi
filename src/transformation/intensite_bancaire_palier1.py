from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    add_months,
    coalesce,
    col,
    date_trunc,
    least,
    lit,
    make_date,
    max,
    min,
    months_between,
    sum,
    to_date,
    trim,
    when,
)


def is_csc_sec(
    compte: DataFrame,
) -> DataFrame:
    window = Window.partitionBy("numerotiers")
    csc_natures = ["010", "011", "014"]
    return compte.withColumn(
        "is_csc_sec", when(col("naturecompte").isin(csc_natures), 1).otherwise(0)
    ).withColumn("is_csc_sec", min(col("is_csc_sec")).over(window))


def select_mouvement_comptable(
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    partition_date: str,
) -> DataFrame:
    partition_date_2_month_eralier_start = date_trunc(
        "MM", add_months(to_date(lit(partition_date)), -2)
    )

    taux_change_bam = taux_change_bam.withColumn(
        "cd_dev_oper", trim(col("cd_dev_oper"))
    ).select(
        "cd_dev_oper",
        "midbam",
        # "date_cours",
        "partitiondate",
    )

    mouvement_comptable = (
        mouvement_comptable.withColumn(
            "date_op",
            make_date(col("annee_traitement"), col("mois_traitement"), lit(1)),
        )
        .filter(
            (to_date(col("date_op")) >= partition_date_2_month_eralier_start)
            & (to_date(col("date_op")) <= to_date(lit(partition_date)))
        )
        .filter(col("sens") == "C")
        .withColumn("devise", trim("devise"))
        .join(
            taux_change_bam,
            (
                mouvement_comptable["datetraitement"]
                == taux_change_bam["partitiondate"]
            )
            & (col("devise") == taux_change_bam["cd_dev_oper"]),
            how="left",
        )
        .withColumn(
            "midbam", when(col("devise") == lit("000C"), 1).otherwise(col("midbam"))
        )
        .withColumn(
            "month_m",
            col("date_op")
            == date_trunc("MM", to_date(lit(partition_date))).cast("date"),
        )
        .withColumn(
            "month_m-1",
            col("date_op")
            == date_trunc("MM", add_months(to_date(lit(partition_date)), -1)).cast(
                "date"
            ),
        )
        .withColumn(
            "month_m-2",
            col("date_op")
            == date_trunc("MM", add_months(to_date(lit(partition_date)), -2)).cast(
                "date"
            ),
        )
        .withColumn("montant", (col("midbam") * col("montantecriture")) / 100)
        .withColumn("flag_2000", when(col("montant") >= 2000, 1).otherwise(0))
        .groupBy("numerodecompte")
        .agg(
            max(when(col("month_m"), col("flag_2000")).otherwise(0)).alias(
                "m_flag_2000"
            ),
            max(when(col("month_m-1"), col("flag_2000")).otherwise(0)).alias(
                "m-1_flag_2000"
            ),
            max(when(col("month_m-2"), col("flag_2000")).otherwise(0)).alias(
                "m-2_flag_2000"
            ),
            sum(
                when(col("month_m"), coalesce(col("montant"), lit(0))).otherwise(0)
            ).alias("total_montant"),
        )
        .withColumn("date_op", to_date(date_trunc("MM", lit(partition_date))))
        .select(
            "numerodecompte",
            "date_op",
            # "dateoperation",
            "total_montant",
            "m_flag_2000",
            "m-1_flag_2000",
            "m-2_flag_2000",
            # "codeoperation",
            # "libellecomplementaire",
        )
    )

    return mouvement_comptable


def build_monthly_totals(
    mouvement_comptable: DataFrame,
    taux_change_bam: DataFrame,
    compte: DataFrame,
    tiers: DataFrame,
    partition_date: str,
) -> DataFrame:
    compte = (
        compte.transform(is_csc_sec)
        .filter(
            col("naturecompte").isin(
                ["001", "002", "003", "004", "005", "006", "010", "011", "014"]
            )
        )
        .select(
            "numerodecompte",
            "numerotiers",
            "naturecompte",
            "is_csc_sec",
        )
    )

    tiers = (
        tiers.withColumn(
            "anciennete",
            months_between(
                to_date(lit(partition_date)),
                add_months(date_trunc("MM", col("date_creation")), 1),
            ),
        )
        .select(
            "date_creation",
            "anciennete",
            "numerotiers",
            "is_mre",
            "code_csp",
        )
        .drop_duplicates()
    )

    virements_anciennete = select_mouvement_comptable(
        mouvement_comptable,
        taux_change_bam,
        partition_date,
    )

    return (
        compte.join(virements_anciennete, ["numerodecompte"], how="left")
        .groupBy("numerotiers")
        .agg(
            max("m_flag_2000").alias("m_flag_2000"),
            max("m-1_flag_2000").alias("m-1_flag_2000"),
            max("m-2_flag_2000").alias("m-2_flag_2000"),
            sum(coalesce(col("total_montant"), lit(0))).alias("total_montant"),
            min("is_csc_sec").alias("is_csc_sec"),
        )
        .join(tiers, ["numerotiers"], how="left")
        .withColumn(
            "3_flags_sum",
            (col("m_flag_2000") + col("m-1_flag_2000") + col("m-2_flag_2000")),
        )
        .withColumn(
            "score_flag_2000_M",
            when((col("anciennete") >= 3) & (col("3_flags_sum") < 2), 0)
            .when((col("anciennete") >= 3) & (col("3_flags_sum") >= 2), 1)
            .when(
                col("anciennete") <= 2,
                least(coalesce(col("3_flags_sum"), lit(0)), lit(1)),
            )
            .otherwise(0),
        )
        .withColumn("date_op", to_date(date_trunc("MM", lit(partition_date))))
        .select(
            "numerotiers",
            "date_op",
            "anciennete",
            "total_montant",
            # "m-1_total_montant",
            # "m-2_total_montant",
            # "montant_rolling_avg",
            "m_flag_2000",
            "m-1_flag_2000",
            "m-2_flag_2000",
            "3_flags_sum",
            "score_flag_2000_M",
            "is_csc_sec",
            "is_mre",
            "code_csp",
        )
    )
