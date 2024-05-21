from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import (
    abs,
    approx_count_distinct,
    array_join,
    coalesce,
    col,
    collect_list,
    collect_set,
    from_unixtime,
    exp,
    expr,
    lag,
    last,
    lit,
    regexp_extract,
    split,
    substring,
    sum,
    trim,
    unix_timestamp,
    when,
)

from src.utils import (
    extract_field_from_message,
    extract_field_with_brackets_from_message,
    ffill,
    is_ip,
    to_unix,
)

MONEY_OPERATIONS = ["2", "5", "B"]
MAX_DISPOSITION = 2_000
MAX_VIREMENT = 50_000
MAX_CASHEXPRESS = 80_000
SIGNIFICANT_DISPOSITION = 1_500
SIGNIFICANT_VIREMENT = 20_000
LOW_VIREMENT_THRESHOLD = 500


def transform(
    audit_ebk: DataFrame, mobile_device: DataFrame, device_history: DataFrame
) -> DataFrame:
    df = to_unix(audit_ebk, "audit_fact_date")

    df = amount_virement(df)
    df = amount_disposition(df)
    df = amount_cashexpress(df)
    df = amount_scaled(df)
    df = device_pattern_features(df, mobile_device, device_history, unix_timestamp())
    facture = df.filter(col("event_type") == lit("A"))
    df = reset_pattern_features(df)
    df = fill_all(df)
    extra_cols = [column for column in df.columns if column not in facture.columns]
    for column in extra_cols:
        facture = facture.withColumn(column, lit(None))
    return df.unionByName(facture).withColumn(
        "audit_fact_date", from_unixtime("audit_fact_date")
    )


def fill_all(df: DataFrame) -> DataFrame:
    df = df.fillna(
        0,
        [
            "device_change",
            "device_count",
            "cumsum_disposition_12_hours",
            "cumsum_virement_12_hours",
            "average_amount_scaled_12h",
        ],
    ).fillna(-1, ["time_diff_to_attijari_secure", "device_age"])

    return device_age_normalized(time_diff_to_attijari_secure_normalized(df))


def reset_pattern_features(df: DataFrame) -> DataFrame:
    df = df.filter((col("event_type") != lit("8")) & (col("event_type") != lit("A")))
    df = rank(df, "rank", "9")

    df = is_reset_pattern_disposition(df)
    df = is_reset_pattern_virement(df)
    df = is_reset(df).drop("rank")

    df = time_diff(df)
    df = time_diff_inv(df)
    df = time_diff_to_reset(df)
    df = time_diff_to_reset_normalized(df)
    df = time_diff_to_reset_normalized_sigmoid(df)
    df = criterion(df)
    return df


def ip_features(df: DataFrame) -> DataFrame:
    df = accounts_count(df)
    df = time_diff_to_last_money_operation_by_ip(df)
    return df


def device_pattern_features(
    audit_ebk: DataFrame,
    mobile_device: DataFrame,
    device_history: DataFrame,
    relative_to: Column,
) -> DataFrame:
    mobile_device = to_unix(mobile_device, "creation_date")
    device_history = to_unix(device_history, "connection_date")

    df = join_tables(
        audit_ebk.filter(
            col("audit_fact_date") >= unix_timestamp(lit("2022-02-18"), "yyyy-MM-dd")
        ),
        mobile_device,
        device_history,
    )
    # audit_ebk_with_device_id = audit_ebk.filter(col("device_id").isNotNull()).withColumnRenamed("device_id", "mobile_device_id")

    df = ffill(df, "uuid")
    df = ffill(df, "creation_date")
    # df = ffill(df, "device_id")
    df = ffill(df, "name")
    df = ffill(df, "os")
    # since join on login only, we should forward fill on mobile_device_id to have device infos for other events
    df = ffill(df, "mobile_device_id")

    df = device_change(df)
    df = device_count(df)
    df = is_attijari_secure_deactivated_pattern(df)
    df = is_attijari_secure_activated_pattern(df)
    df = time_diff_to_attijari_secure(df)
    df = time_diff_to_attijari_secure_normalized(df)

    df = device_age(df, relative_to)
    df = device_age_normalized(df)

    df = cumsum_disposition_12_hours(df)
    df = cumsum_virement_12_hours(df)
    df = cumsum_cashexpress_12_hours(df)

    df = average_amount_scaled_12h(df)
    df = is_safe_facture_device(df)
    df = is_awb_secure_deactivated_safe_device(df)
    df = is_awb_secure_activated_safe_device(df)

    return df


def is_awb_secure_deactivated_safe_device(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_awb_secure_deactivated_safe_device",
        when(
            col("is_safe_facture_device") == lit(0),
            col("is_attijari_secure_deactivated_pattern"),
        ).otherwise(0),
    )


def is_awb_secure_activated_safe_device(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_awb_secure_activated_safe_device",
        when(
            col("is_safe_facture_device") == lit(0),
            col("is_attijari_secure_activated_pattern"),
        ).otherwise(0),
    )


def join_tables(
    audit_ebk: DataFrame, mobile_device: DataFrame, device_history: DataFrame
) -> DataFrame:
    history_with_uuid = (
        device_history.select("mobile_device_id", "connection_date")
        .join(
            mobile_device.withColumnRenamed("id", "mobile_device_id"),
            ["mobile_device_id"],
        )
        .select(
            "id_user",
            "mobile_device_id",
            "name",
            "os",
            "uuid",
            "creation_date",
            "connection_date",
        )
    )
    return audit_ebk.join(
        history_with_uuid,
        (col("user_id") == col("id_user"))
        & (col("event_type") == lit("8"))
        & (abs(col("audit_fact_date") - col("connection_date")) < 5),
        "left_outer",
    ).drop("id_user", "connection_date")


def rank(df: DataFrame, rank_name: str, event_trigger: str) -> DataFrame:
    return df.withColumn(
        rank_name,
        sum(
            when(
                col("event_type") == lit(event_trigger),
                1,
            ).otherwise(0)
        ).over(Window.partitionBy("user_id").orderBy("audit_fact_date")),
    )


def is_reset_pattern_disposition(df: DataFrame) -> DataFrame:
    conditions = regexp_extract(
        "pattern",
        r"^9,.*3,",
        0,
    ) != lit("")

    df = (
        df.withColumn(
            "pattern",
            when(
                col("event_type").isin("5"),
                collect_list(col("event_type")).over(
                    Window.partitionBy("user_id", "rank")
                    .orderBy("audit_fact_date")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            ),
        )
        .withColumn("pattern", array_join(col("pattern"), ","))
        .withColumn("is_reset_pattern_disposition", when(conditions, 1).otherwise(0))
        .drop("pattern")
    )

    return df


def amount_scaled(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "amount_virement_scaled",
            col("amount_virement") / MAX_VIREMENT,
        )
        .withColumn(
            "amount_disposition_scaled",
            col("amount_disposition") / MAX_DISPOSITION,
        )
        .withColumn(
            "amount_cashexpress_scaled",
            col("amount_cashexpress") / MAX_CASHEXPRESS,
        )
        .withColumn(
            "amount_scaled",
            col("amount_virement_scaled")
            + col("amount_disposition_scaled")
            + col("amount_cashexpress_scaled"),
        )
        .drop(
            "amount_virement_scaled",
            "amount_disposition_scaled",
            "amount_cashexpress_scaled",
        )
    )


def is_reset(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_reset",
        col("is_reset_pattern_virement") + col("is_reset_pattern_disposition"),
    )


def is_reset_pattern_virement(df: DataFrame) -> DataFrame:
    conditions = regexp_extract(
        "pattern",
        r"^9,.*3,.*1,",
        0,
    ) != lit("")

    df = (
        df.withColumn(
            "pattern",
            when(
                col("event_type").isin("2"),
                collect_list(col("event_type")).over(
                    Window.partitionBy("user_id", "rank")
                    .orderBy("audit_fact_date")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            ),
        )
        .withColumn("pattern", array_join(col("pattern"), ","))
        .withColumn("is_reset_pattern_virement", when(conditions, 1).otherwise(0))
        .drop("pattern")
    )

    return df


def time_diff(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff",
        col("audit_fact_date")
        - lag(col("audit_fact_date"), 1).over(
            Window.partitionBy("user_id").orderBy("audit_fact_date")
        ),
    ).fillna(-1, ["time_diff"])


def time_diff_inv(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_inv",
        when(col("time_diff") == lit(0), exp(lit(1)))
        .when(col("time_diff").isNull(), lit(1))
        .otherwise(exp(1 / col("time_diff"))),
    )


def time_diff_to_reset(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_reset",
        when(
            (
                (col("is_reset_pattern_virement") == lit(1))
                | (col("is_reset_pattern_disposition") == lit(1))
            )
            & col("event_type").isin(MONEY_OPERATIONS),
            col("audit_fact_date")
            - last(
                when(
                    col("event_type") == lit("9"),
                    col("audit_fact_date"),
                ),
                ignorenulls=True,
            ).over(
                Window.partitionBy("user_id")
                .orderBy("audit_fact_date")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow),
            ),
        ).otherwise(lit(-1)),
    )


def time_diff_to_reset_normalized(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_reset_normalized",
        when(col("time_diff_to_reset") == lit(-1), 10_000_000).otherwise(
            col("time_diff_to_reset")
        ),
    ).withColumn(
        "time_diff_to_reset_normalized",
        lit(1) / (1 + col("time_diff_to_reset_normalized")),
    )


def time_diff_to_reset_exp(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_reset_exp",
        when(col("time_diff_to_reset") == lit(-1), 1).otherwise(
            col("time_diff_to_reset")
        ),
    ).withColumn(
        "time_diff_to_reset_exp",
        exp(lit(1) / col("time_diff_to_reset_exp")),
    )


def time_diff_to_reset_inv(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_reset_inv",
        when(col("time_diff_to_reset") == lit(-1), 1).otherwise(
            col("time_diff_to_reset")
        ),
    ).withColumn(
        "time_diff_to_reset_inv",
        lit(1) / col("time_diff_to_reset_inv"),
    )


def time_diff_to_reset_caped(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_reset_caped",
        when(col("time_diff_to_reset") <= 86400, col("time_diff_to_reset")).otherwise(
            0
        ),
    )


def time_diff_to_reset_sigmoid(df: DataFrame) -> DataFrame:
    def sigmoid(x: Column):
        return 1 / (1 + exp(-x))

    return df.withColumn(
        "time_diff_to_reset_sigmoid", sigmoid(col("time_diff_to_reset"))
    )


def time_diff_to_reset_normalized_sigmoid(df: DataFrame) -> DataFrame:
    def normalized_sigmoid(x: Column):
        return 1 / (1 + exp(-x / 86_400))

    return df.withColumn(
        "time_diff_to_reset_normalized_sigmoid",
        when(col("time_diff_to_reset") == lit(-1), 10_000_000).otherwise(
            col("time_diff_to_reset")
        ),
    ).withColumn(
        "time_diff_to_reset_normalized_sigmoid",
        normalized_sigmoid(col("time_diff_to_reset_normalized_sigmoid")),
    )


def criterion(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "criterion",
        (col("is_reset_pattern_disposition") + col("is_reset_pattern_virement"))
        * col("time_diff_to_reset_normalized"),
    )


def accounts_count(df: DataFrame) -> DataFrame:
    only_ips = df.filter(is_ip())
    ips_with_count = (
        only_ips.withColumn(
            "lag_user_id",
            lag("user_id").over(
                Window.partitionBy("user_ip_address").orderBy("audit_fact_date")
            ),
        )
        .withColumn(
            "change",
            sum(
                when(
                    col("user_id") != col("lag_user_id"),
                    lit(1),
                ).otherwise(lit(0)),
            ).over(Window.partitionBy("user_ip_address").orderBy("audit_fact_date")),
        )
        .withColumn(
            "accounts_count",
            when(
                col("event_type").isin(MONEY_OPERATIONS),
                col("change") + 1,
            ),
        )
        .select("id", "accounts_count")
    )
    return df.join(ips_with_count, ["id"], "left_outer")


def time_diff_to_last_money_operation_by_ip(df: DataFrame) -> DataFrame:
    only_ips = df.filter(is_ip()).filter(col("event_type").isin(MONEY_OPERATIONS))

    ips_with_time_diff = only_ips.withColumn(
        "time_diff_to_last_money_operation_by_ip",
        col("audit_fact_date")
        - lag(col("audit_fact_date"), count=1).over(
            Window.partitionBy("user_ip_address").orderBy("audit_fact_date")
        ),
    ).select("id", "time_diff_to_last_money_operation_by_ip")

    return df.join(ips_with_time_diff, ["id"], "left_outer")


def device_change(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("user_id").orderBy("audit_fact_date")

    return (
        df.withColumn("phone", split(col("os"), "/")[1])
        .withColumn(
            "device_change",
            (col("uuid").isNotNull() & lag(col("uuid")).over(w).isNull())
            | (col("uuid") != lag(col("uuid")).over(w))
            & (col("phone") != lag(col("phone")).over(w))
            & (col("name") != lag(col("name")).over(w)),
        )
        .withColumn("device_change", when(col("device_change"), 1).otherwise(0))
    )


def device_count(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "device_count",
        sum(col("device_change")).over(
            Window.partitionBy("user_id").orderBy("audit_fact_date")
        ),
    )


def is_pattern(pattern: str):
    return regexp_extract(
        "pattern",
        pattern,
        0,
    ) != lit("")


def is_attijari_secure_deactivated_pattern(df: DataFrame) -> DataFrame:
    return (
        rank(df, "rank", "7")
        .withColumn(
            "pattern",
            when(
                col("event_type").isin(MONEY_OPERATIONS)
                & (col("device_count") > 0),  # TODO: add event for cashexpres B
                collect_list(col("event_type")).over(
                    Window.partitionBy("user_id", "rank")
                    .orderBy("audit_fact_date")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            ),
        )
        .withColumn("pattern", array_join(col("pattern"), ","))
        .withColumn(
            "is_attijari_secure_deactivated_pattern",
            when(
                (is_pattern(r"^7,.*6,.*1,") & col("event_type").isin("2"))
                | (is_pattern(r"^7,.*6,") & col("event_type").isin("5"))
                | (is_pattern(r"^7,.*6,.*C,") & col("event_type").isin("B")),
                # TODO: add pattern for cashexpres B
                1,
            ).otherwise(0),
        )
        .drop("pattern", "rank")
    )


def is_attijari_secure_activated_pattern(df: DataFrame) -> DataFrame:
    return (
        rank(df, "rank", "6")
        .withColumn(
            "pattern",
            when(
                col("event_type").isin(MONEY_OPERATIONS)
                & (col("device_count") > 0),  # TODO: Add event B
                collect_list(col("event_type")).over(
                    Window.partitionBy("user_id", "rank")
                    .orderBy("audit_fact_date")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            ),
        )
        .withColumn("pattern", array_join(col("pattern"), ","))
        .withColumn(
            "is_attijari_secure_activated_pattern",
            when(
                (is_pattern(r"^6,.*1,") & col("event_type").isin("2"))
                | (is_pattern(r"^6,") & col("event_type").isin("5"))
                | (is_pattern(r"^6,.*C,") & col("event_type").isin("B")),
                # TODO: Add pattern for cashexpres B
                1,
            ).otherwise(0),
        )
        .drop("pattern", "rank")
    )


def time_diff_to_attijari_secure(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_attijari_secure",
        when(
            (
                (col("is_attijari_secure_activated_pattern") == lit(1))
                | (col("is_attijari_secure_deactivated_pattern") == lit(1))
            )
            & col("event_type").isin(MONEY_OPERATIONS),
            col("audit_fact_date")
            - last(
                when(
                    col("event_type") == lit("6"),
                    col("audit_fact_date"),
                ),
                ignorenulls=True,
            ).over(
                Window.partitionBy("user_id")
                .orderBy("audit_fact_date")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow),
            ),
        ).otherwise(lit(-1)),
    )


def time_diff_to_attijari_secure_normalized(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "time_diff_to_attijari_secure_normalized",
        when(
            (col("time_diff_to_attijari_secure") == lit(-1)), lit(10_000_000)
        ).otherwise(col("time_diff_to_attijari_secure")),
    ).withColumn(
        "time_diff_to_attijari_secure_normalized",
        lit(1) / (1 + col("time_diff_to_attijari_secure_normalized")),
    )


def device_age(df: DataFrame, relative_to: Column) -> DataFrame:
    return df.withColumn(
        "device_age",
        when(
            col("event_type").isin(MONEY_OPERATIONS),
            relative_to - col("creation_date"),
        ).otherwise(-1),
    )


def device_age_normalized(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "device_age_normalized",
        when(
            (col("device_age") == lit(-1)),
            lit(10_000_000),
        ).otherwise(col("device_age")),
    ).withColumn(
        "device_age_normalized",
        lit(1) / (1 + col("device_age_normalized")),
    )


def amount_virement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "amount_virement",
        when(
            col("event_type") == lit("2"),
            regexp_extract(col("audit_message"), r"Montant \[(.*?)\]", 1),
        ),
    ).withColumn(
        "amount_virement", coalesce(col("amount_virement").cast("double"), lit(0.0))
    )


def amount_cashexpress(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "amount_cashexpress",
        when(
            col("event_type") == lit("B"),
            regexp_extract(col("audit_message"), r"Montant \[(.*?)\]", 1),
        ),
    ).withColumn(
        "amount_cashexpress",
        coalesce(col("amount_cashexpress").cast("double"), lit(0.0)),
    )


def amount_disposition(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "amount_disposition",
        when(
            col("event_type") == lit("5"),
            regexp_extract(col("audit_message"), r"montant=(.*?),", 1),
        ),
    ).withColumn(
        "amount_disposition",
        coalesce(col("amount_disposition").cast("double"), lit(0.0)),
    )


def device_count_last_7_days(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -7 * 24 * 60 * 60, 0
        )  # 24 * 60 * 60 = 86400 = number of seconds in a day
    )

    return df.withColumn(
        "device_count_last_7_days",
        when(
            col("event_type").isin(MONEY_OPERATIONS),
            sum(col("device_change")).over(w),
        ).otherwise(0),
    )


def device_count_last_14_days(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -14 * 24 * 60 * 60, 0
        )  # 24 * 60 * 60 = 86400 = number of seconds in a day
    )

    return df.withColumn(
        "device_count_last_14_days",
        when(
            col("event_type").isin(MONEY_OPERATIONS),
            sum(col("device_change")).over(w),
        ).otherwise(0),
    )


def device_count_last_30_days(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -30 * 24 * 60 * 60, 0
        )  # 24 * 60 * 60 = 86400 = number of seconds in a day
    )

    return df.withColumn(
        "device_count_last_30_days",
        when(
            col("event_type").isin(MONEY_OPERATIONS),
            sum(col("device_change")).over(w),
        ).otherwise(0),
    )


def cumsum_disposition_12_hours(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -12 * 60 * 60, 0
        )  # 60 * 60 = 3600 = number of seconds in a 12 hours
    )

    return df.withColumn(
        "cumsum_disposition_12_hours",
        when(
            col("event_type") == lit("5"),
            sum(col("amount_disposition")).over(w),
        ).otherwise(0),
    )


def cumsum_virement_12_hours(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -12 * 60 * 60, 0
        )  # 60 * 60 = 3600 = number of seconds in a 12 hours
    )

    return df.withColumn(
        "cumsum_virement_12_hours",
        when(
            col("event_type") == lit("2"),
            sum(col("amount_virement")).over(w),
        ).otherwise(0),
    )


def cumsum_cashexpress_12_hours(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -12 * 60 * 60, 0
        )  # 60 * 60 = 3600 = number of seconds in a 12 hours
    )

    return df.withColumn(
        "cumsum_cashexpress_12_hours",
        when(
            col("event_type") == lit("B"),
            sum(col("amount_cashexpress")).over(w),
        ).otherwise(0),
    )


def ratio_amount_cumsum_virement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "ratio_amount_cumsum_virement",
        when(
            col("event_type") == lit("2"),
            col("amount_virement") / col("cumsum_virement_12_hours"),
        ).otherwise(0),
    )


def count_device_per_uuid(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("uuid")
        .orderBy("audit_fact_date")
        .rangeBetween(
            -3 * 24 * 60 * 60, 0
        )  # 24 * 60 * 60 = 86400 = number of seconds in a day
    )

    return df.withColumn(
        "count_device_per_uuid",
        when(
            col("event_type").isin(MONEY_OPERATIONS),
            approx_count_distinct(col("user_id"), rsd=0.1).over(w),
        ).otherwise(0),
    )


def time_diff_to_last_significant_transaction(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return df.withColumn(
        "time_diff_to_last_significant_transaction",
        when(
            col("event_type") == lit("2"),
            col("audit_fact_date")
            - last(
                when(
                    (col("event_type") == lit("2"))
                    & (col("amount_virement") >= SIGNIFICANT_VIREMENT),
                    col("audit_fact_date"),
                ),
                ignorenulls=True,
            ).over(w),
        )
        .when(
            col("event_type") == lit("5"),
            col("audit_fact_date")
            - last(
                when(
                    (col("event_type") == lit("5"))
                    & (col("amount_disposition") >= SIGNIFICANT_DISPOSITION),
                    col("audit_fact_date"),
                ),
                ignorenulls=True,
            ).over(w),
        )
        .otherwise(lit(-1)),
    ).fillna(-1, ["time_diff_to_last_significant_transaction"])


def time_diff_to_last_maxed_transaction(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    return df.withColumn(
        "time_diff_to_last_maxed_transaction",
        when(
            col("event_type") == lit("2"),
            col("audit_fact_date")
            - last(
                when(
                    (col("event_type") == lit("2"))
                    & (col("amount_virement") == MAX_VIREMENT),
                    col("audit_fact_date"),
                ),
                ignorenulls=True,
            ).over(w),
        )
        .when(
            col("event_type") == lit("5"),
            col("audit_fact_date")
            - last(
                when(
                    (col("event_type") == lit("5"))
                    & (col("amount_disposition") == MAX_DISPOSITION),
                    col("audit_fact_date"),
                ),
                ignorenulls=True,
            ).over(w),
        )
        .otherwise(lit(-1)),
    ).fillna(-1, ["time_diff_to_last_maxed_transaction"])


def is_facture_reset_pattern_shutdown(df: DataFrame) -> DataFrame:
    condition_virement = (
        regexp_extract(
            "pattern",
            r"^9,(.+,)*(((.+,)*3,(.+,)*(A,)+)|((A,)+(.+,)*3,(.+,)*))(.+,)*1,",
            0,
        )
        != lit("")
    ) & (col("event_type") == lit(2))

    condition_disposition = (
        regexp_extract(
            "pattern",
            r"^9,(.+,)*(((.+,)*3,(.+,)*(A,)+)|((A,)+(.+,)*3,(.+,)*))(.+,)*",
            0,
        )
        != lit("")
    ) & (col("event_type") == lit(5))

    conditions = condition_disposition | condition_virement

    df = (
        df.withColumn(
            "pattern",
            when(
                col("event_type").isin("2", "5"),  # TODO: add event cashexpress B
                collect_list(col("event_type")).over(
                    Window.partitionBy("user_id", "rank")
                    .orderBy("audit_fact_date")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            ),
        )
        .withColumn("pattern", array_join(col("pattern"), ","))
        .withColumn(
            "is_facture_reset_pattern_shutdown", when(conditions, 1).otherwise(0)
        )
        .drop("pattern")
    )

    return df


def is_safe_facture_device(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "bill_device", when(col("event_type") == lit("A"), col("mobile_device_id"))
        )
        .withColumn(
            "old_devices",
            when(
                col("event_type").isin(MONEY_OPERATIONS),
                collect_set(col("bill_device")).over(
                    Window.partitionBy("user_id")
                    .orderBy("audit_fact_date")
                    .rowsBetween(-30 * 24 * 60 * 60, -1)  # Last 30 days
                ),
            ),
        )
        .withColumn(
            "is_safe_facture_device",
            expr("array_contains(old_devices, mobile_device_id)"),
        )
        .withColumn(
            "is_safe_facture_device",
            when(col("is_safe_facture_device") == lit(True), 1).otherwise(0),
        )
        .drop("bill_device", "old_devices")
    )


def is_new_beneficiary(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(-3 * 24 * 60 * 60, 0)  # Last 3 days
    )

    return (
        df.withColumn(
            "beneficiary",
            when(
                col("event_type") == lit("1"),
                trim(extract_field_from_message("RIB ajouté:")),
            ),
        )
        # RIB: 3 digits code banque - 3 digits code ville - 16 digits num compte - 2 digits cle rib
        .withColumn(
            "beneficiary", substring(col("beneficiary"), 7, 23)
        )  # 7+16 numero compte
        .withColumn(
            "new_beneficiary_list",
            when(
                col("event_type")
                == lit("2"),  # TODO: maybe do the same to event cashexpress B
                collect_set(col("beneficiary")).over(w),
            ),
        )
        .withColumn(
            "current_beneficiary",
            trim(extract_field_with_brackets_from_message("Compte bénéficiaire")),
        )
        .withColumn(
            "is_new_beneficiary",
            expr("array_contains(new_beneficiary_list, current_beneficiary)"),
        )
        .withColumn(
            "is_new_beneficiary",
            when(col("is_new_beneficiary") == lit(True), 1).otherwise(0),
        )
        .drop("beneficiary", "new_beneficiary_list", "current_beneficiary")
    )


def is_low_virement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "is_low_virement",
        when(
            (col("event_type") == lit("2"))
            & (col("amount_virement") < LOW_VIREMENT_THRESHOLD),
            1,
        ).otherwise(0),
    )


def average_amount_scaled_12h(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy("user_id")
        .orderBy("audit_fact_date")
        .rangeBetween(-12 * 60 * 60, 0)  # Last 12 hours
    )
    return (
        df.withColumn(
            "count_events_12h",
            sum(when(col("event_type").isin(MONEY_OPERATIONS), 1)).over(w),
        )
        .withColumn(
            "sum_amount_scaled_12h",
            sum(
                when(col("event_type").isin(MONEY_OPERATIONS), col("amount_scaled"))
            ).over(w),
        )
        .withColumn(
            "average_amount_scaled_12h",
            when(
                col("event_type").isin(MONEY_OPERATIONS),
                col("sum_amount_scaled_12h") / col("count_events_12h"),
            ).otherwise(0),
        )
    ).drop("count_events_12h", "sum_amount_scaled_12h")
