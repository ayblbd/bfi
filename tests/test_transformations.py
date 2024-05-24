import pandas as pd

from tests.conftest import compare_dataframes


def placeholder(spark):
    df = spark.createDataFrame(
        [
            ("ID1",),
            ("ID1",),
            ("ID2",),
            ("ID2",),
        ],
        ["user_id"],
    )

    actual = df

    expected = pd.DataFrame(
        [
            ("ID1",),
            ("ID1",),
            ("ID2",),
            ("ID2",),
        ],
        columns=[
            "user_id",
        ],
    )

    compare_dataframes(actual, expected, sort_by=["user_id"])
