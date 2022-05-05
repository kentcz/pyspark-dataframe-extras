import unittest
import pyspark.sql.functions as F

from pyspark_dataframe_extras.common.dataframe_ext import frequency_count, first_in_group
from chispa.dataframe_comparer import assert_df_equality, assert_approx_df_equality


def test_frequency_count(spark):

    data = [
        ("A", 1),
        ("B", 2),
        ("C", 3),
        ("C", 4),
        ("C", 5),
        ("C", 6),
        ("A", 7),
        ("A", 8),
        ("C", 9),
        ("B", 10)
    ]

    df = spark.createDataFrame(data, ["letter", "extra_column"])
    df = frequency_count(df, "letter", "letter_count", "letter_frequency")

    expected_output = [
        ("C", 5, 0.5),
        ("A", 3, 0.3),
        ("B", 2, 0.2)
    ]

    expected_df = spark.createDataFrame(expected_output, ["letter", "letter_count", "letter_frequency"])

    assert_approx_df_equality(df, expected_df, 0.0001, ignore_nullable=True)


def test_first_in_group(spark):

    data = [
        ("George", "Washington", "1789-04-30", None),
        ("John", "Adams", "1797-03-04", "Federalist"),
        ("James", "Madison", "1809-03-04", "Democratic-Republican"),
        ("James", "Madison", "1813-03-04", "Democratic-Republican"),
        ("James", "Monroe", "1817-03-04", "Democratic-Republican"),
        ("James", "Monroe", "1821-03-04", "Democratic-Republican"),
        ("John Quincy", "Adams", "1825-03-04", "Democratic-Republican"),
    ]

    df = spark.createDataFrame(data, ["first_name", "last_name", "start_date", "party"])
    df = first_in_group(df, ["first_name", "last_name"], F.col("start_date")).orderBy("start_date")

    expected_output = [
        ("George", "Washington", "1789-04-30", None),
        ("John", "Adams", "1797-03-04", "Federalist"),
        ("James", "Madison", "1809-03-04", "Democratic-Republican"),
        ("James", "Monroe", "1817-03-04", "Democratic-Republican"),
        ("John Quincy", "Adams", "1825-03-04", "Democratic-Republican"),
    ]

    expected_df = spark.createDataFrame(expected_output, ["first_name", "last_name", "start_date", "party"])

    assert_df_equality(df, expected_df)


if __name__ == '__main__':
    unittest.main()
