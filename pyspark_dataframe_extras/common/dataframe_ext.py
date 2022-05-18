import sys
import csv
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, count, format_number, desc, avg, expr


def print_count(self, label: Optional[str] = None):
    """
    Print the datafame count without terminating a method chain

    :param self: The dataframe
    :param label: Optional label to prefix the print statement
    :return: The dataframe
    """

    if label:
        print(f"{label}: {self.count():,}")
    else:
        print(f"{self.count():,}")

    return self


def show_csv(self, n: int = 20, delimiter: str = ","):
    """
    An alternative to .show() which prints the table as a CSV, instead of an ASCII table

    :param self: The dataframe
    :param n: Number of rows to include
    :param delimiter: Column delimiter for the CSV
    :return: The dataframe
    """
    rows = self.limit(n).collect()

    fields = [col for col, col_dtype in self.dtypes]

    writer = csv.DictWriter(sys.stdout, fieldnames=fields, delimiter=delimiter)
    writer.writeheader()

    for row in rows:
        writer.writerow(
            {
                field: row[field] if field in row else None
                for field in fields
            }
        )


def split_by_column(self, col):
    """
    Splits the dataframe into multiple dataframes, based on the values in column `col`

    :param self: The dataframe
    :param col: The column to split on
    :return: The dataframe
    """
    value_df = self.groupBy(col).count()

    values = [
        col
        for col, col_dtype in value_df.dtypes
        if col != "count"
    ]

    return {
        col: self.filter(col == val)
        for val in values
    }


def frequency_count(self, group_by_column, count_column="count", frequency_column="frequency"):
    """
    Group by `group_by_column` then generate `count` and `frequency` columns for the grouped data

    :param self: The dataframe
    :param group_by_column: The column to group by
    :param count_column: The desired name for the `count` column
    :param frequency_column: The desired name for the `frequency` column
    :return: The dataframe
    """
    df_count = self.count()

    return (
        self
        .groupBy(group_by_column)
        .agg(
            count("*").alias(count_column)
        )
        .withColumn(frequency_column, col(count_column) / df_count)
        .orderBy(desc(count_column))
    )


def show_frequency_count(self, group_by_column, count_column="count", frequency_column="frequency", limit=50,
                         truncate=False, pretty=True):
    """
    Show the .frequencyCount() of the dataframe, using a pretty numerical values

    :param self: The dataframe
    :param group_by_column: The column to group by
    :param count_column: The desired name of the `count` column
    :param frequency_column: The desired name of the `frequency` column
    :param limit: The number of rows
    :param truncate: Truncate long columns
    :param pretty: Pretty print the numerical values
    :return: The dataframe
    """
    frequency_df = frequency_count(
        self=self,
        group_by_column=group_by_column,
        count_column=count_column,
        frequency_column=frequency_column
    )

    if pretty:
        cleaned_df = (
            frequency_df
            .withColumn(count_column, format_number(col(count_column), 0))
            .withColumn(frequency_column, format_number(col(frequency_column), 2))
        )
    else:
        cleaned_df = frequency_df

    cleaned_df.show(limit, truncate)


def conditional_group_by(self, boolean_exp, group_by_column):
    """
    Conditionally apply a Group By

    :param self: The dataframe
    :param boolean_exp: The conditional expression
    :param group_by_column: The column to group by
    :return: The dataframe
    """
    if boolean_exp:
        return self.groupBy(group_by_column)
    else:
        return self


def percentile_agg(
        self, column, group_by_column=None, count_column="count", average_column="average", additional_aggs=[],
        percentile_prefix="_p", percentiles=[0.0, 0.25, 0.50, 0.75, 1.0]
):
    """
    Calculates the percentiles of an aggregation

    :param self: The dataframe
    :param column: The primary column
    :param group_by_column: The column to group by
    :param count_column: The name of the `count` output column
    :param average_column: The name of the `average` output column
    :param additional_aggs: List of additional aggregation columns
    :param percentile_prefix: String prefix for the name of the output percentile columns
    :param percentiles: List of percentiles
    :return: The dataframe
    """
    percentile_columns = [
        expr(f"percentile({column}, {percentile})").alias(f"{column}{percentile_prefix}{int(percentile * 100)}")
        for percentile in percentiles
    ]

    return conditional_group_by(self, group_by_column is not None, group_by_column) \
        .agg(
        *[
            count("*").alias(count_column),
            avg(column).alias(average_column),
            *percentile_columns,
            *additional_aggs
        ]
    )


def _is_numeric_dtype(dtype):
    numeric_dtypes = [
        "short",
        "int",
        "bigint",
        "float",
        "double",
        "decimal"
    ]
    return dtype in numeric_dtypes


def numeric_fields(self):
    """
    Returns a list of the numeric columns in the dataframe
    :param self: The dataframe
    :return: A list of column names
    """
    numeric_dtypes = [
        "short",
        "int",
        "bigint",
        "float",
        "double",
        "decimal"
    ]

    return [
        field
        for field, dtype in self.dtypes
        if dtype in numeric_dtypes
    ]


def format_fields(self, exclude_columns=[], d=0):
    """
    Formats the numeric columns for displaying

    :param self: The dataframe
    :param exclude_columns: List of columns to exclude
    :param d: The number of decimal places to round to
    :return: The dataframe
    """
    result_df = self
    for field in numeric_fields(self):
        if field not in exclude_columns:
            result_df = result_df.withColumn(field, format_number(col(field), d))
    return result_df


def first_in_group(df, group_by, order_by):
    """
    Remove duplicates rows from the `group_by` group, retaining the first occurrence based on `order_by` order
    :param df: The dataframe
    :param group_by: The columns to group by
    :param order_by: The columns to order by
    :return: The dataframe
    """
    return (
        df
        .withColumn(
            "group_row_number",
            row_number().over(
                Window
                .partitionBy(group_by)
                .orderBy(order_by)
            )
        )
        .filter(col("group_row_number") == 1)
        .drop("group_row_number")
    )


def pretty_count(self):
    return f"{self.count():,}"


DataFrame.groupByConditional = conditional_group_by  # type: ignore[attr-defined]
DataFrame.numericFields = numeric_fields             # type: ignore[attr-defined]
DataFrame.formatFields = format_fields               # type: ignore[attr-defined]
DataFrame.frequencyCount = frequency_count           # type: ignore[attr-defined]
DataFrame.showFrequencyCount = show_frequency_count  # type: ignore[attr-defined]
DataFrame.percentileAgg = percentile_agg             # type: ignore[attr-defined]
DataFrame.printCount = print_count                   # type: ignore[attr-defined]
DataFrame.showCSV = show_csv                         # type: ignore[attr-defined]
DataFrame.firstInGroup = first_in_group              # type: ignore[attr-defined]
DataFrame.prettyCount = pretty_count                 # type: ignore[attr-defined]
