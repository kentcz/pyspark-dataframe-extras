import sys
import csv
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, count, format_number, desc, avg, expr


def print_count(self, label: Optional[str] = None):

    if label:
        print(f"{label}: {self.count():,}")
    else:
        print(f"{self.count():,}")

    return self


def show_csv(self, n: int = 20, delimiter: str = ","):
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


def select_duplicates(self, group_by_columns, order_by_columns, sentinel_column="duplicate_row_number"):
    return self\
        .withColumn(
            sentinel_column,
            row_number().over(
                Window.partitionBy(group_by_columns).orderBy(order_by_columns)
            )
        )\
        .filter(col(sentinel_column) == 1)\
        .drop(sentinel_column)


def frequency_count(self, group_by_column, count_column="count", frequency_column="frequency"):
    df_count = self.count()

    return self \
        .groupBy(group_by_column) \
        .agg(
            count("*").alias(count_column)
        ) \
        .withColumn(frequency_column, col(count_column) / df_count) \
        .orderBy(desc(count_column))


def show_frequency_count(self, group_by_column, count_column="count", frequency_column="frequency", limit=50,
                         truncate=False, pretty=True):
    frequency_df = frequency_count(
        self=self,
        group_by_column=group_by_column,
        count_column=count_column,
        frequency_column=frequency_column
    )

    if pretty:
        cleaned_df = frequency_df \
            .withColumn(count_column, format_number(col(count_column), 0)) \
            .withColumn(frequency_column, format_number(col(frequency_column), 2))
    else:
        cleaned_df = frequency_df

    cleaned_df.show(limit, truncate)


def group_by_conditional(self, boolean_exp, group_by_column):
    if boolean_exp:
        return self.groupBy(group_by_column)
    else:
        return self


def percentile_agg(
        self, column, group_by_column=None, count_column="count", average_column="average", additional_aggs=[],
        percentile_prefix="_p", percentiles=[0.0, 0.25, 0.50, 0.75, 1.0]
):
    percentile_columns = [
        expr(f"percentile({column}, {percentile})").alias(f"{column}{percentile_prefix}{int(percentile * 100)}")
        for percentile in percentiles
    ]

    return group_by_conditional(self, group_by_column is not None, group_by_column) \
        .agg(
        *[
            count("*").alias(count_column),
            avg(column).alias(average_column),
            *percentile_columns,
            *additional_aggs
        ]
    )


def is_numeric_dtype(dtype):
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
    return [
        field
        for field, dtype in self.dtypes
        if is_numeric_dtype(dtype)
    ]


def format_fields(self, exclude_columns=[], d=0):
    result_df = self
    for field in numeric_fields(self):
        if field not in exclude_columns:
            result_df = result_df.withColumn(field, format_number(col(field), d))
    return result_df


def first_in_group(df, group_by, order_by):
    return df\
        .withColumn(
            "group_row_number",
            row_number().over(
                Window
                .partitionBy(group_by)
                .orderBy(order_by)
            )
        )\
        .filter(col("group_row_number") == 1)\
        .drop("group_row_number")


def pretty_count(self):
    return f"{self.count():,}"


DataFrame.groupByConditional = group_by_conditional  # type: ignore[attr-defined]
DataFrame.numericFields = numeric_fields             # type: ignore[attr-defined]
DataFrame.formatFields = format_fields               # type: ignore[attr-defined]
DataFrame.frequencyCount = frequency_count           # type: ignore[attr-defined]
DataFrame.showFrequencyCount = show_frequency_count  # type: ignore[attr-defined]
DataFrame.percentileAgg = percentile_agg             # type: ignore[attr-defined]
DataFrame.printCount = print_count                   # type: ignore[attr-defined]
DataFrame.showCSV = show_csv                         # type: ignore[attr-defined]
DataFrame.firstInGroup = first_in_group              # type: ignore[attr-defined]
DataFrame.prettyCount = pretty_count                 # type: ignore[attr-defined]
