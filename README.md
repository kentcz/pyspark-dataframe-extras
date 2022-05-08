# pyspark-dataframe-extras
Utility functions for PySpark

![Tests](https://github.com/kentcz//pyspark-dataframe-extras/actions/workflows/tests.yml/badge.svg)
[![PyPI version](https://badge.fury.io/py/pyspark-dataframe-extras.svg)](https://badge.fury.io/py/pyspark-dataframe-extras)

PySpark-DataFrame-Extras expands the `pyspark.sql.DataFrame` [class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html) by adding functions for common data engineering operations. This collection of generic utility functions aims to improve the readability and maintainability PySpark projects.

## Installation

```bash
pip install pyspark-dataframe-extras
```

## Usage

```python
from pyspark_dataframe_extras.common import dataframe_ext

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
df.show()

# +------+------------+
# |letter|extra_column|
# +------+------------+
# |     A|           1|
# |     B|           2|
# |     C|           3|
# |     C|           4|
# |     C|           5|
# |     C|           6|
# |     A|           7|
# |     A|           8|
# |     C|           9|
# |     B|          10|
# +------+------------+


df.showFrequencyCount("letter")

# +------+-----+---------+
# |letter|count|frequency|
# +------+-----+---------+
# |C     |5    |0.50     |
# |A     |3    |0.30     |
# |B     |2    |0.20     |
# +------+-----+---------+
```