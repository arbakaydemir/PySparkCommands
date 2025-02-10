# PySpark Cheatsheet

## Table of Contents

1. [Introduction](#introduction)
2. [Creating a SparkSession](#creating-a-sparksession)
3. [Reading Data](#reading-data)
4. [Basic DataFrame Operations](#basic-dataframe-operations)
5. [Filtering and Selecting Data](#filtering-and-selecting-data)
6. [Aggregations](#aggregations)
7. [Window Functions](#window-functions)
8. [Data Transformation](#data-transformation)
9. [Joining DataFrames](#joining-dataframes)
10. [Saving Data](#saving-data)

---

## Introduction

PySpark is the Python API for Apache Spark, used for big data processing. This cheatsheet provides quick reference code snippets with explanations.

---

## Installing PySpark 
We use below code to install PySpark in Google Colab. 

```python
# Install PySpark in Google Colab
!pip install pyspark
```


## Creating a SparkSession and Importing Necessary Libraries
Below is for libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Below is for creating a Spark session
spark = SparkSession.builder.appName("Practise_PySpark").getOrCreate()

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

**Explanation:** This initializes a Spark session, required for all PySpark operations.

---

## Reading Data

```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show(5)
```

**Explanation:** Reads a CSV file into a DataFrame with headers and inferred data types.

---

## Basic DataFrame Operations

```python
df.printSchema()  # Shows schema
print(df.count())  # Number of rows
df.describe().show()  # Summary statistics
```

**Explanation:** Essential functions for understanding data structure and basic statistics.

---

## Filtering and Selecting Data

```python
df.filter(df["age"] > 30).show()
df.select("name", "age").show()
```

**Explanation:** Filters rows and selects specific columns.

---

## Aggregations

```python
from pyspark.sql.functions import avg, count

df.groupBy("department").agg(avg("salary"), count("id")).show()
```

**Explanation:** Performs group-wise aggregations like average salary and count of employees.

---

## Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", row_number().over(window_spec)).show()
```

**Explanation:** Assigns a ranking within each department based on salary.

---

## Data Transformation

```python
df = df.withColumn("new_salary", df["salary"] * 1.1)
df.show()
```

**Explanation:** Creates a new column by modifying an existing one.

---

## Joining DataFrames

```python
df1.join(df2, df1["id"] == df2["id"], "inner").show()
```

**Explanation:** Performs an inner join on two DataFrames.

---

## Saving Data

```python
df.write.csv("output.csv", header=True)
```

**Explanation:** Saves DataFrame as a CSV file with headers.

---

### Notes

- Use `.cache()` to cache a DataFrame in memory for faster operations.
- Use `.explain()` to analyze query execution plans.

---

### Next Steps

- Add more transformations, optimizations, and real-world use cases!

Happy Coding! 🚀

