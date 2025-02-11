# PySpark Cheatsheet

## Table of Contents

1. [Introduction](#introduction)
2. [Installing PySpark and Importing](#Installing-PySpark-and-Importing)
3. [Creating a SparkSession and Importing Necessary Libraries](#Creating-a-SparkSession-and-Importing-Necessary-Libraries)
4. [Authorize access to Google Drive account and Mount Google Drive to the Colab environment](#Authorize-access-to-Google-Drive-account-and-Mount-Google-Drive-to-the-Colab-environment)
5. [Reading JSON File](#reading-JSON-File)
6. [Basic DataFrame Operations](#basic-dataframe-operations)
7. [DataFrame Transformation](#DataFrame-Transformation)
8. [Filtering and Selecting Data](#filtering-and-selecting-data)
9. [Aggregations](#aggregations)
10. [Window Functions](#window-functions)
11. [Data Transformation](#data-transformation)
12. [Joining DataFrames](#joining-dataframes)
13. [Saving Data](#saving-data)

---

## Introduction

PySpark is the Python API for Apache Spark, used for big data processing. This cheatsheet provides quick reference code snippets with explanations.

---

## Installing PySpark and Importing
* We use below code to install PySpark in Google Colab. 

```python
# Install PySpark in Google Colab
!pip install pyspark
```
* import pyspark is used to import Pyspark library in Python. Pyspark is the Python API for Apache Spark.

```python
#Import the pySpark
import pyspark
```

---

## Creating a SparkSession and Importing Necessary Libraries
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Practise_PySpark").getOrCreate()
```

Above is for creating a Spark session. This code is essential for starting any PySpark application as it sets up the necessary context for Spark operations. This initializes a Spark session, required for all PySpark operations.

```python
from pyspark.sql import SparkSession:
```

SparkSession: This is the entry point to programming with DataFrame and SQL functionality in PySpark. It allows you to create a Spark session, which is necessary to interact with Spark's functionalities. You can think of it as the main access point for Spark applications.

```python
from pyspark.sql.functions import *
```

This imports all the functions available in the pyspark.sql.functions module. These functions are used to perform various operations on DataFrames, such as data manipulation, aggregation, and transformation.

---

##  Authorize access to Google Drive account and Mount Google Drive to the Colab environment
* To give authorization we need to use below
```python
from google.colab import drive
```

---

## Reading JSON File

* First we set a variable "file path", than we use it in spark.read.option
* spark.read: This is the DataFrameReader object, which is used to read data into a DataFrame.
* option("multiline","true"): This sets an option for reading the JSON file. The multiline option is set to true, which means that the JSON file can contain multiple lines, and each line is treated as a separate JSON object.
* json(file_path): This reads the JSON file specified by file_path into a DataFrame.

```python
# Loading the dataset
file_path = "/content/gdrive/MyDrive/Colab Notebooks/Third Project/dataset.json"

# Read the JSON file into a DataFrame
df = spark.read.option("multiline","true").json(file_path)
```

---

## Basic DataFrame Operations

*    df.printSchema(): To understand the structure of data. It provides information regarding the datatype of each column, column names etc.  nullable = true it means that the column can contain missing or null values. In other words, it's permissible for some rows in the DataFrame to have no value in that specific column.
*    df.count(): We use it to know how many rows we have in total in the table.
*    len(df.columns): To get the number of columns in DataFrame. 
*    df.describe().show(): This shows detailed information about summary statistics for specified DataFrame.
*    df.show(): This shows first 20 rows of the Df. Number of rows can be specified inside the paranthesis.
*    display(df): Used specifically in Databricks, to show richer display of the DataFrame.
*    df.distinct().count(): This is for returning the number of distinct rows.

```python
df.printSchema()
print(df.count())
df.describe().show()
df.show()
display(df)
len(df.columns)
df.count()
df.distinct().count()
```

**Explanation:** Essential functions for understanding data structure and basic statistics.

---

## DataFrame Transformation
### Column Operations
*     df.drop(): This is used to drop a specific column from the df.
*     df.withColumnRenamed(): It renames a column in the DataFrame. It needs two arguments, first old column name, then new column name.
*     df.withColumnRenamed().withColumnRenamed(): It renames multiple columns, you need to give two column names and separetely add two withColumnRenamed() arguments.
*    columns_to_rename {"country":"Country","population":"Population"}
     for old_name, new_name in columns_to_rename.items():
        df = withColumnRenamed(old_name, new_name)
     This is for loop to rename multiple columns.
*    df.select().show(): This method used to select specific columns from the DataFrame and displays the result using show() method.
*    df.select('*').show(): It selects all columns from DataFrame, and show() is used to display the result.
*    df.select(["country"]).show(): In this one, there is one difference from df.select("column1"). Columns are provided as list.
*    df.select(["country", "density_per_square_km"]).show(): In this one, there are two difference from df.select("column1"). Columns are provided as lists and multiple columns are selected.
            
```python
df.drop("density_per_square_km")
df.withColumnRenamed()
df.withColumnRenamed("country", "Country").withColumnRenamed(
    "density_per_square_km", "density_squarekm"

# Rename multiple columns using a loop
columns_to_rename = {"country": "Country", "density_per_square_km": "density_squarekm"}
for old_name, new_name in columns_to_rename.items():
    df = df.withColumnRenamed(old_name, new_name)

df.select("name", "age").show()
df.select('*').show()
df.select(["country"]).show()
df.select(["country", "density_per_square_km"]).show()
)

```

### Row Operations Filtering and Selecting Data
df.drop("column1"):
df.filter(df["column1"] > 30).show():
df.where(df["age"] > 30).show():

```python
df.drop("density_per_square_km")
df.filter(df["age"] > 30).show()
df.where(df["age"] > 30).show()
)

```

### Aggregations

```python
df.groupBy("department").agg(avg("salary"), count("id")).show()
Window.partitionBy("col").orderBy("col")
```

**Explanation:** Performs group-wise aggregations like average salary and count of employees.

---

### Joins
Inner Join: df1.join(df2, on="key", how="inner")

Left Join: df1.join(df2, on="key", how="left")


```python
df1.join(df2, on="key", how="inner")
df1.join(df2, on="key", how="left")

window_spec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", row_number().over(window_spec)).show()
```

**Explanation:** Assigns a ranking within each department based on salary.

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

