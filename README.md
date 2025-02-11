# PySpark Cheatsheet

## Table of Contents

1. [Introduction](#introduction)
2. [Installing PySpark and Importing](#Installing-PySpark-and-Importing)
3. [Creating a SparkSession and Importing Necessary Libraries](#Creating-a-SparkSession-and-Importing-Necessary-Libraries)
4. [Authorize access to Google Drive account and Mount Google Drive to the Colab environment](#Authorize-access-to-Google-Drive-account-and-Mount-Google-Drive-to-the-Colab-environment)
5. [Reading JSON File](#reading-JSON-File)
6. [Basic DataFrame Operations](#basic-dataframe-operations)
7. [Decision of integer, float and double](#Decision-of-integer,-float-and-double)
8. [DataFrame Transformation](#DataFrame-Transformation)
9. [Some of the most commonly used operators](#Some-of-the-most-commonly-used-operators)
10. [Window Functions](#window-functions)
11. [Data Cleaning and Casting](#Data-Cleaning-and-Casting)
12. [Data Cleaning and Casting Examples](#Data-Cleaning-and-Casting-Examples)
13. [Joining DataFrames](#joining-dataframes)
14. [Saving Data](#saving-data)

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
*    print("{} rows".format(df_new1.count()))
     print("{} columns".format(len(df_new1.columns))): By using both. we get the size of a DataFrame.
*    print(df_new1.dtypes): We get data types of a DataFrame's columns

```python
df.printSchema()
print(df.count())
df.describe().show()
df.show()
display(df)
len(df.columns)
df.count()
df.distinct().count()
print("{} rows".format(df_new1.count()))
print("{} columns".format(len(df_new1.columns)))
print(df_new1.dtypes)
```

**Explanation:** Essential functions for understanding data structure and basic statistics.

---

## Decision of integer, float and double

**Precision:**

Float: A float (single-precision) has a precision of about 7 decimal digits. It occupies 4 bytes (32 bits) of memory.

Double: A double (double-precision) has a precision of about 15-16 decimal digits. It occupies 8 bytes (64 bits) of memory.

**Range:**

Float: Can represent a smaller range of values compared to double. It’s typically used for numerical data that does not require a high degree of precision.

Double: Can represent a larger range of values and is used when precision is more critical.

**Memory Usage:**

Float: Requires less memory (4 bytes) and is faster to process.

Double: Requires more memory (8 bytes) but offers greater precision and range.

**When to Use Float:**
Performance: If memory usage and performance are critical (e.g., large datasets or real-time processing) and the precision of 7 decimal digits is sufficient.

Scientific Calculations: Often used in scientific calculations where a rough estimate is acceptable.

**When to Use Double:**
High Precision: When dealing with financial data, scientific calculations, or any application where precise decimal representation is important.

Large Range: When the range of values is significant, such as in large numerical datasets.

**When to use Integer:**
Precision: Exact numerical values without fractional parts.

Use Case: When your data consists of whole numbers.

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
*    df.select(df["country"],df["density_per_square_km"]+1000).show(): It selects country column, then creates another column by adding 1000 to the values in the "density_per_square_km".
*    df.select(df["country"],(df["population"] > 100000000).alias("is_population_greater_than_100M")).show(): This will select country column along wit population column with the values over 100000000. At the end this shows boolean values in new column.
*    df.select(col("country"), (col("population") > 100000000).alias("is_population_greater_than_100M")).show(): This is similar to previous exmaple however this uses col function from pyspark.sql.functions to refer the columns. The col function is often used for better readability and to avoid potential issues with column names that contain spaces or special characters. At the end this shows boolean values in new column.
*    Select is different than filter, because filter is focusing on values in dataframe as whole. With select function, we choose a specific column and seperate it from dataframe.
*    df_new1.select(
    col("country"),
    F.when(col("population") > 100000000, col("population"))
    .otherwise("Less than 100M")
    .alias("is_population_greater_than_100M")
    ).show(): when function is used to create a conditional column. In this example, it checks if the "population" column is greater than 100 million. Than the otherwise method is specifies what to return if the condition in the when function is false. In this case, it returns the string "Less than 100M".
            
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
df.select(df["country"],df["density_per_square_km"]+1000).show()
df.select(df["country"],(df["population"] > 100000000).alias("is_population_greater_than_100M")).show()
df.select(col("country"), (col("population") > 100000000).alias("is_population_greater_than_100M")).show()

df_new1.select(
    col("country"),
    F.when(col("population") > 100000000, col("population"))
    .otherwise("Less than 100M")
    .alias("is_population_greater_than_100M")
    ).show()

```

---

### Row Operations Filtering and Selecting Data
*     df.drop("column1"): Drop method is used for dropping a column. 
*     df.filter(df["column1"] > 30).show(): This method is used for filtering DataFrame to include only rows where the value in the "age" column is greater than 30.
*     df.where(df["age"] > 30).show(): where() method is an alias for the filter() method.  They have same purpose.
*     df_filtered = df_new1.filter(col("population") > 100000000).select(
    col("country"),
    col("population").alias("population_greater_than_100M")
): In this example, we see that filter and select are combined to select specific columns from filtered dataframe.
*     df_startwith = df_new1.filter(col("country").startswith("A")):We get countries where it start with the letter 'A'.
*     df_new1.where(col("country") == "China").show():This filters the DataFrame df_new1 to include only rows where the "country" column is equal to "China". The where method is an alias for the filter method.
*     df_new1.filter(col("country") == "China").show():This filters the DataFrame df_new1 to include only rows where the "country" column is equal to "China". This is functionally identical to the where method.
*     df_new1.filter(col("fertility_rate") > 1.5).show():This filters the DataFrame df_new1 to include only rows where the "fertility_rate" column is greater than 1.5.
*     df_new1.filter(((col("population") > 50000000) & (col("fertility_rate") > 5))).show():This filters the DataFrame df_new1 to include only rows where the "population" column is greater than 50 million and the "fertility_rate" column is greater than 5. The & operator is used for the logical AND condition.
*     df_new1.where(col("country").isin(["Germany", "Turkey"])).show(): This filters the DataFrame df_new1 to include only rows where the "country" column is either "Germany" or "Turkey". The isin method checks if the column value is in the specified list.
*     df_zor = df_new1.where(~col("country").isin(["China"])): This filters the DataFrame df_new1 to exclude rows where the "country" column is "China". The ~ operator is used for the logical NOT condition.
*     df_new1.where(df_new1.country.contains("Turkey")).show(): This filters the DataFrame df_new1 to include only rows where the "country" column contains the substring "Turkey". The contains method checks if the column value contains the specified substring.
*     df_new1.where(col("country").like("T%")).show(): This filters the DataFrame df_new1 to include only rows where the "country" column starts with the letter "T". The like method uses SQL-like pattern matching, where % is a wildcard character.
*     df_new1.where(length(col("country")) < 5).show():This filters the DataFrame df_new1 to include only rows where the length of the "country" column is less than 5 characters. The length function calculates the length of the string in the column.
*     df_new1.limit(10).show():This limits the DataFrame df_new1 to the first 10 rows.


```python
df.drop("density_per_square_km")
df.filter(df["age"] > 30).show()
df.where(df["age"] > 30).show()
)
df_filtered = df_new1.filter(col("population") > 100000000).select(
    col("country"),
    col("population").alias("population_greater_than_100M")
)
df_startwith = df_new1.filter(col("country").startswith("A"))
df_new1.where(col("country") == "China").show()
df_new1.filter(col("country") == "China").show()
df_new1.filter(col("fertility_rate") > 1.5).show()
df_new1.filter(((col("population") > 50000000) & (col("fertility_rate") > 5))).show()
df_new1.where(col("country").isin(["Germany", "Turkey"])).show()
df_zor = df_new1.where(~col("country").isin(["China"]))
df_new1.where(df_new1.country.contains("Turkey")).show()
df_new1.where(col("country").like("T%")).show()
df_new1.where(length(col("country")) < 5).show()
df_new1.limit(10).show()
```

---
## Some of the most commonly used operators

**Comparison Operators:**

Equality: ==

Not Equal: !=

Greater Than: >

Greater Than or Equal To: >=

Less Than: <

Less Than or Equal To: <=


**Logical Operators:**

AND: &

OR: |

NOT: ~

---

### Aliasing
*     df.alias(): It gives a new name to the DataFrame. New names can be used in joins.
*     df.select(df["col1"].alias("new_name")): This gives a new name for a specified column.
*     grouped_df.agg(
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary"),
    count("id").alias("employee_count")
).show()

```python
df1 = df.alias("new_name")
df.select(df["col1"].alias("new_name"))
grouped_df.agg(
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary"),
    count("id").alias("employee_count")
).show()
```

---

### Aggregations
*     grouped_df = df.groupBy("column1").show(): This groups the dataframe based on given column.
*     grouped_df.agg(count"id").show(): Counts the number of rows for each group.
*     grouped_df.agg(avg("salary")).show(): It calculates the average value of the specified column for each group.
*     grouped_df.agg(sum("salary")).show(): It calculates the sum value of the specified column for each group.
*     grouped_df.agg(max("salary")).show(): It calculates the maximum value of the specified column for each group.
*     grouped_df.agg(min("salary")).show(): It calculates the minimum value of the specified column for each group.
*     grouped_df.agg(avg("salary"), sum("salary"), count("id")).show(): This is for performing multiple aggregations. It calculates avg and sum values for salary for each group then count id for each group.
*     grouped_df.agg(
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary"),
    count("id").alias("employee_count")
).show(): This shows how to rename of each aggregated columns using alias() method.

*     duplicates_df = df_new1.groupBy("country").count()
      duplicates_df = duplicates_df.filter(col("count") > 1):To get the number of duplicates values in each country.

```python
df.groupBy("department").agg(avg("salary"), count("id")).show()
df.agg(count("column_name"))
sum("column_name")
max("column_name")
min("column_name")
grouped_df.agg(avg("salary"), sum("salary"), count("id")).show()
grouped_df.agg(
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary"),
    count("id").alias("employee_count")
).show()

# Group by the column "population" and count the occurrences
duplicates_df = df_new1.groupBy("country").count()

# Filter to show only the duplicates
duplicates_df = duplicates_df.filter(col("count") > 1)

```

## Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", row_number().over(window_spec)).show()
```

**Explanation:** Assigns a ranking within each department based on salary.

---
## Data Cleaning and Casting

### Check for Duplicates in Each Column: Iterate over each column, group by that column, count the occurrences, and filter to show only duplicates.

#### Below is a function for checking duplicates in each column. 

*     Importing Functions: Import col and count from pyspark.sql.functions.
*     Defining the Function: Define a function check_duplicates that:
     *     Retrieves the list of column names.
     *     Iterates over each column.
     *     Groups by the column and counts occurrences.
     *     Filters for counts greater than 1 (duplicates).
     *     Orders by count in descending order.
     *     Prints and shows the duplicates for each column.
*     Calling the Function: Call the function with the DataFrame to check for duplicates in each column.

```python
from pyspark.sql.functions import col, count

def check_duplicates(df_new1):
    columns = df_new1.columns
    for column in columns:
        duplicates_df1 = df_new1.groupBy(column).count().filter(col("count") > 1).orderBy(col("count").desc())
        print(f"Duplicates in column '{column}':")
        duplicates_df1.show()

# Check for duplicates in each column of df_new1
check_duplicates(df_new1)
```

### Handling Missing Values
*     In case of dot: It is advisible to change datatype to double or float in case we have data such as '2.5' as string. We don't need to remove dots if we want to convert them to a numerical data type. The dot represents a decimal point, and retaining it is essential for preserving the fractional part of the number.

*     In case of comma: Let's say we have comma in the data, and they intend to represent decimal points. In those conditions, we must convert commas to dots, and then we can cast the strings to a nmumerical data type such as float or double.

*     In case of percentage: First we need to remove the percentage sign and then convert the remaining string to a numerical data type, such as float. After removing, we should divide the resulting number by 100 to convert it to a decimal.

* Check below example:

```python
# Remove percentage sign and cast to float

df = df.withColumn("value", regexp_replace(col("value"), " %", "").cast("float") / 100
```
*     In case of negative number: In this condition, we don't need to remove any character. We can directly convert it to an integer.

*     In case of negative number contains comma:

1. Comma as Thousands Separator: If the comma is intended to separate thousands, you can remove the comma and then cast the string to an integer or float.

```python
# Remove commas and cast to integer

df = df.withColumn("value", regexp_replace(col("value"), ",", "").cast("integer"))
```

2. Comma as Decimal Separator: If the comma is meant to be a decimal point, you should replace the comma with a dot before casting it to a float or double.

```python
# Replace commas with dots and cast to float

df = df.withColumn("value", regexp_replace(col("value"), ",", ".").cast("float"))
```

*    In case of plus sign with commas:Remove the plus sign: The plus sign typically indicates that the value is greater than or equal to the specified number. Remove commas: If the commas are used as thousands separators. Cast to an appropriate numerical type: Depending on your need, you can cast it to an integer or float.

```python
# Remove plus sign, remove commas, and cast to integer

df = df.withColumn("value", regexp_replace(col("value"), "[,+]", "").cast("integer"))
```

*     Using different methods to do same job

```python
# Remove plus sign, remove commas, and cast to integer

df = df.withColumn("value", regexp_replace(col("value"), "[,+]", "").cast("integer"))
```
*     Alternatively, we can choose to use

```python
df = df.withColumn("value", regexp_replace(col("value"), "[^0-9]", "").cast("integer"))
```
*     [^0-9]:This regex pattern matches any character that is not a digit (0-9). The regexp_replace function will remove all non-digit characters from the "value" column.

**Differences and Similarities:**

**Differences:**

Scope of Removal: The first snippet only removes commas and plus signs, whereas the second snippet removes all non-digit characters. This means the second snippet is more general and can handle various types of non-digit characters.

Use Case: The first snippet is suitable for cases where you specifically want to remove commas and plus signs. The second snippet is suitable for cases where you want to remove any non-numeric characters, ensuring that only digits remain.

**Similarities:**

Outcome: For the specific examples given (like "1,000,000+"), both snippets will produce the same cleaned value ("1000000").

Casting: Both snippets cast the cleaned value to an integer using the cast("integer") method.

**When to Use Each Snippet:**

Use Code Snippet 1:

When your data specifically contains commas and plus signs that you want to remove.

Example: "1,000,000+" to "1000000".

Use Code Snippet 2:

When your data may contain various non-digit characters, and you want to ensure that only numeric digits remain.

Example: "1,000,000+ USD" or "3.5%" to "1000000" and "35", respectively.

## Data Cleaning and Casting Examples
**Tasks**
1.   Commas in "density per square km", "land are in square km", "migrants_net", "net change", "population"
```python
df1.join(df2, on="key", how="inner")
df1.join(df2, on="key", how="left")

window_spec = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", row_number().over(window_spec)).show()
```python
# Let's start with first cleaning necessity. Commas in "density per square km", "land are in square km", "migrants_net", "net change", "population"

df_new1 = df.withColumn("density_per_square_km", regexp_replace(col("density_per_square_km"), ",", "").cast("integer"))\
          .withColumn("land_are_in_square_km", regexp_replace(col("land_are_in_square_km"), ",", "").cast("integer"))\
          .withColumn("migrants_net", regexp_replace(col("migrants_net"), ",", "").cast("integer"))\
          .withColumn("net_change", regexp_replace(col("net_change"), ",", "").cast("integer"))\
          .withColumn("population", regexp_replace(col("population"), ",", "").cast("integer"))\
          .withColumn("fertility_rate", col("fertility_rate").cast("float"))
---

2.   Percentage sign in "urban population", "world share" and "yearly change" columns.

```python
#Let's start to clean percentage signs, and then we will be converting dataType to the integer in urban population as it doesn't have any decimal point. Then we will convert world_share and yearly change columns into float. Lastly, we need to convert those values into its correct decimal representation to reflect its proper numberical value. For example, "18.47%" should be converted to 0.1847 to represent 18.47 percent as a decimal.
df_new1 = df_new1.withColumn("urban_population", round(regexp_replace(col("urban_population"), "%", "").cast("float") / 100, 5))\
          .withColumn("world_share", round(regexp_replace(col("world_share"), "%", "").cast("float") / 100, 7))\
          .withColumn("yearly_change", round(regexp_replace(col("yearly_change"), "%", "").cast("float") / 100, 7))
```

* Changing Data Type of a Single Column

```python
df_new1 = df_new1.withColumn("position", col("position").cast("integer"))
```

### Filling Null Values in Specific Column

*     df_A = df_new1.fillna({"population": 0}):This line fills any null (missing) values in the "population" column with 0. The fillna method is used to replace null values with a specified value.

```python
df_A = df_new1.fillna({"population": 0})
```

### Count the Number if 0 in the column
*     zero_count = df_A.filter(col("population") == 0).count()
      print("Number of rows with population = 0:", zero_count):This line filters the DataFrame df_A to include only rows where the "population" column is 0 and then counts the number of such rows. The result is stored in the variable zero_count. Then it prints the number of rows where the "population" column is 0.

```python
zero_count = df_A.filter(col("population") == 0).count()
      print("Number of rows with population = 0:", zero_count)
```  

### Fill NULL values with column average
*     df_A = df_A.fillna({"population": df_A.agg(avg("population")).first()[0]})
      df_A.show():This line fills any remaining null values in the "population" column with the average population value. The agg method is used to calculate the average population, and first()[0] retrieves the average value from the result.
  
```python
df_A = df_A.fillna({"population": df_A.agg(avg("population")).first()[0]})
      df_A.show()
```

---

## Joining DataFrames

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

## Saving Data

```python
df.write.csv("output.csv", header=True)
```

**Explanation:** Saves DataFrame as a CSV file with headers.

---

### Next Steps

- Add more transformations, optimizations, and real-world use cases!

