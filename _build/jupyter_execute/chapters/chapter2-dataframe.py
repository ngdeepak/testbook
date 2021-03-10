```{figure} ../images/banner.png
---
align: center
name: banner
---
```

<style>body {text-align: justify}</style>

# Chapter 2 : DataFrames

## Learning Objectives

- Understand dataframe basics.
- Create syntax to create spark dataframe from different data sources.
- Basic dataframe operations.

## Chapter Outline

- [1. What is spark dataframe](#1)
- [2. Creating a spark dataframe](#2)
    - [2a. from RDD](#3)
    - [2b. from List](#4)
    - [2c. from pandas dataframe](#5)
- [3. Basic dataframe operations](#6)
    - [3a. Selecting columns](#7)
    - [3b. Renaming a column](#8)
    - [3c. Deleting a column](#9) 
    - [3d. Renaming a column](#10)
    - [3e. Changing the data type of a column](#11)
    - [3f. Filtering the data](#12)


from IPython.display import display_html
import pandas as pd 
import numpy as np
def display_side_by_side(*args):
    html_str=''
    for df in args:
        html_str+=df.to_html(index=False)
        html_str+= "\xa0\xa0\xa0"*10
    display_html(html_str.replace('table','table style="display:inline"'),raw=True)
space = "\xa0" * 10

## Chapter Outline - Gallery

click on  | any image
---: |:---  
[![alt](img/chapter2/1.png)](#3) | [![alt](img/chapter2/2.png)](#4)
[![alt](img/chapter2/3.png)](#5) | [![alt](img/chapter2/4.png)](#7)
[![alt](img/chapter2/5.png)](#8) | [![alt](img/chapter2/6.png)](#9)
[![alt](img/chapter2/7.png)](#10) | [![alt](img/chapter2/8.png)](#11)
[![alt](img/chapter2/9.png)](#12) |


<a id='1'></a>

## 1. What is spark dataframe?
A DataFrame simply represents a table of data with rows and columns. A simple analogy would be a spreadsheet with named columns.

Spark DataFrame is a distributed collection of data organized into named columns. It can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDD, Lists, Pandas data frame. 

- Immutable in nature : We can create DataFrame once but can’t change it. And we can transform a DataFrame after applying transformations.
- Lazy Evaluations: This means that a task is not executed until an action is performed.
- Distributed: DataFrame is distributed in nature.

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently.


Advantages of lazy evaluation.

- It is an optimization technique i.e. it provides optimization by reducing the number of queries.

- It saves the round trips between driver and cluster, thus speeds up the process. 
- Saves Computation and increases Speed
 
### Why are DataFrames Useful ?

- DataFrames are designed for processing large collections of structured or semi-structured data.
Observations in Spark DataFrame are organised under named columns, which helps Apache Spark to understand the schema of a DataFrame. This helps Spark optimize execution plan on these queries.
- DataFrame in Apache Spark has the ability to handle petabytes of data.
- DataFrame has a support for a wide range of data formats and sources.
```{figure} img/chapter2/0.png
---
align: center
---
```

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). 


<a id='2'></a>

##  2. Creating a spark dataframe 

Lets first understand the syntax

```{admonition} Syntax
<b>createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)</b>

<b>Parameters</b>:

data – RDD,list, or pandas.DataFrame.

schema – a pyspark.sql.types.DataType or a datatype string or a list of column names, default is None. 

samplingRatio – the sample ratio of rows used for inferring

verifySchema – verify data types of every row against schema.
```

<a id='3'></a>

### 2a. from RDD


```{figure} img/chapter2/1.png
---
align: center
---
```

<b>What is RDD?</b>

Resilient Distributed Datasets (RDDs)

At a high level, every Spark application consists of a driver program that runs the user’s main function and executes various parallel operations on a cluster. 

The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. 

RDDs are created by starting with a file in the Hadoop file system (or any other Hadoop-supported file system), or an existing Scala collection in the driver program, and transforming it. 

Users may also ask Spark to persist an RDD in memory, allowing it to be reused efficiently across parallel operations. Finally, RDDs automatically recover from node failures.

<b>How to create RDD?</b>

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
rdd_spark = spark.sparkContext.parallelize([('John', 'Seattle', 60, True, 1.7, '1960-01-01'),
 ('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'),
 ('Mike', 'New York', 40, True, 1.65, '1980-01-01')]).collect()


print(rdd_spark)

pd.set_option('display.max_colwidth', 40)
pd.DataFrame([[rdd_spark]])

<b>Creating a spark dataframe from RDD:</b>

df = spark.createDataFrame(rdd_spark)
df.show()

pd.set_option('display.max_colwidth', 20)
print("Input                                               ",            "Output")
display_side_by_side(pd.DataFrame([[rdd_spark]]),df.toPandas())

<a id='4'></a>

### 2b. from List


```{figure} img/chapter2/2.png
---
align: center
---
```

alist = [('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')]
spark.createDataFrame(alist).show()

<a id='5'></a>

### 2c. from pandas dataframe


```{figure} img/chapter2/3.png
---
align: center
---
```

<b>How to create pandas dataframe?</b>

import pandas as pd
df_pd = pd.DataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],columns=["name","city","age","smoker","height", "birthdate"])
df_pd

<b>How to create spark dataframe from pandas dataframe</b>

spark.createDataFrame(df_pd).show()

<a id='6'></a>

##  3. Basic data frame operations

<a id='6'></a>

<a id='7'></a>

### 3a. How to select columns?


```{figure} img/chapter2/4.png
---
align: center
---
```

<b>Input: Spark dataframe</b>

df = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df.show()

<b>Output: spark dataframe with selected columns</b>

 Method#1

df_out = df.select("name","city","age")
df_out.show()

 Method#2

df.select(df.name,df.city,df.age).show()

 Method#3

from pyspark.sql.functions import col
df.select(col("name"),col("city"),col("age")).show()

<b> Summary</b>

print("Input                                               ",            "Output")
display_side_by_side(df.toPandas(),df_out.toPandas())

<a id='8'></a>

### 3b. How to rename columns?


```{figure} img/chapter2/5.png
---
align: center
---
```

<b>Input: Spark dataframe with one of the column "age"</b>

df = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df.show()

<b>Output: spark dataframe with column "age" renamed to "age_in_years"</b>

df.select(col("name").alias("firstname"),df.city.alias("birthcity"),df.age.alias("age_in_years")).show()

df_rename = df.withColumnRenamed("age","age_in_years")
df_rename.show()

print("Input                                               ",            "Output")
display_side_by_side(df.toPandas(),df_rename.toPandas())

<a id='9'></a>

### 3c. How to add new columns?


```{figure} img/chapter2/6.png
---
align: center
---
```

<b>Input: Spark dataframe</b>

df = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df.show()

<b>Output: spark dataframe with a new column added</b>

Method#1

df.withColumn("ageplusheight", df.age+df.height).show()

Method#2

df.select("*",(df.age+df.height).alias("ageplusheight")).show()

Method#3

from pyspark.sql.functions import lit
df.select("*",lit("true").alias("male")).show()

<a id='10'></a>

### 3d. How to delete columns?


```{figure} img/chapter2/7.png
---
align: center
---
```

<b>Input: Spark dataframe</b>

df = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df.show()

<b>Output: spark dataframe with one of the column deleted</b>

dropping a single column

df.drop('city').show()

dropping multiple columns

df.drop('city','birthdate').show()

<a id='11'></a>

### 3e. How to change the data type of columns?


```{figure} img/chapter2/8.png
---
align: center
---
```

<b>Input: Spark dataframe with column "age" of string type</b>

df = spark.createDataFrame([('John', 'Seattle', "60", True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', "30", False, 1.8, '1990-01-01'), 
('Mike', 'New York', "40", True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df.show()
print("Data types:")
df.dtypes

<b>Output: spark dataframe with column "age" of integer data type</b>

df.select("*",df.age.cast("int").alias("age_inttype")).show()
df.select("*",df.age.cast("int").alias("age_inttype")).dtypes

<a id='12'></a>

### 3f. How to filter the data?


```{figure} img/chapter2/9.png
---
align: center
---
```

<b>Input: Spark dataframe</b>

df = spark.createDataFrame([('John', 'Seattle', "60", True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', "30", False, 1.8, '1990-01-01'), 
('Mike', 'New York', "40", True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df.show()
print("Data types:")
df.dtypes

<b>Output: spark dataframe containing people whose age is more than 39</b>

df.filter("age > 39").show()

df.filter(df.age > 39).show()