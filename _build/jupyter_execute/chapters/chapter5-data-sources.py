```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 2 : DataFrames

## Chapter Learning Objectives

- understand the syntax to create dataframe
- create spark dataframe from different data sources

## Chapter Outline

- [1. What is spark dataframe](#1)
- [2. Creating a spark dataframe](#2)
    - [2a. from RDD](#3)
    - [2b. from List](#4)
    - [2c. from pandas dataframe](#5)

If you do not want to go through the entire chapter, 
## Click on one of the image below that achieves the outcome

A | B
- | - 
[![alt](img/chapter2/rdd_dataframe.png)](#23) | [![alt](img/chapter2/list_dataframe.png)](#33)
[![alt](img/chapter2/pd_spark.png)](#23) | 
<hr></hr>

[![alt text](img/chapter2/rdd_dataframe.png "Title")](#23)
***

[![alt text](img/chapter2/list_dataframe.png "Title")](#23)
***

[![alt text](img/chapter2/pd_spark.png "Title")](#23)
***

<a id='1'></a>

## 1. What is spark dataframe?
A DataFrame simply represents a table of data with rows and columns. A simple analogy would be a spreadsheet with named columns.

Spark Data Frame is a distributed collection of data organized into named columns. It can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDD, Lists, Pandas data frame. 


```{figure} img/chapter2/spark_dataframe.png
---
align: center
---
```

<a id='2'></a>

###  2. Creating a spark dataframe 

Lets first understand the syntax

```{admonition} Syntax
<b>createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)</b>

<b>Parameters</b>:

data – RDD,list, or pandas.DataFrame.

schema – a pyspark.sql.types.DataType or a datatype string or a list of column names, default is None. 

samplingRatio – the sample ratio of rows used for inferring

verifySchema – verify data types of every row against schema.
```

<a id='33'></a>

## 2a. from RDD


```{figure} img/chapter2/rdd_dataframe.png
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

<b>Creating RDD :</b>

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

<b>Creating a spark dataframe:</b>

spark.createDataFrame(rdd_spark).show()

<a id='4'></a>

## 2b. from List


```{figure} img/chapter2/list_dataframe.png
---
align: center
---
```

spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')]).show()

<a id='23'></a>

## 2c. from pandas dataframe


```{figure} img/chapter2/pd_spark.png
---
align: center
---
```

<b>Input: pandas dataframe</b>

<b>Creating pandas dataframe</b>

import pandas as pd
df_pd = pd.DataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')])
df_pd

<b>Output: spark dataframe</b>

spark.createDataFrame(df_pd).show()

## .  &emsp; 3a. test1
<a id='4'></a>

##  &emsp;  5. &emsp; &emsp; test2
<a id='5'></a>