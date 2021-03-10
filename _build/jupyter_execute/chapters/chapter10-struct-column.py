```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 10 : Struct Column

## Chapter Learning Objectives

- Various data operations on columns containing map. 

## Chapter Outline

- [1. How to deal with struct column?](#1)
    - [1a. How to create a struct column?](#2)
    - [1b. How to read individual elements of a struct column ?](#3)
    - [1c. How to add new field to struct column?](#4)
    - [1d. How to drop field in struct column?](#5)
    - [1e. How to flatten a struct in a Spark dataframe?](#6)


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
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

import panel as pn

css = """
div.special_table + table, th, td {
  border: 3px solid orange;
}
"""
pn.extension(raw_css=[css])

<a id='1'></a>

##  Chapter Outline - Gallery



[![alt](img/chapter10/1.png)](#15)

click on  | any image
---: |:---  
[![alt](img/chapter10/2.png)](#2)| [![alt](img/chapter10/3.png)](#3)
[![alt](img/chapter10/4.png)](#4)| [![alt](img/chapter10/5.png)](#5)



<a id='2'></a>

## 1a. How to create a struct column ?



```{figure} img/chapter10/1.png
---
align: center
---
```

<b>Input:  Spark dataframe</b>

df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_mul.show()

<b>Output:  Spark dataframe containing struct column</b>

from pyspark.sql.functions import struct
df_stru = df_mul.select(struct(["name","age","height"]).alias("struct_column"))
df_stru.show()

df_stru.dtypes

<a id='3'></a>

## 1b. How to read individual elements of a struct column ?


```{figure} img/chapter10/2.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>getField(name)</b>

An expression that gets a field by name in a StructField.
'''


<b>Input:  Spark dataframe containing struct column</b>

df_stru = df_mul.select(struct(["name","age","height"]).alias("struct_column"))
df_stru.show()

<b>Output :  Spark dataframe containing individual struct elements </b>

df_ele = df_stru.select(df_stru.struct_column.getField("name").alias("name"),  df_stru.struct_column.getField("age").alias("age"), df_stru.struct_column.getField("height").alias("height"))
df_ele.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_stru.toPandas(),df_ele.toPandas())

<a id='4'></a>

## 1c. How to add new field to struct column?


```{figure} img/chapter10/3.png
---
align: center
---
```

<b>Input:  Spark data frame with a struct column </b>

from pyspark.sql.functions import struct
df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_stru = df_mul.select(struct(["name","age","height"]).alias("struct_column"))
df_stru.show()

<b>Output :  Spark data frame with a struct column with a new element added </b>

df_stru_new = df_stru.withColumn("new",df_stru.struct_column.age+df_stru.struct_column.height )
df_stru_new = df_stru_new.select(struct("struct_column.name", "struct_column.age", df_stru_new.struct_column.height, "new").alias("struct_column"))
df_stru_new .show(3,False)

print(df_stru_new.schema)

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df_stru.toPandas(),df_stru_new.toPandas())

<a id='5'></a>

## 1d. How to drop field in struct column?


```{figure} img/chapter10/4.png
---
align: center
---
```

<b>Input:  Spark data frame with a struct column  </b>

from pyspark.sql.functions import struct
df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_stru = df_mul.select(struct(["name","age","height"]).alias("struct_column"))
df_stru.show()

<b>Output :  Spark data frame with a struct column with an element deleted </b>

df_stru_new = df_stru_new.select(struct("struct_column.name", "struct_column.age").alias("struct_column"))
df_stru_new .show(3,False)

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df_stru.toPandas(),df_stru_new.toPandas())

<a id='6'></a>

## 1e. How to flatten a struct in a Spark dataframe?



```{figure} img/chapter10/5.png
---
align: center
---
```

<b>Input:  Spark data frame with a struct column </b>

from pyspark.sql.functions import struct
df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_stru = df_mul.select(struct(["name","age","height"]).alias("struct_column"))
df_stru.show()

<b>Output :  Spark dataframe with flattened struct column</b>

df_flat = df_stru.select("struct_column.*")
df_flat.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_stru.toPandas(),df_flat.toPandas())

<a id='9'></a>

<a id='9'></a>