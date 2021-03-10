```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 9 : Map Column

## Chapter Learning Objectives

- Various data operations on columns containing map. 

## Chapter Outline

- [1. How to deal with map column?](#1)
    - [1a. How to create a column of map type?](#2)
    - [1b. How to read individual elements of a map column ?](#3)
    - [1c. How to extract the keys from a map column?](#4)
    - [1d. How to extract the values from a map column?](#5)
    - [1e. How to convert a map column into an array column?](#6)
    - [1f. How to create a map column from multiple array columns?](#7)
    - [1g. How to combine multiple map columns into one?](#8)


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

<div class="special_table"></div>

click on  | any image
---: |:--- 
[![alt](img/chapter9/1.png)](#2)| [![alt](img/chapter9/2.png)](#3)
[![alt](img/chapter9/3.png)](#4)| [![alt](img/chapter9/4.png)](#5)
[![alt](img/chapter9/5.png)](#6)| [![alt](img/chapter9/6.png)](#7)
[![alt](img/chapter9/7.png)](#8)|

<a id='2'></a>

## 1a. How to create a column of map type?



```{figure} img/chapter9/1.png
---
align: center
---
```

df = spark.createDataFrame([({"a":1,"b": 2,"c":3},)],["data"])
df.show(truncate=False)
print(df.dtypes)

<a id='3'></a>

## 1b. How to read individual elements of a map column ?


```{figure} img/chapter9/2.png
---
align: center
---
```

<b>Input:  Spark dataframe containing map column</b>

df1 = spark.createDataFrame([({"a":1,"b": 2,"c":3},)],["data"])
df1.show(truncate=False)

<b>Output :  Spark dataframe containing map keys as column and its value </b>

df_map = df1.select(df1.data.a.alias("a"), df1.data.b.alias("b"), df1.data.c.alias("c") )
df_map.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df1.toPandas(),df_map.toPandas())

<a id='4'></a>

## 1c. How to extract the keys from a map column?


```{figure} img/chapter9/3.png
---
align: center
---
```

Lets first understand the syntax



```{admonition} Syntax
<b>pyspark.sql.functions.map_keys(col)</b>

Returns an unordered array containing the keys of the map.


<b>Parameters</b>:

- col – name of column or expression


'''

<b>Input:  Spark data frame consisting of a map column </b>

df2 = spark.createDataFrame([({"a":1,"b":"2","c":3},)],["data"])
df2.show(truncate=False)

<b>Output :  Spark data frame consisting of a column of keys </b>

from pyspark.sql.functions import map_keys
df_keys = df2.select(map_keys(df2.data).alias("keys"))
df_keys.show()

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df2.toPandas(),df_keys.toPandas())

<a id='5'></a>

## 1d. How to extract the values from a map column?


```{figure} img/chapter9/4.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.map_values(col)</b>

Collection function: Returns an unordered array containing the values of the map.
 

<b>Parameters</b>
- col – name of column or expression

'''


<b>Input:  Spark data frame consisting of a map column  </b>

df3 = spark.createDataFrame([({"a":1,"b":"2","c":3},)],["data"])
df3.show(truncate=False)

<b>Output :  Spark data frame consisting of a column of values </b>

from pyspark.sql.functions import map_values
df_values = df3.select(map_values(df3.data).alias("values"))
df_values.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df3.toPandas(),df_values.toPandas())

<a id='6'></a>

## 1e. How to convert a map column into an array column?



```{figure} img/chapter9/5.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.map_entries(col)</b>

Collection function: Returns an unordered array of all entries in the given map.

<b>Parameters</b>
- col – name of column or expression
'''

<b>Input:  Spark data frame with map column </b>

df4 = spark.createDataFrame([({"a":1,"b": 2,"c":3},)],["data"])
df4.show(truncate=False)

<b>Output :  Spark dataframe containing an array</b>

from pyspark.sql.functions import map_entries
df_array = df4.select(map_entries(df4.data).alias("array"))
df_array.show(truncate=False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df4.toPandas(),df_array.toPandas())

<a id='7'></a>

## 1f. How to create a map column from multiple array columns?



```{figure} img/chapter9/6.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.map_from_arrays(col1, col2)</b>

Creates a new map from two arrays.

<b>Parameters</b>
- col1 – name of column containing a set of keys. All elements should not be null
- col2 – name of column containing a set of values

'''

<b>Input:  Spark data frame with a column </b>

df5 = spark.createDataFrame([([2, 5], ['a', 'b'])], ['k', 'v'])
df5.show()

<b>Output :  Spark data frame with a column of array of repeated values</b>

from pyspark.sql.functions import map_from_arrays
df_map1 = df5.select(map_from_arrays(df5.k, df5.v).alias("map"))
df_map1.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df5.toPandas(),df_map1.toPandas())

<a id='8'></a>

## 1g. How to combine multiple map columns into one?



```{figure} img/chapter9/7.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.map_concat(*cols)</b>

Returns the union of all the given maps.

<b>Parameters</b>

- col – name of columns 


'''


<b>Input:  Spark data frame with multiple map columns </b>

df6 = spark.sql("SELECT map(1, 'a', 2, 'b') as map1, map(3, 'c') as map2")
df6.show()

<b>Output :  Spark data frame with an array column with an element removed</b>

from pyspark.sql.functions import map_concat
df_com = df6.select(map_concat("map1", "map2").alias("combined_map"))
df_com.show(truncate=False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df6.toPandas(),df_com.toPandas())

<a id='9'></a>

<a id='9'></a>