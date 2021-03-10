```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 9 : Null & NaN Column

## Chapter Learning Objectives

- Various data operations on Null & NaN columns. 

## Chapter Outline

- [1. How to deal with Null & NaN column?](#1)
    - [1a. How to count the Null & NaN in Spark DataFrame ?](#2)
    - [1b. How to filter the rows that contain NaN & Null?](#3)
    - [1c. How to replace Null values in the dataframe?](#4)
    - [1d. How to replace NaN values in the dataframe?](#4)


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
[![alt](img/chapter12/1.png)](#2)| [![alt](img/chapter12/2.png)](#3)
[![alt](img/chapter12/3.png)](#4)| [![alt](img/chapter12/4.png)](#5)

<a id='2'></a>

## 1a. How to count the Null & NaN in Spark DataFrame ?

Null values represents "no value" or "nothing"
it's not even an empty string or zero. 
A null value indicates a lack of a value

NaN stands for "Not a Number"
It's usually the result of a mathematical operation that doesn't make sense, e.g. 0.0/0.0

 Unlike Pandas, PySpark doesn’t consider NaN values to be NULL.

```{figure} img/chapter12/1.png
---
align: center
---
```

<b>Input:  Spark dataframe</b>

df = spark.createDataFrame([(None, 3,float('nan')), (6,5, 2.0), (5,5, float("nan")),
                            (8, None, 2.0), (12,21,3.0),], ["a", "b","c"])
df.show()

df.printSchema()

<b>Count of NaN in a Spark Data Frame</b>

from pyspark.sql.functions import isnan, when, count, col
df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()

<b>Count of Null in a Spark Data Frame</b>

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

<b>Count of Null and NaN  in a Spark Data Frame</b>

df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

<a id='3'></a>

## 1b. How to filter the rows that contain NaN & Null?


```{figure} img/chapter12/2.png
---
align: center
---
```

<b>Input:  Spark dataframe containing JSON column</b>

df = spark.createDataFrame([(None, 3,float('nan')), (6,5, 2.0), (5,5, float("nan")),
                            (8, None, 2.0), (12,21,3.0),], ["a", "b","c"])
df.show()

#### Learn to build the Boolean Expressions

from functools import reduce
filter_remove_nan_null_rows =reduce(lambda x, y: x & y, [~isnan(col(x)) for x in df.columns]+[col(x).isNotNull() for x in df.columns])
filter_remove_nan_null_rows

from functools import reduce
df.where(filter_remove_nan_null_rows).show()

df.where(~isnan(col("c")) & ~isnan(col("b"))).show()

df.where(col("a").isNotNull()).show()

df.where(col("a").isNull()).show()

df.where(isnan(col("c"))).show()

df.where(~isnan(col("c"))).show()

<b>Output :  Spark dataframe containing individual JSON elements </b>

<a id='3'></a>

## 1c. How to replace Null values in the dataframe? 


```{figure} img/chapter12/3.png
---
align: center
---
```

<b>Input:  Spark dataframe containing Null and NaN column</b>

df = spark.createDataFrame([(None, 3,float('nan')), (6,5, 2.0), (5,5, float("nan")),
                            (8, None, 2.0), (12,21,3.0),], ["a", "b","c"])
df.show()

<b>Output :  Spark dataframe containing individual JSON elements </b>

df.fillna(1000).show()

Filling only for selected columns

df.fillna(1000,subset=["a"]).show()

Filling with a different value for each of the Null Column

df.fillna(1000,subset=["a"]).fillna(500,subset=["b"]).show()

<b> Summary:</b>

print("Input                     ",            "Output")
print("                         ",   "Filling all columns","        ", "Filling selected columns","   ", "Filling a different value")
print("                         ",   "with a same value","        ", "                               "  "for each column")
display_side_by_side(df.toPandas(),df.fillna(1000).toPandas(), df.fillna(1000,subset=["a"]).toPandas(), 
                     df.fillna(1000,subset=["a"]).fillna(500,subset=["b"]).toPandas())

<a id='4'></a>

## 1d. How to replace NaN values in the dataframe?


```{figure} img/chapter12/4.png
---
align: center
---
```

<b>Input:  Spark data frame  </b>

df = spark.createDataFrame([(None, 3,float('nan')), (6,5, 2.0), (5,5, float("nan")),
                            (8, None, 2.0), (12,21,3.0),], ["a", "b","c"])
df.show()

<b>Output :  Spark data frame with a struct column with a new element added </b>

df.replace(float('nan'),10).show()

<a id='5'></a>

<a id='9'></a>

<a id='9'></a>