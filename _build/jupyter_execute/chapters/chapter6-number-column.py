```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 6 : Number Columns

## Chapter Learning Objectives

- Various data operations on columns containing numbers. 

## Chapter Outline

- [1. Various data operations on columns containing numbers ](#1)
    - [1a. How to calculate the minimum value in a column?](#2)
    - [1b. How to calculate the maximum value in a column?](#3)
    - [1c. How to calculate the sum of a column?](#4)
    - [1d. How to round values in a column?](#5)
    - [1e. How to calculate the mean of a column?](#6)
    - [1f. How to calculate the standard deviation of a column?](#7)


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
[![alt](img/chapter6/1.png)](#2)| [![alt](img/chapter6/2.png)](#3)
[![alt](img/chapter6/3.png)](#4) | [![alt](img/chapter6/4.png)](#5)
[![alt](img/chapter6/5.png)](#6) | [![alt](img/chapter6/6.png)](#7)


<a id='2'></a>

## 1a. How to calculate the minimum value in a column?



```{figure} img/chapter6/1.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.min(col)</b>

Aggregate function: returns the minimum value of the expression in a group.

<b>Parameters</b>:

- col : column

'''

<b>Input:  Spark data frame with a column having a string</b>

df_min = spark.createDataFrame([(1,),(2,),(3,)],['data'])
df_min.show()

<b>Output :  Spark data frame with a column with a split string</b>

from pyspark.sql.functions import min
df_min.select(min(df_min.data)).first()[0]

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_min.toPandas(),df_min.select(min(df_min.data)).toPandas())

<a id='3'></a>

## 1b. How to calculate the maximum value in a column?


```{figure} img/chapter6/2.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.max(col)</b>

Aggregate function: returns the maximum value of the expression in a group


<b>Parameters</b>:

- col : column
'''

<b>Input:  Spark data frame with a column having a string</b>

df_max = spark.createDataFrame([(1,),(2,),(3,)],['data'])
df_max.show()

<b>Output :  Spark data frame with a column with a split string</b>

from pyspark.sql.functions import max
df_max.select(max(df_max.data)).first()[0]

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_max.toPandas(),df_max.select(max(df_max.data)).toPandas())

<a id='4'></a>

## 1c. How to calculate the sum of a column?


```{figure} img/chapter6/3.png
---
align: center
---
```

Lets first understand the syntax

Converts a string expression to upper case.

```{admonition} Syntax
<b>pyspark.sql.functions.sum(col)</b>

Aggregate function: returns the sum of all values in the expression.


<b>Parameters</b>:

- col : column
'''

<b>Input:  Spark data frame with a column having a lowercase string</b>

df_sum = spark.createDataFrame([(1,),(2,),(3,)],['data'])
df_sum.show()

<b>Output :  Spark data frame with a column having a uppercase string</b>

from pyspark.sql.functions import sum
df_sum.select(sum(df_sum.data)).first()[0]

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df_sum.toPandas(),df_sum.select(sum(df_sum.data)).toPandas())

<a id='5'></a>

## 1d. How to round values in a column?



```{figure} img/chapter6/4.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.round(col, scale=0)</b>

Round the given value to scale decimal places using HALF_UP rounding mode if scale >= 0 or at integral part when scale < 0.

<b>Parameters</b>:

- col : column
'''

<b>Input:  Spark data frame with a column having a string</b>

df_round = spark.createDataFrame([(1.453,),(2.65433,),(3.765,),(2.985,)],['data'])
df_round.show()

<b>Output :  Spark data frame with a column with a sliced string</b>

from pyspark.sql.functions import round
df_round1 = df_round.select(round(df_round.data,2))
df_round1.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_round.toPandas(),df_round1.toPandas())

<a id='6'></a>

## 1e. How to calculate the mean of a column?



```{figure} img/chapter6/5.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.mean(col)</b>

Aggregate function: returns the average of the values in a group

<b>Parameters</b>:

- col : column
'''

<b>Input:  Spark data frame with a column having a string</b>

df_mean = spark.createDataFrame([(1,),(2,),(3,)],['data'])
df_mean.show()

<b>Output :  Spark data frame with a column with a split string</b>

from pyspark.sql.functions import mean
df_mean.select(mean(df_mean.data)).first()[0]

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_mean.toPandas(),df_mean.select(mean(df_mean.data)).toPandas())

<a id='7'></a>

## 1f. How to calculate the standard deviation of a column?



```{figure} img/chapter6/6.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.stddev(col)</b>

Aggregate function: alias for stddev_samp.

'''

<b>Input:  Spark data frame with a column having a string</b>

df_stddev = spark.createDataFrame([(1,),(2,),(3,)],['data'])
df_stddev.show()

<b>Output :  Spark data frame with a column with a regex</b>

from pyspark.sql.functions import stddev
df_stddev.select(stddev(df_stddev.data)).first()[0]

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_stddev.toPandas(),df_stddev.select(stddev(df_stddev.data)).toPandas())