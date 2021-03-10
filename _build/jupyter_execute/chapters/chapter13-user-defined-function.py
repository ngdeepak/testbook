```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 13 : User Defined Functions(UDFs)

## Chapter Learning Objectives

- What is User Defined Function and how to use it?. 

## Chapter Outline

- [1. What is User Defined Function?](#1)
    - [1a. How to create User-defined functions ?](#2)
    - [1b. How to use UDF in data frames?](#3)



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

<a id='1'></a>

## 1. What is User Defined Function?

User-Defined Functions (UDFs) are user-programmable routines that act on one row.
Note: UDF’s are the most expensive operations hence use them only you have no choice and when essential

<a id='2'></a>

## 1a. How to create User-defined functions ?



<b>Method:1</b>

def squared(s):
  return s * s
spark.udf.register("squared", squared)

You can optionally set the return type of your UDF. The default return type is StringType.

from pyspark.sql.types import LongType
def squared(s):
  return s * s
spark.udf.register("squaredWithPython", squared, LongType())

<b>Method:2</b>

from pyspark.sql.functions import udf
@udf("long")
def squared(s):
  return s * s

<b>Method:3</b>

from pyspark.sql.types import IntegerType
squared = udf(lambda x: x*x, IntegerType())

<a id='3'></a>

## 1b. How to use UDF in data frames?



<b>Input:  Spark dataframe </b>

df = spark.createDataFrame([(1,),(2,),(3,)],["value"])
df.show()

Use:1
<b>Output :  Spark dataframe containing transformed column using UDF </b>

df.select(squared(df.value).alias("squared")).show()

Use:2
<b>Output :  Spark dataframe containing transformed column using UDF </b>

df.withColumn("squared", squared("value")).show()

df.select(squared(df.value).alias("squared")).show()

Evaluation order and null checking

Spark DataFrame API does not guarantee the order of evaluation of subexpressions. In particular, the inputs of an operator or function are not necessarily evaluated left-to-right or in any other fixed order.

df = spark.createDataFrame([(1,),(2,),(3,),(None,)],["value"])
df.show()

Below code will fail if you execute

df.select(squared(df.value).alias("squared")).show() # fail

from pyspark.sql.types import LongType
def squared(s):
    if s is not None:
        return s * s
spark.udf.register("squaredWithPython", squared, LongType())

df.select(squared(df.value).alias("squared")).show()

<a id='3'></a>

<a id='9'></a>

<a id='9'></a>