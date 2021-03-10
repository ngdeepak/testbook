```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 15 : Aggregate Operations

## Chapter Learning Objectives

- Various aggregate  operations on data frame. 

## Chapter Outline

- [1. Dataframe Aggregation](#1)
- [2. Dataframe Groupby operations](#2)


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

## 1a. DataFrame Aggregations



```{figure} img/chapter15/1.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.DataFrame.agg(*exprs)</b>

Aggregate on the entire DataFrame without groups (shorthand for df.groupBy.agg()).

'''

#### Input: Spark dataframe 

df = spark.createDataFrame([(1,"north",100,"walmart"),(2,"south",300,"apple"),(3,"west",200,"google"),
                            (1,"east",200,"google"),(2,"north",100,"walmart"),(3,"west",300,"apple"),
                            (1,"north",200,"walmart"),(2,"east",500,"google"),(3,"west",400,"apple"),],
                          ["emp_id","region","sales","customer"])
                     
df.show()#show(truncate=False)


print(df.sort('customer').toPandas().to_string(index=False))#show()

<a id='3'></a>

df.agg({"sales": "sum"}).show()

df.agg({"sales": "min"}).show()

df.agg({"sales": "max"}).show()

df.agg({"sales": "count"}).show()

df.agg({"sales": "mean"}).show()

df.agg({"sales": "mean","customer":"count"}).show()

<a id='2'></a>

## 1b. DataFrame Aggregations


```{figure} img/chapter15/2.png
---
align: center
---
```

df.groupby("emp_id").agg({"sales": "sum"}).orderBy('emp_id').toPandas()#show()

df.groupby("emp_id").agg({"sales": "max"}).orderBy('emp_id').toPandas()

df.groupby("emp_id").agg({"sales": "last"}).orderBy('emp_id').toPandas()

df.groupby("region").agg({"sales": "sum"}).orderBy('region').show()

df.groupby("customer").agg({"sales": "sum"}).orderBy('customer').show()

<a id='4'></a>

<a id='9'></a>

<a id='9'></a>