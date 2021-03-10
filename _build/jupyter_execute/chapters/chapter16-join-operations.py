```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 16 : Join operations

## Chapter Learning Objectives

- Various join operations on data frame. 

## Chapter Outline

- [1. Join operations](#1)
    - [1a. Inner Join](#2)
    - [1b. Left Join](#3)
    - [1c. Right Join](#4)
    - [1d. Full Join](#5)
    - [1e. Cross Join](#6)
    - [1f. Anti Join](#7)


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

##  Chapter Outline - Gallery

<div class="special_table"></div>

click on  | any image
---: |:--- 
[![alt](img/chapter16/1.png)](#2)| [![alt](img/chapter16/2.png)](#3)
[![alt](img/chapter16/3.png)](#4)| [![alt](img/chapter16/4.png)](#5)
[![alt](img/chapter16/5.png)](#6)| [![alt](img/chapter16/6.png)](#7)


<a id='1'></a>

Lets first understand the syntax



```{admonition} Syntax
<b>pyspark.sql.DataFrame.join(other, on=None, how=None)</b>

Joins with another DataFrame, using the given join expression.

<b>Parameters</b>:

- other – Right side of the join
- on – a string for the join column name, a list of column names, a join expression (Column), or a list of Columns.   If on is a string or a list of strings indicating the name of the join column(s), the column(s) must exist on both sides, and this performs an equi-join.
- how – str, default inner. Must be one of: inner, cross, outer, full, fullouter, full_outer, left, leftouter, left_outer, right, rightouter, right_outer, semi, leftsemi, left_semi, anti, leftanti and left_anti.


'''


At the top level there are mainly 3 types of joins:

INNER
OUTER
CROSS

INNER JOIN - fetches data if present in both the tables.

OUTER JOIN are of 3 types:

LEFT OUTER JOIN - fetches data if present in the left table.
RIGHT OUTER JOIN - fetches data if present in the right table.
FULL OUTER JOIN - fetches data if present in either of the two tables.

CROSS JOIN, as the name suggests, does [n X m] that joins everything to everything.
Similar to scenario where we simply lists the tables for joining (in the FROM clause of the SELECT statement), using commas to separate them.

<a id='2'></a>

### 1a. Inner Join

df_left = spark.createDataFrame([(1001,1,100),(1002,2,200),(1003,3,300),
                            (1004,1,200),(1005,6,200)
                            ],
                          ["order_id","customer_id","amount"])
                     
df_left.show(truncate=False)
df_right = spark.createDataFrame([(1,"john"), (2,"mike"),(3,"tony"),(4,"kent")],
                          ["customer_id","name"])
df_right.show()

<a id='1'></a>

```{figure} img/chapter16/1a.png
---
align: center
---
```
```{figure} img/chapter16/1b.png
---
align: center
---
```

Inner Join

The inner join is the default join in Spark SQL. It selects rows that have matching values in both relations.

df_left.join(df_right,on="customer_id",how="inner").show()

<a id='3'></a>

### 1b. Left Join

<a id='2'></a>

```{figure} img/chapter16/2a.png
---
align: center
---
```
```{figure} img/chapter16/2b.png
---
align: center
---
```

Left Join

A left join returns all values from the left relation and the matched values from the right relation, or appends NULL if there is no match. It is also referred to as a left outer join.
LEFT JOIN and LEFT OUTER JOIN are equivalent.

LEFT  JOIN - fetches data if present in the left table and only matching records from the right table.

df_left.join(df_right,on="customer_id",how="left").toPandas()#show()

df_left.join(df_right,on="customer_id",how="left_outer").show()

df_left.join(df_right,on="customer_id",how="leftouter").show()

<a id='4'></a>

### 1c. Right Join

<a id='3'></a>

```{figure} img/chapter16/3a.png
---
align: center
---
```

Right Join

RIGHT JOIN - fetches data if present in the right table even if there is no matching records in the right table.

df_left.join(df_right,on="customer_id",how="right").toPandas()#show()

df_left.join(df_right,on="customer_id",how="right_outer").show()

df_left.join(df_right,on="customer_id",how="rightouter").show()

<a id='5'></a>

### 1d. Full Join

<a id='4'></a>

```{figure} img/chapter16/4a.png
---
align: center
---
```

FULL JOIN - fetches data if present in either of the two tables.

df_left.join(df_right,on="customer_id",how="full").toPandas()#show()

df_left.join(df_right,on="customer_id",how="fullouter").show()

df_left.join(df_right,on="customer_id",how="full_outer").show()

df_left.join(df_right,on="customer_id",how="outer").show()

<a id='6'></a>

### 1e. Cross Join

<a id='5'></a>

```{figure} img/chapter16/5a.png
---
align: center
---
```

cross join

spark.conf.get("spark.sql.crossJoin.enabled")

spark.conf.set("spark.sql.crossJoin.enabled", "true")

df_left.crossJoin(df_right).toPandas()#show()

df_left.join(df_right,on="customer_id",how="semi").show()
#semi, leftsemi, left_semi, anti, leftanti and left_anti.

df_left.join(df_right,on="customer_id",how="leftsemi").show()

df_left.join(df_right,on="customer_id",how="left_semi").show()

<a id='7'></a>

### 1f. Anti Join

<a id='6'></a>

```{figure} img/chapter16/5a.png
---
align: center
---
```

df_left.join(df_right,on="customer_id",how="anti").show()

df_left.join(df_right,on="customer_id",how="leftanti").show()

df_left.join(df_right,on="customer_id",how="left_anti").show()



<a id='9'></a>

<a id='9'></a>