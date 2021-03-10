```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 17 : Window operations

## Chapter Learning Objectives

- Various window operations on data frame. 

## Chapter Outline

- [1. Window Operations](#1)
    - [1a. How to calculate a new column for each group whose row value is equal to sum of the current row and  
       previous 2 rows?](#2)
    - [1b. How to calculate a new column for each group whose row value is equal to sum of the current row and 2 
      following rows?](#3)
    - [1c. How to calculate a new column whose row value is equal to sum of the current row & following 2 rows?](#4)
    - [1d. How to calculate a new column whose row value is equal to sum of the current row & following 1 row?](#5)
    - [1e. How to calculate the dense rank?](#6)
    - [1f. How to calculate the rank?](#7)
    - [1g. How to calculate the ntile?](#8)
    - [1h. How to calculate the lag?](#9)
    - [1i. How to calculate the lead?](#10)
    - [1j. How to calculate the percent rank?](#11)
    - [1k. How to calculate the row number?](#12)
    - [1l. How to calculate the cume dist?](#13)


import pyspark
from pyspark.sql import functions as func
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

## 1. Window Operations

What are Window Functions?

A window function performs a calculation across a set of table rows that are somehow related to the current row. This is comparable to the type of calculation that can be done with an aggregate function. But unlike regular aggregate functions, use of a window function does not cause rows to become grouped into a single output row — the rows retain their separate identities. Behind the scenes, the window function is able to access more than just the current row of the query result.

In the DataFrame API, we provide utility functions to define a window specification. Taking Python as an example, users can specify partitioning expressions and ordering expressions as follows.

from pyspark.sql.window import Window
windowSpec = \
  Window \
    .partitionBy(...) \
    .orderBy(...)
    
In addition to the ordering and partitioning, users need to define the start boundary of the frame, the end boundary of the frame, and the type of the frame, which are three components of a frame specification.

There are five types of boundaries, which are unboundedPreceding, unboundedFollowing, currentRow, <value> Preceding, and <value> Following. 
    
unboundedPreceding and unboundedFollowing represent the first row of the partition and the last row of the partition, respectively. 
    
For the other three types of boundaries, they specify the offset from the position of the current input row and their specific meanings are defined based on the type of the frame. 
    
There are two types of frames, ROW frame and RANGE frame.
    
ROW frame

ROW frames are based on physical offsets from the position of the current input row, which means thatcurrentRow, <value> Preceding, and <value> Following specifies a physical offset. 
    
If currentRow is used as a boundary, it represents the current input row. 
<value> Preceding and <value> Following describes the number of rows appear before and after the current input row, respectively. 
    
The following figure illustrates a ROW frame with a 1 Preceding as the start boundary and 1 FOLLOWING as the end boundary (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING in the SQL syntax).

df = spark.createDataFrame([
    ("sales",10,6000),("hr",7,3000),("it",5,5000),("sales",2,6000),
    ("hr",3,2000),("hr",4,6000),("it",8,8000),("sales",9,5000),
    ("sales",1,7000),("it",6,6000)],
    ["dept_id","emp_id","salary"])
d1 = df.toPandas()
print(d1.to_string(index=False))#show(truncate=False)print()

<a id='2'></a>

###  1a. How to calculate a new column for each group whose row value is equal to sum of the current row and previous 2 rows?

```{figure} img/chapter17/1.png
---
align: center
---
```
```{figure} img/chapter17/2.png
---
align: center
---
```

from pyspark.sql import functions as func
from pyspark.sql import Window
window = Window.partitionBy("dept_id").orderBy("emp_id").rowsBetween(-2,  0)
print(df.withColumn("sum", func.sum("salary").over(window)).toPandas().to_string(index=False))#show()

<a id='3'></a>

###  1b. How to calculate a new column for each group whose row value is equal to sum of the current row and 2 following  rows?

```{figure} img/chapter17/3.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy("emp_id").rowsBetween(0, 2)
print(df.withColumn("sum", func.sum("salary").over(window)).toPandas().to_string(index=False))#show()

<a id='4'></a>

###  1c. How to calculate a new column whose row value is equal to sum of the current row and following 2 rows?

```{figure} img/chapter17/4.png
---
align: center
---
```

window = Window.rowsBetween(-2,  0)
print(df.withColumn("sum", func.sum("salary").over(window)).toPandas().to_string(index=False))#show()

<a id='5'></a>

###  1d. How to calculate a new column whose row value is equal to sum of the current row and following 1 row?

```{figure} img/chapter17/5.png
---
align: center
---
```

window = Window.rowsBetween(0,  1)
df.withColumn("sum", func.sum("salary").over(window)).show()
print(df.withColumn("sum", func.sum("salary").over(window)).toPandas().to_string(index=False))

<a id='6'></a>

###  1e. How to calculate the dense rank?

```{figure} img/chapter17/6.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary").desc())
print(df.withColumn("rank", func.dense_rank().over(window)).toPandas().to_string(index=False))#show()

<a id='7'></a>

###  1f. How to calculate the rank?

```{figure} img/chapter17/7.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary").desc())
print(df.withColumn("rank", func.rank().over(window)).toPandas().to_string(index=False))#show()

<a id='8'></a>

###  1g. How to calculate the ntile?

```{figure} img/chapter17/8.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary").desc())
print(df.withColumn("salary_bucket", func.ntile(4).over(window)).toPandas().to_string(index=False))#show()

<a id='9'></a>

###  1h. How to calculate the lag?

```{figure} img/chapter17/9.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary").desc())
print(df.withColumn("previousrow_salary", func.lag('salary',1).over(window)).toPandas().to_string(index=False))#.show()

<a id='10'></a>

###  1i. How to calculate the lead?

```{figure} img/chapter17/10.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary").desc())
print(df.withColumn("nextrow_salary", func.lead('salary',1).over(window)).toPandas().to_string(index=False))#show()

<a id='11'></a>

###  1j. How to calculate the percent rank?

```{figure} img/chapter17/11.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary"))
print(df.withColumn("percentile", func.percent_rank().over(window)).toPandas().to_string(index=False))#show()

<a id='12'></a>

###  1k. How to calculate the row number?

```{figure} img/chapter17/12.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary"))
print(df.withColumn("row_no", func.row_number().over(window)).toPandas().to_string(index=False))#show()

<a id='13'></a>

###  1l. How to calculate the cume dist?

```{figure} img/chapter17/13.png
---
align: center
---
```

window = Window.partitionBy("dept_id").orderBy(func.col("salary"))
print(df.withColumn("cume_dist", func.cume_dist().over(window)).toPandas().to_string(index=False))#show()

<a id='9'></a>

<a id='9'></a>