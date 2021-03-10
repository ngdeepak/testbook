```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 5 : String Columns

## Chapter Learning Objectives

- Various data operations on columns containing string. 

## Chapter Outline

- [1. Various data operations on columns containing string ](#1)
    - [1a. How to split a string?](#2)
    - [1b. How to slice a string?](#3)
    - [1d. How to convert lowercase to uppercase?](#4)
    - [1c. How to convert uppercase to lowercase?](#5)
    - [1e. How to extract a specific group matched by a Java regex?](#6)
    - [1f. How to replace a specific group matched by a Java regex?](#7)

<style>body {text-align: justify}</style>

# import panel as pn
# css = """
# div.special_table + table, th, td {
#   border: 3px solid orange;
# }
# """
# pn.extension(raw_css=[css])
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

## Chapter Outline - Gallery

click on  | any image
---: |:--- 
[![alt](img/chapter5/1.png)](#2)| [![alt](img/chapter5/2.png)](#3)
[![alt](img/chapter5/3.png)](#4) | [![alt](img/chapter5/4.png)](#5)
[![alt](img/chapter5/5.png)](#6) | [![alt](img/chapter5/6.png)](#7)


<a id='2'></a>

## 1a. How to split a string?



```{figure} img/chapter5/1.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.split(str, pattern, limit=-1)</b>

Splits str around matches of the given pattern.

<b>Parameters</b>:

- str : a string expression to split

- pattern : a string representing a regular expression. The regex string should be a Java regular expression.

- limit : an integer which controls the number of times pattern is applied.

limit > 0: The resulting array’s length will not be more than limit, and the
resulting array’s last entry will contain all input beyond the last matched pattern.

limit <= 0: pattern will be applied as many times as possible, and the resulting
array can be of any size.
'''

<b>Input:  Spark data frame with a column having a string</b>

df_string = spark.createDataFrame([('abc__def__ghc',)], ['string',])
df_string.show()

<b>Output :  Spark data frame with a column with a split string</b>

from pyspark.sql.functions import split
df_split = df_string.select(split(df_string.string,'__').alias('split_string'))
df_split.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_string.toPandas(),df_split.toPandas())



<a id='3'></a>

## 1b. How to slice a string?


```{figure} img/chapter5/4.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.slice(x, start, length)</b>

Collection function: returns an array containing all the elements in x from index start (array indices start at 1, or from the end if start is negative) with the specified length.

<b>Parameters</b>:

- x : the array to be sliced

- start : the starting index

- length : the length of the slice
'''

<b>Input:  Spark data frame with a column having a string</b>

df_string = spark.createDataFrame([('abcdefghi',)], ['string',])
df_string.show()

<b>Output :  Spark data frame with a column with a sliced string</b>

from pyspark.sql.functions import substring
df_sub = df_string.select(substring(df_string.string,1,4).alias('substring'))
df_sub.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_string.toPandas(),df_sub.toPandas())

<a id='4'></a>

## 1c. How to convert lowercase to uppercase?


```{figure} img/chapter5/3.png
---
align: center
---
```

Lets first understand the syntax

Converts a string expression to upper case.

```{admonition} Syntax
<b>pyspark.sql.functions.upper(col)</b>


Converts a string expression to lower case.

<b>Parameters</b>:

- col : column
'''

<b>Input:  Spark data frame with a column having a lowercase string</b>

df_upper = spark.createDataFrame([('ABCDEFGHI',)], ['uppercase',])
df_upper.show()

<b>Output :  Spark data frame with a column having a uppercase string</b>

from pyspark.sql.functions import lower
df_lower= df_upper.select(lower(df_upper.uppercase).alias('lowercase'))
df_lower.show()

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df_upper.toPandas(),df_lower.toPandas())



<a id='5'></a>

## 1d. How to convert uppercase to lowercase?


```{figure} img/chapter5/2.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.lower(col)</b>

Converts a string expression to lower case.

<b>Parameters</b>:

- col : column
'''

<b>Input:  Spark data frame with a column having a string</b>

df_string = spark.createDataFrame([('abcdefghc',)], ['lowercase',])
df_string.show()

<b>Output :  Spark data frame with a column with a split string</b>

from pyspark.sql.functions import upper
df_upper= df_string.select(upper(df_string.lowercase).alias('uppercase'))
df_upper.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_string.toPandas(),df_upper.toPandas())

<a id='6'></a>

## 1e. How to extract a specific group matched by a Java regex?



```{figure} img/chapter5/5.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.regexp_extract(str, pattern, idx)</b>

Extract a specific group matched by a Java regex, from the specified string column. If the regex did not match, or the specified group did not match, an empty string is returned.
'''

<b>Input:  Spark data frame with a column having a string</b>

df = spark.createDataFrame([('100-200',)], ['str'])
df.show()

<b>Output :  Spark data frame with a column with a split string</b>

from pyspark.sql.functions import regexp_extract
df_regex1 = df.select(regexp_extract('str', r'(\d+)-(\d+)', 1).alias('regex'))
df_regex1.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_regex1.toPandas())

<a id='7'></a>

## 1f. How to replace a specific group matched by a Java regex?



```{figure} img/chapter5/6.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.regexp_replace(str, pattern, replacement)</b>

Replace all substrings of the specified string value that match regexp with rep.
'''

<b>Input:  Spark data frame with a column having a string</b>

df = spark.createDataFrame([('100-200',)], ['string'])
df.show()

<b>Output :  Spark data frame with a column with a regex</b>

from pyspark.sql.functions import regexp_replace
df_regex2 = df.select(regexp_replace('string', r'(\d+)', '--').alias('replace'))
df_regex2.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_regex2.toPandas())