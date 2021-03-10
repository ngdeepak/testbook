```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 8 : Array Columns

## Chapter Learning Objectives

- Various data operations on columns containing date strings, date and timestamps. 

## Chapter Outline

- [1. How to deal with Array columns?](#1)
    - [1a. How to create a array column from multiple columns?](#2)
    - [1b. How to remove duplicate values from an array column?](#3)
    - [1c. How to check if a value is in an array column?](#4)
    - [1d. How to find the list of elements in column A, but not in column B without duplicates?](#5)
    - [1e. How to sort the column array in ascending order?](#6)
    - [1f. How to create an array from a  column value  repeated  many times?](#7)
    - [1g. How to remove all elements equal to an element from the given array in a column?](#8)
    - [1h. How to locate the position of first occurrence of the given value in the given array in a column?](#9)
    - [1i. How to find the minimum value of an array in a column?](#10)
    - [1j. How to find the maximum value of an array in a column?](#11)
    - [1k. How to convert a column of nested arrays into a map column?](#12)
    - [1l. How to sort an array in a column in ascending or descending order?](#13)
    - [1m. How to slice an array in a column?](#14)
    - [1n. How to shuffle a column containing an array?](#15)
    - [1o. How to create a  array column  containing elements with sequence(start, stop, step)?](#16)
    - [1p. How to reverse the order(not reverse sort) of an array in a column ?](#17)
    - [1q. How to combine two array columns into a map column?](#18)
    - [1r. How to convert unix timestamp to a string timestamp?](#19)
    - [1s. How to find overlap between 2 array columns?](#20)
    - [1t. How to flatten a column containing nested arrays?](#21)
    - [1u. How to concatenate the elements of an array in a column?](#22)
    - [1v. How to zip 2 array columns ?](#23)


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



[![alt](img/chapter8/1.png)](#2)

click on  | any image
---: |:--- 
[![alt](img/chapter8/2.png)](#3)| [![alt](img/chapter8/3.png)](#4)
[![alt](img/chapter8/4.png)](#5)| [![alt](img/chapter8/5.png)](#6)
[![alt](img/chapter8/6.png)](#7)| [![alt](img/chapter8/7.png)](#8)
[![alt](img/chapter8/8.png)](#9)| [![alt](img/chapter8/9.png)](#10)
[![alt](img/chapter8/10.png)](#11)| [![alt](img/chapter8/11.png)](#12)
[![alt](img/chapter8/12.png)](#13)| [![alt](img/chapter8/13.png)](#14)
[![alt](img/chapter8/14.png)](#15)| [![alt](img/chapter8/15.png)](#16)
[![alt](img/chapter8/16.png)](#17)| [![alt](img/chapter8/17.png)](#18)
[![alt](img/chapter8/18.png)](#19)| [![alt](img/chapter8/19.png)](#20)
[![alt](img/chapter8/20.png)](#21)| [![alt](img/chapter8/21.png)](#22)
[![alt](img/chapter8/22.png)](#23)| 

<a id='2'></a>

<a id='2'></a>

## 1a. How to create a array column from multiple columns?



```{figure} img/chapter8/1.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.array(*cols)</b>

Creates a new array column.

<b>Parameters</b>

cols – list of column names (string) or list of Column expressions that have the same data type.

'''

<b>Input:  Spark data frame with multiple columns</b>

df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_mul.show()

<b>Output :  Spark data frame with a array column </b>

from pyspark.sql.functions import array
df_array = df_mul.select(array(df_mul.age,df_mul.height,df_mul.city).alias("array_column"))
df_array.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_mul.toPandas(),df_array.toPandas())

<a id='3'></a>

## 1b. How to remove duplicate values from an array column?


```{figure} img/chapter8/2.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>ppyspark.sql.functions.array_distinct(col)</b>

removes duplicate values from the array


<b>Parameters</b>:

col – name of column or expression
'''

<b>Input:  Spark data frame with a array column with duplicates</b>

df_array = spark.createDataFrame([([1, 2, 3, 2, 4],), ([4, 5, 5, 4, 6],)], ['data'])
df_array.show()

<b>Output :  Spark data frame with a array column with no duplicates</b>

from pyspark.sql.functions import array_distinct
df_array_no = df_array.select(array_distinct(df_array.data).alias("array_no_dup"))
df_array_no.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_array.toPandas(),df_array_no.toPandas())

<a id='4'></a>

## 1c. How to check if a value is in an array column?


```{figure} img/chapter8/3.png
---
align: center
---
```

Lets first understand the syntax



```{admonition} Syntax
<b>pyspark.sql.functions.array_contains(col, value)</b>

returns null if the array is null, true if the array contains the given value, and false otherwise.



<b>Parameters</b>:

- col – name of column containing array
- value – value or column to check for in array

'''

<b>Input:  Spark data frame with a array column</b>

df1 = spark.createDataFrame([([1, 2, 3],), ([],),([None, None],)], ['data'])
df1.show()

<b>Output :  Spark data frame with a column to indicate if a value exists </b>

from pyspark.sql.functions import array_contains
df2 = df1.select(array_contains(df1.data, 1).alias("if_1_exists"))
df2.show()

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df1.toPandas(),df2.toPandas())

<a id='5'></a>

## 1d. How to find the list of elements in column A, but not in column B without duplicates?



```{figure} img/chapter8/4.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.array_except(col1, col2)</b>

returns an array of the elements in col1 but not in col2, without duplicates.
 

<b>Parameters</b>
- col1 – name of column containing array
- col2 – name of column containing array
'''

<b>Input:  Spark data frame with 2 array columns </b>

df3 = spark.createDataFrame([([1, 2, 3, 4, 5],[6, 7, 8, 9, 10]), ([4, 5, 5, 4, 6],[6, 2, 3, 2, 4])], ['A', 'B'])
df3.show()

<b>Output :  Spark data frame with a result array column </b>

from pyspark.sql.functions import array_except
df4 = df3.select(array_except(df3.A, df3.B).alias("in_A_not_in_B"))
df4.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df3.toPandas(),df4.toPandas())

<a id='6'></a>

## 1e.How to sort the column array in ascending order?



```{figure} img/chapter8/5.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.array_sort(col)</b>

sorts the input array in ascending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array.

<b>Parameters</b>
- col – name of column or expression
'''

<b>Input:  Spark data frame with an array column </b>

df_arr = spark.createDataFrame([([2, 1, None, 3, 8, 3, 5],),([1],),([],)], ['data'])
df_arr.show()

<b>Output :  Spark data frame with a sorted array column </b>

from pyspark.sql.functions import array_sort
df_sort =df_arr.select(array_sort(df_arr.data).alias('sort'))
df_sort.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_arr.toPandas(),df_sort.toPandas())

<a id='7'></a>

## 1f. How to create an array from a  column value  repeated  many times?



```{figure} img/chapter8/6.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.array_repeat(col, count)</b>


Collection function: creates an array containing a column repeated count times.

'''

<b>Input:  Spark data frame with a column </b>

df_val = spark.createDataFrame([(5,)], ['data'])
df_val.show()

<b>Output :  Spark data frame with a column of array of repeated values</b>

from pyspark.sql.functions import array_repeat
df_repeat = df_val.select(array_repeat(df_val.data, 3).alias('repeat'))
df_repeat.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_val.toPandas(),df_repeat.toPandas())

<a id='8'></a>

## 1g. How to remove all elements equal to an element from the given array in a column?



```{figure} img/chapter8/7.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.array_remove(col, element)</b>

Remove all elements that equal to element from the given array.

<b>Parameters</b>

- col – name of column containing array
- element – element to be removed from the array

'''

<b>Input:  Spark data frame with an array column </b>

df_arr2 = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df_arr2.show()

<b>Output :  Spark data frame with an array column with an element removed</b>

from pyspark.sql.functions import array_remove
df_arr3 = df_arr2.select(array_remove(df_arr2.data, 4).alias("array_remove_4"))
df_arr3.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_arr2.toPandas(),df_arr3.toPandas())

<a id='9'></a>

<a id='9'></a>

## 1h . How to locate the position of first occurrence of the given value in the given array in a column?



```{figure} img/chapter8/8.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.array_position(col, value)</b>

Collection function: Locates the position of the first occurrence of the given value in the given array. Returns null if either of the arguments are null.

'''

<b>Input:  Spark data frame with an array column</b>

df_pos1 = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df_pos1.show()

<b>Output :  Spark data frame with column giving the position of the element </b>

from pyspark.sql.functions import array_position
df_pos2 = df_pos1.select(array_position(df_pos1.data, 4).alias("array_position_4"))
df_pos2.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_pos1.toPandas(),df_pos2.toPandas())

<a id='10'></a>

## 1i. How to find the minimum value of an array in a column?


```{figure} img/chapter8/9.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.array_min(col)</b>

returns the minimum value of the array.

<b>Parameters</b>
- col – name of column or expression
'''

<b>Input:  Spark data frame with an array columns</b>

df_arr = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df_arr.show()

<b>Output :  Spark data frame with a column </b>

from pyspark.sql.functions import array_min
df_min = df_arr.select(array_min(df_arr.data).alias("array_min"))
df_min.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_arr.toPandas(),df_min.toPandas())

<a id='11'></a>

## 1j. How to find the maximum value of an array in a column?


```{figure} img/chapter8/10.png
---
align: center
---
```

Lets first understand the syntax


```{admonition} Syntax
<b>pyspark.sql.functions.array_max(col)</b>

returns the maximum value of the array.

<b>Parameters</b>
- col – name of column or expression
'''

<b>Input:  Spark data frame with an array column</b>

df = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df.show()

<b>Output :  Spark data frame with a column</b>

from pyspark.sql.functions import array_max
df_max = df.select(array_max(df.data).alias("array_max"))
df_max.show()

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df.toPandas(),df_max.toPandas())

<a id='12'></a>

## 1k. How to convert a column of nested arrays into a map column?



```{figure} img/chapter8/11.png
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

<b>Input:  Spark data frame with a map column </b>

df = spark.sql("SELECT array(struct(1, 'a'), struct(2, 'b')) as data")
df.show()

<b>Output :  Spark data frame with a date column</b>

from pyspark.sql.functions import map_from_entries
df_map = df.select(map_from_entries("data").alias("map"))
df_map.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_map.toPandas())

<a id='13'></a>

## 1l. How to sort an array in a column in ascending or descending order?



```{figure} img/chapter8/12.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.sort_array(col, asc=True)</b>

sorts the input array in ascending or descending order according to the natural ordering of the array elements. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order.

<b>Parameters</b>
- col – name of column or expression
'''

<b>Input:  Spark data frame with an array column </b>

df = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df.show()

<b>Output :  Spark data frame with a sorted array column  </b>

from pyspark.sql.functions import sort_array
df_asc = df.select(sort_array(df.data, asc=True).alias('asc'))
df_asc.show()

from pyspark.sql.functions import sort_array
df_desc = df.select(sort_array(df.data, asc=False).alias('desc'))
df_desc.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_asc.toPandas(), df_desc.toPandas())

<a id='15'></a>

<a id='14'></a>

## 1m. How to slice an array in a column?



```{figure} img/chapter8/13.png
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

<b>Input:  Spark data frame with an array column </b>

df = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df.show()

<b>Output :  Spark data frame with an array column</b>

from pyspark.sql.functions import slice
df.select(slice(df.data, 2, 3).alias('slice')).show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_map.toPandas())

<a id='15'></a>

## 1n. How to shuffle a column containing an array?



```{figure} img/chapter8/14.png
---
align: center
---
```



```{admonition} Syntax

<b>pyspark.sql.functions.shuffle(col)</b>

Generates a random permutation of the given array.


'''

<b>Input:  Spark data frame with an array column </b>

df = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df.show()

<b>Output :  Spark data frame with shuffled array column</b>

from pyspark.sql.functions import shuffle
df_shu = df.select(shuffle(df.data).alias('shuffle'))
df_shu.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_shu.toPandas())

<a id='16'></a>

## 1o. How to create a  array column  containing elements with sequence(start, stop, step)?



```{figure} img/chapter8/15.png
---
align: center
---
```



```{admonition} Syntax

<b>pyspark.sql.functions.sequence(start, stop, step=None)</b>

Generate a sequence of integers from start to stop, incrementing by step. If step is not set, incrementing by 1 if start is less than or equal to stop, otherwise -1.


'''

<b>Input:  Spark data frame </b>

df = spark.createDataFrame([(-2, 2)], ('A', 'B'))
df.show()

<b>Output :  Spark data frame with an array sequence</b>

from pyspark.sql.functions import sequence
df_seq = df.select(sequence('A', 'B').alias('seq'))
df_seq.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_seq.toPandas())

<a id='17'></a>

## 1p. How to reverse the order(not reverse sort) of an array in a column ?


```{figure} img/chapter8/16.png
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

<b>Input:  Spark data frame with an array column </b>

df_arr = spark.createDataFrame([([1, 2, 3, 8, 4],), ([4, 5, 32, 32, 6],)], ['data'])
df_arr.show()

<b>Output :  Spark data frame with a reverse ordered array column</b>

from pyspark.sql.functions import reverse
df_rev = df_arr.select(reverse(df_arr.data).alias('reverse_order'))
df_rev.show(truncate=False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_arr.toPandas(),df_rev.toPandas())

<a id='18'></a>

## 1q. How to combine two array columns into a map column?



```{figure} img/chapter8/17.png
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

<b>Input:  Spark data frame with 2 array columns </b>

df_arrm = spark.createDataFrame([([1, 2, 3, 4, 5],[6, 7, 8, 9, 10]), ([4, 5, 6, 7, 8],[6, 2, 3, 9, 4])], ['A','B'])
df_arrm.show()

<b>Output :  Spark data frame with a map column</b>

from pyspark.sql.functions import map_from_arrays
df_map = df_arrm.select(map_from_arrays(df_arrm.A, df_arrm.B).alias('map'))
df_map.show(truncate=False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_arrm.toPandas(),df_map.toPandas())

<a id='19'></a>

## 1r. How to concatenate the elements of an array in a column?


```{figure} img/chapter8/18.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.concat(*cols)</b>

Concatenates multiple input columns together into a single column. The function works with strings, binary and compatible array columns.


'''

<b>Input:  Spark data frame with a map column </b>

df_arr1 = spark.createDataFrame([([1, 2, 3, 4, 5],[6, 7, 8, 9, 10]), ([4, 5, 6, 7, 8],[6, 2, 3, 9, 4])], ['A','B'])
df_arr1.show()

<b>Output :  Spark data frame with a date column</b>

from pyspark.sql.functions import concat
df_con = df_arr1.select(concat(df_arr1.A, df_arr1.B).alias("concatenate"))
df_con.show(2,False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_arr1.toPandas(),df_con.toPandas())

<a id='20'></a>

## 1s. How to find overlap between 2 array columns?



```{figure} img/chapter8/19.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.arrays_overlap(a1, a2)</b>

Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise



'''

<b>Input:  Spark data frame with array columns </b>

df_over = spark.createDataFrame([(["a", "b"], ["b", "c"],), (["a"], ["b", "c"],),(["a", None], ["b", None],) ], ['A', 'B'])
df_over.show()

<b>Output :  Spark data frame </b>

from pyspark.sql.functions import arrays_overlap
df_overlap = df_over.select(arrays_overlap(df_over.A, df_over.B).alias("overlap"))
df_overlap.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_over.toPandas(),df_overlap.toPandas())

<a id='21'></a>

## 1t. How to flatten a column containing nested arrays?



```{figure} img/chapter8/20.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.flatten(col)</b>

creates a single array from an array of arrays. If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.

'''

<b>Input:  Spark data frame with nested array column </b>

df4 = spark.createDataFrame([([[1, 2, 3, 8, 4],[6,8, 10]],), ([[4, 5, 32, 32, 6]],)], ['data'])
df4.show(truncate=False)

<b>Output :  Spark data frame with a flattended array column</b>

from pyspark.sql.functions import flatten
df_flat = df4.select(flatten(df4.data).alias('flatten'))
df_flat.show(truncate=False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df4.toPandas(),df_flat.toPandas())

<a id='22'></a>

## 1u. How to concatenate the elements of an array in a column?


```{figure} img/chapter8/21.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.array_join(col, delimiter, null_replacement=None)</b>

Concatenates the elements of column using the delimiter. Null values are replaced with null_replacement if set, otherwise they are ignored.

'''

<b>Input:  Spark data frame with an array column </b>

df_a1 = spark.createDataFrame([([1, 2, 3, 4, 5],), ([4, 5, None, 4, 6],)], ['A'])
df_a1.show()

<b>Output :  Spark data frame with a concatenated array element column</b>

from pyspark.sql.functions import array_join
df_j1 = df_a1.select(array_join(df_a1.A,',').alias("array_join"))
df_j1.show()

df_j2 = df_a1.select(array_join(df_a1.A,',', null_replacement="NA").alias("array_join"))
df_j2.show()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_a1.toPandas(),df_j1.toPandas(), df_j2.toPandas())

<a id='23'></a>

## 1v. How to zip 2 array columns ?



```{figure} img/chapter8/22.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.arrays_zip(*cols)</b>


Collection function: Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.

Parameters
cols – columns of arrays to be merged.

'''

<b>Input:  Spark data frame with an array column </b>

dfz = spark.createDataFrame([(([1, 2, 3], [4, 5, 6]))], ['A', 'B'])
dfz.show()

<b>Output :  Spark data frame with a zipped array column</b>

from pyspark.sql.functions import arrays_zip
df_zip = dfz.select(arrays_zip(dfz.A, dfz.B).alias('zipped'))
df_zip.show(truncate=False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(dfz.toPandas(),df_zip.toPandas())