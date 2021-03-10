```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 11 : JSON Column

## Chapter Learning Objectives

- Various data operations on columns containing Json string. 

## Chapter Outline

- [1. How to deal with JSON string column?](#1)
    - [1a. How to create a  spark dataframe with a JSON string column ?](#2)
    - [1b. How to infer a schema in DDL format from  a JSON string column ?](#3)
    - [1c. How to extract elements from JSON column ?](#4)
    - [1d. How to convert a data frame to JSON string?](#5)
    - [1e. How to convert JSON string column into StructType column?](#6)
    - [1f. How to convert JSON string column into MapType column?](#7)
    - [1g. How to convert JSON string column into ArrayType column?](#8)
    - [1h. How to convert StructType/MapType/ArrayType  column into JSON string?](#8)  
    - [1i. How to extract JSON object from a JSON  string column?](#9)
    - [1j. How to extract JSON objects based on list of field names?](#10)


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

## 1a. How to create a  spark dataframe with a JSON string column ?

Explanation here.

```{figure} img/chapter11/1.png
---
align: center
---
```

json_string = '{"me": {"name":"tony"},"myfamily_heirarchy":{"myfather":"mike","myfather_childs": ["me","kent"],"my_childs":{"name": "susan","susan_childs":{"name":"suzy"}}}}'
rdd = spark.sparkContext.parallelize([json_string])
df_json = spark.createDataFrame([(json_string,)],["json_string"])
df_json.show(1,False)

df_json.printSchema()

<a id='3'></a>

## 1b. How to infer a schema in DDL format from  a JSON string column ?


```{figure} img/chapter11/2.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.schema_of_json(json, options={})</b>

Parses a JSON string and infers its schema in DDL format.

Parameters
- json – a JSON string or a string literal containing a JSON string.
- options – options to control parsing. accepts the same options as the JSON datasource
'''

<b>Input:  Spark dataframe containing JSON string column</b>

json_string = '{"me": {"name":"tony"},"myfamily_heirarchy":{"myfather":"mike","myfather_childs": ["me","kent"],"my_childs":{"name": "susan","susan_childs":{"name":"suzy"}}}}'
rdd = spark.sparkContext.parallelize([json_string])
df_json = spark.createDataFrame([(json_string,)],["jsonstring"])
df_json.show(1,False)

<b>Output :  Spark dataframe containing individual JSON elements </b>

from pyspark.sql.functions import schema_of_json,col
schema = schema_of_json(json_string,  {'allowUnquotedFieldNames':'true'})
schema = schema_of_json(json_string)
df_json.select(schema.alias("schema")).collect()[0][0]


schema

<a id='3'></a>

## 1c. How to extract elements from JSON column ?


```{figure} img/chapter11/3.png
---
align: center
---
```

<b>Input:  Spark dataframe containing JSON column</b>

json_string = '{"me": {"name":"tony"},"myfamily_heirarchy":{"myfather":"mike","myfather_childs": ["me","John"],"my_childs":{"name": "susan","susan_childs":{"name":"suzy"}}}}'
rdd = spark.sparkContext.parallelize([json_string])
df_json = spark.read.json(rdd)
df_json.show(1,False)
df_json.printSchema()

<b>Output :  Spark dataframe containing individual JSON elements </b>

from pyspark.sql.functions import col
df_json_ele = df_json.select("me.name","myfamily_heirarchy.myfather",col("myfamily_heirarchy.myfather_childs").getItem(1).alias("mybrother"), 
               col("myfamily_heirarchy.my_childs.name").alias("my daughter"),
               col("myfamily_heirarchy.my_childs.susan_childs.name").alias("my grand daughter"))
df_json_ele.toPandas()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_json.toPandas(),df_json_ele.toPandas())

<a id='4'></a>

## 1d. How to convert a data frame to JSON string?


```{figure} img/chapter11/4.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>toJSON(use_unicode=True)</b>

Converts a DataFrame into a RDD of string.
Each row is turned into a JSON document as one element in the returned RDD.

'''

<b>Input:  Spark data frame  </b>

df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_mul.show()

<b>Output :  Spark data frame with a struct column with a new element added </b>

df_mul.toJSON(use_unicode=True).collect()

df_mul.repartition(1).write.json("/Users/deepak/Documents/json1.json",mode="overwrite")

<a id='5'></a>

## 1e. How to convert JSON string column into StructType column?

Explanation here.

```{figure} img/chapter11/5.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.from_json(col, schema, options={})</b>

Parses a column containing a JSON string into a MapType with StringType as keys type, StructType or ArrayType with the specified schema. Returns null, in the case of an unparseable string.


Parameters
- col – string column in json format
- schema – a StructType or ArrayType of StructType to use when parsing the json column.
options – options to control parsing. accepts the same options as the json datasource
'''

<b>Input:  Spark data frame with a JSON string column  </b>

from pyspark.sql.types import *
data = [('{"name": "tony","id":111}',)]
schema1 = StructType([StructField("name", StringType()), StructField("id", IntegerType())])
schema2 = "name string, id int"
df = spark.createDataFrame(data, ["jsonstring"])
df.show(1,False)
df.printSchema()

<b>Output :  Spark data frame with a struct column </b>

from pyspark.sql.functions import from_json
df_stru = df.select(from_json(df.jsonstring, schema1).alias("struct"))

df.select(from_json(df.jsonstring, schema2).alias("struct")).show()
df_stru.printSchema()

from pyspark.sql.functions import to_json
df_jsonstroing = df_stru.select(to_json(df_stru.struct))
df_jsonstroing.show(1, False)
df_stru.select(to_json(df_stru.struct)).dtypes

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df.toPandas(),df.select(from_json(df.jsonstring, schema1).alias("struct")).toPandas())

<a id='6'></a>

## 1f. How to convert JSON string column into MapType column?

Explanation here.

```{figure} img/chapter11/6.png
---
align: center
---
```

<b>Input:  Spark data frame with a JSON string column </b>

from pyspark.sql.types import *
data = [('{"name": "tony","id":111}',)]
schema = StructType([StructField("name", StringType()), StructField("id", IntegerType())])
df = spark.createDataFrame(data, [ "jsonstring"])
df.show(1,False)
df.printSchema()

<b>Output :  Spark dataframe with a MapType column</b>

df_map = df.select(from_json(df.jsonstring, "MAP<string,string>").alias("MapType"))
df_map.show(1,False)
df_map.printSchema()

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_map.toPandas())
print(df.dtypes,df_map.dtypes)                    




<a id='7'></a>

## 1g. How to convert JSON string column into ArrayType column?

Explanation here.

```{figure} img/chapter1/7.png
---
align: center
---
```

<b>Input:  Spark data frame with a JSON string column </b>

from pyspark.sql.types import *
data = [('[{"name": "tony","id":111}]',)]
schema = StructType([StructField("name", StringType()), StructField("id", IntegerType())])
df = spark.createDataFrame(data, [ "jsonstring"])
df.show(1,False)
df.printSchema()

<b>Output :  Spark dataframe with a ArrayType column</b>

schema = ArrayType(StructType([StructField("name", StringType()), StructField("id", IntegerType())]))
df_arr = df.select(from_json(df.jsonstring, schema).alias("ArrayType"))
df_arr.show(1,False)
df_arr.printSchema()

schema = ArrayType(StructType([StructField("name", StringType()), StructField("id", IntegerType())]))
df_arr = df.select(from_json(df.jsonstring, schema).alias("ArrayType"))
df_arr.show(1,False)
print(df_arr.printSchema())

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df.toPandas(),df_arr.toPandas())
print(df.dtypes,df_arr.dtypes)                    


<a id='8'></a>

## 1h. How to convert StructType/MapType/ArrayType  column into JSON string?

```{figure} img/chapter11/8.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.to_json(col, options={})</b>

Converts a column containing a StructType, ArrayType or a MapType into a JSON string. Throws an exception, in the case of an unsupported type.

Parameters
- col – name of column containing a struct, an array or a map.
- options – options to control converting. accepts the same options as the JSON datasource. Additionally the function supports the pretty option which enables pretty JSON generation.
'''

<b>Input :  Spark data frame with a struct column </b>

data = [([{"name": "Alice"}, {"name": "Bob"}],)]
df = spark.createDataFrame(data, ["ArrayofMap"])
df.show(1,False)
df.printSchema()

from pyspark.sql.functions import to_json
df.select(to_json(df.ArrayofMap).alias("json")).show(1,False)
df.select(to_json(df.ArrayofMap).alias("json")).printSchema()

<b> Summary:</b>

print("input                     ",            "output")
display_side_by_side(df.toPandas(),df.select(to_json(df.ArrayofMap).alias("json")).toPandas())

<a id='9'></a>

## 1h. How to extract JSON object from a JSON  string column?

Explanation here.

```{figure} img/chapter11/9.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.get_json_object(col, path)</b>

Extracts json object from a json string based on json path specified, and returns json string of the extracted json object. It will return null if the input json string is invalid.

Parameters
- col – string column in json format
- path – path to the json object to extract
'''

<b>Input:  Spark data frame with a JSON string column </b>

json_string = '{"me": {"name":"tony"},"myfamily_heirarchy":{"myfather":"mike","myfather_childs": ["me","John"],"my_childs":{"name": "susan","susan_childs":{"name":"suzy"}}}}'
df_json = spark.createDataFrame([(json_string,)],["jsonstring"])
df_json.show(1,False)

<b>Output :  Spark dataframe with column containing a JSON object</b>

from pyspark.sql.functions import get_json_object
df_obj = df_json.select(get_json_object(df_json.jsonstring  , "$.myfamily_heirarchy.my_childs.susan_childs.name").alias("jsonobject"))
df_obj.show(1,False)
df_obj.dtypes

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_json.toPandas(),df_obj.toPandas())
                

<a id='10'></a>

## 1i. How to extract JSON objects based on list of field names?

Explanation here.

```{figure} img/chapter11/10.png
---
align: center
---
```

Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.functions.json_tuple(col, *fields)</b>

Creates a new row for a json column according to the given field names.

Parameters

- col – string column in json format
- fields – list of fields to extract
'''

<b>Input:  Spark data frame with a JSON string column </b>

json_string = '{"me": {"name":"tony"},"myfamily_heirarchy":{"myfather":"mike","myfather_childs": ["me","John"],"my_childs":{"name": "susan","susan_childs":{"name":"suzy"}}}}'
df_json = spark.createDataFrame([(json_string,)],["jsonstring"])
df_json.show(1,False)

<b>Output :  Spark dataframe with a MapType column</b>

from pyspark.sql.functions import json_tuple
df_out = df_json.select(json_tuple(df_json.jsonstring, 'me', 'myfamily_heirarchy',))
df_out.show(1,False)

<b> Summary:</b>

print("Input                     ",            "Output")
display_side_by_side(df_json.toPandas(),df_out.toPandas())

<a id='9'></a>

<a id='9'></a>