```{figure} ../images/banner.png
---
align: center
name: banner
---
```

# Chapter 4 : Data Types & Schema

## Chapter Learning Objectives

- Various Data types in Spark. 
- Use of Schema. 

## Chapter Outline

- [1. Various data types ](#1)
    - [1a. How to get the data type of a column/data frame?](#2)
    - [1b. How to change the data type of a column?](#3)
- [2. Various Schema operations](#4)
    - [2a. How to get the schema of a data frame?](#5)
    - [2b. How to define a schema?](#6)
    - [2c. How to use the schema?](#7)
    - [2d. How to save the schema?](#8)
    - [2e. How to load the saved schema?](#9)
    - [2f. How to get the names of all fields in the schema?](#10)

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
def display_side_by_side(*args,space):
    html_str=''
    for df in args:
        html_str+=df.to_html(index=False)
        html_str+= "\xa0\xa0\xa0"*space
    display_html(html_str.replace('table','table style="display:inline"'),raw=True)
space = "\xa0" * 10

<a id='1'></a>

### Common data types in spark




<b>Numeric types</b>

| Name          | Description  | Example |                               
| :-------------------- | :--------- | :------------- | 
| IntegerType              | Represents 4-byte signed integer numbers.        |64   |                      
| LongType |Represents 8-byte signed integer numbers   | 1000  |                 |                                
| FloatType              |Represents 4-byte single-precision floating point numbers.    | 1000.45 |               
| DoubleType              |Represents 8-byte double-precision floating point numbers.    | 10000000.45 |    
                        
                        
<b>String type</b>

| Name          | Description  | Example |                               
| :-------------------- | :--------- | :------------- |
| StringType              |Represents character string values.       |"tony"   |                      

<b>Boolean type</b>

| Name          | Description  | Example |                               
| :-------------------- | :--------- | :------------- |
| BooleanType              |Represents boolean values.        |"true"   | 

<b>Datetime type</b>

| Name          | Description  | Example |                               
| :-------------------- | :--------- | :------------- |
| TimestampType             |Represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time       |2019-11-03 05:30:00 UTC-05:00   | 
| DateType             |Represents values comprising values of fields year, month and day, without a time-zone.       |2019-11-03  | 


<b>Complex types</b>

| Name          | Definition  | Description | Example |                               
| :-------------------- | :--------- | :------------- |:------------- |
| ArrayType             |ArrayType(elementType, containsNull)       |Represents values comprising a sequence of elements with the type of elementType. containsNull is used to indicate if elements in a ArrayType value can have null values.  | ["tony","kent", "mike"]   |
| MapType              |MapType(keyType, valueType, valueContainsNull):      | Represents values comprising a set of key-value pairs. The data type of keys is described by keyType and the data type of values is described by valueType. For a MapType value, keys are not allowed to have null values. valueContainsNull is used to indicate if values of a MapType value can have null values.   |  {"name":"tony"}  |
| StructType              |StructType(fields)       |Represents values with the structure described by a sequence of StructFields (fields).StructField(name, dataType, nullable): Represents a field in a StructType. The name of a field is indicated by name. The data type of a field is indicated by dataType. nullable is used to indicate if values of these fields can have null values.   |   {"name":"tony","age":30,"city":""seattle"}    |

<a id='2'></a>

## 1a.  How to get the data type of a column/data frame?



Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.DataFrame.dtypes</b>

Returns all column names and their data types as a list.

'''

<b>Input:  Spark data frame</b>

df_mul = spark.createDataFrame([('John',  60, True, 1.7, '1960-01-01'), 
('Tony', 30, False, 1.8, '1990-01-01'), 
('Mike',  40, True, 1.65, '1980-01-01')],['name',  'age', 'smoker','height', 'birthdate'])
df_mul.show()

<b>Output :  Spark data frame column types</b>

df_mul.dtypes

<b> Summary:</b>

print("Input                                        ",            "Output")
display_side_by_side(df_mul.toPandas(),pd.DataFrame([str(df_mul.dtypes[0:6])],columns=["."]),space=1)

<a id='3'></a>

## 1b. How to change the data type of a column?



Lets first understand the syntax

```{admonition} Syntax
<b>pyspark.sql.Column.cast </b>

Convert the column into type dataType.


'''

<b>Input:  Spark data frame with a column "age" of integer type</b>

df_mul = spark.createDataFrame([('John',  60, True, 1.7), 
('Tony', 30, False, 1.8, ), 
('Mike',  40, True, 1.65, )],['name',  'age', 'smoker','height'])
df_mul.show()

df_mul.dtypes

<b>Output :  Spark data frame with a column with a split string</b>

df_cast = df_mul.select("name",df_mul.age.cast("string").alias('age'), "smoker", "height")
df_cast.show()

df_cast.dtypes

<b> Summary:</b>

print("Input                                                       ",            "Output")
display_side_by_side(df_mul.toPandas(),df_cast.toPandas(),space=25)
print("Data types                                                      ",            "Data types")
display_side_by_side(pd.DataFrame([str(df_mul.dtypes)],columns=["."]),pd.DataFrame([str(df_cast.dtypes)],columns=["."]),space=5)

<a id='4'></a>

## what is  Schema ?
A schema is the description of the structure of your data 

<a id='5'></a>

## 2a. How to get the schema of a data frame?


Lets first understand the syntax

Converts a string expression to upper case.

```{admonition} Syntax
<b>pyspark.sql.DataFrame.schema</b>
Returns the schema of this DataFrame as a pyspark.sql.types.StructType.

'''

<b>Input:  Spark data frame </b>

df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_mul.show()

<b>Output :  Schema of Spark data frame</b>

df_mul.schema

### How to print the schema in a tree format?

df_mul.printSchema()

<a id='6'></a>

## 2b. How to define a schema?

Lets first understand the syntax

```{admonition} Syntax

<b>pyspark.sql.functions.slice(x, start, length)</b>

Collection function: returns an array containing all the elements in x from index start (array indices start at 1, or from the end if start is negative) with the specified length.

<b>Parameters</b>:

- x : the array to be sliced

- start : the starting index

- length : the length of the slice
'''

#### Example#1

from pyspark.sql.types import *

schema1 = StructType([
StructField("name", StringType(), True), 
StructField("city", StringType(), True), 
StructField("age", IntegerType(), True), 
StructField("smoker", BooleanType(), True), 
StructField("height", FloatType(), True), 
StructField("birthdate", StringType(), True), 
])
schema1

schema1.fieldNames()

#### Example#2

schema2 = StructType([
    StructField("name", StringType()),
    StructField("weight", LongType()),
    StructField("smoker", BooleanType()),
    StructField("height", DoubleType()),
    StructField("birthdate", StringType()),
    StructField("phone_nos", MapType(StringType(),LongType(),True),True),  
    StructField("favorite_colors", ArrayType(StringType(),True),True),  
    StructField("address", StructType([
    StructField("houseno", IntegerType(),True),
    StructField("street", StringType(),True),
    StructField("city", StringType(),True),
    StructField("zipcode", IntegerType(),True),
    ])) 
    
])
print(schema2)


schema2.fieldNames()


<a id='7'></a>

##  2c. How to use the schema?

from pyspark.sql.types import *
from pyspark.sql import functions as func
schema = StructType([
    StructField("name", StringType()),
    StructField("weight", LongType()),
    StructField("smoker", BooleanType()),
    StructField("height", DoubleType()),
    StructField("birthdate", StringType()),
    StructField("phone_nos", MapType(StringType(),LongType(),True),True),  
    StructField("favorite_colors", ArrayType(StringType(),True),True),  
    StructField("address", StructType([
    StructField("houseno", IntegerType(),True),
    StructField("street", StringType(),True),
    StructField("city", StringType(),True),
    StructField("zipcode", IntegerType(),True),
    ])) 
    
])

df = spark.createDataFrame((
    [["john",180,True,1.7,'1960-01-01',{'office': 123456789, 'home': 223456789},["blue","red"],(100,'street1','city1',12345)],
    ["tony",180,True,1.8,'1990-01-01',{'office': 223456789, 'home': 323456789},["green","purple"],(200,'street2','city2',22345)],
    ["mike",180,True,1.65,'1980-01-01',{'office': 323456789, 'home': 423456789},["yellow","orange"],(300,'street3','city3',32345)]]
),schema=schema)
df.toPandas()#(3,False)
df.printSchema()
df.toPandas()

<a id='8'></a>

## 2d. How to save the schema?

df = spark.createDataFrame((
    [["john",180,True,1.7,'1960-01-01',{'office': 123456789, 'home': 223456789},["blue","red"],(100,'street1','city1',12345)],
    ["tony",180,True,1.8,'1990-01-01',{'office': 223456789, 'home': 323456789},["green","purple"],(200,'street2','city2',22345)],
    ["mike",180,True,1.65,'1980-01-01',{'office': 323456789, 'home': 423456789},["yellow","orange"],(300,'street3','city3',32345)]]
),schema=schema)
df.printSchema()

df.schema

spark.conf.set("spark.hadoop.validateOutputSpecs", "false")
rdd_schema = spark.sparkContext.parallelize(df.schema)
#rdd_schema.coalesce(1).saveAsPickleFile("data/text/schema_file")

<a id='9'></a>

## 2e. How to load the saved schema?

schema_rdd = spark.sparkContext.pickleFile("data/text/schema_file")
schema = StructType(schema_rdd.collect())
print(schema)    

<a id='10'></a>

## 2f. How to get the names of all fields in the schema?

df_mul = spark.createDataFrame([('John', 'Seattle', 60, True, 1.7, '1960-01-01'), 
('Tony', 'Cupertino', 30, False, 1.8, '1990-01-01'), 
('Mike', 'New York', 40, True, 1.65, '1980-01-01')],['name', 'city', 'age', 'smoker','height', 'birthdate'])
df_mul.show()

df_mul.schema.fieldNames()

<b> Summary:</b>

print("input                                    ",            "output")
display_side_by_side(df_mul.toPandas(),pd.DataFrame([[df_mul.schema.fieldNames()]],columns=["."]),space=2)

<style> 
table td, table th, table tr {text-align:left !important;}
</style>

<a id='6'></a>