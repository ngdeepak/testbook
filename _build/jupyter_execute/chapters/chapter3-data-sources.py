```{figure} ../images/banner.png
---
align: center
name: banner
---
```

<style>body {text-align: justify}</style>

# Chapter 3 : Data Sources

## Chapter Learning Objectives

- Various data sources & file formats.
- General methods to read the data into spark dataframe. 
- General methods to write the spark dataframe into an external data source/file. 

## Chapter Outline

- [1. Various data sources](#1)
    - [1a. Text file](#2)
    - [1b. CSV file](#3)
    - [1c. JSON file](#4)
    - [1d. Parquet file](#5)
    - [1e. ORC file](#6)
    - [1f. AVRO file](#7)
    - [1g. Whole Binary file](#8)
- [2. Reading & Writing data from various data sources](#9)
    - [2a. from text file](#10)
    - [2b. from CSV file](#11)
    - [2c. from JSON file](#12)
    - [2d. from Parquet file](#13)
    - [2e. from ORC file](#14)
    - [2f. from AVRO file](#15)
    - [2g. from whole Binary file](#16)


## Visual Outline


```{figure} img/chapter3/datasources.png
---
align: center
---
```

click on  | any image
---: |:--- 
[![alt](img/chapter3/1a.png)](#2)| [![alt](img/chapter3/1b.png)](#3)
[![alt](img/chapter3/1c.png)](#4) | [![alt](img/chapter3/1d.png)](#5)
[![alt](img/chapter3/1e.png)](#6) | [![alt](img/chapter3/1f.png)](#7)
[![alt](img/chapter3/1g.png)](#8) | 


<a id='1'></a>

## 1. Various data sources

As a general computing engine, Spark can process data from various data management/storage systems, including HDFS, Hive, Cassandra and Kafka. For flexibility and high throughput, Spark defines the Data Source API, which is an abstraction of the storage layer. The Data Source API has two requirements.

1) Generality: support reading/writing most data management/storage systems.

2) Flexibility: customize and optimize the read and write paths for different systems based on their capabilities. 


```{figure} img/chapter3/datasources.png
---
align: center
---
```

<a id='2'></a>

### 1a. Text file

A text file is a kind of computer file that is structured as a sequence of lines of electronic text.
Because of their simplicity, text files are commonly used for storage of information. They avoid some of the problems encountered with other file formats, such as  padding bytes, or differences in the number of bytes in a machine word. Further, when data corruption occurs in a text file, it is often easier to recover and continue processing the remaining contents. 
A disadvantage of text files is that they usually have a low entropy, meaning that the information occupies more storage than is strictly necessary.

A simple text file may need no additional metadata (other than knowledge of its character set) to assist the reader in interpretation. 

<a id='3'></a>

### 1b. CSV file

A comma-separated values (CSV) file is a delimited text file that uses a comma to separate values. Each line of the file is a data record. Each record consists of one or more fields, separated by commas. The use of the comma as a field separator is the source of the name for this file format. A CSV file typically stores tabular data (numbers and text) in plain text, in which case each line will have the same number of fields.

<a id='4'></a>

### 1c.JSON file

JSON (JavaScript Object Notation) is an open standard file format, and data interchange format, that uses human-readable text to store and transmit data objects consisting of attribute–value pairs and array data types (or any other serializable value.

<a id='5'></a>

### 1d. Parquet file

Apache Parquet is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in Hadoop namely RCFile and ORC. It is compatible with most of the data processing frameworks in the Hadoop environment. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk.


Parquet uses the record shredding and assembly algorithm which is superior to simple flattening of nested namespaces. Parquet is optimized to work with complex data in bulk and features different ways for efficient data compression and encoding types.  This approach is best especially for those queries that need to read certain columns from a large table. Parquet can only read the needed columns therefore greatly minimizing the IO.

Advantages of Storing Data in a Columnar Format:

- Columnar storage like Apache Parquet is designed to bring efficiency compared to row-based files like CSV. When querying columnar storage you can skip over the non-relevant data very quickly. As a result, aggregation queries are less time consuming compared to row-oriented databases. This way of storage has translated into hardware savings and minimized latency for accessing data.
- Apache Parquet is built from the ground up. Hence it is able to support advanced nested data structures. The layout of Parquet data files is optimized for queries that process large volumes of data, in the gigabyte range for each individual file.
- Parquet is built to support flexible compression options and efficient encoding schemes. As the data type for each column is quite similar, the compression of each column is straightforward (which makes queries even faster). Data can be compressed by using one of the several codecs available; as a result, different data files can be compressed differently.

The values in each column are physically stored in contiguous memory locations and this columnar storage provides the following benefits:

- Column-wise compression is efficient and saves storage space
- Compression techniques specific to a type can be applied as the column values tend to be of the same type
- Queries that fetch specific column values need not read the entire row data thus improving performance
- Different encoding techniques can be applied to different columns


<a id='6'></a>

### 1e. ORC file

Apache ORC (Optimized Row Columnar) is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in the Hadoop ecosystem such as RCFile and Parquet. It is compatible with most of the data processing frameworks in the Hadoop environment.

ORC is a self-describing type-aware columnar file format designed for Hadoop workloads. It is optimized for large streaming reads, but with integrated support for finding required rows quickly. Storing data in a columnar format lets the reader read, decompress, and process only the values that are required for the current query. Because ORC files are type-aware, the writer chooses the most appropriate encoding for the type and builds an internal index as the file is written. Predicate pushdown uses those indexes to determine which stripes in a file need to be read for a particular query and the row indexes can narrow the search to a particular set of 10,000 rows. ORC supports the complete set of types in Hive, including the complex types: structs, lists, maps, and unions.

The ORC file format provides the following advantages:

- Efficient compression: Stored as columns and compressed, which leads to smaller disk reads. The columnar format is also ideal for vectorization optimizations in Tez.
- Fast reads: ORC has a built-in index, min/max values, and other aggregates that cause entire stripes to be skipped during reads. In addition, predicate pushdown pushes filters into reads so that minimal rows are read. And Bloom filters further reduce the number of rows that are returned.
- Proven in large-scale deployments: Facebook uses the ORC file format for a 300+ PB deployment.

<a id='7'></a>

### 1e. AVRO file

Avro is a row-oriented remote procedure call and data serialization framework developed within Apache's Hadoop project. 

It uses JSON for defining data types and protocols, and serializes data in a compact binary format. Its primary use is in Apache Hadoop, where it can provide both a serialization format for persistent data, and a wire format for communication between Hadoop nodes, and from client programs to the Hadoop services. 

Avro uses a schema to structure the data that is being encoded. It has two different types of schema languages; one for human editing (Avro IDL) and another which is more machine-readable based on JSON.

The Avro data source supports:

- Schema conversion: Automatic conversion between Apache Spark SQL and Avro records.
- Partitioning: Easily reading and writing partitioned data without any extra configuration.
- Compression: Compression to use when writing Avro out to disk. The supported types are uncompressed, snappy, and deflate. 
- Record names: Record name and namespace by passing a map of parameters with recordName and recordNamespace.

<a id='8'></a>

### 1f. Whole Binary file

A binary file is a computer file that is not a text file.

Binary files are usually thought of as being a sequence of bytes, which means the binary digits (bits) are grouped in eights. 

Binary files typically contain bytes that are intended to be interpreted as something other than text characters. Compiled computer programs are typical examples; indeed, compiled applications are sometimes referred to, particularly by programmers, as binaries. But binary files can also mean that they contain images, sounds, compressed versions of other files, etc. – in short, any type of file content whatsoever

#./bin/spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.2 ...
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.jars.packages', 'org.apache.spark:spark-avro_2.12:3.0.2')\
    .getOrCreate()
spark.createDataFrame([("John",180,True, 1.7, "1960-01-01", '{“home”: 123456789, “office”:234567567}'),]).show(1,False)        

<a id='9'></a>

## 2. Reading & Writing data from various data sources

<a id='10'></a>

### 2a. Reading from text file into spark data frame

```{admonition} Syntax
<b>text(paths, wholetext=False, lineSep=None, pathGlobFilter=None, recursiveFileLookup=None)</b>
Loads text files and returns a DataFrame whose schema starts with a string column named “value”, and followed by partitioned columns if there are any. The text files must be encoded as UTF-8.
By default, each line in the text file is a new row in the resulting DataFrame.

<b>Parameters</b>:

- paths – string, or list of strings, for input path(s).
- wholetext – if true, read each file from input path(s) as a single row.
- lineSep – defines the line separator that should be used for parsing. 
   If None is set, it covers all \r, \r\n and \n.
- pathGlobFilter – an optional glob pattern to only include files with paths matching the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
- recursiveFileLookup – recursively scan a directory for files. Using this option disables partition discovery.


```

<b>Input: Text File</b>

```{figure} img/chapter3/2a.png
---
align: center
---
```

<b>Input: Spark data frame</b>

df_text = spark.read.text("data/text/sample.txt")
df_text.show(2,False)

#### Saving the spark data frame content into a text file

```{admonition} Syntax
<b>text(path, compression=None, lineSep=None)</b>
Saves the content of the DataFrame in a text file at the specified path. The text files will be encoded as UTF-8.
The DataFrame must have only one column that is of string type. Each row becomes a new line in the output file.

<b>Parameters</b>:

- path – the path in any Hadoop supported file system
- compression – compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
- lineSep – defines the line separator that should be used for writing. 
   If None is set, it uses the default value, \n.

```

df_text.write.mode("overwrite").text("data/text/sample_write")

To verify the save, let us read the file into data frame


spark.read.text("data/text/sample_write").show(2,False)

<a id='11'></a>

### 2b. Reading from CSV file into spark data frame

```{admonition} Syntax
<b>csv(path, schema=None, sep=None, encoding=None, quote=None, escape=None, comment=None, header=None, inferSchema=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, nullValue=None, nanValue=None, positiveInf=None, negativeInf=None, dateFormat=None, timestampFormat=None, maxColumns=None, maxCharsPerColumn=None, maxMalformedLogPerPartition=None, mode=None, columnNameOfCorruptRecord=None, multiLine=None, charToEscapeQuoteEscaping=None, samplingRatio=None, enforceSchema=None, emptyValue=None, locale=None, lineSep=None, pathGlobFilter=None, recursiveFileLookup=None)</b>

Loads a CSV file and returns the result as a DataFrame.

This function will go through the input once to determine the input schema if inferSchema is enabled. To avoid going through the entire data once, disable inferSchema option or specify the schema explicitly using schema.

<b>Parameters</b>:
- path – string, or list of strings, for input path(s), or RDD of Strings storing CSV rows.
- schema – an optional pyspark.sql.types.StructType for the input schema or a DDL-formatted string (For example col0 INT, col1 DOUBLE).
- sep – sets a separator (one or more characters) for each field and value. If None is set, it uses the default value, ,.
- encoding – decodes the CSV files by the given encoding type. If None is set, it uses the default value, UTF-8.
- quote – sets a single character used for escaping quoted values where the separator can be part of the value. If None is set, it uses the default value, ". If you would like to turn off quotations, you need to set an empty string.
- escape – sets a single character used for escaping quotes inside an already quoted value. If None is set, it uses the default value, \.
- comment – sets a single character used for skipping lines beginning with this character. By default (None), it is disabled.
- header – uses the first line as names of columns. If None is set, it uses the default value, false. .. note:: if the given path is a RDD of Strings, this header option will remove all lines same with the header if exists.
- inferSchema – infers the input schema automatically from data. It requires one extra pass over the data. If None is set, it uses the default value, false.
- enforceSchema – If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files or the first header in RDD if the header option is set to true. Field names in the schema and column names in CSV headers are checked by their positions taking into account spark.sql.caseSensitive. If None is set, true is used by default. Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results.
- ignoreLeadingWhiteSpace – A flag indicating whether or not leading whitespaces from values being read should be skipped. If None is set, it uses the default value, false.
- ignoreTrailingWhiteSpace – A flag indicating whether or not trailing whitespaces from values being read should be skipped. If None is set, it uses the default value, false.
- nullValue – sets the string representation of a null value. If None is set, it uses the default value, empty string. Since 2.0.1, this nullValue param applies to all supported types including the string type.
- nanValue – sets the string representation of a non-number value. If None is set, it uses the default value, NaN.
- positiveInf – sets the string representation of a positive infinity value. If None is set, it uses the default value, Inf.
- negativeInf – sets the string representation of a negative infinity value. If None is set, it uses the default value, Inf.
- dateFormat – sets the string that indicates a date format. Custom date formats follow the formats at datetime pattern. This applies to date type. If None is set, it uses the default value, yyyy-MM-dd.
timestampFormat – sets the string that indicates a timestamp format. Custom date formats follow the formats at datetime pattern. This applies to timestamp type. If None is set, it uses the default value, yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX].
- maxColumns – defines a hard limit of how many columns a record can have. If None is set, it uses the default value, 20480.
- maxCharsPerColumn – defines the maximum number of characters allowed for any given value being read. If None is set, it uses the default value, -1 meaning unlimited length.
- maxMalformedLogPerPartition – this parameter is no longer used since Spark 2.2.0. If specified, it is ignored.
- mode –
allows a mode for dealing with corrupt records during parsing. If None is
set, it uses the default value, PERMISSIVE. Note that Spark tries to parse only required columns in CSV under column pruning. Therefore, corrupt records can be different based on required set of fields. This behavior can be controlled by spark.sql.csv.parser.columnPruning.enabled (enabled by default).
PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. A record with less/more tokens than schema is not a corrupted record to CSV. When it meets a record having fewer tokens than the length of the schema, sets null to extra fields. When the record has more tokens than the length of the schema, it drops extra tokens.
DROPMALFORMED: ignores the whole corrupted records.
FAILFAST: throws an exception when it meets corrupted records.
columnNameOfCorruptRecord – allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord. If None is set, it uses the value specified in spark.sql.columnNameOfCorruptRecord.
- multiLine – parse records, which may span multiple lines. If None is set, it uses the default value, false.
charToEscapeQuoteEscaping – sets a single character used for escaping the escape for the quote character. If None is set, the default value is escape character when escape and quote characters are different, \0 otherwise.
- samplingRatio – defines fraction of rows used for schema inferring. If None is set, it uses the default value, 1.0.
- emptyValue – sets the string representation of an empty value. If None is set, it uses the default value, empty string.
- locale – sets a locale as language tag in IETF BCP 47 format. If None is set, it uses the default value, en-US. For instance, locale is used while parsing dates and timestamps.
- lineSep – defines the line separator that should be used for parsing. If None is set, it covers all \\r, \\r\\n and \\n. Maximum length is 1 character.
- pathGlobFilter – an optional glob pattern to only include files with paths matching the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
- recursiveFileLookup – recursively scan a directory for files. Using this option disables partition discovery.

```


df_csv = spark.read.csv('/Users/deepak/Documents/sparkbook/chapters/data/people.csv', header=True)
df_csv.show(3,False)

#### Saving the spark data frame content into a CSV file

```{admonition} Syntax
<b>csv(path, mode=None, compression=None, sep=None, quote=None, escape=None, header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, timestampFormat=None, ignoreLeadingWhiteSpace=None, ignoreTrailingWhiteSpace=None, charToEscapeQuoteEscaping=None, encoding=None, emptyValue=None, lineSep=None)</b>
Saves the content of the DataFrame in CSV format at the specified path.

<b>Parameters</b>:
- path – the path in any Hadoop supported file system
- mode –
   specifies the behavior of the save operation when data already exists.
    append: Append contents of this DataFrame to existing data.
    overwrite: Overwrite existing data.
    ignore: Silently ignore this operation if data already exists.
    error or errorifexists (default case): Throw an exception if data already
    exists.
- compression – compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
- sep – sets a separator (one or more characters) for each field and value. If None is set, it uses the default value, ,.
- quote – sets a single character used for escaping quoted values where the separator can be part of the value. If None is set, it uses the default value, ". If an empty string is set, it uses u0000 (null character).
- escape – sets a single character used for escaping quotes inside an already quoted value. If None is set, it uses the default value, \
- escapeQuotes – a flag indicating whether values containing quotes should always be enclosed in quotes. If None is set, it uses the default value true, escaping all values containing a quote character.
- quoteAll – a flag indicating whether all values should always be enclosed in quotes. If None is set, it uses the default value false, only escaping values containing a quote character.
- header – writes the names of columns as the first line. If None is set, it uses the default value, false.
- nullValue – sets the string representation of a null value. If None is set, it uses the default value, empty string.
- dateFormat – sets the string that indicates a date format. Custom date formats follow the formats at datetime pattern. This applies to date type. If None is set, it uses the default value, yyyy-MM-dd.
- timestampFormat – sets the string that indicates a timestamp format. Custom date formats follow the formats at datetime pattern. This applies to timestamp type. If None is set, it uses the default value, yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX].
- ignoreLeadingWhiteSpace – a flag indicating whether or not leading whitespaces from values being written should be skipped. If None is set, it uses the default value, true.
- ignoreTrailingWhiteSpace – a flag indicating whether or not trailing whitespaces from values being written should be skipped. If None is set, it uses the default value, true.
- charToEscapeQuoteEscaping – sets a single character used for escaping the escape for the quote character. If None is set, the default value is escape character when escape and quote characters are different, \0 otherwise..
- encoding – sets the encoding (charset) of saved csv files. If None is set, the default UTF-8 charset will be used.
- emptyValue – sets the string representation of an empty value. If None is set, it uses the default value, "".
- lineSep – defines the line separator that should be used for writing. If None is set, it uses the default value, \\n. Maximum length is 1 character.


```


df_csv.write.csv("data/people_csv_write",mode='overwrite',header=True)

spark.read.csv("data/people_csv_write",header=True).show()

<a id='12'></a>

### 2c. Reading from JSON file into spark data frame

```{admonition} Syntax
<b>json(path, schema=None, primitivesAsString=None, prefersDecimal=None, allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None, allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None, mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None, multiLine=None, allowUnquotedControlChars=None, lineSep=None, samplingRatio=None, dropFieldIfAllNull=None, encoding=None, locale=None, pathGlobFilter=None, recursiveFileLookup=None)
</b>
Loads JSON files and returns the results as a DataFrame.
JSON Lines (newline-delimited JSON) is supported by default. For JSON (one record per file), set the multiLine parameter to true.

<b>Parameters</b>:
- path – string represents path to the JSON dataset, or a list of paths, or RDD of Strings storing JSON objects.
- schema – an optional pyspark.sql.types.StructType for the input schema or a DDL-formatted string (For example col0 INT, col1 DOUBLE).
- primitivesAsString – infers all primitive values as a string type. If None is set, it uses the default value, false.
- prefersDecimal – infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles. If None is set, it uses the default value, false.
- allowComments – ignores Java/C++ style comment in JSON records. If None is set, it uses the default value, false.
- allowUnquotedFieldNames – allows unquoted JSON field names. If None is set, it uses the default value, false.
- allowSingleQuotes – allows single quotes in addition to double quotes. If None is set, it uses the default value, true.
- allowNumericLeadingZero – allows leading zeros in numbers (e.g. 00012). If None is set, it uses the default value, false.
- allowBackslashEscapingAnyCharacter – allows accepting quoting of all character using backslash quoting mechanism. If None is set, it uses the default value, false.
- mode –
    allows a mode for dealing with corrupt records during parsing. If None is
    set, it uses the default value, PERMISSIVE.
    PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema.
    DROPMALFORMED: ignores the whole corrupted records.
    FAILFAST: throws an exception when it meets corrupted records.
- columnNameOfCorruptRecord – allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord. If None is set, it uses the value specified in spark.sql.columnNameOfCorruptRecord.
- dateFormat – sets the string that indicates a date format. Custom date formats follow the formats at datetime pattern. This applies to date type. If None is set, it uses the default value, yyyy-MM-dd.
- timestampFormat – sets the string that indicates a timestamp format. Custom date formats follow the formats at datetime pattern. This applies to timestamp type. If None is set, it uses the default value, yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX].
- multiLine – parse one record, which may span multiple lines, per file. If None is set, it uses the default value, false.
- allowUnquotedControlChars – allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.
encoding – allows to forcibly set one of standard basic or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. If None is set, the encoding of input JSON will be detected automatically when the multiLine option is set to true.


```


jsonStrings = ['{"name":"Yin","age":45,"smoker": true,"test":34, "address":{"city":"Columbus","state":"Ohio"},"favorite_colors": ["blue","green"] }',]
otherPeopleRDD = spark.sparkContext.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)
df_json = spark.read.format("json").load("data/json")
df_json.show(3,False)

#### Saving the spark data frame content into a JSON  file

```{admonition} Syntax
<b>json(path, mode=None, compression=None, dateFormat=None, timestampFormat=None, lineSep=None, encoding=None, ignoreNullFields=None)</b>
Saves the content of the DataFrame in JSON format (JSON Lines text format or newline-delimited JSON) at the specified path.

<b>Parameters</b>:

- path – the path in any Hadoop supported file system
- mode –
   specifies the behavior of the save operation when data already exists.
    append: Append contents of this DataFrame to existing data.
    overwrite: Overwrite existing data.
    ignore: Silently ignore this operation if data already exists.
    error or errorifexists (default case): Throw an exception if data already exists.
- compression – compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
- dateFormat – sets the string that indicates a date format. Custom date formats follow the formats at datetime pattern. This applies to date type. If None is set, it uses the default value, yyyy-MM-dd.
- timestampFormat – sets the string that indicates a timestamp format. Custom date formats follow the formats at datetime pattern. This applies to timestamp type. If None is set, it uses the default value, yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX].
- encoding – specifies encoding (charset) of saved json files. If None is set, the default UTF-8 charset will be used.
- lineSep – defines the line separator that should be used for writing. If None is set, it uses the default value, \n.
- ignoreNullFields – Whether to ignore null fields when generating JSON objects. If None is set, it uses the default value, true.

```

df_json.write.json("data/json_write",mode='overwrite')

spark.read.json("data/json_write").show()

<a id='13'></a>

### 2d. Reading from Parquet file into spark data frame

```{admonition} Syntax
<b>parquet(*paths, **options)</b>
Loads Parquet files, returning the result as a DataFrame.

<b>Parameters</b>:

- mergeSchema – sets whether we should merge schemas collected from all Parquet part-files. This will override spark.sql.parquet.mergeSchema. The default value is specified in spark.sql.parquet.mergeSchema.
- pathGlobFilter – an optional glob pattern to only include files with paths matching the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
- recursiveFileLookup – recursively scan a directory for files. Using this option disables partition discovery. None is set, it uses the default value, \n.

```

df_parquet = spark.read.parquet("data/parquetfile")
df_parquet.show(3,False)

#### Saving the spark data frame content into a Parquet  file

```{admonition} Syntax
<b>parquet(path, mode=None, partitionBy=None, compression=None)</b>
Saves the content of the DataFrame in Parquet format at the specified path.

<b>Parameters</b>:

- path – the path in any Hadoop supported file system
- mode –
  specifies the behavior of the save operation when data already exists.
    append: Append contents of this DataFrame to existing data.
    overwrite: Overwrite existing data.
    ignore: Silently ignore this operation if data already exists.
    error or errorifexists (default case): Throw an exception if data already exists.
- partitionBy – names of partitioning columns
- compression – compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, uncompressed, snappy, gzip, lzo, brotli, lz4, and zstd). This will override spark.sql.parquet.compression.codec. If None is set, it uses the value specified in spark.sql.parquet.compression.codec.
```

df_parquet.write.parquet("data/parquetfile_write",mode='overwrite')

spark.read.parquet("data/parquetfile_write").show(3,False)

<a id='14'></a>

### 2e. Reading from an ORC file into spark data frame

```{admonition} Syntax
<b>orc(path, mergeSchema=None, pathGlobFilter=None, recursiveFileLookup=None)</b>
Loads ORC files, returning the result as a DataFrame.

<b>Parameters</b>:

- mergeSchema – sets whether we should merge schemas collected from all ORC part-files. This will override spark.sql.orc.mergeSchema. The default value is specified in spark.sql.orc.mergeSchema.
- pathGlobFilter – an optional glob pattern to only include files with paths matching the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.
- recursiveFileLookup – recursively scan a directory for files. Using this option disables partition discovery.
```

df_orc = spark.read.orc("data/orcfile")
df_orc.show(3,False)

#### Saving the spark data frame content into an ORC  file

```{admonition} Syntax
<b>orc(path, mode=None, partitionBy=None, compression=None)</b>
Saves the content of the DataFrame in ORC format at the specified path.

<b>Parameters</b>:

- path – the path in any Hadoop supported file system
- mode –
   specifies the behavior of the save operation when data already exists.
    append: Append contents of this DataFrame to existing data.
    overwrite: Overwrite existing data.
    ignore: Silently ignore this operation if data already exists.
    error or errorifexists (default case): Throw an exception if data already exists.
- partitionBy – names of partitioning columns
- compression – compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, snappy, zlib, and lzo). This will override orc.compress and spark.sql.orc.compression.codec. If None is set, it uses the value specified in spark.sql.orc.compression.codec.
```

df_orc.write.orc("data/orcfile_write",mode='overwrite')

spark.read.orc("data/orcfile_write").show(3,False)


<a id='15'></a>

### 2f. Reading from an AVRO file into spark data frame

#!./bin/spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.2 ...
##.config('spark.jars', 'org.apache.spark:spark-avro_2.12:3.0.2')\
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config('spark.jars.packages', 'org.apache.spark:spark-avro_2.12:3.0.2')\
    .getOrCreate()
spark.createDataFrame([("John",180,True, 1.7, "1960-01-01", '{“home”: 123456789, “office”:234567567}'),]).show(1,False)        

df_avro = spark.read.format("avro").load("data/avrofile")
df_avro.show(3,False)

#### Saving the spark data frame content into an AVRO  file

df_avro.write.format("avro").save("data/avrofile_write",mode='overwrite')

spark.read.format("avro").load("data/avrofile_write").show(3,False)

<a id='16'></a>

### 2g. Reading from an Whole Binary file into spark data frame

spark.read.format("binaryFile").load("../images/banner.png").show()