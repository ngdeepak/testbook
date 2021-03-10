# Creating Spark Data Frame

### What is a Spark Data frame?

Spark Data Frame is a distributed collection of data organized into named columns. It can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDD, Lists, Pandas data frame, json files, CSV files, parquet files etc

```{figure} /images/dataframe.png
---
width: 400px
height: 250px
align: center
---
Spark DataFrame
```

# Creating a Spark DataFrame
 - from list
 - from dict


# Creating a Spark DataFrame123


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
df = spark.createDataFrame([(1,2),(5,6)],["col1","col2"])
df.show()

df2 = spark.createDataFrame([(1,2),(7,6)],["col1","col2"])

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
df1 = pd.DataFrame(np.arange(12).reshape((3,4)),columns=['A','B','C','D',])
df2 = pd.DataFrame(np.arange(16).reshape((4,4)),columns=['A','B','C','D',])
print("input                     ",            "output")
display_side_by_side(df1,df2)

display(df.show())
display(df2.show())

