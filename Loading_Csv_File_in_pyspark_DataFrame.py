#!/usr/bin/env python
# coding: utf-8


import findspark

# your path will likely not have 'andonshkurti' in it. Change it to reflect your path.
findspark.init('/home/andonshkurti/spark-2.4')

from pyspark.sql import SQLContext

df = sqlContext.read.csv('export.csv', header='true', inferSchema='true')

df.show()


df.printSchema()


df.columns


# Count total number of rows
df.count()


# Count the total number of columns
len(df.columns)


df.describe('color').show()


# Show values from specific columns
df.select('cut','price').show()


# Filter columns and return the rows that meet a condition
df.filter((df.price=='326') & (df.z=='2.43')).show()


# Count rows that meet the condition
df.filter(df.price=='326').count()


# Order data by groups
df.orderBy(df.depth).show()


# Create data frame as a table
sqlContext.registerDataFrameAsTable(df, "table1")


# Query the table which we created above by using SQL
df2 = sqlContext.sql("SELECT * from table1 Where cut='Premium' and carat > 1")


df2.show()

