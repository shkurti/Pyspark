#!/usr/bin/env python
# coding: utf-8


import findspark

# your path will likely not have 'andonshkurti' in it. Change it to reflect your path.
findspark.init('/home/andonshkurti/spark-2.4')


from pyspark.sql import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
sc = SparkContext()
sqlContext = SQLContext(sc)


Employee = Row("firstName","lastName", "email", "salary")


print(Employee)


employee1 = Employee("Donald","Omins","bashararmush@yahoo.com", 100000)
employee2 = Employee("Andi","Join","andi@yahoo.com", 120000)
employee3 = Employee("Loreta","Will","loreta@yahoo.com", 150000)
employee4 = Employee("James","Smith","james@yahoo.com", 170000)
employee5 = Employee("Elion","Doee","migena@yahoo.com", 123000)


department1 = Row(id='123456', name="HR")
department2 = Row(id='723456', name="OPS")
department3 = Row(id='823456', name="FN")
department4 = Row(id='923456', name="DEV")


departmentWithEmployees1 = Row(department=department1, employees=[employee1,employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3,employee4,employee5])



print(departmentWithEmployees1)


departmentWithEmployees_Sql = [departmentWithEmployees1,departmentWithEmployees2]
dframe = sqlContext.createDataFrame(departmentWithEmployees_Sql)


dframe.show()





