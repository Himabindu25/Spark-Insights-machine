import re
import pandas
import matplotlib.pyplot as plt
import seaborn as sns
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession
from datetime import datetime
from dateutil import parser
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Insight").getOrCreate()
file = 'C:\\Users\\M1055990\\Desktop\\New folder\\code - changes\\Dateincrement.xls'

with pandas.ExcelFile(file) as xls:
    for sheet_name in xls.sheet_names:
        pdf = pandas.read_excel(xls, sheet_name='Sheet1')  # 'Lagacy_Knowledge_builder')
mySchema = StructType([StructField("JOB NAME", StringType(), True)
                          , StructField("Date", DateType(), True)
                          , StructField("OWNER", StringType(), True)
                          , StructField("JOB CLASS", StringType(), True)
                          , StructField("JOB ID", StringType(), True)
                          , StructField("RUN TIME", StringType(), True)
                          , StructField("CPU", StringType(), True)
                          , StructField("File name", StringType(), True)
                          , StructField("STEPS IN JCL", StringType(), True)
                          , StructField("UTILITY", StringType(), True)
                          , StructField("COND CODES", StringType(), True)
                          , StructField("RUN STATUS", StringType(), True)
                          , StructField("Inventory info", StringType(), True)
                       # , StructField("MORE ABEND INFO", StringType(), True)
                       # , StructField("STEP WISE DATA SETS", StringType(), True)
                          , StructField("STARTED TIME", StringType(), True)
                          , StructField("ENDED TIME", StringType(), True)
                          , StructField("System Parameters", StringType(), True)
                          , StructField("Application Program summary", StringType(), True)])

df = spark.createDataFrame(pdf, schema=mySchema)
df.show()

df1 = df
df1 = df1.withColumn("Date", F.date_add(df1['Date'], 7))
df1 = df1.withColumn('JOB ID', df1['JOB ID'].substr(4, 5))
df1 = df1.withColumn('JOB ID', df1["JOB ID"].cast(IntegerType()))
df1 = df1.withColumn("JOB ID", df1['JOB ID'] + 1)


# df1.show()
# df1 = df1.withColumn("JOB ID",F.format_string('JOB0' + str(df1['JOB ID'])))
# df1 = df1.withColumn("JOB ID", when(df1['JOB ID'].isNull(),df1['JOB ID']).otherwise('JOB0' + str(df1['JOB ID'])))
def increment(x):
    if x == 'None':
        return x
    else:
        x = 'JOB0' + str(x)
        return x


udffun = F.udf(increment, returnType=StringType())
df1 = df1.withColumn("JOB ID", udffun("JOB ID"))
df1 = df1.withColumn('JOB ID', regexp_replace('JOB ID', 'JOB0None', 'NaN'))
df1.show()
# date = '2020-04-06 00:00:00'

df2 = df1
df2 = df2.withColumn("Date", F.date_add(df2['Date'], 7))
df2 = df2.withColumn('JOB ID', df2['JOB ID'].substr(4, 5))
df2 = df2.withColumn('JOB ID', df2["JOB ID"].cast(IntegerType()))
df2 = df2.withColumn("JOB ID", df2['JOB ID'] + 1)


# df1.show()
# df1 = df1.withColumn("JOB ID",F.format_string('JOB0' + str(df1['JOB ID'])))
# df1 = df1.withColumn("JOB ID", when(df1['JOB ID'].isNull(),df1['JOB ID']).otherwise('JOB0' + str(df1['JOB ID'])))
def increment(x):
    if x == 'None':
        return x
    else:
        x = 'JOB0' + str(x)
        return x


udffun = F.udf(increment, returnType=StringType())
df2 = df2.withColumn("JOB ID", udffun("JOB ID"))
df2 = df2.withColumn('JOB ID', regexp_replace('JOB ID', 'JOB0None', 'NaN'))
df2.show()

df3 = df2
df3 = df3.withColumn("Date", F.date_add(df3['Date'], 7))
df3 = df3.withColumn('JOB ID', df3['JOB ID'].substr(4, 5))
df3 = df3.withColumn('JOB ID', df3["JOB ID"].cast(IntegerType()))
df3 = df3.withColumn("JOB ID", df3['JOB ID'] + 1)


# df1.show()
# df1 = df1.withColumn("JOB ID",F.format_string('JOB0' + str(df1['JOB ID'])))
# df1 = df1.withColumn("JOB ID", when(df1['JOB ID'].isNull(),df1['JOB ID']).otherwise('JOB0' + str(df1['JOB ID'])))
def increment(x):
    if x == 'None':
        return x
    else:
        x = 'JOB0' + str(x)
        return x


udffun = F.udf(increment, returnType=StringType())
df3 = df3.withColumn("JOB ID", udffun("JOB ID"))
df3 = df3.withColumn('JOB ID', regexp_replace('JOB ID', 'JOB0None', 'NaN'))
df3.show()

df4 = df3
df4 = df4.withColumn("Date", F.date_add(df4['Date'], 7))
df4 = df4.withColumn('JOB ID', df4['JOB ID'].substr(4, 5))
df4 = df4.withColumn('JOB ID', df4["JOB ID"].cast(IntegerType()))
df4 = df4.withColumn("JOB ID", df4['JOB ID'] + 1)


# df1.show()
# df1 = df1.withColumn("JOB ID",F.format_string('JOB0' + str(df1['JOB ID'])))
# df1 = df1.withColumn("JOB ID", when(df1['JOB ID'].isNull(),df1['JOB ID']).otherwise('JOB0' + str(df1['JOB ID'])))
def increment(x):
    if x == 'None':
        return x
    else:
        x = 'JOB0' + str(x)
        return x


udffun = F.udf(increment, returnType=StringType())
df4 = df4.withColumn("JOB ID", udffun("JOB ID"))
df4 = df4.withColumn('JOB ID', regexp_replace('JOB ID', 'JOB0None', 'NaN'))
df4.show()

df5 = df4
df5 = df5.withColumn("Date", F.date_add(df5['Date'], 7))
df5 = df5.withColumn('JOB ID', df5['JOB ID'].substr(4, 5))
df5 = df5.withColumn('JOB ID', df5["JOB ID"].cast(IntegerType()))
df5 = df5.withColumn("JOB ID", df5['JOB ID'] + 1)


# df1.show()
# df1 = df1.withColumn("JOB ID",F.format_string('JOB0' + str(df1['JOB ID'])))
# df1 = df1.withColumn("JOB ID", when(df1['JOB ID'].isNull(),df1['JOB ID']).otherwise('JOB0' + str(df1['JOB ID'])))
def increment(x):
    if x == 'None':
        return x
    else:
        x = 'JOB0' + str(x)
        return x


udffun = F.udf(increment, returnType=StringType())
df5 = df5.withColumn("JOB ID", udffun("JOB ID"))
df5 = df5.withColumn('JOB ID', regexp_replace('JOB ID', 'JOB0None', 'NaN'))
# df5.show()

# dfunion = df.unionAll(df1,df2,df3,df4,df5).show()

pdf = df.toPandas()
pdf1 = df1.toPandas()
pdf2 = df2.toPandas()
pdf3 = df3.toPandas()
pdf4 = df4.toPandas()


# pdf5 = df5.toPandas()
# df4 = spark.createDataFrame(df3)
# pdf1 = df3.toPandas()
# writer = pandas.ExcelWriter('C:\\Users\\M1055990\\Desktop\\fileOutput.xls')
# writer.save()
# dfunion.toPandas().to_excel('C:\\Users\\M1055990\\Desktop\\fileOutput.xls', sheet_name='Sheet1', index=False)
# df3.write.excel('C:\\Users\\M1055990\\Desktop\\Datareplication.xls')

frames = [pdf, pdf1, pdf2, pdf3]
result = pandas.concat(frames)
# result = result.fillna('')
result = result.replace('NaN', "")
result.to_excel('C:\\Users\\M1055990\\Desktop\\Dateincrementweek1.xls', sheet_name='Sheet1', index=False)
