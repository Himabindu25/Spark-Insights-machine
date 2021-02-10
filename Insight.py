import re
import pandas
import matplotlib.pyplot as plt
import seaborn as sns
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class Insight:
    spark = SparkSession.builder.appName("Insight").getOrCreate()
    file = 'MOPAR INC Ticket Data Analysis1.xlsx'

    with pandas.ExcelFile(file) as xls:
        for sheet_name in xls.sheet_names:
            pdf = pandas.read_excel(xls, sheet_name='Sheet1')

    # print(pdf)
    df = spark.createDataFrame(pdf)
    # df.show()

    xdf = df.select("Configuration item", "Priority")
    # xdf.show()

    xdf = xdf.groupBy("Configuration item", "Priority").agg(F.count("Priority").alias('Incident count'))
    xdf = xdf.orderBy(xdf['Incident count'].desc())
    xdf.show(6)

    xdf1 = df.select("Configuration item", "Total Time to Resolved", "Created", "Resolved")

    # Function to calculate time delta
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss"

    timeDiff = (F.unix_timestamp('Resolved', format=timeFmt)
                - F.unix_timestamp('Created', format=timeFmt))
    xdf1 = xdf1.withColumn("Duration", timeDiff)
    # xdf1.show()

    xdf1_mm = xdf1.groupBy(xdf1['Configuration item']).agg(
        F.min(xdf1['Total Time to Resolved']).alias('Minimum Time Taken'),
        F.max(xdf1['Total Time to Resolved']).alias('Maximum Time Taken'),
        F.avg(xdf1['Duration']).alias('Average Time Taken'))

    # xdf1_mm.show()

    writer = pandas.ExcelWriter("Description.xlsx", engine='xlsxwriter')

    xdf1_mm.toPandas().to_excel(writer, sheet_name='Configuration', index=False)

    def parsing(x):
        r = re.compile(r"(?:(?<=\s)|^)(?:[a-z]|\d+)", re.I)
        return ''.join(r.findall(x))

    udffun = F.udf(parsing, returnType=StringType())
    xdf = xdf.withColumn("Parse Configuration", udffun('Configuration item'))
    # xdf.show()

    xdf_priority1 = xdf.select("Configuration item", "Parse Configuration", "Priority", "Incident count").filter(
        xdf.Priority == '1 - High')
    ydf_1 = xdf_priority1.orderBy(xdf_priority1['Incident count'].desc())
    # Tasks = ydf_1['Incident count']
    # my_labels = ydf_1["Configuration item"]
    # plt.pie(Tasks, labels=my_labels, autopct='%1.0f%%', shadow=True, startangle=140)
    # plt.title('Top six Configuration item')
    # plt.axis('equal')
    # plt.show()

    xdf_priority2 = xdf.select("Parse Configuration", "Priority", "Incident count").filter(xdf.Priority == '2 - Medium')
    ydf_2 = xdf_priority2.orderBy(xdf_priority2['Incident count'].desc())
    # ydf_2.show(6)

    xdf_priority3 = xdf.select("Parse Configuration", "Priority", "Incident count").filter(xdf.Priority == '3 - Low')
    ydf_3 = xdf_priority3.orderBy(xdf_priority3['Incident count'].desc())
    # ydf_3.show(6)

    pdf1 = xdf_priority1.toPandas()
    pdf2 = xdf_priority2.toPandas()
    pdf3 = xdf_priority3.toPandas()

    # sns.barplot(pdf1["Parse Configuration"], pdf1["Incident count"], data=pdf1, color='red', label='High')
    sns.barplot(pdf2["Parse Configuration"], pdf2["Incident count"], data=pdf2, color='yellow', label='medium')
    sns.barplot(pdf3["Parse Configuration"], pdf3["Incident count"], data=pdf3, color='green', label='low')

    # Create legend.

    plt.legend(loc='upper right', title='Priority')
    plt.ylabel('Incident count')
    plt.xticks(
        rotation=45,
        horizontalalignment='right',
        fontweight='light',
        fontsize='small'
    )
    plt.xlabel('Configuration Item')
    plt.savefig("Configuration_count.png")
    # ['Medium', 'Low'],


if __name__ == '__main__':
    enabler = Insight()
