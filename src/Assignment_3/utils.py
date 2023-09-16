import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
def create_SparkSession():
    spark=SparkSession.builder.appName("Assignment_2").getOrCreate()
    return spark

def create_dataframe(spark,data,schema):
    dataframe=spark.createDataFrame(data=data,schema=schema)
    return dataframe
def first_row(df):
    window1=window.partitionBy("Department").orderBy("Salary")
    df1=df.withColumn("row",row_number().over(window1)).filter(col("row")==1).drop("row")
    return df1

def highest_salary(df):
    window1=window.partitionBy("department").orderBy(col("salary").desc())
    df2=df.withColumn("row",row_number().over(window1)).filter(col("row")==1).drop("row")
    return df2

def totalsal_avg_high_low(df):
    window2=window.partitionBy("department").orderBy("salary")
    real_data=window.partitionBy("department")
    df3=df.withColumn("row",row_number().over(window2))\
            .withColumn("Average",avg(col("salary")).over(real_data))\
            .withColumn("highest_salary",max(col("salary")).over(real_data))\
            .withColumn("lowest_salary",min(col("salary")).over(real_data))\
            .withColumn("total_salary",sum(col("salary")).over(real_data))\
            .where(col("row")==1).drop("row").select("department","Average","highest_salary","lowest_salary","total_salary")
    return df3