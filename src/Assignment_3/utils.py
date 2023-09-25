import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
def create_SparkSession():
    spark=SparkSession.builder.appName("Assignment_3").getOrCreate()
    return spark

def create_dataframe(spark,data,schema):
    dataframe=spark.createDataFrame(data=data,schema=schema)
    return dataframe
def first_row(df):
    window_spec = Window.partitionBy("department").orderBy("salary")
    df = df.withColumn("row_num", F.row_number().over(window_spec))
    first_row_df = df.filter(df.row_num == 1).drop("row_num")
    return first_row_df
def row_data_df(spark,data,schema):
    row_data = [schema(*record) for record in data]
    row_data_df=spark.createDataFrame(row_data)
    return row_data_df
def highest_salary(df):
    window1=Window.partitionBy("department").orderBy(col("salary").desc())
    df=df.withColumn("row_no",row_number().over(window1)).filter(col("row_no")==1).drop("row_no")
    return df

def totalsal_avg_high_low(df):
    window=Window.partitionBy("department").orderBy("salary")
    real_data=Window.partitionBy("department")
    df=df.withColumn("row_no",row_number().over(window))\
            .withColumn("Average",avg(col("salary")).over(real_data))\
            .withColumn("highest_salary",max(col("salary")).over(real_data))\
            .withColumn("lowest_salary",min(col("salary")).over(real_data))\
            .withColumn("total_salary",sum(col("salary")).over(real_data))\
            .where(col("row_no")==1).drop("row_no").select("department","Average","highest_salary","lowest_salary","total_salary")
    return df