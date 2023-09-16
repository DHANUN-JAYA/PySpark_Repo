import pyspark
from  pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
def create_SparkSession():
    spark=SparkSession.builder.appName('Pyspark_Assignment_1').getOrCreate()
    return spark
def create_dataframe(spark,data , schema):
    data_frame=spark.createDataFrame(data=data,schema=schema)
    return data_frame
def selecting(dataframe,firstname,lastname,salary):
    selectt = dataframe.select(firstname,lastname,salary)
    return selectt
def add_column(df,column_name,column_val):
    data_frame=df.withColumn(column_name,lit(column_val))
    return data_frame
def Change_val_col(df,salary,val):
    dataframe=df.withColumn('salary',col('salary')+val)
    return dataframe
def change_datatype(df,name,type):
    df = df.withColumn("New_name", col(name).cast(type))
    print("dfsafvrw")
    return df
def new_column(df,salary_added,val):
    df=df.withColumn(salary_added,col('salary')+val)
    return df
def change_col_name(df):
    df = df.withColumn("name",col("name.firstname")).withColumn('sfdgasudgu',col("name.middlename")).withColumn('dfhASDGH',col("name.lastname"))
    # df=df.withColumn('name.fname',col('name.firstname'))
    # df.withColumn("name", struct(col("product.{firstname}").alias("fname")))
    return df

def maxSalary(df,column):
    # max_salary=df.max(column)
    df1=change_datatype(df,"salary","integer")
    max_salary = df1.select(col("name"),col("salary")).filter(max(col("salary")))
    return max_salary
    # return max_salary

def drop_column(df,column):
    data_frame=df.drop(column)
    return data_frame
def non_duplicate(df,column):
    df1=df.select(column).distinct()
    return df1