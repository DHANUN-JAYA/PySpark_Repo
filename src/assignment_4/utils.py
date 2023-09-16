import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark():
    spark = SparkSession.builder.appName('pysparkAssignment4').getOrCreate()
    return spark
def dataframe(spark,dataDF,schema):
    data = spark.createDataFrame(data = dataDF, schema = schema)
    return data
def format_date(df,colName1):
    column1 =  df.withColumn(colName1, from_unixtime(df[colName1] / 1000).cast("timestamp"))
    return column1
def format_type(df,colName1):
    columnname = df.withColumn(colName1, to_date(df[colName1]))
    return columnname
def trim_str(df,ColName2):
    columnname = df.withColumn(ColName2, trim(df[ColName2]))
    return columnname
def replace_null(df,colName):
     values = df.na.fill(" ", subset=[colName])
     return values

#question2

def rename_column(df,existingName,newName):
    rename = df.withColumn(existingName,newName)
    return rename

def format_unix(df,colName1):
    timestamp_format = "yyy-MM-dd'T'HH:mm:ss.SSSXXX"
    timestampp = df.withColumn('timestamp_format',unix_timestamp(df[colName1],timestamp_format).cast(DoubleType()))
    return timestampp

def join_table(df,df2,colName,colName1):
    joining = df.join(df2,df[colName]==df2[colName1],"outer")
    return joining