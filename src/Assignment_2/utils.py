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

def pivot_Df(df,Product,Country,Amount):
    pivoitDf = df.groupBy(Product).pivot(Country).agg(sum(Amount))
    return pivoitDf

def unpivoit_Df(pivoitDf):
    unpivoitDf = pivoitDf.select('Product', expr(
        "stack(6, 'India', India, 'Sweden', Sweden, 'UK', UK,'China',China,'UAE',UAE,'USA',USA) as (Country, Amount)")).where("Amount is not null")
    return unpivoitDf