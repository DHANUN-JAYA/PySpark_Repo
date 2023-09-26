import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import *
spark=create_SparkSession()
schema=StructType([
    StructField("Product",StringType()),
    StructField("Amount",IntegerType()),
    StructField("Country",StringType())
])
data=[
    ("Banana",1000,'USA'),
    ("Carrots",1500,"INDIA"),
    ("Beans",1600,"Sweden"),
    ("Orange",2000,"UK"),
    ("Orange",2000,"UAE"),
    ("Banana",400,"China"),
    ("Carrots",1200,"China")
]
df=create_dataframe(spark,data,schema)
df.show()
#11.Find total amount exported  to each country of each product.
pivoitDf=pivot_Df(df,'Product','Country','Amount')
pivoitDf.show()
#12.Perform unpivot function on output of question 2.
unpivoitDf=unpivoit_Df(pivoitDf)
unpivoitDf.show()

