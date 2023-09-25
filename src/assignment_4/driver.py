import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import *


spark = create_spark()

dataDF = [('WashingMashine',1648770933000,20000,'Samsung','India',1),
          ('Refrigerator',1648770999000,35000,' LG',None,2),
          ('AirCooler',1648770948000,45000,' Voltas',None,3)
  ]
schema=["Product Name","Issue Date","Price","Brand","Country","Product number"]

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema_1 = StructType([
    StructField("SourceId",IntegerType(),True),
    StructField("TranasactionNumber",IntegerType(),True),
    StructField("Language",StructType(),True),
    StructField("ModelNumber",IntegerType(),True),
    StructField("StartTime",StringType(),True),
    StructField("ProductNumber",IntegerType(),True)
])

#data for dataDF_2
dataDF2 = [
    (150711, 123456, 'EN',456789,'2021-12-27T08:20:29.842+0000', 1),
    (150439, 234567,'UK', 345678,'2021-12-27T08:21:14.645+0000',2),
    (150647, 345678,'ES', 234567,'2021-12-27T08:22:42.445+0000',3)
]

colName1="Issue Date"
colName="Country"
ColName2="Brand"

df = dataframe(spark,dataDF,schema)
#df.show()

#Convert the Issue Date with the timestamp format.
df = format_date(df,colName1)
df.show()

#Convert timestamp to date type
df = format_type(df,colName1)
df.show()

#Remove the starting extra space in Brand column for LG and Voltas fields
df = trim_str(df,ColName2)
df.show()

#Replace null values with empty values in Country column
df = replace_null(df,colName)
df.show()

"question2"
#create Data Frame

df2 = dataframe(spark,dataDF2,schema_1)

df = rename_column(df,'SourceId','source_id')

df = rename_column(df,'TransactionNumber','transactionnumber')

#timestamp to unix

df_format = format_unix(df,'StartTime')
df_format.show()

#Join the table

df_joined_table = join_table(df,'Product number','Product number')
df_joined_table.show()