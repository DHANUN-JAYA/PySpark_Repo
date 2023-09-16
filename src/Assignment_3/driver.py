import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import *
spark=create_SparkSession()
schema=StructType([
    StructField("employee_name",StringType()),
    StructField("department",StringType()),
    StructField("salary",IntegerType())
])
data=[
    ("James","Sales",3000),
    ("Michael","Sales",4600),
    ("Robert","Sales",4100),
    ("Raman","Finance",3000),
    ("Scott","Finance",3000),
    ("Jen","Finance",3300),
    ("Jeff","Marketing",3000),
    ("Kumar","Marketing",2000)
]
df=create_dataframe(spark,data,schema)

# Select first row from each department group.
df1=first_row(df).show()

# Retrieve Employees who earns the highest salary.
df2=highest_salary(df)
df2.show()

# Select the highest, lowest, average, and total salary for each department group.
df3=totalsal_avg_high_low(df)
df3.show()