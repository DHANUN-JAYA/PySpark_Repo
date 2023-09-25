import pyspark
from pyspark.sql import SparkSession, window
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
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
employee_df=create_dataframe(spark,data,schema)

# Select first row from each department group.
first_row_employee_df=first_row(employee_df)

#3.	Create a Dataframe from Row and List of tuples.
schema = Row("employee_name", "department", "salary")
row_data_df=row_data_df(spark,data,schema)

# 5 Retrieve Employees who earns the highest salary.
max_salary_employee_df=highest_salary(employee_df)

# 6 Select the highest, lowest, average, and total salary for each department group.
col_employee_df=totalsal_avg_high_low(employee_df)
