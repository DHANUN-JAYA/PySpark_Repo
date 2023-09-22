import pyspark
from pyspark.sql import SparkSession
from utils import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = create_SparkSession()
schema=StructType([
        StructField('name',StructType([
            StructField('firstname',StringType(),True),
            StructField('middlename',StringType(),True),
            StructField('lastname',StringType(),True)])),
        StructField('dob',LongType(),True),
        StructField('gender',StringType(),True),
        StructField('salary',IntegerType(),True)
        ])

data=[
        (("james","","Smith"),3011998,'M',3000),
        (("Michael","Rose",""),10111998,'M',20000),
        (("Robert","","Williams"),2023000,'M',3000),
        (("Maria","Anne","Jones"),3011998,'F',1100),
        (("Jen","Mary","Brown"),4101998,'F',10000)
    ]
# Create Data Frame
employee_df= create_dataframe(spark , data, schema)

#1.	Select firstname, lastname and salary from Dataframe.
select_df=selecting(employee_df,employee_df.name.firstname,employee_df.name.lastname,employee_df.salary)

#2.	Add Country, department, and age column in the dataframe.
employee_df=add_column(employee_df,"Country","India")
employee_df=add_column(employee_df,"department","DataEngineer")
employee_df=add_column(employee_df,"age",23)


#3.	Change the value of salary column
employee_df=Change_val_col(employee_df,'salary',200)


#4.	Change the data types of DOB and salary to String
employee_df=change_datatype(employee_df,'dob','string')

employee_df=change_datatype(employee_df,'salary','string')


#5.	Derive new column from salary column.
employee_df=new_column(employee_df,'salary_added',500)
employee_df.show()

#7.	Filter the name column whose salary in maximum.
employee_df=change_datatype(employee_df,'salary','integer')
max_salary=maxSalary(employee_df,'salary')
max_salary.show()

#8.	Drop the department and age column.
employee_df=drop_column(employee_df,'department')
employee_df=drop_column(employee_df,'age')

# 9.List out distinct value of dob and salary
employee_df_dist_dob=non_duplicate(employee_df,column='dob')
employee_df_dist_salary=non_duplicate(employee_df,column=('salary'))


