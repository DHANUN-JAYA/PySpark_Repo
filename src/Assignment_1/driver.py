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
        StructField('dob',StringType(),True),
        StructField('gender',StringType(),True),
        StructField('salary',IntegerType(),True)
        ])

data=[
        (("james","","Smith"),'03011998','M',3000),
        (("Michael","Rose",""),'10111998','M',20000),
        (("Robert","","Williams"),'02023000','M',3000),
        (("Maria","Anne","Jones"),'03011998','F',1100),
        (("Jen","Mary","Brown"),'04101998','F',10000)
    ]
# Create Data Frame
df= create_dataframe(spark , data,schema)

#1.	Select firstname, lastname and salary from Dataframe.
abc = selecting(df,df.name.firstname,df.name.lastname,df.salary)

#2.	Add Country, department, and age column in the dataframe.
df=add_column(df,"Country","India")
df=add_column(df,"department","DataEngineer")
df=add_column(df,"age",23)

#3.	Change the value of salary column
df=Change_val_col(df,'salary',200)


#4.	Change the data types of DOB and salary to String
df=change_datatype(df,'dob','string')
df=change_datatype(df,'salary','string')

#5.	Derive new column from salary column.
df=new_column(df,'salary_added',500)


#7.	Filter the name column whose salary in maximum.
df=change_datatype(df,'salary','integer')
max_salary=maxSalary(df,'salary')


#8.	Drop the department and age column.
df=drop_column(df,'department')
df=drop_column(df,'age')
#9.	List out distinct value of dob and salary
df1=non_duplicate(df,column=('dob'))
df2=non_duplicate(df,column=('salary'))


