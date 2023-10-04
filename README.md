# PySpark_Repo
PySpark_Assignment:
Question_1:
-	Imported SparkSession from pyspark.sql lib
-	Imported  pyspark.sql.types 
-	Imported  pyspark.sql.functions
-	Created a function to create SparkSession (create_session())
-	schema and data are defined in the driver file
-	note: for each question coded dynamically, functiona  calling in driver with requried parameters and function defination in utils file
-	Create_datafrme fun is called to create a employee_dataframe writen in the utils file
1.	Select firstname, lastname and salary from Dataframe.
-	selecting function is called with parametres selecting(firstname, lastname salary)
2.	Add Country, department, and age column in the dataframe.
-	add_column function(), withColumn inbuild function is used to add columns
-	dynamicaly in driver file add_column function() called to columns "Country","department","age"
 3.	Change the value of salary column
-	Change_val_col function() , withColumn inbuild function is used to chage column value along with lit() function
4.	Change the data types of DOB and salary to String
- change_datatype(), withColumn inbuild function is used to exising column or add columns, passsing cast type 
5.	Derive new column from salary column.
-	new_column(),  withColumn inbuild function is used to chage column value along with lit() function
7.	Filter the name column whose salary in maximum.
-	change_datatype(), using select in which max() is used on salary
8.	Drop the department and age column.
- drop_column(), directly drop function is used to delete
9.List out distinct value of dob and salary
-	non_duplicate(), select on distinct() column is passed dinamically

Question_2:
-	Imported SparkSession from pyspark.sql lib
-	Imported  pyspark.sql.types 
-	Imported  pyspark.sql.functions
-	Created a function to create SparkSession (create_session())
-	schema and data are defined in the driver file 
-	Create_dataframe() is called to create a employee_dataframe writen in the utils file
-	note: for each question coded dynamically, functiona  calling in driver and function defination in utils file
1. Find total amount exported  to each country of each product.
- pivot_Df(), here I have used groupby by "product" & pivoted on "country" column on aggregation of "Amount"
2. #12.Perform unpivot function on output of question 2.
- unpivoit_Df(), As we don't have stright forward to this, I used to select product along with expr & stack function poping mechanism

Question_3:
-	Imported SparkSession from pyspark.sql lib
-	Imported  pyspark.sql.types 
-	Imported  pyspark.sql.functions
-	Created a function to create SparkSession (create_session())
-	schema and data are defined in the driver file
-	note: for each question coded dynamically, functiona  calling in driver and function defination in utils file
1. Select first row from each department group.
- first_row(),
- I have imported pyspark.sql.window as Window
- In Window() , partitionBy "department" column and sorted by salary asc order
- using withColumn created column to same dataframe wit row numbers
2.Create a Dataframe from Row and List of tuples.
- row_data_df(), here use list comprehension for which data is iterated row wise to attached with schema each row
3. Retrieve Employees who earns the highest salary.
- highest_salary,
- In Window() , partitionBy "department" column and sorted by salary disc order
- using withColumn created column to same dataframe with row numbers
- As it is decending order rownumber equal to 1 is taken in each group
4. Select the  highest, lowest, average, and total salary for each department group.
- totalsal_avg_high_low(),empolyee_df dataframe is passed as parameter
- In Window() , partitionBy "department" column and sorted by salary assign to Window dataframe
- One more, In Window() , partitionBy "department" column assign to real_data dataframe
- for exisiting dataframe added given new columns with respect values using withColumn(), and aggrigate function to insert values in columns


