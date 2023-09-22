import unittest
from PySpark_Repo.src.Assignment_1.utils import *


class MyTestCase(unittest.TestCase):
    spark=create_SparkSession()
    def test_something(self):
        schema = StructType([
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)])),
            StructField('dob', LongType(), True),
            StructField('gender', StringType(), True),
            StructField('salary', IntegerType(), True)
        ])

        data = [
            (("james", "", "Smith"), 3011998, 'M', 3000),
            (("Michael", "Rose", ""), 10111998, 'M', 2000),
            (("Robert", "", "Williams"), 2023000, 'M', 3000),
            (("Maria", "Anne", "Jones"), 3011998, 'F', 3000),
            (("Jen", "Mary", "Brown"), 4101998, 'F', 1000)
        ]
        # # Create Data Frame
        employee_df = create_dataframe(self.spark, data, schema)


        # 1 Select firstname, lastname and salary from Dataframe.
        expected_schema=StructType([
            StructField("name.firstname",StringType(),True),
            StructField("name.lastname",StringType(),True),
            StructField("salary",IntegerType(),True)
        ])
        expected_data=[
            ("james","Smith",3000),
            ("Michael","",2000),
            ("Robert","Williams",3000),
            ("Maria","Jones",3000),
            ("Jen","Brown",1000)
        ]
        expected_employee_df=create_dataframe(self.spark, expected_data, expected_schema)
        actual__employee_df=selecting(employee_df, employee_df.name.firstname, employee_df.name.lastname, employee_df.salary)
        self.assertEqual(actual__employee_df.collect(),expected_employee_df.collect())

        # 2.	Add Country, department, and age column in the dataframe.
        employee_df = add_column(employee_df, "Country", "India")
        employee_df = add_column(employee_df, "department", "DataAnalyst")
        employee_df = add_column(employee_df, "age", 27)

        expected_schema = StructType([
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)])),
            StructField('dob', LongType(), True),
            StructField('gender', StringType(), True),
            StructField('salary', IntegerType(), True),
            StructField('Country', StringType(), True),
            StructField('department', StringType(), True),
            StructField('age', IntegerType(), True)
        ])

        expected_data = [
            (("james", "", "Smith"), 3011998, 'M', 3000,'India','DataAnalyst',27),
            (("Michael", "Rose", ""), 10111998, 'M', 2000,'India','DataAnalyst',27),
            (("Robert", "", "Williams"), 2023000, 'M', 3000,'India','DataAnalyst',27),
            (("Maria", "Anne", "Jones"), 3011998, 'F', 3000,'India','DataAnalyst',27),
            (("Jen", "Mary", "Brown"), 4101998, 'F', 1000,'India','DataAnalyst',27)
        ]
        # # Create Data Frame
        expected_employee_df = create_dataframe(self.spark, expected_data, expected_schema)
        self.assertEqual(sorted(employee_df.collect()), sorted(expected_employee_df.collect()))

        # 3. Change the value of salary column
        val=250
        employee_df = Change_val_col(employee_df, 'salary', val)
        expected_schema = StructType([
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)])),
            StructField('dob', LongType(), True),
            StructField('gender', StringType(), True),
            StructField('salary', IntegerType(), True),
            StructField('Country', StringType(), True),
            StructField('department', StringType(), True),
            StructField('age', IntegerType(), True)
        ])

        expected_data = [
            (("james", "", "Smith"), 3011998, 'M', 3000+val, 'India', 'DataAnalyst', 27),
            (("Michael", "Rose", ""), 10111998, 'M', 2000+val, 'India', 'DataAnalyst', 27),
            (("Robert", "", "Williams"), 2023000, 'M', 3000+val, 'India', 'DataAnalyst', 27),
            (("Maria", "Anne", "Jones"), 3011998, 'F', 3000+val, 'India', 'DataAnalyst', 27),
            (("Jen", "Mary", "Brown"), 4101998, 'F', 1000+val, 'India', 'DataAnalyst', 27)
        ]
        # # Create Data Frame
        expected_employee_df = create_dataframe(self.spark, expected_data, expected_schema)
        self.assertEqual(sorted(employee_df.collect()), sorted(expected_employee_df.collect()))

        # 5.	Derive new column from salary column.
        column_name='salary_added'
        column_val=400
        employee_df = new_column(employee_df, column_name, column_val)
        expected_schema = StructType([
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)])),
            StructField('dob', LongType(), True),
            StructField('gender', StringType(), True),
            StructField('salary', IntegerType(), True),
            StructField('Country', StringType(), True),
            StructField('department', StringType(), True),
            StructField('age', IntegerType(), True),
            StructField(column_name, IntegerType(), True),
        ])

        expected_data = [
            (("james", "", "Smith"), 3011998, 'M', 3000+val, 'India', 'DataAnalyst', 27,3000+val+column_val),
            (("Michael", "Rose", ""), 10111998, 'M', 2000+val, 'India', 'DataAnalyst', 27,2000+val+column_val),
            (("Robert", "", "Williams"), 2023000, 'M', 3000+val, 'India', 'DataAnalyst', 27,3000+val+column_val),
            (("Maria", "Anne", "Jones"), 3011998, 'F', 3000+val, 'India', 'DataAnalyst', 27,3000+val+column_val),
            (("Jen", "Mary", "Brown"), 4101998, 'F', 1000+val, 'India', 'DataAnalyst', 27,1000+val+column_val)
        ]
        # # Create Data Frame
        expected_employee_df = create_dataframe(self.spark, expected_data, expected_schema)
        self.assertEqual(sorted(employee_df.collect()), sorted(expected_employee_df.collect()))

        # 7.	Filter the name column whose salary in maximum.
        max_salary = maxSalary(employee_df, 'salary')
        schema=["max(salary)"]
        data=[(3250,)]
        expected_max_salary_df=self.spark.createDataFrame(data,schema)
        actual_employee_df = change_datatype(employee_df, 'salary', 'integer')
        actual_employee_max_salary_df = maxSalary(employee_df, 'salary')
        self.assertEqual(actual_employee_max_salary_df.collect() , expected_max_salary_df.collect())

        # 9.List out distinct value of dob and salary
        # distinct value dob
        actual_employee_df_dist_dob = non_duplicate(employee_df, column='dob')
        schema = ["dob"]
        data = [(3011998,),
                (10111998,),
                (2023000,),
                (4101998,)]
        expected_employee_df_dist_dob=create_dataframe(self.spark,data,schema)
        self.assertEqual(actual_employee_df_dist_dob.collect(), expected_employee_df_dist_dob.collect())
        #distinct value salary
        actual_employee_df_dist_salary = non_duplicate(employee_df, column=('salary'))
        schema = ["salary"]
        data = [(3250,),
                (2250,),
                (1250,)]
        expected_employee_df_dist_salary = create_dataframe(self.spark, data, schema)
        self.assertEqual(actual_employee_df_dist_salary.collect(), expected_employee_df_dist_salary .collect())


if __name__ == '__main__':
    obj=unittest.main()

