import unittest
from PySpark_Repo.src.Assignment_3.utils import *

class MyTestCase(unittest.TestCase):
    spark=create_SparkSession()
    def test_something(self):
        schema = StructType([
            StructField("employee_name", StringType()),
            StructField("department", StringType()),
            StructField("salary", IntegerType())
        ])
        data = [
            ("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 41050),
            ("Raman", "Finance", 3000),
            ("Scott", "Finance", 3700),
            ("Jen", "Finance", 3300),
            ("Jeff", "Marketing", 3800),
            ("Kumar", "Marketing", 2000)
        ]
        expected_employee_df=self.spark.createDataFrame(data,schema)
        employee_df = create_dataframe(self.spark, data, schema)
        self.assertEqual(employee_df.collect(), expected_employee_df.collect())  # add assertion here

        # Select first row from each department group.
        actual_first_row_employee_df = first_row(employee_df)
        schema = StructType([
            StructField("employee_name", StringType()),
            StructField("department", StringType()),
            StructField("salary", IntegerType())
        ])
        data = [
            ("Raman", "Finance", 3000),
            ("Kumar", "Marketing", 2000),
            ("James","Sales",3000)
        ]
        expected_first_row_employee_df = create_dataframe(self.spark, data, schema)
        self.assertEqual(actual_first_row_employee_df.collect(), expected_first_row_employee_df.collect())  # add assertion here

        # 3.	Create a Dataframe from Row and List of tuples.
        schema = StructType([
            StructField("employee_name", StringType()),
            StructField("department", StringType()),
            StructField("salary", IntegerType())
        ])
        data = [
            ("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 41050),
            ("Raman", "Finance", 3000),
            ("Scott", "Finance", 3700),
            ("Jen", "Finance", 3300),
            ("Jeff", "Marketing", 3800),
            ("Kumar", "Marketing", 2000)
        ]
        expected_row_data_dff = self.spark.createDataFrame(data, schema)
        schema = Row("employee_name", "department", "salary")
        actual_row_data_dff = row_data_df(self.spark, data, schema)
        self.assertEqual(actual_row_data_dff.collect(), expected_row_data_dff.collect())

    # 5 Retrieve Employees who earns the highest salary.
        actual_max_salary_employee_df = highest_salary(employee_df)
        schema = StructType([
            StructField("employee_name", StringType()),
            StructField("department", StringType()),
            StructField("salary", IntegerType())
        ])
        data = [
            ("Scott", "Finance", 3700),
            ("Jeff","Marketing",3800),
            ("Robert", "Sales", 41050),

        ]
        expected_max_salary_employee_df=create_dataframe(self.spark,data,schema)
        self.assertEqual(actual_max_salary_employee_df.collect(),expected_max_salary_employee_df.collect())

        # 6 Select the highest, lowest, average, and total salary for each department group.
        actual_col_employee_df = totalsal_avg_high_low(employee_df)
        schema=StructType([
            StructField("department",StringType(),True),
            StructField("Average", FloatType(), True),
            StructField("highest_salary", IntegerType(), True),
            StructField("lowest_salary", IntegerType(), True),
            StructField("total_salary", IntegerType(), True),
        ])
        data=[
            ("Finance",3333.33,3700,3000,10000),
            ("Marketing",2900.0,3800,2000,5800),
            ("Sales",6216.67,41050,3000,48650)
        ]
        expected_col_employee_df = create_dataframe(self.spark, data, schema)
        actual_col_employee_df.show()
        actual_col_employee_df = actual_col_employee_df.withColumn("Average",
                                                                   round(actual_col_employee_df["Average"], 2))
        actual_col_employee_df.show()
        expected_col_employee_df = expected_col_employee_df.withColumn("Average",
                                                                       round(expected_col_employee_df["Average"], 2))

        # self.assertEqual(actual_col_employee_df.collect(), expected_col_employee_df.collect())


if __name__ == '__main__':
    unittest.main()
