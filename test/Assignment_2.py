import unittest
from PySpark_Repo.src.Assignment_2.utils import *

class MyTestCase(unittest.TestCase):
    def test_something(self):
        spark=create_SparkSession()
        schema = StructType([
            StructField("Product", StringType()),
            StructField("Amount", IntegerType()),
            StructField("Country", StringType())
        ])
        data = [
            ("Banana", 1000, 'USA'),
            ("Carrots", 1500, "INDIA"),
            ("Beans", 1600, "Sweden"),
            ("Orange", 2000, "UK"),
            ("Orange", 2000, "UAE"),
            ("Banana", 400, "China"),
            ("Carrots", 1200, "China")
        ]
        actual_df = create_dataframe(spark, data, schema)
        expected_schema = StructType([
            StructField("Product", StringType()),
            StructField("Amount", IntegerType()),
            StructField("Country", StringType())
        ])
        expected_data = [
            ("Banana", 1000, 'USA'),
            ("Carrots", 1500, "INDIA"),
            ("Beans", 1600, "Sweden"),
            ("Orange", 2000, "UK"),
            ("Orange", 2000, "UAE"),
            ("Banana", 400, "China"),
            ("Carrots", 1200, "China")
        ]
        expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_df.collect()), sorted(expected_df.collect())) # add assertion here

        # 11.Find total amount exported to each country of each product.
        actual_pivot_df=pivoitDf=pivot_Df(actual_df,'Product','Country','Amount')
        expected_schema = StructType([
            StructField("Product", StringType()),
            StructField("China", IntegerType()),
            StructField("INDIA", IntegerType()),
            StructField("Sweden", IntegerType()),
            StructField("UAE", IntegerType()),
            StructField("UK", IntegerType()),
            StructField("USA", IntegerType()),
        ])

        expected_data = [
            ("Orange",None,None,None,2000,2000,None),
            ("Beans", None, None, 1600, None, None, None),
            ("Banana", 400, None, None, None, None, 1000),
            ("Carrots", 1200, 1500, None, None, None, None)

        ]
        expected_pivot_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_pivot_df.collect()), sorted(expected_pivot_df.collect()))

        # 12.Perform unpivot function on output of question 2.
        actual_Unpivot_df = unpivoit_Df(actual_pivot_df)
        expected_schema = StructType([
            StructField("Product", StringType()),
            StructField("Country", StringType()),
            StructField("Amount", IntegerType())


        ])
        expected_data = [
            ("Banana", 'USA',1000),
            ("Carrots", "India", 1500),
            ("Beans", "Sweden", 1600),
            ("Orange", "UK", 2000),
            ("Orange", "UAE", 2000),
            ("Banana", "China", 400),
            ("Carrots", "China", 1200)
        ]
        expected_Unpivot_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
        self.assertEqual(sorted(actual_Unpivot_df.collect()), sorted(expected_Unpivot_df .collect()))
if __name__ == '__main__':
    unittest.main()

