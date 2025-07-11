import unittest
from pyspark.sql import SparkSession
from src.assignment_5.util import EmployeeAnalysisUtils


class EmployeeAnalysisTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("EmployeeAnalysisTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_employee_df_creation(self):
        df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        self.assertEqual(df.count(), 7)
        self.assertEqual(len(df.columns), 6)

    def test_department_df_creation(self):
        df = EmployeeAnalysisUtils.create_department_df(self.spark)
        self.assertEqual(df.count(), 5)
        self.assertEqual(len(df.columns), 2)

    def test_country_df_creation(self):
        df = EmployeeAnalysisUtils.create_country_df(self.spark)
        self.assertEqual(df.count(), 3)
        self.assertEqual(len(df.columns), 2)

    def test_avg_salary_by_department(self):
        df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        avg_salary_df = EmployeeAnalysisUtils.find_avg_salary_by_department(df)
        self.assertEqual(avg_salary_df.count(), 3)  # Expecting 3 departments
        self.assertIn("avg_salary", avg_salary_df.columns)

    def test_employees_with_m(self):
        df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        result = EmployeeAnalysisUtils.find_employees_with_m(df)
        self.assertEqual(result.count(), 2)  # Expecting 2 employees starting with 'm'

    def test_add_bonus_column(self):
        df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        updated_df = EmployeeAnalysisUtils.add_bonus_column(df)
        self.assertIn("bonus", updated_df.columns)
        self.assertEqual(updated_df.count(), df.count())

    def test_reorder_columns(self):
        df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        reordered_df = EmployeeAnalysisUtils.reorder_columns(df)
        self.assertEqual(reordered_df.columns, ["employee_id", "employee_name", "salary", "State", "Age", "department"])

    def test_replace_state_with_country(self):
        emp_df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        country_df = EmployeeAnalysisUtils.create_country_df(self.spark)
        result_df = EmployeeAnalysisUtils.replace_state_with_country(emp_df, country_df)
        self.assertIn("country_name", result_df.columns)

    def test_convert_columns_to_lowercase(self):
        emp_df = EmployeeAnalysisUtils.create_employee_df(self.spark)
        country_df = EmployeeAnalysisUtils.create_country_df(self.spark)
        replaced_df = EmployeeAnalysisUtils.replace_state_with_country(emp_df, country_df)
        final_df = EmployeeAnalysisUtils.convert_columns_to_lowercase(replaced_df)
        self.assertTrue(all(col.islower() for col in final_df.columns))


if __name__ == "__main__":
    unittest.main()