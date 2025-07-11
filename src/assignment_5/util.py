from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, current_date

class EmployeeAnalysisUtils:

    def create_employee_df(spark: SparkSession):
        schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("State", StringType(), True),
            StructField("salary", FloatType(), True),
            StructField("Age", IntegerType(), True)
        ])
        data = [
            (11, "james", "D101", "ny", 9000, 34),
            (12, "michel", "D101", "ny", 8900, 32),
            (13, "robert", "D102", "ca", 7900, 29),
            (14, "scott", "D103", "ca", 8000, 36),
            (15, "jen", "D102", "ny", 9500, 38),
            (16, "jeff", "D103", "uk", 9100, 35),
            (17, "maria", "D101", "ny", 7900, 40)
        ]

        return spark.createDataFrame(data, schema=schema)

    def create_department_df(spark: SparkSession):
        schema = StructType([
            StructField("dept_id", StringType(), True),
            StructField("dept_name", StringType(), True)
        ])

        data = [
            ("D101", "sales"),
            ("D102", "finance"),
            ("D103", "marketing"),
            ("D104", "hr"),
            ("D105", "support")
        ]

        return spark.createDataFrame(data, schema=schema)

    def create_country_df(spark: SparkSession):
        schema = StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True)
        ])

        data = [
            ("ny", "newyork"),
            ("ca", "California"),
            ("uk", "Russia")
        ]

        return spark.createDataFrame(data, schema=schema)

    def find_avg_salary_by_department(df: DataFrame):
        return df.groupBy("department").agg({"salary": "avg"}).withColumnRenamed("avg(salary)", "avg_salary")

    def find_employees_with_m(df: DataFrame):
        return df.filter(col("employee_name").startswith("m")).select("employee_name", "department")

    def add_bonus_column(df: DataFrame):
        return df.withColumn("bonus", col("salary") * 2)

    def reorder_columns(df: DataFrame):
        return df.select("employee_id", "employee_name", "salary", "State", "Age", "department")

    def perform_joins(emp_df: DataFrame, dept_df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
        inner_join = emp_df.join(dept_df, emp_df.department == dept_df.dept_id, "inner")
        left_join = emp_df.join(dept_df, emp_df.department == dept_df.dept_id, "left")
        right_join = emp_df.join(dept_df, emp_df.department == dept_df.dept_id, "right")
        return inner_join, left_join, right_join

    def replace_state_with_country(emp_df: DataFrame, country_df: DataFrame):
        return emp_df.join(country_df, emp_df.State == country_df.country_code, "inner") \
            .select("employee_id", "employee_name", "department", "country_name", "salary", "Age")

    def convert_columns_to_lowercase(df: DataFrame):
        lower_case_df = df.toDF(*[c.lower() for c in df.columns])
        return lower_case_df.withColumn("load_date", current_date())

    def write_external_tables(df: DataFrame, spark: SparkSession):
        df.write.mode("overwrite").parquet("user/employee_parquet")
        df.write.mode("overwrite").csv("user/employee_csv", header=True)