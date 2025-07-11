from pyspark.sql import SparkSession
from src.assignment_5.util import EmployeeAnalysisUtils

def main():
    spark = SparkSession.builder \
        .appName("EmployeeAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        employee_df = EmployeeAnalysisUtils.create_employee_df(spark)
        department_df = EmployeeAnalysisUtils.create_department_df(spark)
        country_df = EmployeeAnalysisUtils.create_country_df(spark)

        avg_salary_df = EmployeeAnalysisUtils.find_avg_salary_by_department(employee_df)
        avg_salary_df.show()


        employees_with_m_df = EmployeeAnalysisUtils.find_employees_with_m(employee_df)
        employees_with_m_df.show()


        employee_with_bonus_df = EmployeeAnalysisUtils.add_bonus_column(employee_df)
        employee_with_bonus_df.show()


        reordered_employee_df = EmployeeAnalysisUtils.reorder_columns(employee_df)
        reordered_employee_df.show()


        inner_join_df, left_join_df, right_join_df = EmployeeAnalysisUtils.perform_joins(employee_df, department_df)
        inner_join_df.show()
        left_join_df.show()
        right_join_df.show()


        country_replaced_df = EmployeeAnalysisUtils.replace_state_with_country(employee_df, country_df)
        country_replaced_df.show()


        final_df = EmployeeAnalysisUtils.convert_columns_to_lowercase(country_replaced_df)
        final_df.show()


        EmployeeAnalysisUtils.write_external_tables(final_df, spark)

    finally:

        spark.stop()

if __name__ == "__main__":
    main()