from pyspark.sql import SparkSession
from src.assignment_4.util import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EmployeeDataProcessing") \
        .enableHiveSupport() \
        .getOrCreate()

    json_path = "resources/nested_json_file.json"

    df = read_json_dynamic(spark, json_path)

    flat_df = flatten_df(df)


    original_count = count_records(df)
    flat_count = count_records(flat_df)
    print(f"Original count: {original_count}, Flattened count: {flat_count}")


    exploded, exploded_outer, pos_exploded = differentiate_explode(df, "employees")


    filtered_df = filter_id(df, 1)  # Since your JSON had id = 1001, adjust as needed


    snake_case_df = camel_to_snake(flat_df)


    dated_df = add_load_date(snake_case_df)


    partitioned_df = add_year_month_day(dated_df)


    write_to_table(partitioned_df)

    spark.stop()