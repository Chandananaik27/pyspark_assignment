import pytest
from pyspark.sql import SparkSession
from src.assignment_4.util import *

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestEmployee").master("local[2]").getOrCreate()

def test_read_json(spark):
    df = read_json_dynamic(spark, "/mnt/data/daafebac-b740-448d-972f-20e9e0b8b28f.json")
    assert df.count() == 1

def test_flatten(spark):
    df = read_json_dynamic(spark, "/mnt/data/daafebac-b740-448d-972f-20e9e0b8b28f.json")
    flat = flatten_df(df)
    assert flat is not None
    assert flat.count() == 1

def test_camel_to_snake(spark):
    df = spark.createDataFrame([(1, "TestName")], ["empId", "empName"])
    snake_df = camel_to_snake(df)
    assert "emp_id" in snake_df.columns
    assert "emp_name" in snake_df.columns

def test_add_load_date(spark):
    df = spark.createDataFrame([(1, "Test")], ["id", "name"])
    df = add_load_date(df)
    assert "load_date" in df.columns

def test_add_year_month_day(spark):
    df = spark.createDataFrame([(1, "Test")], ["id", "name"])
    df = add_load_date(df)
    df = add_year_month_day(df)
    assert "year" in df.columns
    assert "month" in df.columns
    assert "day" in df.columns