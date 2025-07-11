from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

def read_json_dynamic(spark, path, schema: StructType = None):
    if schema:
        return spark.read.schema(schema).json(path)
    else:
        return spark.read.json(path)

def flatten_df(df: DataFrame):

    complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if isinstance(field.dataType, StructType)])
    while complex_fields:
        col_name = list(complex_fields.keys())[0]
        expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in complex_fields[col_name]]]
        df = df.select("*", *expanded).drop(col_name)
        complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if isinstance(field.dataType, StructType)])
    return df

def count_records(df: DataFrame):
    return df.count()

def differentiate_explode(df: DataFrame, col_name: str):
    exploded = df.withColumn("exploded", F.explode(F.col(col_name)))
    exploded_outer = df.withColumn("exploded_outer", F.explode_outer(F.col(col_name)))
    pos_exploded = df.withColumn("pos_exploded", F.posexplode(F.col(col_name)))
    return exploded, exploded_outer, pos_exploded

def filter_id(df: DataFrame, id_value):
    return df.filter(F.col("id") == id_value)

def camel_to_snake(df: DataFrame):
    new_columns = [F.col(col).alias(''.join(['_' + c.lower() if c.isupper() else c for c in col]).lstrip('_')) for col in df.columns]
    return df.select(*new_columns)

def add_load_date(df: DataFrame):
    return df.withColumn("load_date", F.current_date())

def add_year_month_day(df: DataFrame):
    return df \
        .withColumn("year", F.year(F.col("load_date"))) \
        .withColumn("month", F.month(F.col("load_date"))) \
        .withColumn("day", F.dayofmonth(F.col("load_date")))

def write_to_table(df: DataFrame):
    df.write.mode("overwrite") \
        .format("json") \
        .partitionBy("year", "month", "day") \
        .option("path", "/tmp/employee/employee_details") \
        .saveAsTable("employee.employee_details")