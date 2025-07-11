from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, current_date, datediff


class UserActivityUtils:

    def create_user_activity_df(spark: SparkSession) -> DataFrame:
        schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("user_activity", StringType(), True),
            StructField("time_stamp", TimestampType(), True)
        ])

        data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]

        return spark.createDataFrame(data, schema=schema)

    def calculate_user_actions(df: DataFrame) -> DataFrame:
        return (df.filter(datediff(current_date(), col("time_stamp")) <= 7)
                .groupBy("user_id")
                .count()
                .withColumnRenamed("count", "action_count"))

    def convert_timestamp_to_date(df: DataFrame) -> DataFrame:
        return df.withColumn("login_date", to_date(col("time_stamp"))) \
            .drop("time_stamp")


    def write_to_csv(df: DataFrame, path: str):
        df.write.csv(path, mode="overwrite", header=True)


    def write_to_managed_table(df: DataFrame, spark: SparkSession):
        df.write.mode("overwrite").saveAsTable("user.login_details")