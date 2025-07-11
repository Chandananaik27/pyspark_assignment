from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


class CreditCardAnalysisUtils:

    def create_credit_card_df(spark) :
        card_data = [
            ("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)
        ]
        return spark.createDataFrame(card_data, ["card_number"])

    def print_number_of_partitions(df: DataFrame):
        print(f"Number of partitions: {df.rdd.getNumPartitions()}")


    def increase_partition_size(df: DataFrame, num_partitions: int) :
        return df.repartition(num_partitions)


    def decrease_partition_size(df: DataFrame, num_partitions: int):
        return df.coalesce(num_partitions)


    def mask_card_number(card_number: str) -> str:
        return '*' * (len(card_number) - 4) + card_number[-4:]


    def create_masked_card_number_df(df: DataFrame) -> DataFrame:
        mask_udf = udf(CreditCardAnalysisUtils.mask_card_number, StringType())
        return df.withColumn("masked_card_number", mask_udf(col("card_number")))