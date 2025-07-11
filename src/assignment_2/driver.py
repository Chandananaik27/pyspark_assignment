from pyspark.sql import SparkSession
from src.assignment_2.util import CreditCardAnalysisUtils


def main():
    spark = SparkSession.builder \
        .appName("CreditCardAnalysis") \
        .getOrCreate()

    try:

        credit_card_df = CreditCardAnalysisUtils.create_credit_card_df(spark)

        CreditCardAnalysisUtils.print_number_of_partitions(credit_card_df)

        credit_card_df = CreditCardAnalysisUtils.increase_partition_size(credit_card_df, 5)
        CreditCardAnalysisUtils.print_number_of_partitions(credit_card_df)

        credit_card_df = CreditCardAnalysisUtils.decrease_partition_size(credit_card_df, 1)
        CreditCardAnalysisUtils.print_number_of_partitions(credit_card_df)

        masked_card_df = CreditCardAnalysisUtils.create_masked_card_number_df(credit_card_df)
        masked_card_df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()