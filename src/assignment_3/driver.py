from pyspark.sql import SparkSession
from src.assignment_3.util import UserActivityUtils

def main():
    spark = SparkSession.builder \
        .appName("User ActivityAnalysis") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        user_activity_df = UserActivityUtils.create_user_activity_df(spark)

        user_actions_df = UserActivityUtils.calculate_user_actions(user_activity_df)
        user_actions_df.show()

        updated_df = UserActivityUtils.convert_timestamp_to_date(user_activity_df)
        updated_df.show()

        UserActivityUtils.write_to_csv(updated_df, "user_activity.csv")

        UserActivityUtils.write_to_managed_table(updated_df, spark)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()