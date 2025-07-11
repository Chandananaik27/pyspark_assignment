import pytest
from pyspark.sql import SparkSession
from src.assignment_3.util import UserActivityUtils


class TestUserActivity:

    def setup_class(cls):
        cls.spark = SparkSession.builder \
            .appName("UserActivityTests") \
            .master("local[2]") \
            .getOrCreate()

    def teardown_class(cls):
        cls.spark.stop()

    def test_user_activity_df_creation(self):
        df = UserActivityUtils.create_user_activity_df(self.spark)
        assert df.count() == 8
        assert len(df.columns) == 4

    def test_calculate_user_actions(self):
        df = UserActivityUtils.create_user_activity_df(self.spark)
        actions_df = UserActivityUtils.calculate_user_actions(df)
        assert actions_df.count() == 3  # Expecting 3 users with actions in the last 7 days
        assert "action_count" in actions_df.columns

    def test_convert_timestamp_to_date(self):
        df = UserActivityUtils.create_user_activity_df(self.spark)
        updated_df = UserActivityUtils.convert_timestamp_to_date(df)
        assert "login"