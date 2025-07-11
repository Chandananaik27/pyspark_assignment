import pytest
import logging
from pyspark.sql import SparkSession
from src.assignment_1.util import *

class TestCustomerProductPurchase:

    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.appName("TestCustomerProductPurchase").getOrCreate()
        cls.purchase_data_df = purchased_data(cls.spark)
        cls.product_data_df = create_product_data_df(cls.spark)

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    def setup_method(self):
        self.logger = logging.getLogger(__name__)

    def test_find_customers_with_only_iphone13(self):
        find_customers_with_iphone13(self.purchase_data_df, self.logger)

    def test_find_customers_upgraded_to_iphone14(self):
        find_customers_upgraded_to_iphone14(self.purchase_data_df, self.logger)

    def test_find_customers_bought_all_products(self):
        find_customers_bought_all_products(self.purchase_data_df, self.product_data_df, self.logger)