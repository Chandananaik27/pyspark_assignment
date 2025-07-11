import logging
from pyspark.sql import SparkSession

from util import purchased_data, create_product_data_df, find_customers_with_iphone13, \
    find_customers_upgraded_to_iphone14, find_customers_bought_all_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("Customer Product Purchase").getOrCreate()

purchase_data_df = purchased_data(spark)
product_data_df = create_product_data_df(spark)

logger.info("Customers who bought only iphone13:")
find_customers_with_iphone13(purchase_data_df, logger)

logger.info("Customers who upgraded from iphone13 to iphone14:")
find_customers_upgraded_to_iphone14(purchase_data_df, logger)

logger.info("Customers who bought all products:")
find_customers_bought_all_products(purchase_data_df, product_data_df, logger)