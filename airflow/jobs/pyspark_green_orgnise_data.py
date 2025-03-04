import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.functions import md5, concat, col, coalesce, lit
import logging

import os 
import sys

current_file_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(current_file_path))
sys.path.append(parent_directory)

from common.file_config import Taxi_Config
from common.schema import GREEN_SCHEMA

logger = logging.getLogger(__name__)

logger.info('go into this file')

spark = SparkSession.builder \
                .master('local') \
                .appName('orgnise_green_dataset') \
                .getOrCreate()

taxi = Taxi_Config('green')
schema = GREEN_SCHEMA

dataset = spark.read.parquet(f'{taxi.local_input_path}/{taxi.table_name}', schema = schema)

dataset = dataset.withColumnRenamed(taxi.pickup_col_name(), 'pickup_datetime')\
                        .withColumnRenamed(taxi.dropoff_col_name(), 'dropoff_datetime')\
                        .withColumn('service_type', funcs.lit(taxi.service_type))\
                        .withColumn('pickup_month', funcs.to_date(funcs.date_trunc('month', 'pickup_datetime')))

dataset = dataset.withColumn("passenger_count", col("passenger_count").cast("int"))

dataset = dataset.withColumn("unique_row_id",
                                md5(
                                        concat(
                                        coalesce(col("VendorID").cast("string"), lit('')),
                                        coalesce(col("pickup_datetime").cast("string"), lit('')),
                                        coalesce(col("dropoff_datetime").cast("string"), lit('')),
                                        coalesce(col("PULocationID").cast("string"), lit('')),
                                        coalesce(col("DOLocationID").cast("string"), lit('')),
                                        coalesce(col("fare_amount").cast("string"), lit('')),
                                        coalesce(col("trip_distance").cast("string"), lit(''))
                                        )
                                )
                                )

logger.info(f'{taxi.service_type} data has changed column names and add new column service_type')
dataset.write\
        .parquet(f'{taxi.local_output_path}/', mode='overwrite') 

        # .partitionBy(["pickup_month"]) \