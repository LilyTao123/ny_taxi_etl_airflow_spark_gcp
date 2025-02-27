#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs

import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-staging-us-central1-167271535503-o6nylhh4')


df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow)

df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')\
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_cols = []
yellow_cols = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_cols:
        common_cols.append(col)

df_green_sel = df_green.select(common_cols)\
        .withColumn('service_type', funcs.lit('green'))
df_yellow_sel = df_yellow.select(common_cols)\
        .withColumn('service_type', funcs.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable("trips_data")

df_results = spark.sql("""
SELECT  
    -- Revenue grouping 
    date_trunc('month', pickup_datetime) as revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) as revenue_monthly_fare,
    SUM(extra) as revenue_monthly_extra,
    SUM(mta_tax) as revenue_monthly_mta_tax,
    SUM(tip_amount) as revenue_monthly_tip_amount,
    SUM(tolls_amount) as revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    SUM(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    AVG(passenger_count) as avg_monthly_passenger_count,
    AVG(trip_distance) as avg_monthly_trip_distance

FROM trips_data
    GROUP BY revenue_month, service_type

        """)


df_results.write.format('bigquery')\
        .option('table', output).save()