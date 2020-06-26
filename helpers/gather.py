import findspark
findspark.init()


from pyspark.sql.types import StructField,StructType, IntegerType,StringType,DoubleType
from pyspark import SparkContext
import os
class Gather():
    csv_schema = StructType([
        StructField('vendor_id', StringType()),
        StructField('pickup_datetime', StringType()),
        StructField('dropoff_datetime', StringType()),
        StructField('passenger_count', IntegerType()),
        StructField('trip_distance', DoubleType()),
        StructField('pickup_longitude', DoubleType()),
        StructField('pickup_latitude', DoubleType()),
        StructField('rate_code', IntegerType()),
        StructField('store_and_fwd_flag', StringType()),
        StructField('dropoff_longitude', DoubleType()),
        StructField('dropoff_latitude', DoubleType()),
        StructField('payment_type', StringType()),
        StructField('fare_amount', DoubleType()),
        StructField('surcharge', DoubleType()),
        StructField('mta_tax', DoubleType()),
        StructField('tip_amount', DoubleType()),
        StructField('tolls_amount', DoubleType()),
        StructField('total_amount', DoubleType())
    ])
    sc = SparkContext.getOrCreate()
    def __init__(self,spark,paths):
        self.spark = spark
        self.paths = paths

    # Reading Files Types Helpers
    def read_csv_data(self,file_path,delimiter=","):
        return self.spark.read.option("delimiter",delimiter).option('header','true').csv(file_path,schema=Gather.csv_schema)

    def get_taxi_nyc_data(self):
        return self.read_csv_data(self.paths["taxidata"])