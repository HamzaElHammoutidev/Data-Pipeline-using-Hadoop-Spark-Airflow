import findspark
findspark.init()


from pyspark.sql.types import StructField,StructType, IntegerType,StringType
from pyspark import SparkContext
import os
class Gather():

    sc = SparkContext.getOrCreate()
    def __init__(self,spark,paths):
        self.spark = spark
        self.paths = paths

    # Reading Files Types Helpers
    def read_csv_data(self,file_path,delimiter=","):
        return self.spark.read.option("delimiter",delimiter).option('header','true').csv(file_path)

    def get_taxi_nyc_data(self):
        return self.read_csv_data(self.paths["taxidata"])