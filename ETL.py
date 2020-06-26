import  findspark
findspark.init()

from helpers.gather import Gather
from helpers.sampler import Sampler
from helpers.processer import Processer
from helpers.checker import  Checker

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('hadoop-nyc-taxi-driver').getOrCreate()


paths = {
    #"taxidata":"hdfs://localhost:9000/user/Hadoop/taxidriver/nyc_taxi_data_2014.csv"
    "taxidata":"data/nyc_taxi_data_2014.csv"
}

# Gathering
Gather = Gather(spark,paths)
taxi_data = Gather.get_taxi_nyc_data()

# Sampling for exploration
#Sampler.taxi_data_sampling(taxi_data,"sampled/taxi_sample.xlsx")

# Processing
taxi_processed_data = Processer.processing_data(taxi_data)

# Data partiitoning by hour
taxi_processed_data = taxi_processed_data.repartition("pickup_hour")


# export data to parquets :
taxi_processed_data.write.partitionBy("pickup_hour").parquet("data/output/taxidriverdata")

# data quality checks
checker = Checker(spark)
checker.data_quatity(taxi_processed_data)



