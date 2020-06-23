import  findspark
findspark.init()

from helpers.gather import Gather
from helpers.sampler import Sampler

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('hadoop-nyc-taxi-driver').getOrCreate()


paths = {
    "taxidata":"hdfs://localhost:9000/user/Hadoop/taxidriver/nyc_taxi_data_2014.csv"
}

# Gathering
Gather = Gather(spark,paths)
taxi_data = Gather.get_taxi_nyc_data()

# Sampling
Sampler.taxi_data_sampling(taxi_data,"sampled/taxi_sample.xlsx")


