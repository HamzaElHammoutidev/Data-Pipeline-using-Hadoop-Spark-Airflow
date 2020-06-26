import findspark
findspark.init()


from pyspark.sql.functions import  hour,minute,month,dayofweek,year
from pyspark.sql import functions as F
import os


class Processer():
    @staticmethod
    def processing_data(df):
        df = df.withColumn("pickup_hour", hour(F.col('pickup_datetime'))).withColumn('pickup_dayoftheweek', dayofweek(
            F.col('pickup_datetime'))). \
            withColumn('pickup_month', month(F.col('pickup_datetime'))).withColumn('year_pickup', year(
            F.col('pickup_datetime'))).withColumn('pickup_minute', minute(F.col('pickup_datetime')))

        df = df.withColumn("dropoff_hour", hour(F.col('dropoff_datetime'))).withColumn('dropoff_dayoftheweek',
                                                                                       dayofweek(
                                                                                           F.col('dropoff_datetime'))). \
            withColumn('dropoff_month', month(F.col('dropoff_datetime'))).withColumn('dropoff_year', year(
            F.col('dropoff_datetime'))).withColumn('dropoff_minute', minute(F.col('dropoff_datetime')))

        # df = df.withColumn('duration',(F.col('dropoff_minute') - F.col('pickup_minute')))
        # df = df.withColumn('duration',(datediff(F.col('dropoff_datetime'),F.col('pickup_datetime'))))
        df = df.withColumn('duration', (
                    (F.unix_timestamp('dropoff_datetime') - F.unix_timestamp('pickup_datetime')) / 60)).dropDuplicates()

        return df