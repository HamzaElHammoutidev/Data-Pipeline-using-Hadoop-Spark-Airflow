import findspark
findspark.init()



class Checker():
    def __init__(self,spark):
        self.spark = spark

    def data_quatity(self,df):
        # running data quality checks by running some queries to check if it is the right data that I am lookgin for
        df.createOrReplaceTempView('taxidriver')

        # Number of Courses By hour
        self.spark.sql(
            'SELECT pickup_hour as Pickup_Hour, COUNT(*) Courses_Number FROM taxidriver GROUP BY pickup_hour ORDER BY Courses_Number DESC ').show()
        # Number Of Course By Vendor
        self.spark.sql('SELECT vendor_id as Vendor, COUNT(*) Courses_Number FROM taxidriver GROUP BY vendor_id ').show()
        # Number of Courses By Week Day
        self.spark.sql(
            'SELECT pickup_dayoftheweek as Week_Day, COUNT(*) Courses_Number FROM taxidriver GROUP BY pickup_dayoftheweek ').show()
        # Number of Courses By Payment  Type
        self.spark.sql(
            "SELECT payment_type as Payment_Type, COUNT(*) as Courses_Number from taxidriver GROUP BY payment_type").show()
        # Sum of trip distance by month and vendor
        self.spark.sql((
                      "SELECT  pickup_month as MONTH, vendor_id as VENDOR , SUM(trip_distance) as SUM_DISTANCE from taxidriver GROUP BY pickup_month , vendor_id ")).show()
        # Average Trip Duration By day
        self.spark.sql(
            'SELECT pickup_dayoftheweek as Week_Day , AVG(duration) as Average_trip_duration ,COUNT(*) Courses_Number FROM taxidriver GROUP BY pickup_dayoftheweek').show()

