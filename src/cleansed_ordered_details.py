from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from env import sfOptions
import logging
from os.path import abspath

class Starting():
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession.builder.enableHiveSupport().config("spark.sql.warehouse.dir", warehouse_location).config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()
    df = spark.read.csv(
        r"C:\Users\Sunil Kumar\PycharmProjects\Reatail_project\outputs\Raw_ordered_file.csv\part-00000-ef580ff9-377c-49ca-8a68-3002ff27478c-c000.csv", header=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def cleansed(self):
        try:
            self.df = self.spark.read.csv(r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\Raw_ordered_file.csv\\part-00000-ef580ff9-377c-49ca-8a68-3002ff27478c-c000.csv" , header=True)
            self.df.show()

            self.df = self.df.dropDuplicates()\
                         .withColumn("ProductName", regexp_replace("ProductName", ",", "-"))\
                         .withColumn('OrderQuantity', regexp_replace('OrderQuantity', 'Nan', '1')) \
                        .withColumn('OrderQuantity', col('OrderQuantity').cast('int'))\
                         .withColumn(('SalesAmount'),col('OrderQuantity')*col('UnitPrice'))

            #.withColumn("Orderdate", to_date("Orderdate", "d/MM/yyyy")) \
                #  .withColumn("Duedate",to_date("Duedate","d/MM/yyyy"))\
        #  .withColumn("Shipdate",to_date("Shipdate","d/M/yyyy")) \
            self.df.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()

    def hive(self):
            self.df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
                 "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\cleansed_ordered_file.csv")
            self.df.coalesce(1).write.format("csv").mode("overwrite").option("header", True).saveAsTable("cleansed")
    def snowflake(self):
            self.df.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
                  r"cleansed_order_details")).mode("overwrite").options(header=True).save()
            self.df.show(truncate=False)

if __name__ == "__main__":
    starting = Starting()
    try:
        starting.cleansed()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    try:
        starting.snowflake()
    except Exception as e:
        logging.error('Error at %s', 'snowflake', exc_info=e)
        sys.exit(1)

    try:
        starting.hive()
    except Exception as e:
        logging.error('Error at %s', 'hive', exc_info=e)
        sys.exit(1)