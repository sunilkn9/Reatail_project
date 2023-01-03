from pyspark.sql import *
from pyspark.sql.functions import *
import logging
from env import sfOptions
from os.path import abspath

class Start():
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()
    df = spark.read.csv(r"C:\kafka\Retail_result_op.csv",header=True)


    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3(self):
        try:
            self.df =self.spark.read.csv(r"C:\kafka\Retail_result_op.csv",header=True)
            self.df.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()
            self.df.show(truncate = False)


    def write_to_hive(self):

        self.df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save("C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\Raw_ordered_file.csv")
        self.df.coalesce(1).write.format("csv").mode("overwrite").option("header", True).saveAsTable("raw")
        self.df.show(truncate=False)

    def connect_to_snowflake(self):

        self.df.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
            r"raw_ordered_details")).mode("overwrite").options(header=True).save()


if __name__ == "__main__":
    # Start
    start = Start()
    try:
        start.read_from_s3()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    try:
        start.connect_to_snowflake()
    except Exception as e:
        logging.error('Error at %s', 'write to snowflake', exc_info=e)

    try:
        start.write_to_hive()
    except Exception as e:
        logging.error('Error at %s', 'write_to_hive', exc_info=e)
        sys.exit(1)
