from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from env import sfOptions
import logging

class Starting():
    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()
    df = spark.read.csv(
        r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\Raw_ordered_file.csv\\part-00000-89d37724-aa2f-4077-a945-2866107d8341-c000.csv", header=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def cleansed(self):
        try:
            self.df = self.spark.read.csv(r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\Raw_ordered_file.csv\\part-00000-89d37724-aa2f-4077-a945-2866107d8341-c000.csv" , header=True)
            self.df.show()

            self.df = self.df.withColumn("Orderdate",to_date("Orderdate","M/d/yyyy"))\
                         .withColumn("Duedate",to_date("Duedate","d/M/yyyy"))\
                         .withColumn("Shipdate",to_date("Shipdate","d/M/yyyy"))\
                         .withColumn('OrderQuantity', regexp_replace('OrderQuantity', 'Nan', '1'))

            self.df.printSchema()
            self.df.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()
    def color_list(self):
            self.color_list = (self.df.select('color').distinct().
                           rdd.flatMap(lambda x: x).collect())

    def transformation(self):
            self.df = self.df.select(F.col("OrderNumber"),
                    F.split(F.col("ProductName"), ",").getItem(0).alias("ProductName"),
                    F.split(F.col("ProductName"), ",").getItem(1).alias("size"),
                    F.col("Color"),
                    F.col("Category"),
                    F.col("Subcategory"),
                    F.col("ListPrice"),
                    F.col("Orderdate"),
                    F.col("Duedate"),
                    F.col("Shipdate"),
                    F.col("SalesRegion"),
                    F.col("OrderQuantity"),
                    F.col("UnitPrice"),
                    F.col("SalesAmount"),
                    F.col("DiscountAmount"),
                    F.col("TaxAmount"),
                    F.col("Freight"),
                    F.col("PromotionName"))
            self.df.show()

    def trans(self):
            self.df = self.df.withColumn('ProductName', F.regexp_replace('ProductName', '|'.join(self.color_list), ''))\
                         .withColumn('ProductName', F.regexp_replace('ProductName', '- ', ''))\
                         .withColumn('size',F.regexp_replace('size','|'.join(self.color_list),'Na'))\
                         .na.fill("Na")\
                         .withColumn('OrderQuantity', col('OrderQuantity').cast('int'))\
                         .withColumn('size', regexp_replace('size', 'null','Na'))
    def hive(self):
            self.df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
                 "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\cleansed_ordered_file.csv")

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
        starting.color_list()
    except Exception as e:
        logging.error('Error at %s', 'color list', exc_info=e)
        sys.exit(1)
    try:
        starting.transformation()
    except Exception as e:
        logging.error('Error at %s', 'color list', exc_info=e)
        sys.exit(1)
    try:
        starting.trans()
    except Exception as e:
        logging.error('Error at %s', 'trans', exc_info=e)
        sys.exit(1)

    try:
        starting.hive()
    except Exception as e:
        logging.error('Error at %s', 'hive', exc_info=e)
        sys.exit(1)
    try:
        starting.snowflake()
    except Exception as e:
        logging.error('Error at %s', 'hive', exc_info=e)
        sys.exit(1)
