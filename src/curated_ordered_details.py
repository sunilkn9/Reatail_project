from pyspark.sql import *
from pyspark.sql.functions import *
from env import sfOptions
import logging
import pyspark.sql.functions as F
from os.path import abspath

class Started():
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession.builder.enableHiveSupport().config("spark.sql.warehouse.dir", warehouse_location).config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()
    df = spark.read.csv(r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\cleansed_ordered_file.csv\\part-00000-8dcd9b07-5406-4619-84a2-8a164d262aa9-c000.csv", header=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def cleansed(self):
        try:
            self.df = self.spark.read.csv(r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\cleansed_ordered_file.csv\\part-00000-8dcd9b07-5406-4619-84a2-8a164d262aa9-c000.csv", header=True)
            self.df.show()
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)

        else:
            self.df.printSchema()
            self.df.show(truncate=False)

    def curated(self):

          self.df2 = self.df.withColumn("salesamt-pstded",self.df["SalesAmount"]-self.df["TaxAmount"]-self.df["Freight"]-self.df["DiscountAmount"])

          self.df2.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
                 "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\curated_ordered_file.csv")


    def category(self):
          self.df1 = self.df2.groupBy("ProductName").agg(F.sum("salesamt-pstded").alias("salesamt-pstded"), F.sum("SalesAmount").alias("SalesAmount")).orderBy(F.col("salesamt-pstded").desc())
          self.w3 = Window.partitionBy("ProductName").orderBy(F.col("salesamt-pstded").desc())
          self.df3 = self.df1.withColumn("row", F.row_number().over(self.w3)) \
                         .filter(F.col("row") < 11).drop("row")
          self.df3.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
              "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\curated_ordered_file.csv")

    def products(self):
          self.df4 = self.df2.groupBy("ProductName","Category").agg(F.sum("salesamt-pstded").alias("salesamt-pstded")).orderBy(F.col("salesamt-pstded").desc())
          self.w2 = Window.partitionBy("ProductName").orderBy(F.col("salesamt-pstded").desc())
          self.df5 = self.df4.withColumn("row", F.row_number().over(self.w2)) \
                             .filter(F.col("row") < 11).drop("row")

          self.df5.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\outputs\\products_ordered_file.csv")

    def snowflake(self):

          self.df2.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
                    r"curated_ordered_details")).mode("overwrite").options(header=True).save()
          self.df5.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
              r"category_ordered_details")).mode( "overwrite").options(header=True).save()
          self.df3.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
              r"products_ordered_details")).mode("overwrite").options(header=True).save()

    def hive(self):
        self.df2.coalesce(1).write.format("csv").mode("overwrite").option("header", True).saveAsTable("curated")\
             .df1.coalesce(1).write.format("csv").mode("overwrite").option("header", True).saveAsTable("category")\
             .df3.coalesce(1).write.format("csv").mode("overwrite").option("header", True).saveAsTable("products")
if __name__ == "__main__":
    started = Started()
    try:
        started.cleansed()
    except Exception as e:
        logging.error('Error at %s', 'cleansed', exc_info=e)
        sys.exit(1)

    try:
        started.curated()
    except Exception as e:
        logging.error('Error at %s', 'curated', exc_info=e)
        sys.exit(1)

    try:
        started.products()
    except Exception as e:
        logging.error('Error at %s', 'subcategory', exc_info=e)

    try:
        started.category()
    except Exception as e:
        logging.error('Error at %s', 'category', exc_info=e)

    try:
        started.snowflake()
    except Exception as e:
        logging.error('Error at %s', 'snowflake', exc_info=e)

    try:
        started.hive()
    except Exception as e:
        logging.error('Error at %s', 'snowflake', exc_info=e)

