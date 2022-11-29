from pyspark.sql import *
from pyspark.sql.functions import *
from env import sfOptions
import logging
import pyspark.sql.functions as F

class Started():
    spark = SparkSession.builder.enableHiveSupport().config('spark.jars.packages',
                                                            'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()
    df = spark.read.csv(r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\cleansed_ordered_file.csv\\part-00000-d8cd1c9a-6fa0-4bd5-b8d4-37a5c77b6701-c000.csv", header=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def cleansed(self):
        try:
            df = self.spark.read.csv(r"C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\cleansed_ordered_file.csv\\part-00000-d8cd1c9a-6fa0-4bd5-b8d4-37a5c77b6701-c000.csv", header=True)
            self.df.show()
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)

        else:
            self.df.printSchema()
            self.df.show(truncate=False)

    def curated(self):

          self.df2 = self.df.withColumn("sales_frei_tax_dis",self.df["SalesAmount"]-self.df["TaxAmount"]-self.df["Freight"]-self.df["DiscountAmount"])

          self.df2.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
                 "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\curated_ordered_file.csv")

          self.df2.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
              r"curated_ordered_details")).mode(
              "overwrite").options(header=True).save()


    def category(self):
          self.df1 = self.df.groupBy("Category").agg(sum("SalesAmount").alias("total")).sort(col("total").desc())
          self.df1.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
              "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\category_ordered_file.csv")

          self.df1.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
              r"category_ordered_details")).mode(
              "overwrite").options(header=True).save()

    def subcategory(self):
          self.df2 = self.df.select(self.df1.Category,self.df2.Subcategory,self.df2.SalesAmount)
          self.df3 =self.df2.groupBy("Subcategory").agg(sum("SalesAmount").alias("total")).sort(col("total").desc()).limit(10)
          self.df3.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Users\\Sunil Kumar\\PycharmProjects\\Reatail_project\\src\\internalfiles\\sub_category_ordered_file.csv")

          self.df3.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
            r"sub_category_ordered_details")).mode(
            "overwrite").options(header=True).save()


if __name__ == "__main__":
    started = Started()
    try:
        started.cleansed()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    try:
        started.curated()
    except Exception as e:
        logging.error('Error at %s', 'write_to_s3', exc_info=e)
        sys.exit(1)

    try:
        started.category()
    except Exception as e:
        logging.error('Error at %s', 'write to hive', exc_info=e)

    try:
      started.subcategory()
    except Exception as e:
      logging.error('Error at %s', 'write to hive', exc_info=e)


