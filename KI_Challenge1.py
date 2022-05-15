from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Streaming:
    """
    This class is used for streaming the data from json files and storing it in csv and parquet formats
    in production 1 and production 2 respectively.
    """
    def __init__(self, json_path, csv_path, parquet_path):
        self.json_path = json_path
        self.csv_path = csv_path
        self.parquet_path = parquet_path

    def production_env_1(self, spark_session):
        """
        This method is used for streaming json files to production env 1 and storing it in csv format.
        :param spark_session:
        :return:
        """
        # Read JSON file into dataframe
        print(f"You are in production environment 1")
        spark = spark_session

        prod1_schema = StructType(
            [StructField('analytics', StructType([StructField('clicks',LongType(), True),
                                                  StructField('impressions',LongType(), True)]), True),
             StructField('datetime', StringType(), True),
             StructField('sales', StructType([StructField('quantity', LongType(), True),
                                              StructField('total_price', DoubleType(), True)]), True)]
        )

        raw_data = spark.readStream.format('json').schema(prod1_schema)\
            .option('multiline', 'true').option('maxFilesPerTrigger',1)\
            .load(self.json_path)\
            .withColumn("current_time_stamp", current_timestamp())

        #raw_data.printSchema()

        query1=raw_data.select(raw_data.datetime,(raw_data.analytics.clicks).alias("analytics_clicks"),\
                        (raw_data.analytics.clicks).alias("analytics_impressions"), \
                        (raw_data.sales.quantity).alias("sales_quantity"), \
                        (raw_data.sales.total_price).alias("sales_total_price"), \
                               (raw_data.current_time_stamp)\
                         )
        #query1.withColumn("current_time_stamp",current_timestamp())
        query1.printSchema()
        #query1.coalesce(1).write.option("header", True).csv(self.csv_path)
        #query1.show()

        print(f"Please press CTRL+C to stop the stream context after few seconds.")
        query1.writeStream\
            .format("csv") \
            .option("header", "true")\
            .option("path", self.csv_path) \
            .option("checkpointLocation", "data/csv_checkpoint") \
            .outputMode('append')\
            .start()\
            .awaitTermination(10)
        spark.stop()

    def production_env_2(self, spark_session):
        """
        This method is used for streaming csv files from production env 1 and
        transform and store in parquet format in production env 2
        :param spark_session:
        :return:
        """
        print(f"You are in production environment 2")
        spark= spark_session

        prod2_schema = StructType(
            [StructField('datetime', StringType(), True),
             StructField('analytics_clicks', LongType(), True),
             StructField('analytics_impressions', LongType(), True),
             StructField('sales_quantity', LongType(), True),
             StructField('sales_total_price', DoubleType(), True),
             StructField('current_time_stamp', TimestampType(), True)]
        )
        print(self.csv_path)

        csv_data = spark.readStream.schema(prod2_schema) \
            .format('csv')\
            .option('maxFilesPerTrigger', 1) \
            .option('header', 'true')\
            .load(self.csv_path)

        csv_data.printSchema()

        query2 = csv_data.select('analytics_clicks', 'analytics_impressions','sales_quantity', 'sales_total_price')\
                .groupBy().sum()

        """
        query2 = csv_data.select(csv_data.datetime, (csv_data.analytics.clicks).alias("analytics_clicks"), \
                                 (csv_data.analytics.clicks).alias("analytics_impressions"), \
                                 (csv_data.sales.quantity).alias("sales_quantity"), \
                                 (csv_data.sales.total_price).alias("sales_total_price"), \
                                 )
        query2.writeStream\
              .format("console")\
              .start()\
              .awaitTermination(5)
         """
        csv_data.writeStream \
            .format("parquet") \
            .option("header", "true") \
            .option("path", self.parquet_path) \
            .outputMode('append') \
            .option("checkpointLocation", "data/parquet_checkpoint") \
            .start() \
            .awaitTermination(10)


        spark.stop()



    def flatten_df(self, nested_df):
        """
        This method is used for flatteing data from json files
        :param nested_df:
        :return:
        """
        stack = [((), nested_df)]
        columns = []

        while len(stack) > 0:
            parents, df = stack.pop()

            flat_cols = [
                col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct"
            ]

            nested_cols = [c[0] for c in df.dtypes
                if c[1][:6] == "struct"
            ]

            columns.extend(flat_cols)

            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))

        return nested_df.select(columns)

    def main(self):
        """
        This main method is used for calling other class methods that exists here.
        :return:
        """
        spark_sess = SparkSession.builder \
          .master("local[12]") \
          .appName("Stream App") \
          .getOrCreate()
        spark_sess.conf.set("mapreduce.fileoutputcommiter.marksuccessfuljobs","false")
        print("You can execute the production 2 pipeline after production 1 pipeline is finished.values ")
        #self.production_env_1(spark_sess) #  Streaming for Production env 1
        self.production_env_2(spark_sess) # Streaming for Production env 2
        spark_sess.stop()


if __name__ == "__main__":
        json_path = "data/json_files/"
        csv_path = "data/csv_files/"
        parquet_path = "data/parquet_files/"
        stream_call = Streaming(json_path, csv_path, parquet_path)
        stream_call.main()

