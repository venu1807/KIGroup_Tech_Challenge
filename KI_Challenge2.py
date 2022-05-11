from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import parser


class BatchProcessing:
    """
    This class is used for batch processing the data from the Met Museum Collection in Newyork.
    """
    def __init__(self, csv_path_in, csv_path_out):
        self.csv_path_in = csv_path_in
        self.csv_path_out = csv_path_out

    def data_consumption(self, spark_session):
        """
        This method is used to load the data by the user defined schema
        :param spark_session: instantiate the spark session
        :return: Return the loaded data frame
        """
        spark = spark_session

        batch_schema = StructType([StructField('Object Number', StringType(), True ),
                                   StructField('Is Highlight', StringType(), True),
                                   StructField('Is Timeline Work', StringType(), True),
                                   StructField('Is Public Domain', StringType(), True),
                                   StructField('Object ID', StringType(), True),
                                   StructField('Gallery Number', StringType(), True),
                                   StructField('Department', StringType(), True),
                                   StructField('AccessionYear', StringType(), True),
                                   StructField('Object Name', StringType(), True),
                                   StructField('Title', StringType(), True),
                                   StructField('Culture', StringType(), True),
                                   StructField('Period', StringType(), True),
                                   StructField('Dynasty', StringType(), True),
                                   StructField('Reign', StringType(), True),
                                   StructField('Portfolio', StringType(), True),
                                   StructField('Constituent ID', StringType(), True),
                                   StructField('Artist Role', StringType(), True),
                                   StructField('Artist Prefix', StringType(), True),
                                   StructField('Artist Display Name', StringType(), True),
                                   StructField('Artist Display Bio', StringType(), True),
                                   StructField('Artist Suffix', StringType(), True),
                                   StructField('Artist Alpha Sort', StringType(), True),
                                   StructField('Artist Nationality', StringType(), True),
                                   StructField('Artist Begin Date', StringType(), True),
                                   StructField('Artist End Date', StringType(), True),
                                   StructField('Artist Gender', StringType(), True),
                                   StructField('Artist ULAN URL', StringType(), True),
                                   StructField('Artist Wikidata URL', StringType(), True),
                                   StructField('Object Date', StringType(), True),
                                   StructField('Object Begin Date', StringType(), True),
                                   StructField('Object End Date', StringType(), True),
                                   StructField('Dimensions', StringType(), True),
                                   StructField('Geography Type', StringType(), True),
                                   StructField('City', StringType(), True),
                                   StructField('State', StringType(), True),
                                   StructField('County', StringType(), True),
                                   StructField('Country', StringType(), True),
                                   StructField('Region', StringType(), True),
                                   StructField('Subregion', StringType(), True),
                                   StructField('Locale', StringType(), True),
                                   StructField('Locus', StringType(), True),
                                   StructField('Excavation', StringType(), True),
                                   StructField('River', StringType(), True),
                                   StructField('Classification', StringType(), True),
                                   StructField('Rights and Reproduction', StringType(), True),
                                   StructField('Link Resource', StringType(), True),
                                   StructField('Object Wikidata URL', StringType(), True),
                                   StructField('Metadata Date', StringType(), True),
                                   StructField('Repository', StringType(), True),
                                   StructField('Tags', StringType(), True),
                                   StructField('Tags AAT URL', StringType(), True),
                                   StructField('Tags Wikidata URL', StringType(), True)
                                   ]

                )
        csv_big_data = spark.read.format("csv")\
                       .option("header", "true")\
                       .option("mode", "DROPMALFORMED")\
                       .load("MetObjects.csv")

        csv_big_data.printSchema()
        csv_big_data.count()
        return csv_big_data

    def pre_processing(self, spark_session):
        """
        This method is used for pre processing loaded data frame
        :param spark_session:
        :return:
        """
        csv_big_data = self.data_consumption(spark_session)
        dimension_query = csv_big_data.select("Dimensions")
        dimension_query.withColumn("height", \
                      when((dimension_query.Dimensions != "Dimensions unavailable"), lit("0.5")) \
                      .otherwise(lit("null")) \
                      ).show(20, truncate=False)

    def dimension_value(self, dimension_query):
        p = re.compile('(?<!\d|\.)\d+(?:\.\d+)?\s*?(?:cm)(?!\w)')
        print(dimension_query)
        x = dimension_query
        units = p.findall(x)
        print(units)
        values = []
        for i in range(len(units)):
            print(units[i])
            p2 = re.compile('(?<!\d|\.)\d+(?:\.\d+)')
            value = p2.findall(units[i])
            print(value)
            values.append(value[0])
        print(values)
        return values

    def main(self):
        """
        This main method is used for calling other class methods
        :return:
        """
        spark_sess = SparkSession.builder \
            .master("local[12]") \
            .appName("Batch Processing App") \
            .getOrCreate()
        spark_sess.conf.set("mapreduce.fileoutputcommiter.marksuccessfuljobs", "false")
        self.pre_processing(spark_sess)
        spark_sess.stop()



if __name__ == "__main__":
    csv_path_in = "."
    csv_path_out = "."
    batch_process = BatchProcessing(csv_path_in, csv_path_out)
    batch_process.main()