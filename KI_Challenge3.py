from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



class DimensionalModelling:
    """
    This is the class for dimensional modelling of a e-commerce company data which includes customer data,
    transaction data and product data
    """
    def __init__(self, csv_path_in, json_path_in, xml_path_in):
        self.csv_path_in = csv_path_in
        self.json_path_in = json_path_in
        self.xml_path_in = xml_path_in

    @classmethod
    def modelling(self, spark_session):
        pass

    def main(self):
        spark_sess = SparkSession.builder \
            .master("local[12]") \
            .appName("Dimensional Modelling App") \
            .getOrCreate()
        spark_sess.conf.set("mapreduce.fileoutputcommiter.marksuccessfuljobs", "false")
        self.modelling(spark_sess)
        spark_sess.stop()

if __name__ == "__main__":
    csv_path_in ="customer_data.csv"
    json_path_in = "transactions.json"
    xml_path_in = "products.xml"
    dimensional_modelling = DimensionalModelling(csv_path_in, json_path_in, xml_path_in)
    dimensional_modelling.main()