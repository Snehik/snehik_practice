from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType, StringType


def data_read(file_path, schema):
    spark_obj = SparkSession.builder.getOrCreate()
    data_df = spark_obj.read.csv(file_path, schema)
    return data_df