from logger import Log4j
from pyspark.sql import SparkSession


class md_read:
    def __int__(self):
        pass

    logger = Log4j(spark=SparkSession.builder.getOrCreate())

    def data_read(self, file_path, schema):
        spark_obj = SparkSession.builder.getOrCreate()
        self.logger.info("read the file")
        data_df = spark_obj.read.csv(file_path, schema)
        return data_df