import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from com.snehik.data_conversion import upper_attribute
from com.snehik.logger import Log4j
from com.snehik.read_from_file import data_read

if __name__ == "__main__":
    spark = SparkSession.builder.appName("testing_log").getOrCreate()
    file_path = r'D:\snehik_practice\spark_test\input\students.csv'

    logger = Log4j(spark)
    logger.info("Started Application with application id {0}".format(spark.conf.get("spark.app.id")))

    schema = StructType([StructField("student_id", IntegerType(), True),
                         StructField("student_name", StringType(), True),
                         StructField("student_address", StringType(), True)])

    try:
        in_df = data_read(file_path, schema)
        out_df = upper_attribute(in_df)
        out_df.show(truncate=False)
        logger.info("Converting Name Attribute To Upper Is Completed")
    except:
        logger.error("File does not exist")
        logger.error("Termination Application")
        sys.exit(1)
    logger.info("Application Ended")