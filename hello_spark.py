import sys
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from data_conversion import convert
from logger import Log4j
from read_from_file import md_read


if __name__ == "__main__":
    spark = SparkSession.builder.appName("testing_log").getOrCreate()

    logger = Log4j(spark)
    obj_rd = md_read(spark, logger)
    obj_cnv = convert(logger)

    config = configparser.ConfigParser()
    config.read(r'config.ini')
    sec = config.sections()
    inp_fl = config[sec[0]].get('file_path')

    logger.info("Started Application with application id {0}".format(spark.conf.get("spark.app.id")))
    logger.info("Application name is {0}".format(spark.conf.get("spark.app.name")))
    logger.info("Input file name for the application is {0}".format(inp_fl))

    schema = StructType([StructField("student_id", IntegerType(), True),
                         StructField("student_name", StringType(), True),
                         StructField("student_address", StringType(), True)])

    try:
        in_df = obj_rd.data_read(inp_fl, schema)
        out_df = obj_cnv.upper_attribute(in_df)
        out_df.show(truncate=False)
        logger.info("Converting Name Attribute To Upper Is Completed")
    except:
        logger.error("File does not exist")
        logger.error("Termination Application")
        sys.exit(1)
    logger.info("Application Ended")
