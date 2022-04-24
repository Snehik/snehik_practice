from logger import Log4j
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper


class convert:

    def __init__(self):
        pass

    logger = Log4j(spark=SparkSession.builder.getOrCreate())

    def upper_attribute(self, in_df):
        self.logger.info("convert to uppercase method called")
        out_df = in_df.select(upper('student_name').alias('student_name'))
        return out_df