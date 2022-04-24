from pyspark.sql.functions import upper

class convert:
    def __init__(self, logger):
        self.logger = logger

    def upper_attribute(self, in_df):
        self.logger.info("convert to uppercase method called")
        out_df = in_df.select(upper('student_name').alias('student_name'))
        return out_df