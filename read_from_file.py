class md_read:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def data_read(self, file_path, schema):
        self.logger.info("read the file")
        data_df = self.spark.read.csv(file_path, schema)
        return data_df