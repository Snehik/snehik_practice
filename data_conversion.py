from pyspark.sql.functions import upper


def upper_attribute(in_df):
    out_df = in_df.select(upper('student_name').alias('student_name'))
    return out_df
