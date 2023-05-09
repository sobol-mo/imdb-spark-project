"""Task 2. Get the list of peopleʼs names, who were born in the 19th century"""
import pyspark.sql.functions as f

import settings as sts
from read_write import write_spark


def task2(source_df, write_in_file):
    """Task 2. Get the list of peopleʼs names, who were born in the 19th century
    To get the result launch the function in the main
    :param source_df:source data frame to get data
    :param write_in_file:logic key whether to write results in file
    :return:void
    """
    source_df = source_df.withColumn('birthYear', f.when(f.col('birthYear').isin('0', None), None)
                                     .otherwise(f.to_date(f.col('birthYear'), 'y')))
    source_df.printSchema()
    source_df = (source_df.select(f.col('primaryName')
                                  # , f.col('birthYear')
                                  ).where((f.col('birthYear') >= f.lit('1801-01-01'))
                                          & (f.col('birthYear') < f.lit('1901-01-01'))))
    source_df.show()
    if write_in_file:
        write_spark(source_df, sts.BORN_IN_19TH_PATH)
