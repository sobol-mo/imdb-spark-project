"""Task 3. Get titles of all movies that last more than 2 hours"""
import pyspark.sql.functions as f

import settings as sts
from read_write import write_spark


def task3(source_df, write_in_file):
    """Task 3. Get titles of all movies that last more than 2 hours
    To get the result launch the function in the main
    :param source_df:source data frame to get data
    :param write_in_file:logic key whether to write results in file
    :return:void
    """
    source_df = (source_df.select(f.col('primaryTitle')
                                  , f.col('originalTitle')
                                  # , f.col('runtimeMinutes')
                                  ).where((f.col('runtimeMinutes') > 2*60)))
    source_df.show()
    if write_in_file:
        write_spark(source_df, sts.MORE_THAN_2HR_PATH)
