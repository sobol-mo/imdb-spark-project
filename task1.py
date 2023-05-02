"""Task 1. Get all titles of series/movies etc. that are available in Ukrainian"""
import pyspark.sql.functions as f

import settings as sts
from read_write import write

def task1(source_df, write_in_file):
    """Task 1. Get all titles of series/movies etc. that are available in Ukrainian.
    To get the result launch the function in the main
    :param source_df:source data frame to get data
    :param write_in_file:logic key whether to write results in file
    :return:void
    """
    source_df = source_df.select(f.col('title')).where(f.col('region') == 'UA')
    source_df.show()
    if write_in_file:
        write(source_df, sts.UA_TITLES_PATH)
