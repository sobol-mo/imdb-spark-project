import pyspark.sql.functions as f

import settings as sts
from read_write import write


def task1(df, write_in_file):
    df = df.select(f.col('title')).where(f.col('region') == 'UA')
    df.show()
    if write_in_file:
        write(df, sts.UA_TITLES_PATH)
