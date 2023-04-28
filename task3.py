import pyspark.sql.functions as f

import settings as sts
from read_write import write


def task3(df, write_in_file):
    df = (df.select(f.col('primaryTitle')
                    , f.col('originalTitle')
                    # , f.col('runtimeMinutes')
                    ).where((f.col('runtimeMinutes') > 2*60)))
    df.show()
    if write_in_file:
        write(df, sts.MORE_THAN_2HR_PATH)
