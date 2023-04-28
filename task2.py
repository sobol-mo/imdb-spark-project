import pyspark.sql.functions as f

import settings as sts
from read_write import write


def task2(df, write_in_file):
    df = df.withColumn('birthYear', f.when(f.col('birthYear').isin('0', None), None)
                       .otherwise(f.to_date(f.col('birthYear'), 'y')))
    df.printSchema()
    df = (df.select(f.col('primaryName')
                    # , f.col('birthYear')
                    ).where((f.col('birthYear') >= f.lit('1801-01-01')) & (f.col('birthYear') < f.lit('1901-01-01'))))
    df.show()
    if write_in_file:
        write(df, sts.BORN_IN_19TH_PATH)
