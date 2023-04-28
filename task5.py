import pyspark.sql.functions as f

import settings as sts
from read_write import write, read_spark_df


def task5(spark_session, write_in_file):
    title_basics_df = read_spark_df(spark_session, sts.TITLE_BASICS_PATH, sts.title_basics_schema)
    title_akas_df = read_spark_df(spark_session, sts.TITLE_AKAS_PATH, sts.title_akas_schema)

    title_basics_df = title_basics_df.withColumn('isAdult', f.when(f.col('isAdult').isin('0', '1'), f.col('isAdult'))
                                                 .otherwise('\\N'))

    title_basics_df = title_basics_df.select(f.col('tconst'), f.col('isAdult')).where(f.col('isAdult') == '1')
    title_basics_df.show()

    title_akas_df = title_akas_df.withColumnRenamed('titleId', 'tconst')
    title_akas_df = title_akas_df.select(f.col('tconst'), f.col('region'))

    title_akas_df.show()

    expanded_imdb_df = title_basics_df.join(title_akas_df, on='tconst', how='left')
    expanded_imdb_df.show()

    expanded_imdb_df = expanded_imdb_df.groupBy('region').count().orderBy('count', ascending=False)
    expanded_imdb_df = expanded_imdb_df.limit(100)
    expanded_imdb_df.show()

    if write_in_file:
        write(expanded_imdb_df, sts.COUNT_ADULT_PATH)