import pyspark.sql.functions as f

import settings as sts
from read_write import write, read_spark_df


def task4(spark_session, write_in_file):
    title_principals_df = read_spark_df(spark_session, sts.TITLE_PRINCIPALS_PATH, sts.title_principals_schema)
    name_basics_df = read_spark_df(spark_session, sts.NAME_BASICS_PATH, sts.name_basics_schema)
    title_basics_df = read_spark_df(spark_session, sts.TITLE_BASICS_PATH, sts.title_basics_schema)

    title_principals_df = (title_principals_df.select(f.col('tconst'), f.col('nconst'), f.col('category'),
                                                      f.col('characters'))
                           .where((f.col('category').isin('actress', 'actor'))))

    expanded_imdb_df = (title_principals_df.join(name_basics_df, on='nconst', how='left')
                        .join(title_basics_df, on='tconst', how='left'))
    expanded_imdb_df = expanded_imdb_df.select(f.col('primaryName'), f.col('primaryTitle'), f.col('characters'))
    expanded_imdb_df.show()

    if write_in_file:
        write(expanded_imdb_df, sts.ACTORS_PATH)
