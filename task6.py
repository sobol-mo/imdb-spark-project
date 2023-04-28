import pyspark.sql.functions as f

import settings as sts
from read_write import write, read_spark_df


def task6(spark_session, write_in_file):
    title_basics_df = read_spark_df(spark_session, sts.TITLE_BASICS_PATH, sts.title_basics_schema)

    title_episode_df = read_spark_df(spark_session, sts.TITLE_EPISODE_PATH, sts.title_episode_schema)

    title_episode_df = (title_episode_df.withColumn('seasonNumber', f.when(f.col('seasonNumber') == '\\N', 0)
                                                    .otherwise(f.col('seasonNumber')).cast('int')))
    title_episode_df = (title_episode_df.withColumn('episodeNumber', f.when(f.col('episodeNumber') == '\\N', 0)
                                                    .otherwise(f.col('episodeNumber')).cast('int')))

    monk_episodes_df = title_episode_df.where(f.col('parentTconst') == 'tt0312172').orderBy(
        ['seasonNumber', 'episodeNumber'])  # Monk TV Series
    monk_episodes_df.show(monk_episodes_df.count(), False)
    print('Thus, the amount of episodes equals to the amount of parentTconst rows')

    title_basics_df = title_basics_df.withColumnRenamed('tconst', 'parentTconst')
    series_grouped = (title_episode_df.filter((f.col('seasonNumber') != 0) & (f.col('episodeNumber') != 0))
                      .groupBy('parentTconst').count()
                      .orderBy('count', ascending=False))
    series_grouped = series_grouped.limit(50)
    series_grouped.show()
    title_basics_df = title_basics_df.select(f.col('parentTconst'), f.col('primaryTitle'), f.col('titleType'))
    series_grouped = series_grouped.join(title_basics_df, on='parentTconst', how='left')
    series_grouped = series_grouped.orderBy('count', ascending=False)
    series_grouped.show()

    if write_in_file:
        write(series_grouped, sts.AMOUNT_OF_EPISODES_PATH)
