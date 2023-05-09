"""Task 8. Get 10 titles of the most popular movies/series etc. by each genre.
"""
import pyspark.sql.functions as f
from pyspark.sql import Window

import settings as sts
from read_write import write_spark, read_spark_df


def task8(spark_session, write_in_file):
    """Task 8. Get 10 titles of the most popular movies/series etc. by each genre.
    :param spark_session: spark session
    :param write_in_file: logic key whether to write results in file
    :return: void
    """
    title_basics_df = read_spark_df(spark_session,
                                    sts.TITLE_BASICS_PATH, sts.title_basics_schema)
    title_basics_df = (title_basics_df
                       .select(f.col('tconst'), f.col('titleType'), f.col('primaryTitle'),
                               f.explode(f.split('genres', ',')).alias("genre")))
    title_basics_df = (title_basics_df
                       .withColumn('genre', f.when(f.col('genre') == '\\N', None)
                                   .otherwise(f.col('genre'))))
    title_basics_df = title_basics_df.filter(f.col('genre').isNotNull())
    # title_basics_df.show()

    title_ratings_df = read_spark_df(spark_session,
                                     sts.TITLE_RATINGS_PATH, sts.title_ratings_schema)
    # title_ratings_df.show()

    ratings_df = (title_basics_df.join(title_ratings_df, on='tconst', how='left'))
    # ratings_df.show()

    print('The full list of the most popular 10 groups of movies/series etc. by each genre: ')
    window = Window.partitionBy('genre').orderBy(f.desc('averageRating'))
    most_popular_df = (ratings_df.withColumn('rank', f.dense_rank().over(window))
                       .filter('rank<=10')
                       .orderBy(f.asc('genre')))
    most_popular_df.show(50)

    print('The 10 titles of the most popular movies/series etc. by each genre: ')
    window = Window.partitionBy('genre', 'averageRating').orderBy(f.desc('averageRating'))
    most_popular_df = (most_popular_df
                       .withColumn('max_votes', f.max('numVotes')
                                   .over(window)).orderBy(f.asc('genre'), 'rank'))
    # most_popular_df.show(50)
    most_popular_df = (most_popular_df.filter('numVotes == max_votes')
                       .select(f.col('primaryTitle'), f.col('genre'), f.col('averageRating'),
                               f.col('rank'), f.col('max_votes'))
                       .orderBy(f.asc('genre'), 'rank'))
    most_popular_df.show(50)

    if write_in_file:
        write_spark(most_popular_df, sts.MOST_POPULAR_BY_GENRE_PATH)
