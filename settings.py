"""
Module this settings
"""
import pyspark.sql.types as t

TITLE_AKAS_PATH = 'imdb-data/title.akas.tsv.gz'
TITLE_BASICS_PATH = 'imdb-data/title.basics.tsv.gz'
TITLE_CREW_PATH = 'imdb-data/title.crew.tsv.gz'
TITLE_EPISODE_PATH = 'imdb-data/title.episode.tsv.gz'
TITLE_PRINCIPALS_PATH = 'imdb-data/title.principals.tsv.gz'
TITLE_RATINGS_PATH = 'imdb-data/title.ratings.tsv.gz'
NAME_BASICS_PATH = 'imdb-data/name.basics.tsv.gz'

UA_TITLES_PATH = 'output/ua_titles_task1.csv'
BORN_IN_19TH_PATH = 'output/born_in_19th_task2.csv'
MORE_THAN_2HR_PATH = 'output/more_than_2hs_task3.csv'
ACTORS_PATH = 'output/actors_movies_characters_task4.csv'
COUNT_ADULT_PATH = 'output/count_adult_movies_task5.csv'
AMOUNT_OF_EPISODES_PATH = 'output/amount_of_episodes_task6.csv'
MOST_POPULAR_BY_DECADES_PATH = 'output/most_popular_by_decades_task7.csv'
MOST_POPULAR_BY_GENRE_PATH = 'output/most_popular_by_genre_task8.csv'

title_akas_schema = t.StructType([t.StructField('titleId', t.StringType(), False),
                                  t.StructField('ordering', t.IntegerType(), False),
                                  t.StructField('title', t.StringType(), True),
                                  t.StructField('region', t.StringType(), True),
                                  t.StructField('language', t.StringType(), True),
                                  t.StructField('types', t.StringType(), True),
                                  t.StructField('attributes', t.StringType(), True),
                                  t.StructField('isOriginalTitle', t.IntegerType(), True),
                                  ])
title_basics_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                    t.StructField('titleType', t.StringType(), True),
                                    t.StructField('primaryTitle', t.StringType(), True),
                                    t.StructField('originalTitle', t.StringType(), True),
                                    t.StructField('isAdult', t.StringType(), True),
                                    t.StructField('startYear', t.DateType(), True),
                                    t.StructField('endYear', t.StringType(), True),
                                    t.StructField('runtimeMinutes', t.IntegerType(), True),
                                    t.StructField('genres', t.StringType(), True),
                                    ])
title_crew_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                  t.StructField('directors', t.StringType(), True),
                                  t.StructField('writers', t.StringType(), True),
                                  ])
title_episode_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                     t.StructField('parentTconst', t.StringType(), True),
                                     t.StructField('seasonNumber', t.StringType(), True),
                                     t.StructField('episodeNumber', t.StringType(), True),
                                     ])
title_principals_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                        t.StructField('ordering', t.IntegerType(), False),
                                        t.StructField('nconst', t.StringType(), False),
                                        t.StructField('category', t.StringType(), True),
                                        t.StructField('job', t.StringType(), True),
                                        t.StructField('characters', t.StringType(), True),
                                        ])
title_ratings_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                     t.StructField('averageRating', t.StringType(), True),
                                     t.StructField('numVotes', t.IntegerType(), True),
                                     ])
name_basics_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                                   t.StructField('primaryName', t.StringType(), True),
                                   t.StructField('birthYear', t.StringType(), True),
                                   t.StructField('deathYear', t.StringType(), True),
                                   t.StructField('primaryProfession', t.StringType(), True),
                                   t.StructField('knownForTitles', t.StringType(), True),
                                   ])
