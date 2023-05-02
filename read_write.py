"""
Read Write module
"""


def read_spark_df(spark_session, path, schema):
    """Read spark data frame from tab seperated text file
    :param spark_session:
    :param path:
    :param schema:
    :return:
    """
    return spark_session.read.csv(path,
                                  header=True,
                                  nullValue='null',
                                  dateFormat='yyyy',
                                  sep='\t',
                                  schema=schema,
                                  )


def write(source_df, path):
    """Write spark data frame to comma seperated text file
    :param source_df: spark data frame to write
    :param path: path
    :return: void
    """
    source_df.toPandas().to_csv(path, mode='w', index=False)


def write_spark(source_df, path):
    """Write spark data frame to comma seperated text file
    :param source_df:  spark data frame to write
    :param path: path
    :return: void
    """
    source_df.write.csv(path, header=True, mode='overwrite')
    # source_df.coalesce(1).write.csv('output/ua_titles_task1.csv')


def write_2(source_df, path):
    """Experiments
    :param source_df:
    :param path:
    :return: void
    """
    # repartition the DataFrame to a single partition
    df_single = source_df.repartition(1)

    # write the DataFrame to a single file
    df_single.write.mode("overwrite").option("header", "true").csv(path)
