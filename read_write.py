"""
Read Write module
"""


def read_spark_df(spark_session, path, schema):
    return spark_session.read.csv(path,
                                  header=True,
                                  nullValue='null',
                                  dateFormat='yyyy',
                                  sep='\t',
                                  schema=schema,
                                  )


def write(df, path):
    # df.write.csv(path, header=True, mode='overwrite')
    # df.coalesce(1).write.csv('output/ua_titles_task1.csv')
    df.toPandas().to_csv(path, mode='w', index=False)

def write_2(df, path):
    # repartition the DataFrame to a single partition
    df_single = df.repartition(1)

    # write the DataFrame to a single file
    df_single.write.mode("overwrite").option("header", "true").csv(path)

