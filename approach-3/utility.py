from botocore.session import Session
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from functools import reduce


class Utility:

    def __init__(self):
        pass

    def get_spark_context(self, env_context):
        if env_context == 'local':
            session = Session()
            credentials = session.get_credentials()
            current_credentials = credentials.get_frozen_credentials()
            config = (SparkConf().setMaster(env_context).setAppName("Myapp"))
        else:
            config = (SparkConf().setAppName("Myapp"))

        sc = SparkContext(conf=config)

        if env_context == 'local':
            sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", current_credentials.access_key)
            sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", current_credentials.secret_key)
        else:
            pass
        sql_context = SQLContext(sc)
        sc.setLogLevel("ERROR")
        return sc, sql_context


    def get_frame_with_columns(self, raw_data, additional_columns=[]):
        old_columns = raw_data.schema.names[:]
        new_columns = self.get_file_schema('dummy', 'dummy')[:]

        new_columns.extend(additional_columns)
        df = reduce(lambda data, idx: data.withColumnRenamed(old_columns[idx], new_columns[idx]),
                    range(len(old_columns)),
                    raw_data)
        return df
