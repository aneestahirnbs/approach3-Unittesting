import argparse
from datetime import datetime, date, time, timedelta
from pyspark.sql.functions import *
from functools import reduce
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrameReader
from pyspark.sql.window import *

NEW_TIME_STAMP_COLUMN_NAME = 'temp_time_column'
DAILY_DATA_PREFIX = 'daily_data'
EXISTING_DATA_PREFIX = 'existing_data'


def main(argv):

    DAILY_DATA = 'daily_data'
    EPOCH_TIME_COL_NAME = 'epoch_record_time'

    additional_col_name = argv.partition_key + '__temp'
    spark_context, sql_context = get_spark_context(argv.env_context)
    try:
        raw_df = sql_context.read.load(argv.raw_data_source, format=argv.file_format)
        # get_frame_with_columns(raw_df)
        data_with_columns = raw_df.selectExpr(
            *tuple(get_columns_with_table_prefix(DAILY_DATA)))  # .withColumn() , current_timestamp())

        data_with_columns.cache()
        non_current_df = data_with_columns
        daily_uniq_data_df = get_latest_and_previous_data(data_with_columns,
                                                          '{0}_{1}'.format(DAILY_DATA, argv.verison_key),
                                                          '{0}_{1}'.format(DAILY_DATA, argv.primary_key))
        daily_data_df = daily_uniq_data_df.withColumn('{0}_{1}'.format(DAILY_DATA, EPOCH_TIME_COL_NAME),
                                                      current_timestamp())

        part_key_name = '{0}_{1}'.format(DAILY_DATA, argv.partition_key)

        new_cols = daily_data_df.schema.names[:-2]
        partkey_index = new_cols.index(part_key_name)

        all_data_added_column = daily_data_df.withColumn(additional_col_name, daily_data_df[part_key_name])
        new_cols.insert(partkey_index + 1, additional_col_name)
        reordered_data = all_data_added_column.selectExpr(*tuple(new_cols)).withColumnRenamed(part_key_name,
                                                                                              argv.partition_key)
        reordered_data.repartition(30).write.partitionBy(argv.partition_key).mode('overwrite').parquet(
            argv.hdfs_target_path)

        data_added_column = non_current_df.withColumn(additional_col_name, non_current_df[part_key_name]).withColumn(
            '{0}_{1}'.format(DAILY_DATA, EPOCH_TIME_COL_NAME),
            current_timestamp())
        non_current_reordered_data = data_added_column.selectExpr(*tuple(new_cols)).withColumnRenamed(part_key_name,
                                                                                                      argv.partition_key)
        non_current_reordered_data.repartition(30).write.partitionBy(argv.partition_key).mode('overwrite').parquet(
            argv.hdfs_archive_path)

        # cmd = 's3-dist-cp --src {0} --dest {1}'.format(argv.hdfs_target_path[:-1], argv.processed_bucket_path[:-1])
        # subprocess.check_call(cmd, shell=True)
        # cmd = 's3-dist-cp --src {0} --dest {1}'.format(argv.hdfs_archive_path[:-1], argv.archive_target_path[:-1])
        # subprocess.check_call(cmd, shell=True)

    finally:
        spark_context.stop()


##########################################   UTILITY 	################################################

def get_spark_context(env_context):
    # get AWS Access key nad Secret Key
    if (env_context == 'local'):
        config = (SparkConf().setMaster(env_context).setAppName("Myapp"))
    else:
        config = (SparkConf().setAppName("Myapp"))

    sc = SparkContext(conf=config)
    sql_context = SQLContext(sc)

    return sc, sql_context


def get_file_schema(feed_name, file_type):
    return ["property_id", "address", "covered_area", "owner_id", "last_sale_date", "create_date", "status_id",
            "status_desc", "price", "postal_zone", "zip_code", "city", "state", "country", "version", "version_time"]


def get_frame_with_columns(raw_data, additional_columns=[]):
    old_columns = raw_data.schema.names[:]
    new_columns = get_file_schema('dummy', 'dummy')[:]

    new_columns.extend(additional_columns)
    df = reduce(lambda data, idx: data.withColumnRenamed(old_columns[idx], new_columns[idx]), range(len(old_columns)),
                raw_data)
    return df


def get_columns_with_table_prefix(table_name):
    renamed_cols = (['{0} AS {1}_{2}'.format(s, table_name, s) for s in get_file_schema('', '')])
    return renamed_cols


def get_latest_and_previous_data(historical_data, verison_key, primary_key):
    historical_part_data = historical_data.withColumn("row_num", row_number().over(
        Window.partitionBy(primary_key).orderBy(desc(verison_key))))
    latests = historical_part_data.where(historical_part_data.row_num == 1)
    return latests.drop('row_num')


##########################################   UTILITY 	################################################


if __name__ == '__main__':
    print('Job started on: ' + time.strftime(datetime.now().time(), '%Y-%m-%d %I:%M:%S %p'))
    print('\n')
    parser = argparse.ArgumentParser(description="")

    parser.add_argument("-raw_source_path", "--raw_data_source", help="raw data source (daily delta)",
                        default="s3a://snp-poc-raw/Data/Unzipped/InitialLoad50GB-Parquet/")
    parser.add_argument("-proc_dest_path", "--processed_bucket_path", help="processed data path",
                        default='s3://snp-poc-processed/versioned/')  # /user/hadoop/app-3-ammar-target/
    parser.add_argument("-archive_source_path", "--archive_target_path", help="Archive Table Path on S3",
                        default='s3://snp-poc-processed/versioned/')  # /user/hadoop/app-3-ammar-archive/ #

    parser.add_argument("-hdfs_trg_path", "--hdfs_target_path", help="Temp Write location on HDFS",
                        default='/user/hadoop/app-3-ammar-target/')
    parser.add_argument("-hdfs_arch_path", "--hdfs_archive_path", help="Temp Write location on HDFS",
                        default='/user/hadoop/app-3-ammar-archive/')

    parser.add_argument("-app_context", "--env_context", help="local\ EMR", default='no-local')
    parser.add_argument("-file_format", "--file_format", help="Aro / parquet", default='parquet')

    parser.add_argument("-partition_key", "--partition_key", help="Partition Column Name", default='city')
    parser.add_argument("-primary_key", "--primary_key", help="Primary Key Column Name", default='property_id')
    parser.add_argument("-verison_key", "--verison_key", help="On which versioning will be done", default='version')

    args = parser.parse_args()
    main(args)
    print('\n')
    print('Job ended on: ' + time.strftime(datetime.now().time(), '%Y-%m-%d %I:%M:%S %p'))
