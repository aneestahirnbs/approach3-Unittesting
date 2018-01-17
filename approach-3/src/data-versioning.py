import argparse
import boto3
import utility as u
from datetime import datetime, time
from functools import reduce
from pyspark.sql.functions import *


def main(argv):
    DAILY_DATA = 'daily_data'
    EXISTING_DATA = 'existing_data'
    EPOCH_TIME_COL_NAME = 'epoch_record_time'
    TEMP_FOLDER_PATH = argv.temp_folder_path

    additional_col_name = argv.partition_key + '__temp'
    spark_context, sql_context = u.get_spark_context(argv.env_context)

    raw_df = sql_context.read.load(argv.raw_data_source, format=argv.file_format)

    raw_df.cache()

    # renaming column names to avoid duplication after join Transform
    daily_uniq_data_df = raw_df.selectExpr(
        *tuple(get_columns_with_table_prefix(DAILY_DATA)))

    daily_data_df = daily_uniq_data_df.withColumn('{0}_{1}'.format(DAILY_DATA, EPOCH_TIME_COL_NAME),
                                                  current_timestamp())
    # Loading only selected partitions which are going to be updated
    distinct_partitions_name = daily_data_df.select(
        col('{0}_{1}'.format(DAILY_DATA, argv.partition_key)).alias(argv.partition_key)).distinct().collect()

    partition_paths = [
        '{0}{1}={2}/*'.format(argv.processed_bucket_path, argv.partition_key, patition[argv.partition_key].title()) for
        patition in distinct_partitions_name]

    # load_partitions_path = [(argv.processed_bucket_path + 'city=Adjuntas/*'),  (argv.processed_bucket_path + 'city=Aguada/*')]

    loaded_existing_data = sql_context.read.load(partition_paths, format=argv.file_format)
    loaded_existing_data.cache()
    col_list = get_columns_with_table_prefix(EXISTING_DATA)
    col_list.extend(['{0} AS {1}_{2}'.format(EPOCH_TIME_COL_NAME, EXISTING_DATA, EPOCH_TIME_COL_NAME)])
    existed_data = get_frame_with_columns(loaded_existing_data, [EPOCH_TIME_COL_NAME]) \
        .selectExpr(*tuple(col_list))

    existing_non_match = existed_data.join(daily_data_df, (
        daily_data_df['{0}_{1}'.format(DAILY_DATA, argv.primary_key)] == existed_data[
            '{0}_{1}'.format(EXISTING_DATA, argv.primary_key)]), 'left_outer') \
        .where(col('{0}_{1}'.format(DAILY_DATA, argv.primary_key)).isNull())

    cols_to_get = get_file_schema('', '')
    cols_to_get.extend([EPOCH_TIME_COL_NAME])

    existing_non_match_data = existing_non_match.selectExpr(
        *tuple(['{0}_{1}'.format(EXISTING_DATA, s) for s in cols_to_get]))

    combined_data = daily_data_df.union(existing_non_match_data)

    # Adding duplicate column which will be removed after partition
    new_cols = daily_data_df.schema.names[:]
    part_key_name = '{0}_{1}'.format(DAILY_DATA, argv.partition_key)
    partkey_index = new_cols.index(part_key_name)

    all_data_added_column = combined_data.withColumn(additional_col_name, combined_data[part_key_name])

    new_cols.insert(partkey_index + 1, additional_col_name)
    reordered_data = all_data_added_column.selectExpr(*tuple(new_cols)).withColumnRenamed(part_key_name,
                                                                                          argv.partition_key)
    reordered_data.repartition(30).write.partitionBy(argv.partition_key).mode('overwrite').parquet(
        TEMP_FOLDER_PATH)  # E:/SnP/versioned/

    reordered_data.repartition(30).write.partitionBy(argv.partition_key).mode('append').parquet(
        argv.archive_source_path)

    spark_context.stop()
    ###################################################################

    # Moving resultant data from Temp location to S3

    bucket_name, client, dest_prfix = delete_partitions_from_latests(argv, distinct_partitions_name)
    copy_from_temp_to_actual(TEMP_FOLDER_PATH, bucket_name, client, dest_prfix)


def delete_partitions_from_latests(argv, distinct_partitions_name):
    # Deleting partitions from actual location
    partition_names = [patition[argv.partition_key].title() for patition in distinct_partitions_name]
    full_path = argv.processed_bucket_path;
    bucket_path = full_path.split('//')[1]
    bucket_name = bucket_path.split('/')[0]
    bucket_folders = bucket_path.split('/')[1:]
    dest_prfix = '/'.join(bucket_folders)
    client = boto3.client('s3')
    response = client.list_objects(Bucket=bucket_name, Prefix=dest_prfix)
    response_content = response['Contents']
    remote_keys = [item['Key'] for item in response_content]
    matching_keys = []
    for remote_key in remote_keys:
        containing_list = [s for s in partition_names if s in remote_key]
        if len(containing_list) != 0:
            dict = {}
            dict['Key'] = remote_key
            matching_keys.append(dict)
    del_response = client.delete_objects(
        Bucket=bucket_name,
        Delete={
            'Objects': matching_keys, 'Quiet': True
        }
    )
    return bucket_name, client, dest_prfix


def copy_from_temp_to_actual(TEMP_FOLDER_PATH, bucket_name, client, dest_prfix):
    # Copying files from Temporary location to actual locations
    temp_bucket_path = TEMP_FOLDER_PATH.split('//')[1]
    temp_bucket_name = temp_bucket_path.split('/')[0]
    temp_bucket_folders = temp_bucket_path.split('/')[1:]
    temp_prefix = '/'.join(temp_bucket_folders)
    response = client.list_objects(Bucket=temp_bucket_name, Prefix=temp_prefix)
    temp_response_content = response['Contents']
    temp_remote_keys = [item['Key'] for item in temp_response_content]
    matching_keys = []
    s3 = boto3.resource('s3')
    for temp_key in temp_remote_keys:
        copy_source = {
            'Bucket': temp_bucket_name,
            'Key': temp_key
        }
        folders = temp_key.split('/')
        if (len(folders) >= 3):
            key_to_copy = '{0}{1}/{2}'.format(dest_prfix, folders[-2], folders[-1])
            print('::AMMAR::' + key_to_copy)
            s3.meta.client.copy(copy_source, bucket_name, key_to_copy)


##########################################   UTILITY FUNCTIONS	################################################


def get_file_schema(feed_name, file_type):
    return ["property_id", "address", "covered_area", "owner_id", "last_sale_date", "create_date", "status_id",
            "status_desc", "price", "postal_zone", "zip_code", "city", "state", "country"]


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


##########################################  END UTILITY  FUNCTIONS	################################################

if __name__ == '__main__':
    print('Job started on: ' + time.strftime(datetime.now().time(), '%Y-%m-%d %I:%M:%S %p'))
    print('\n')
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("-raw_source_path", "--raw_data_source", help="raw data source (daily delta)",
                        default="s3a://snp-poc-raw/Data/Unzipped/Daily5_5city-Parquet/")
    parser.add_argument("-proc_dest_path", "--processed_bucket_path", help="processed data path",
                        default='s3a://snp-poc-processed/versioned/DEC-15-target/')
    parser.add_argument("-app_context", "--env_context", help="local\ EMR", default='no-local')
    parser.add_argument("-file_format", "--file_format", help="Aro / parquet", default='parquet')
    parser.add_argument("-partition_key", "--partition_key", help="Partition Column Name", default='city')
    parser.add_argument("-primary_key", "--primary_key", help="Primary Key Column Name", default='property_id')
    parser.add_argument("-archive_source_path", "--archive_source_path", help="Primary Key Column Name",
                        default='s3a://snp-poc-processed/versioned/DEC-15-archive/')
    parser.add_argument("-temp_folder_path", "--temp_folder_path", help="Temp Path",
                        default='s3a://snp-poc-processed/TEMP_F_APP-3/')

    args = parser.parse_args()
    main(args)
    print('\n')
    print('Job ended on: ' + time.strftime(datetime.now().time(), '%Y-%m-%d %I:%M:%S %p'))
