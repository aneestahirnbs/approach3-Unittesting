
class RefactorUtility:

    def __init__(self, schema):
        self.schema = schema
        pass

    def get_columns_with_table_prefix(self, table_name):
        renamed_cols = (['{0} AS {1}_{2}'.format(s, table_name, s) for s in self.schema.get_file_schema('', '')])
        return renamed_cols

    def extract_bucket_directories(self,folder_path):
        temp_bucket_path = folder_path.split('//')[1]
        temp_bucket_name = temp_bucket_path.split('/')[0]
        temp_bucket_folders = temp_bucket_path.split('/')[1:]
        temp_prefix = '/'.join(temp_bucket_folders)
        return temp_bucket_path,temp_bucket_name,temp_bucket_folders,temp_prefix

    def copy_data_with_key_to_s3(self,temp_remote_keys,temp_bucket_name,s3_resource,bucket_name,dest_prfix):
        for temp_key in temp_remote_keys:
            copy_source = {
                'Bucket': temp_bucket_name,
                'Key': temp_key
            }
            folders = temp_key.split('/')
            if (len(folders) >= 3):
                key_to_copy = '{0}{1}/{2}'.format(dest_prfix, folders[-2], folders[-1])
                print('::AMMAR::' + key_to_copy)
                s3_resource.meta.client.copy(copy_source, bucket_name, key_to_copy)
        pass



    def extract_matching_keys(self,partition_names,remote_keys):
        matching_keys = []
        for remote_key in remote_keys:
            containing_list = [s for s in partition_names if s in remote_key]
            if len(containing_list) != 0:
                dict = {}
                dict['Key'] = remote_key
                matching_keys.append(dict)
        return matching_keys



    def delete_objects_with_key_from_s3(self,client,partition_names,remote_keys,bucket_name):
        matching_keys = self.extract_matching_keys(partition_names,remote_keys)

        del_response = client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': matching_keys, 'Quiet': True
            }
        )
        pass




