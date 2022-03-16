import os
import boto3
from typing import Iterator


class S3Handler:

    def __init__(self, client=None, resource=None):
        """
        :param client: a boto3 s3 client (eg. boto3.client('s3'))
        :param resource: a boto3 s3 resource (eg. boto3.resource('s3'))
        """
        if client is None:
            self._client = boto3.client('s3')
        else:
            self._client = client
        if resource is None:
            self._resource = boto3.resource('s3')
        else:
            self._resource = resource

    def read_file(self, bucket: str, key: str, decode: str = 'utf-8') -> str:
        """
        Read the contents of given file's key and return it as a string.
        :param bucket: bucket name
        :param key: key full path
        :param decode: apply given decoding on s3 object's contents. use empty string to not apply any decoding
        :return: file's contents
        """
        obj = self._resource.Object(bucket, key)
        content = obj.get()['Body'].read()
        if decode:
            content = content.decode(decode)
        return content

    def write_file(self, bucket: str, key: str, content: str) -> None:
        """
        Create s3 object at the given bucket+key location with the given content.
        :param bucket: bucket name
        :param key: key full path
        :param content: string to be written into the new file
        :return: None
        """
        obj = self._resource.Object(bucket, key)
        obj.put(Body=content)

    def copy_file(self, src_bucket: str, src_key: str, trg_bucket: str, trg_key: str) -> None:
        """
        Copy object from given source bucket and key to given target bucket and key.
        :param src_bucket: source bucket name
        :param src_key: source key
        :param trg_bucket: target bucket name
        :param trg_key:target key
        :return: None
        """
        src_full_key_path = os.path.join(src_bucket, src_key)
        trg_obj = self._resource.Object(trg_bucket, trg_key)
        trg_obj.copy_from(CopySource=src_full_key_path)

    def download_file(self, bucket: str, key: str, local_file_path: str) -> None:
        """
        Download given s3 file into local path.
        :param bucket: bucket where the s3 object sits
        :param key: path to s3 object
        :param local_file_path: path for the s3 object to be download to
        :return: None
        """
        local_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self._client.download_file(bucket, key, local_file_path)

    def upload_file(self, bucket: str, key: str, local_file_path: str) -> None:
        """
        Upload given local file to s3.
        :param bucket: bucket the file will be uploaded to
        :param key: the key for the uploaded file
        :param local_file_path: path for the s3 object to be download to
        :return: None
        """
        bucket_resource = self._resource.Bucket(bucket)
        return bucket_resource.upload_file(local_file_path, key)

    def delete_file(self, bucket: str, key: str) -> None:
        """
        Delete object with the given bucket+key.
        :param bucket: object's bucket
        :param key: object's key
        :return: None
        """
        self._resource.Object(bucket, key).delete()

    def get_file_size(self, bucket: str, key: str) -> int:
        """
        Get the size (in bytes) of specified object.
        :param bucket: object's bucket
        :param key: object's key
        :return: None
        """
        response = self._client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']

    def list_buckets(self) -> list:
        """Return a List of all buckets."""
        buckets = self._client.list_buckets()
        buckets = buckets['Buckets']
        buckets = [b['Name'] for b in buckets]
        return buckets

    def iterate_dirs(self, bucket: str, prefix: str = '') -> Iterator[str]:
        """
        Yield all directories + sub-directories under given bucket and prefix.
        :param bucket: bucket name
        :param prefix: prefix to get level 1 sub-directories under it
        :return: full path to sub-directory
        """

        # init
        page_exist = True
        continuation_token = ''

        while page_exist:

            # get page of dirs
            if continuation_token:
                response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/', ContinuationToken=continuation_token)
            else:
                response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')

            # yield dirs if there are any in current page
            if 'CommonPrefixes' in response:
                for subdir in response['CommonPrefixes']:
                    yield subdir['Prefix']

                    # recursively yield sub-dirs
                    yield from self.iterate_dirs(bucket, prefix=subdir['Prefix'])

            # page consist up to 1000 results (dirs) at a time.
            # if there's another page, the response will include a 'NextContinuationToken'.
            # save 'NextContinuationToken' if exists, for use in the next loop.
            if 'NextContinuationToken' in response:
                continuation_token = response['NextContinuationToken']
            else:
                page_exist = False

    def iterate_keys(self, bucket: str, prefix: str = '') -> Iterator[str]:
        """
        Yield all keys (files paths) under given bucket and prefix.
        :param bucket: bucket name
        :param prefix: prefix to get keys starting with it
        :return: full path to key
        """

        # init
        page_exist = True
        start_after_key = ''
        keys_per_page = 10000  # significantly faster than the default 1000 keys (resulting less boto3 calls)

        while page_exist:

            # get page of keys
            response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=keys_per_page, StartAfter=start_after_key)

            # yield keys from response
            keys = response.get('Contents', [])
            keys = [k['Key'] for k in keys]
            for k in keys:
                yield k

            # save last key from response, so that if there is another page,
            # then we will get all keys starting after the last key in current page.
            page_exist = response['IsTruncated']
            if page_exist:
                start_after_key = keys[-1]
