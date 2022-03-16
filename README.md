# AWS S3 handler

S3 Handler offers an __easy to use (!)__ interface with common AWS S3 operations.

## Quickstart

```python
# init
s3_handler = S3Handler()

# list buckets
my_buckets = s3_handler.list_buckets()  # ['my_bucket', 'my_second_bucket']

# list bucket's keys (files paths)
s3_keys = list(s3_handler.iterate_keys('my_bucket'))  # ['file_1.txt', 'dir/file_2.txt']

# list bucket's directories
s3_dirs = list(s3_handler.iterate_dirs('my_bucket'))  # ['dir/']
```

common file operations:

```python
# reading a file
file_contents = s3_handler.read_file('my_bucket', 'file_1.txt')  # 'file_1.txt contents'

# creating a new file
new_file_contents = 'some spicy new file contents right here!'
s3_handler.write_file('my_bucket', 'new_dir/new_file.txt', new_file_contents)

# deleting a file
s3_handler.delete_file('my_bucket', 'new_dir/new_file.txt')
```

## Dependencies
[boto3](https://github.com/boto/boto3)