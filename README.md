# boto3manager

A Python library for managing file uploads and downloads in Boto3.

## Installation
On Linux:
```
pip3 install --upgrade https://gitlab.nrp-nautilus.io/humboldt/boto3-manager.git
```

On Windows:
```
py -m pip install --upgrade https://gitlab.nrp-nautilus.io/humboldt/boto3-manager.git
```

## Usage
```
from boto3manager import Boto3Manager

### Initialize Boto3Manager object
    # Looks for file called "aws_credentials" containing S3 credentials
    # Uses "default" profile
    # Uses endpoint URL "https://example.com"
    # Uses S3 bucket called "test-bucket"
    # Uses 20 worker threads for uploads and downloads
manager = Boto3Manager('aws_credentials', 'default', 'https://example.com', 'test-bucket', workers=20)

# Upload contents of directory "foo/" to S3 bucket under prefix "bar/"
manager.upload('foo', prefix='bar/')

# Download contents under prefix "bar/" from S3 bucket into local directory "downloaded/"
manager.download('downloaded', prefix='bar/')
```

## Documentation

### Boto3Manager
```
Manages Boto3 resources and handles uploading and downloading to and from S3.

:param credentials_file_path (str): A path to a file containing S3 credentials
:param profile (str): The S3 profile to use from the credentials file
:param endpoint_url (str): The S3 endpoint to upload and download to and from
:param bucket_name (str): The name of the S3 bucket to use
```

#### Methods

**download**
```
Downloads the contents of a bucket or folder recursively from an S3 bucket
        
:param destination (str): The destination to download the files to
:param prefix (str): The prefix of files in the S3 bucket to download from

:return (bool): True if all uploads successful, otherwise False
```

**get_files**
```
Returns a list of files in a directory, recursively
        
:param path (str): A string with the directory to get files from

:return (list): A list containing strings with the files in the directory
```

**get_size**
```
Gets the total size of a list of files.

:param files (list): A list of paths to files

:return (int): The total size of the files, in bytes
```

**list_all_objects**
```
Lists all objects in an S3 bucket
        
:param **kwargs: Keyword arguments to be passed to list_objects_v2 method

:yield: Generator object with contents of bucket
```

**upload**
```
Uploads files in a path recursively
        
:param path (str): The path to upload files from
:param destination (str): The destination in the S3 bucket to upload to

:return (bool): True if all uploads successful, False otherwise
```
