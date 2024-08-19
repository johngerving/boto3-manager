import boto3
import botocore
import boto3.s3.transfer as s3transfer
from botocore.exceptions import ClientError
import configparser
from pathlib import Path
import os
from tqdm import tqdm
from wakepy import keep

class Boto3Manager:
    def __init__(self, credentials_file_path, profile, endpoint_url, bucket_name, workers=20):
        '''
        Manages Boto3 resources and handles uploading and downloading to and from S3.

        :param credentials_file_path (str): A path to a file containing S3 credentials
        :param profile (str): The S3 profile to use from the credentials file
        :param endpoint_url (str): The S3 endpoint to upload and download to and from
        :param bucket_name (str): The name of the S3 bucket to use
        '''

        assert credentials_file_path is not None and isinstance(credentials_file_path, str), "credentials_file_path must be a string"
        assert profile is not None and isinstance(profile, str), "profile must be a string"
        assert endpoint_url is not None and isinstance(endpoint_url, str), "endpoint_url must be a string"
        assert bucket_name is not None and isinstance(bucket_name, str), "bucket_name must be a string"

        self.profile = profile
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name

        # Read credentials from file
        self.credentials_file = configparser.ConfigParser()
        self.credentials_file.read(credentials_file_path)

        self.access_key = self.credentials_file[profile]['aws_access_key_id']
        self.secret_key = self.credentials_file[profile]['aws_secret_access_key']

        # Initialize boto3 session
        self.session = boto3.session.Session(aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
        # Initialize config
        self.config = botocore.config.Config(max_pool_connections=workers)
        # Initialize transfer config
        self.transfer_config = s3transfer.TransferConfig(
            use_threads=True,
            max_concurrency=workers
        )
        # Initialize boto3 client
        self.client = self.session.client(
            service_name='s3',
            endpoint_url=self.endpoint_url
        )
        # Create paginator
        self.paginator = self.client.get_paginator('list_objects_v2')

    def get_files(self, path):
        '''Returns a list of files in a directory, recursively
        
        :param path (str): A string with the directory to get files from
        
        :return (list): A list containing strings with the files in the directory
        '''

        # Check argument
        assert path is not None and isinstance(path, str), "Path must be a string"

        path = Path(path)

        assert path.exists(), "Path does not exist"

        # Get the files recursively
        files = [str(x) for x in path.glob('**/*') if not x.is_dir()]

        return files
    
    def get_size(self, files):
        '''Gets the total size of a list of files.

        :param files (list): A list of paths to files

        :return (int): The total size of the files, in bytes
        '''

        # Check argument
        assert files is not None and isinstance(files, list), "Files must be a list"

        # Get sum of all files in list
        totalsize = sum([os.stat(f).st_size for f in files])

        return totalsize        

    def upload(self, path, prefix=None):
        '''Uploads files in a path recursively
        
        :param path (str): The path to upload files from
        :param destination (str): The destination in the S3 bucket to upload to

        :return (bool): True if all uploads successful, False otherwise
        '''

        # Check parameters
        assert path is not None and isinstance(path, str), "Path must be a string"
        assert prefix is None or isinstance(prefix, str), "Prefix must be a string"

        # Convert path given into a Pure path
        path = Path(path)
        
        # Ensure path exists
        assert path.exists(), "Path does not exist"

        # Create transfer manager
        transfer_manager = s3transfer.create_transfer_manager(self.client, self.transfer_config)

        # Get files in directory recursively, but only if they are not directories
        files = self.get_files(str(path))

        # Get total size of files in bytes
        totalsize = self.get_size(files)

        def wakepy_fail(result):
            print("Warning: Unable to keep system awake during upload.")

        # Prevent the system from sleeping during upload
        with keep.running(on_fail=wakepy_fail):
            # Display a progress bar with the upload progress
            with tqdm(desc='upload', ncols=60, total=totalsize, unit='B', unit_scale=1) as progress_bar:
                for file in files:
                    try:
                        # Remove the path from the beginning of the path to upload to bucket - convert to Posix path for Windows support
                        destination_path = Path(file).relative_to(path).as_posix()

                        # Prefix destination path if provided
                        if prefix is not None:
                            destination_path = prefix + destination_path

                        # Upload the file, trigger a callback function to update the progress bar
                        transfer_manager.upload(file, self.bucket_name, destination_path, subscribers=[s3transfer.ProgressCallbackInvoker(progress_bar.update)])
                    except Exception as e:
                        print(e)
                        return False
                    
                # Wait for upload to be complete to shut down
                transfer_manager.shutdown()

                return True
        
    def list_all_objects(self, **kwargs):
        '''Lists all objects in an S3 bucket
        
        :param **kwargs: Keyword arguments to be passed to list_objects_v2 method
        
        :yield: Generator object with contents of bucket
        '''

        # Paginate the objects to overcome the 1000 object limit
        pages = self.paginator.paginate(Bucket=self.bucket_name, **kwargs)

        # Loop through pages
        for page in pages:
            # Loop through objects in page
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Yield generator object containing objects in bucket
                    yield obj

    def download(self, destination, prefix=''):
        '''Downloads the contents of a bucket or folder recursively from an S3 bucket
        
        :param destination (str): The destination to download the files to
        :param prefix (str): The prefix of files in the S3 bucket to download from

        :return (bool): True if all uploads successful, otherwise False
        '''

        assert destination is not None and isinstance(destination, str), "Destination must be a string"
        assert prefix is not None and isinstance(prefix, str), "Prefix must be a string"

        # Create transfer manager
        transfer_manager = s3transfer.create_transfer_manager(self.client, self.transfer_config)

        # Get contents of bucket with prefix
        contents = list(self.list_all_objects(Prefix=prefix))

        if len(contents) == 0:
            print("Folder is empty.")
            return True

        # Get total size of objects
        totalsize = sum([obj['Size'] for obj in contents])

        def wakepy_fail(result):
            print("Warning: Unable to keep system awake during download.")

        # Prevent the system from sleeping during download
        with keep.running(on_fail=wakepy_fail):
            # Display a progress bar with the download progress
            with tqdm(desc='download', ncols=60, total=totalsize, unit='B', unit_scale=1) as progress_bar:
                for file in contents:
                    try:
                        # Get name of file in S3 bucket
                        key = file['Key']

                        # Split key by slashes - remove prefix from beginning
                        path_list = key.removeprefix(prefix).split('/')
                        # Get the base name of the file
                        base_name = path_list[-1]

                        # Create a Path at the destination specified
                        dir = Path(destination)

                        # If the current item has a prefix, append it to the destination path
                        if len(path_list) > 1:
                            dir = dir / '/'.join(path_list[:-1])

                        # Make a directory at the specified path if it doesn't exist
                        dir.mkdir(parents=True, exist_ok=True)

                        # Create a new file path in the destination directory
                        new_file_path = str(dir / base_name)

                        # Download the file, trigger a callback function to update the progress bar
                        transfer_manager.download(self.bucket_name, key, new_file_path, subscribers=[s3transfer.ProgressCallbackInvoker(progress_bar.update)])
                    except Exception as e:
                        print(e)
                        return False
                
                # Wait for download to be complete to shut down
                transfer_manager.shutdown()

                return True

    def upload_all(self, directory, destination=None):
        '''Uploads all files from a directory to an S3 bucket
        
        :param directory: Directory to upload files from
        :param destination: Folder in S3 bucket to upload files to
        
        :return: True if files were successfully uploaded, else False
        '''

        assert directory is not None and isinstance(directory, str), "Directory must be a string"
        assert destination is None or isinstance(destination, str), "Destination must be a string"

        # Get the path of the directory to upload files from
        directory_path = Path(f'./{directory}')

        # Check if directory exists
        if directory_path.is_dir():
            pass
        else:
            raise FileNotFoundError(f'Directory {directory} does not exist')
        
        # Get files inside directory recursively as posix paths
        paths = list(directory_path.glob('**/*'))
        # Convert to list of string paths
        paths_to_string = list(map(str, paths))

        # Loop through files in directory
        for path in paths_to_string:
            try:
                # Get name of file
                object_name = os.path.basename(path)
                # Upload file
                if destination is None:
                    response = self.client.upload_file(path, self.bucket_name, object_name)
                else:
                    response = self.client.upload_file(path, self.bucket_name, f"{destination}/{object_name}")

            except ClientError as e:
                print(e)
                return False
        
        return True

    def download_folder(self, folder, destination):
        '''Downloads the contents of a folder from the S3 bucket
        
        :param folder: The folder to download from the S3 bucket
        :param destination: The folder in the local machine to download the file to

        :return: True if the download was successful, otherwise false
        '''

        assert folder is not None and isinstance(folder, str), "Folder must be a string"
        assert destination is not None and isinstance(destination, str), "Destination must be a string"

        # Create destination directory if it doesn't exist
        destination_path = Path(destination)
        destination_path.mkdir(parents=True, exist_ok=True)

        # Get contents of bucket with prefix of folder
        contents = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder)['Contents']
        
        # Loop through the files in the folder
        for item in contents:
            try:
                # Get base name of file
                base_name = item['Key'].split('/')[-1]
                # Create a new file path in the destination directory
                new_file_path = destination_path / base_name
                # Download the file from the S3 bucket to the file path on the user's local machine
                self.client.download_file(self.bucket_name, item['Key'], new_file_path)
            except ClientError as e:
                print(e)
                return False
        return True
    
    def delete_folder(self, folder):
        '''Deletes a folder in the S3 bucket
        
        :param folder: Folder to delete in the S3 bucket

        :return: True if deletion successful, False if otherwise
        '''

        assert folder is not None and isinstance(folder, str), "Folder must be a string"

        try:
            # Get contents of the S3 bucket with the prefix of the folder
            contents = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder)['Contents']

            # Map contents of bucket to an array of dictionaries, each of the form {"Key": <filename>}
            objects = list(map(lambda x: {"Key": x["Key"]}, contents))

            # Delete all files in the folder from the bucket, using the mapped array of dictionaries
            self.client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects})
        except ClientError as e:
            print(e)
            return False
        return True
