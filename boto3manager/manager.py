import boto3
from botocore.exceptions import ClientError
import configparser
from pathlib import Path
import os

class Boto3Manager:
    def __init__(self, credentials_file_path, profile, endpoint_url, bucket_name):
        '''
        :param credentials_file_path: A path to a file containing S3 credentials
        :param profile: The S3 profile to use from the credentials file
        :param endpoint_url: The S3 endpoint to upload and download to and from
        :param bucket_name: The name of the S3 bucket to use
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
        # Initialize boto3 client
        self.client = self.session.client(
            service_name='s3',
            endpoint_url=self.endpoint_url
        )

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
                object_name = os.path.base_name(path)
                # Upload file
                if destination is None:
                    response = self.client.upload_file(path, self.bucket_name, object_name)
                else:
                    response = self.client.upload_file(path, self.bucket_name, f"{destination}/{object_name}")

            except ClientError as e:
                print(e)
                return False
        
        return True
    
    def upload_files_recursive(self, path, destination=None):
        '''Uploads files in a path recursively
        
        :param path: The path to upload files from
        :param destination: The destination in the S3 bucket to upload to

        :return: True if all uploads successful, False otherwise
        '''

        # Check parameters
        assert path is not None and isinstance(path, str), "Path must be a string"
        assert destination is None or isinstance(destination, str), "Destination must be a string"

        # Convert path given into a Pure path
        path = Path(path)
        
        # Ensure path exists
        assert path.exists(), "Path does not exist"

        # Get files in directory recursively, but only if they are not directories
        files = [str(x) for x in path.glob('**/*') if not x.is_dir()]
        
        # Loop through files
        for file in files:
            try:
                # Upload file - add destination as prefix if provided
                if destination is None:
                    self.client.upload_file(file, self.bucket_name, file)
                else:
                    self.client.upload_file(file, self.bucket_name, f"{destination}/{file}")
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
