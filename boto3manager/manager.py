import boto3
from botocore.exceptions import ClientError
import configparser
from pathlib import Path
import os

class Boto3Manager:
    def __init__(self, credentialsFilePath, profile, endpointUrl, bucketName):
        '''
        :param credentialsFile: A path to a file containing S3 credentials
        '''

        assert credentialsFilePath is not None and isinstance(credentialsFilePath, str), "credentialsFilePath must be a string"
        assert profile is not None and isinstance(profile, str), "profile must be a string"
        assert endpointUrl is not None and isinstance(endpointUrl, str), "endpointUrl must be a string"
        assert bucketName is not None and isinstance(bucketName, str), "bucketName must be a string"

        self.profile = profile
        self.endpointUrl = endpointUrl
        self.bucketName = bucketName

        # Read credentials from file
        self.credentialsFile = configparser.ConfigParser()
        self.credentialsFile.read(credentialsFilePath)

        self.accessKey = self.credentialsFile[profile]['aws_access_key_id']
        self.secretKey = self.credentialsFile[profile]['aws_secret_access_key']

        # Initialize boto3 session
        self.session = boto3.session.Session(aws_access_key_id=self.accessKey, aws_secret_access_key=self.secretKey)
        # Initialize boto3 client
        self.client = self.session.client(
            service_name='s3',
            endpoint_url=self.endpointUrl
        )

    def uploadAll(self, directory, destination=None):
        '''Uploads all files from a directory to an S3 bucket
        
        :param directory: Directory to upload files from
        :param destination: Folder in S3 bucket to upload files to
        
        :return: True if files were successfully uploaded, else False
        '''

        assert directory is not None and isinstance(directory, str), "Directory must be a string"
        assert destination is None or isinstance(destination, str), "Destination must be a string"

        # Get the path of the directory to upload files from
        directoryPath = Path(f'./{directory}')

        # Check if directory exists
        if directoryPath.is_dir():
            pass
        else:
            raise FileNotFoundError(f'Directory {directory} does not exist')
        
        # Get files inside directory recursively as posix paths
        posixPaths = list(directoryPath.glob('**/*'))
        # Convert to list of string paths
        pathsToString = list(map(str, posixPaths))

        # Loop through files in directory
        for path in pathsToString:
            try:
                # Get name of file
                objectName = os.path.basename(path)
                # Upload file
                if destination is None:
                    response = self.client.upload_file(path, self.bucketName, objectName)
                else:
                    response = self.client.upload_file(path, self.bucketName, f"{destination}/{objectName}")

            except ClientError as e:
                print(e)
                return False
        
        return True
    
    def downloadFolder(self, folder, destination):
        '''Downloads the contents of a folder from the S3 bucket
        
        :param folder: The folder to download from the S3 bucket
        :param destination: The folder in the local machine to download the file to

        :return: True if the download was successful, otherwise false
        '''

        assert folder is not None and isinstance(folder, str), "Folder must be a string"
        assert destination is not None and isinstance(destination, str), "Destination must be a string"

        # Create destination directory if it doesn't exist
        destinationPath = Path(destination)
        destinationPath.mkdir(parents=True, exist_ok=True)

        # Get contents of bucket with prefix of folder
        contents = self.client.list_objects_v2(Bucket=self.bucketName, Prefix=folder)['Contents']
        
        # Loop through the files in the folder
        for item in contents:
            try:
                # Get base name of file
                baseName = item['Key'].split('/')[-1]
                # Create a new file path in the destination directory
                newFilePath = destinationPath / baseName
                # Download the file from the S3 bucket to the file path on the user's local machine
                self.client.download_file(self.bucketName, item['Key'], newFilePath)
            except ClientError as e:
                print(e)
                return False
        return True
    
    def deleteFolder(self, folder):
        '''Deletes a folder in the S3 bucket
        
        :param folder: Folder to delete in the S3 bucket

        :return: True if deletion successful, False if otherwise
        '''

        assert folder is not None and isinstance(folder, str), "Folder must be a string"

        try:
            # Get contents of the S3 bucket with the prefix of the folder
            contents = self.client.list_objects_v2(Bucket=self.bucketName, Prefix=folder)['Contents']

            # Map contents of bucket to an array of dictionaries, each of the form {"Key": <filename>}
            objects = list(map(lambda x: {"Key": x["Key"]}, contents))

            # Delete all files in the folder from the bucket, using the mapped array of dictionaries
            self.client.delete_objects(Bucket=self.bucketName, Delete={"Objects": objects})
        except ClientError as e:
            print(e)
            return False
        return True
