package boto3manager

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/schollz/progressbar/v3"
)

type BucketBasics struct {
	S3Client *s3.Client
}

type FileUpload struct {
	Path string
	Key  string
}

type UploadObjectOptions struct {
	bar *progressbar.ProgressBar
}

// ListObjects takes a bucket name and lists all objects in the bucket.
func (basics BucketBasics) ListObjects(bucketName string) ([]types.Object, error) {
	// Get the objects from the bucket
	result, err := basics.S3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	// Get contents of bucket
	var contents []types.Object
	if err != nil {
		log.Printf("Couldn't list objects in bucket %v: %v\n", bucketName, err)
	} else {
		contents = result.Contents
	}
	return contents, err
}

// UploadObject takes a path to a file, the key to name the object, and a bucket name and uploads the file to the bucket.
func (basics BucketBasics) UploadObject(path string, key string, bucketName string, options UploadObjectOptions) error {
	// Create a new upload manager
	uploader := manager.NewUploader(basics.S3Client)

	// Open the file
	f, err := os.Open(path)

	if err != nil {
		log.Printf("Couldn't read file %v: %v\n", path, err)
		return err
	}

	// Upload the file to the bucket - set the key name to the name of the file
	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   f,
	})

	if options.bar != nil {
		fileInfo, err := os.Stat(path)

		if err != nil {
			log.Printf("Couldn't get size of uploaded file %v: %v", path, err)
		}
		options.bar.Add(int(fileInfo.Size()))
	}

	// fmt.Println("Uploaded", path)

	if err != nil {
		log.Printf("Couldn't upload object %v to bucket %v: %v\n", path, bucketName, err)
	}

	return err
}

// UploadObjects takes a glob pattern for files, a destination path, and a bucket name and uploads all files matching the pattern
// to the destination concurrently. dest must be empty or end with a "/" to signify a prefix
func (basics BucketBasics) UploadObjects(pattern string, dest string, bucketName string) error {
	// Get the files matching the pattern given
	matches, err := filepath.Glob(pattern)

	parentDir := pattern

	globIndex := strings.Index(pattern, "*")
	if globIndex != -1 {
		parentDir = parentDir[:globIndex]
	}

	if err != nil {
		log.Printf("Error parsing file pattern: %v\n", err)
		return err
	}

	// Check that the destination is empty or ends in "/"
	if !(len(dest) == 0 || string(dest[len(dest)-1]) == "/") {
		log.Printf("Destination must be empty or end in '/'\n")
		return err
	}

	// Get total size of files to be uploaded
	totalSize, err := totalFileSize(matches)

	if err != nil {
		log.Printf("Error getting total file size: %v", err)
		return err
	}

	// Make a progress bar
	bar := progressbar.DefaultBytes(totalSize, "uploading")

	// Make a queue for files to upload
	queue := make(chan *FileUpload)

	var wg sync.WaitGroup
	workerCount := 25

	// Create a goroutine for each worker
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Get file upload from queue
			for file := range queue {
				// fmt.Printf("Received %v from queue\n", file.Path)
				basics.UploadObject(file.Path, file.Key, bucketName, UploadObjectOptions{bar: bar})
			}
		}()
	}

	// For each file, create a FileUpload struct instance and send it to the queue
	for _, match := range matches {
		// Get the path of a given file excluding the parent directory - this will be the key of the file upload
		relToParentDir, err := filepath.Rel(parentDir, match)
		if err != nil {
			log.Printf("Couldn't get path of %v relative to %v: %v\n", parentDir, match, err)
		}

		upload := FileUpload{
			Path: match,
			Key:  relToParentDir,
		}

		// fmt.Printf("Sending %v to queue\n", upload.Path)

		queue <- &upload
	}

	close(queue)

	wg.Wait()

	return err
}

// totalFileSize gets the total size of a slice of paths to files.
func totalFileSize(paths []string) (int64, error) {
	var size int64
	for _, path := range paths {
		// Get file info of each path
		fileInfo, err := os.Stat(path)

		if err != nil {
			return 0, err
		}

		// Get size of file if it isn't a directory
		if !fileInfo.IsDir() {
			size += fileInfo.Size()
		}
	}

	return size, nil
}
