package boto3manager

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/schollz/progressbar/v3"
	"gitlab.nrp-nautilus.io/humboldt/boto3-manager/strutil"
)

type BucketBasics struct {
	S3Client *s3.Client
}

type FileUpload struct {
	Path string
	Key  string
}

type FileDownload struct {
	Key         string
	Destination string
}

type UploadObjectOptions struct {
	bar *progressbar.ProgressBar
}

type DownloadObjectOptions struct {
	bar *progressbar.ProgressBar
}

// ListObjects takes a bucket name and lists all objects in the bucket.
func (basics BucketBasics) ListObjects(bucketName string) ([]types.Object, error) {
	// Get every item in bucket
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	// Create the Paginator for the ListObjectsV2 operation
	p := s3.NewListObjectsV2Paginator(basics.S3Client, params)

	results := make([]types.Object, 0)

	// Iterate through S3 object pages
	var i int
	for p.HasMorePages() {
		i++

		// Next Page takes a new context for each page retrieval
		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("Failed to get page %v in bucket %v: %v", i, bucketName, err)
			return nil, err
		}

		// Append to results
		results = append(results, page.Contents...)
	}

	return results, nil
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
	fs := os.DirFS(".")
	matches, err := strutil.Glob(fs, pattern)

	for _, match := range matches {
		fmt.Println(match)
	}

	if err != nil {
		log.Printf("Error parsing file pattern: %v\n", err)
		return err
	}

	dirExcluded := make([]string, 0, len(matches))
	// Filter the matches to only include files
	for _, match := range matches {
		// Get file info of each path
		fileInfo, err := os.Stat(match)

		if err != nil {
			return err
		}

		// Append file path if it isn't a directory
		if !fileInfo.IsDir() {
			dirExcluded = append(dirExcluded, filepath.ToSlash(match))
		}
	}

	parentDir := pattern

	globIndex := strings.Index(pattern, "*")
	if globIndex != -1 {
		parentDir = parentDir[:globIndex]
	}

	// Check that the destination is empty or ends in "/"
	if !(len(dest) == 0 || string(dest[len(dest)-1]) == "/") {
		log.Printf("Destination must be empty or end in '/'\n")
		return err
	}

	// Get total size of files to be uploaded
	totalSize, err := totalFileSize(dirExcluded)
	fmt.Println(totalSize)

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
	for _, path := range dirExcluded {
		// Get the path of a given file excluding the parent directory - this will be the key of the file upload
		relToParentDir, err := filepath.Rel(parentDir, path)
		if err != nil {
			log.Printf("Couldn't get path of %v relative to %v: %v\n", parentDir, path, err)
		}

		upload := FileUpload{
			Path: path,
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

// DownloadObject takes a key, a destination, and a bucket name and downloads the object with that key to the destination.
func (basics BucketBasics) DownloadObject(key string, dest string, bucketName string, options DownloadObjectOptions) error {
	// Create a new download manager
	manager := manager.NewDownloader(basics.S3Client)

	// Create the destination directory if it doesn't exist already
	err := os.MkdirAll(dest, os.ModePerm)

	if err != nil {
		log.Printf("Couldn't create directory %v: %v", dest, err)
	}

	// Get base name of file
	baseName := filepath.Base(key)

	// Create file name from destination path and base name of key in bucket
	fileName := filepath.Join(dest, baseName)

	// Create the file
	f, err := os.Create(fileName)

	if err != nil {
		log.Printf("Couldn't open file %v: %v", fileName, err)
		return err
	}

	// Close the file after everything is finished
	defer f.Close()

	// Download the file
	_, err = manager.Download(context.Background(), f, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Printf("Couldn't download file %v: %v", key, err)
		return err
	}

	fmt.Printf("Downloaded %v\n", key)

	// Get size of downloaded file and update progress bar
	if options.bar != nil {
		fileInfo, err := os.Stat(f.Name())

		if err != nil {
			log.Printf("Couldn't get size of downloaded file %v: %v", f.Name(), err)
		}
		options.bar.Add(int(fileInfo.Size()))
	}

	return nil
}

// DownloadObjects takes a pattern, a destination, and a bucket name and downloads all objects in the bucket matching
// that pattern to the destination.
func (basics BucketBasics) DownloadObjects(pattern string, dest string, bucketName string) error {
	// Get the prefix of the pattern by stopping before the first wildcard
	firstWildcard := strings.Index(pattern, "*")
	prefix := pattern
	if firstWildcard > -1 {
		prefix = pattern[:firstWildcard]
	}

	// Get every item in bucket
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	// If the pattern has a prefix that can be identified, add it to the input struct instance.
	// Otherwise, list all objects.
	if len(prefix) > 0 {
		params.Prefix = &prefix
	}

	// Create the Paginator for the ListObjectsV2 operation
	p := s3.NewListObjectsV2Paginator(basics.S3Client, params)

	results := make([]types.Object, 0)

	// Iterate through S3 object pages
	var i int
	for p.HasMorePages() {
		i++

		// Next Page takes a new context for each page retrieval
		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("Failed to get page %v in bucket %v: %v", i, bucketName, err)
			return err
		}

		// Append to results
		results = append(results, page.Contents...)
	}

	// Create a regular expression from the given pattern
	re := regexp.MustCompile(strutil.WildCardToRegexp(pattern))

	// Create a slice of strings to store matches
	matches := make([]types.Object, 0, len(results))

	// Loop through contents of bucket
	for _, item := range results {
		// Append to slice if the key of the object matches the given pattern
		if re.MatchString(*item.Key) {
			matches = append(matches, item)
		}
	}

	// Get the total size of the objects matching the pattern
	totalSize := totalObjectSize(matches)

	// Make a progress bar
	bar := progressbar.DefaultBytes(totalSize, "uploading")

	// Make a queue for files to download
	queue := make(chan *FileDownload)

	var wg sync.WaitGroup
	workerCount := 50

	// Create a goroutine for each worker
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Get file download from queue
			for file := range queue {
				fmt.Printf("Received %v from queue\n", file.Key)
				basics.DownloadObject(file.Key, file.Destination, bucketName, DownloadObjectOptions{bar: bar})
			}
		}()
	}

	// For each file, create a FileDownload struct instance and send it to the queue
	for _, object := range matches {

		download := FileDownload{
			Key:         *object.Key,
			Destination: filepath.Join(dest, *object.Key), // Write to file in destination directory with the name being the object's key
		}

		fmt.Printf("Sending %v to queue\n", download.Key)

		queue <- &download
	}

	close(queue)

	wg.Wait()

	return nil
}

// totalObjectSize takes a list of items in an S3 bucket and returns the total size in bytes.
func totalObjectSize(objects []types.Object) int64 {
	var size int64
	for _, object := range objects {
		size += *object.Size
	}

	return size
}
