package main

// From https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/s3-example-basic-bucket-operations.html

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
)

func main() {
	// Check CLI arguments
	if len(os.Args) != 3 {
		exitErrorf("bucket and file name required\nUsage: %s bucket_name filename",
			os.Args[0])
	}
	bucket := os.Args[1]
	filename := os.Args[2]

	// Check the input file exists
	inputFile, err := os.Open(filename)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", err)
	}
	defer inputFile.Close()

	// Create a aws session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)
	if err != nil {
		exitErrorf("%v", err)
	}

	// Create S3 service client
	svc := s3.New(sess)

	// Bucket List
	bucketExists := false
	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}
	for _, b := range result.Buckets {
		if aws.StringValue(b.Name) == bucket {
			bucketExists = true
			break
		}
	}

	// Create bucket
	if !bucketExists {
		_, err = svc.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			exitErrorf("Unable to create bucket %q, %v", bucket, err)
		}
		// Wait until bucket is created before finishing
		fmt.Printf("Waiting for bucket %q to be created...\n", bucket)
		err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			exitErrorf("Error occurred while waiting for bucket to be created, %v", bucket)
		}
		fmt.Printf("Bucket %q successfully created\n", bucket)
	}

	// Upload the file in the bucket
	uploader := s3manager.NewUploader(sess)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   inputFile,
	})
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", filename, bucket, err)
	}
	fmt.Printf("Successfully uploaded %q to %q\n", filename, bucket)

	// Download the file from the bucket
	downloader := s3manager.NewDownloader(sess)
	fileOutput, err := os.Create(filename + ".downloaded")
	if err != nil {
		exitErrorf("Unable to open file %q, %v", filename, err)
	}
	defer fileOutput.Close()
	numBytes, err := downloader.Download(fileOutput,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(filename),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", filename, err)
	}
	fmt.Println("Downloaded", fileOutput.Name(), numBytes, "bytes")

	// Delete file in the bucket
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		exitErrorf("Unable to delete object %q from bucket %q, %v", filename, bucket, err)
	}
	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		exitErrorf("Unable to delete item %q, %v", filename, err)
	}
	fmt.Println("Deleted ", filename)

	// Delete the bucket
	// _, err = svc.DeleteBucket(&s3.DeleteBucketInput{
	//     Bucket: aws.String(bucket),
	// })
	// if err != nil {
	//     exitErrorf("Unable to delete bucket %q, %v", bucket, err)
	// }

	// // Wait until bucket is deleted before finishing
	// fmt.Printf("Waiting for bucket %q to be deleted...\n", bucket)

	// err = svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
	//     Bucket: aws.String(bucket),
	// })

	fmt.Print("DONE\n")
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
