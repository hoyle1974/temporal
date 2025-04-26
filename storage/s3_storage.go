package storage

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type s3Storage struct {
	Client     *s3.Client
	BucketName string
}

// NewS3Storage initializes a new S3Storage instance
func NewS3Storage(client *s3.Client, bucketName string) *s3Storage {
	return &s3Storage{Client: client, BucketName: bucketName}
}

func (s *s3Storage) GetKeysWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	var matchedFiles []string
	paginator := s3.NewListObjectsV2Paginator(s.Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.BucketName),
		Prefix: aws.String(prefix), // Only return objects with this prefix
	})

	// Iterate through paginated results
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			matchedFiles = append(matchedFiles, *obj.Key)
		}
	}

	return matchedFiles, nil
}

// Write uploads data to an S3 bucket with a given key
func (s *s3Storage) Write(ctx context.Context, key string, data []byte) error {
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(key),
		Body:   io.NopCloser(bytes.NewReader(data)),
		ACL:    types.ObjectCannedACLPrivate,
	})
	return err
}

type s3StreamWriter struct {
	writer     *bufio.Writer
	pipeWriter *io.PipeWriter
	wg         *sync.WaitGroup
	lastFlush  time.Time
}

// Function to write data to S3 while controlling flushing
func (s *s3StreamWriter) Write(data []byte) (int, error) {
	n, err := s.writer.Write(data)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (s *s3StreamWriter) Close() error {
	// Ensure any remaining data is flushed before closing
	err := s.writer.Flush()
	if err != nil {
		return err
	}
	// Close the pipeWriter to signal the end of data
	s.pipeWriter.Close()
	s.wg.Wait()
	return nil
}

// BeginStream creates a new pipe for streaming data to S3
func (s *s3Storage) BeginStream(ctx context.Context, key string) StreamWriter {
	pipeReader, pipeWriter := io.Pipe()
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Create a buffered writer to manage flushing
	writer := bufio.NewWriter(pipeWriter)

	// Goroutine to upload the data to S3
	go func() {
		defer wg.Done()

		_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(s.BucketName),
			Key:               aws.String(key),
			Body:              pipeReader,
			ChecksumAlgorithm: "",
		}, func(o *s3.Options) {
			o.Retryer = aws.NopRetryer{} // Disable retries
		})

		if err != nil {
			log.Printf("S3 upload failed: %v\n", err)
		}

		// Ensure the reader is closed when done
		pipeReader.Close()
	}()

	// Return a new s3StreamWriter to write data
	return &s3StreamWriter{
		writer:     writer,
		pipeWriter: pipeWriter,
		wg:         wg,
		lastFlush:  time.Now(),
	}
}

// Read downloads data from an S3 bucket for a given key
func (s *s3Storage) Read(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			// Handle the "file doesn't exist" case cleanly
			return nil, ErrDoesNotExist
		}
		// Unexpected error
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrDoesNotExist
		}
		return nil, errors.Wrap(err, "can not read file")
	}
	return data, nil
}

// Delete removes an object from an S3 bucket for a given key
func (s *s3Storage) Delete(ctx context.Context, key string) error {
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchKeyError *types.NoSuchKey
		if errors.As(err, &noSuchKeyError) {
			fmt.Println("NoSuchKey error:", noSuchKeyError)

			return nil // Ignore file not found errors
		}
		return err
	}
	return nil
}
