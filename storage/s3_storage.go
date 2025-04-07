package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	writer *io.PipeWriter
	wg     *sync.WaitGroup
}

func (s *s3StreamWriter) Write(data []byte) (int, error) {
	return s.writer.Write(data)
}

func (s *s3StreamWriter) Close() error {
	// Close writer to indicate the upload is complete
	s.writer.Close()
	s.wg.Wait()

	return nil
}

// This function will return an s3StreamWriter that can be used to stream data to an S3 bucket
// and close when it's done.
func (s *s3Storage) BeginStream(ctx context.Context, key string) StreamWriter {
	pipeReader, pipeWriter := io.Pipe()
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		_, err := s.Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(s.BucketName),
			Key:    aws.String(key),
			Body:   pipeReader,
		})
		if err != nil {
			log.Printf("S3 upload failed: %v", err)
		}
		pipeReader.Close() // Ensure reader is closed once done
	}()

	return &s3StreamWriter{
		pipeWriter,
		wg,
	}
}

// Read downloads data from an S3 bucket for a given key
func (s *s3Storage) Read(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
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
