package temporal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Storage struct {
	Client     *s3.Client
	BucketName string
}

// NewS3Storage initializes a new S3Storage instance
func NewS3Storage(client *s3.Client, bucketName string) *S3Storage {
	return &S3Storage{Client: client, BucketName: bucketName}
}

// Write uploads data to an S3 bucket with a given key
func (s *S3Storage) Write(ctx context.Context, key string, data []byte) error {
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(key),
		Body:   io.NopCloser(bytes.NewReader(data)),
		ACL:    types.ObjectCannedACLPrivate,
	})
	return err
}

// Read downloads data from an S3 bucket for a given key
func (s *S3Storage) Read(ctx context.Context, key string) ([]byte, error) {
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
func (s *S3Storage) Delete(ctx context.Context, key string) error {
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
