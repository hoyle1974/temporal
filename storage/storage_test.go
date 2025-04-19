package storage

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type localStackResolver struct{}

func (r localStackResolver) ResolveEndpoint(service, region string) (aws.Endpoint, error) {
	if service == s3.ServiceID {
		return aws.Endpoint{
			URL:               "https://localhost.localstack.cloud:4566",
			HostnameImmutable: true,
			PartitionID:       "aws",
			SigningRegion:     "us-east-1",
		}, nil
	}
	return aws.Endpoint{}, &aws.EndpointNotFoundError{}
}

func TestStreamWrite(t *testing.T) {
	// Load default config but inject the LocalStack endpoint + dummy creds
	s3cfg, err := s3config.LoadDefaultConfig(context.TODO(),
		s3config.WithRegion("us-east-1"),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		s3config.WithEndpointResolver(localStackResolver{}),
	)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Create S3 client
	s3client := s3.NewFromConfig(s3cfg)

	tests := []struct {
		name    string
		storage System
	}{
		{
			name:    "memory",
			storage: NewMemoryStorage(),
		},
		{
			name:    "disk",
			storage: NewDiskStorage(t.TempDir()),
		},
		{
			name:    "s3",
			storage: NewS3Storage(s3client, "test"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "test_stream"
			data := []byte("hello world")

			stream := tt.storage.BeginStream(context.Background(), key)
			require.NotNil(t, stream)

			n, err := stream.Write(data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)

			err = stream.Close()
			require.NoError(t, err)

			readData, err := tt.storage.Read(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, data, readData)
		})
	}
}
