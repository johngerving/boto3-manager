package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	transport "github.com/aws/smithy-go/endpoints"
	boto3manager "gitlab.nrp-nautilus.io/humboldt/boto3-manager"
)

type Resolver struct {
	URL *url.URL
}

func (r *Resolver) ResolveEndpoint(_ context.Context, params s3.EndpointParameters) (transport.Endpoint, error) {
	u := *r.URL
	u.Path += "/" + *params.Bucket
	return transport.Endpoint{URI: u}, nil
}

func main() {
	config, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration.")
		fmt.Println(err)
		return
	}

	endpointURL, err := url.Parse("https://s3-tide.nrp-nautilus.io")

	if err != nil {
		panic(err)
	}

	s3Client := s3.NewFromConfig(config, func(o *s3.Options) {
		o.EndpointResolverV2 = &Resolver{URL: endpointURL}
	})

	bucketBasics := boto3manager.BucketBasics{S3Client: s3Client}

	contents, err := bucketBasics.ListObjects("humboldt-s3-test")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, item := range contents {
		fmt.Println(*item.Key)
	}
}
