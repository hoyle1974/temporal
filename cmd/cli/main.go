package main

import (
	"fmt"

	"github.com/hoyle1974/temporal"
	"github.com/hoyle1974/temporal/storage"
	flag "github.com/spf13/pflag"
)

func main() {
	source := flag.StringP("source", "s", "disk", "The source to work against")
	uri := flag.StringP("uri", "u", ".", "The uri to the source")

	flag.Parse()
	fmt.Println(*source)
	fmt.Println(*uri)

	var store storage.System
	switch *source {
	case "disk":
		store = storage.NewDiskStorage(*uri)
	case "s3":
		//store = storage.NewS3Storage()
	default:
		fmt.Printf("unsupported storage system: %s\n", *source)
	}

	meta, err := temporal.NewMeta(store)
	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	min, max := meta.GetMinMaxTime()

	fmt.Printf("Source: %s\n", *source)
	fmt.Printf("URI: %s\n", *uri)
	fmt.Printf("Date Range from %v to %v\n", min.UTC(), max.UTC())

}
