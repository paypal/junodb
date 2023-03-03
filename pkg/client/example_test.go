package client_test

import (
	"fmt"

	"juno/pkg/client"
)

func Example_config() {
}

func Example_newClient() {
	// create a Juno client talking to 127.0.0.1:8080 with
	//   namespace: exampleNS, and
	//   applicaiton name: exampleApp
	if cli, err := client.NewClient("127.0.0.1:8080", "exampleNS", "exampleApp"); err == nil {
		cli.Get([]byte("aKey"))
	} else {
		fmt.Println(err)
	}
}
