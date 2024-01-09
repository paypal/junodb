[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Juno Golang SDK

## Sample Code

```
package main

import (
	"crypto/tls"
	"fmt"
	"time"

	"juno/pkg/client"
	cal "juno/pkg/logging/cal/config"
	"juno/pkg/util"
)

// addr is a Juno server endpoint in the form "ip:port".
// getTLSConfig is a func to get *tls.Config.
func createClient(addr string, getTLSConfig func() *tls.Config) (client.IClient, error) {

	cfg := client.Config{
		Appname:           "example",
		Namespace:         "example_namespace",
		DefaultTimeToLive: 60, // seconds
		ConnectTimeout:    util.Duration{1000 * time.Millisecond},
		ResponseTimeout:   util.Duration{500 * time.Millisecond},
	}

	cfg.Server.Addr = addr
	cfg.Server.SSLEnabled = true // Set to true if addr has an SSL port.

	client, err := client.NewWithTLS(cfg, getTLSConfig)
	return client, err
}

// Show metadata.
func showInfo(ctx client.IContext) {
	fmt.Printf("v=%d ct=%d ttl=%d\n", ctx.GetVersion(), ctx.GetCreationTime(),
	    ctx.GetTimeToLive())
}

func basicAPI(cli client.IClient) {
	key := []byte("test_key")
	val := []byte("test_payload")
	ctx, err := cli.Create(key, val)
	if err != nil {
		// log error
	}

	// Update val slice before call Update
	ctx, err = cli.Update(key, val)
	if err == nil {
		showInfo(ctx)	
	} else if err != client.ErrNoKey {
		// log error
	}

	_, err = cli.Set(key, val)
	if err != nil {
		// log error
	}

	val, _, err = cli.Get(key)
	if err != nil && err != client.ErrNoKey {
		// log error
	}

	err = cli.Destroy(key)
	if err != nil {
		// log error
	}
}

// Extend TTL if the value of WithTTL is greater than the current.
func basicAPIwithTTL(cli client.IClient) {

	key := []byte("test_key")
	val := []byte("test_Payload")
	ctx, err := cli.Create(key, val, client.WithTTL(uint32(100)))
	if err == nil {
		showInfo(ctx)
	}
	
	// Update val slice before call Update
	ctx, err = cli.Update(key, val, client.WithTTL(uint32(150)))
	if err == nil {
		showInfo(ctx)
	}

	ctx, err = cli.Set(key, val, client.WithTTL(uint32(200)))
	if err == nil {
		showInfo(ctx)
	}
	
	val, ctx, err = cli.Get(key, client.WithTTL(uint32(500)))
	if err == nil {
		showInfo(ctx)
	}

	err = cli.Destroy(key)
	if err != nil {
		// log error
	}
}

// Test conditional update based on record version.
func condUpdate(cli client.IClient) error {
	key := []byte("new_key")
	val := []byte("new_payload")

	ctx, err := cli.Create(key, val)
	if err != nil {
		return err
	}

	ctx, err = cli.Update(key, val)
	if err != nil {
		return err
	}

	// Update succeeds if current record version is equal to ctx.GetVersion().
	// After the update, record version is incremented.
	_, err = cli.Update(key, val, client.WithCond(ctx))
	if err != nil {
		return err
	}

	// Expect ErrConditionViolation
	// because current record version is not equal to ctx.GetVersion().
	_, err = cli.Update(key, val, client.WithCond(ctx))
	if err != client.ErrConditionViolation {
		return err
	}

	err = cli.Destroy(key)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	// Init variables
	var addr string                      // = ...
	var getTLSConfig func() *tls.Config  // = ...

	// A client object should be created only once per unique addr.
	cli, err := createClient(addr, getTLSConfig)
	if err != nil {
		// log error
		return
	}

	basicAPI(cli)
	basicAPIwithTTL(cli)
	if err := condUpdate(cli); err != nil {
		// log error
	}
}
```

