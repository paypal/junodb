//
//  Copyright 2023 PayPal Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package functest

import (
	"strconv"
	"testing"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/client"
	"github.com/paypal/junodb/pkg/util"
	"github.com/paypal/junodb/test/testutil"
)

/***************************************************
 *  Test Normal Create
 *  Insert normal record, record read succeedly
 ***************************************************/
func TestCreateNormal(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("Haha, the first Create test")
	glog.Debug("request key is " + util.ToPrintableAndHexString(key) + " data is " + string(cvalue))
	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 1000, nil); err != nil {
		t.Error(err)
	}
}

/***************************************************
 *  Test insert NULL key record
 *  Insert NULL key record, get BadParam error
 ***************************************************/
func TestCreateNullKey(t *testing.T) {
	cvalue := []byte("Haha, the null key/ns/app Create test")

	_, err := proxyClient.Create(nil, cvalue, client.WithTTL(10))
	if err != client.ErrBadParam {
		t.Error("error: ", err, " should get Bad Parameter")
	}
}

/****************************************************************
 *  Test insert with key length = max allowed (set in the config)
 *  Insert record with key length = max, insert successful
 ****************************************************************/
func TestCreateMaxLenKey(t *testing.T) {
	key := testutil.GenerateRandomKey(128) //this is set by config prop file
	cvalue := []byte("Haha, the max key length test")
	glog.Debug("length of key is " + strconv.Itoa(len(key)))

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 20, nil); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test insert with key length > max allowed (set in the config)
 *  Insert record with key length > max, get BadParam error
 ***********************************************************/
func TestCreateExceedsMaxLenKey(t *testing.T) {
	key := testutil.GenerateRandomKey(257)
	cvalue := []byte("Haha, longer than max key length test")
	glog.Debug("length of key is " + strconv.Itoa(len(key)))

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 20, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test insert with many special character key or namespace
 *  Insert record with many special character key
 *  Insert record with many special character name space
 *  Both insert succeed
 ***********************************************************/
func TestCreateSpecialCharNSKey(t *testing.T) {
	key := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	NS1 := "Q:你好嗎？A:<?xml version=\"1.0en=\"UTF-8\"" + "id=\"1@@#$%^&*()_+?"
	glog.Debug("length of key is " + strconv.Itoa(len(key)) + "length of NS1 is" + strconv.Itoa(len(NS1)))

	cvalue := []byte("Haha, the max key length test")
	glog.Debug("cfgshare address is ", cfgShare.Server)
	cfgShare.Namespace = "NS"
	cfgShare.Appname = "APP"
	var client1, client2 client.IClient
	var err error

	if client1, err = client.New(cfgShare); err != nil {
		t.Fatal(err)
	}
	cfgShare.Namespace = NS1
	if client2, err = client.New(cfgShare); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	_ = client1.Destroy(key)
	_ = client2.Destroy([]byte("keyh"))
	if err := testutil.CreateAndValidate(client1, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(client2, []byte("keyh"), cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	testutil.DestroyAndValidate(client1, key, nil)
	testutil.DestroyAndValidate(client2, []byte("keyh"), nil)
}

/***********************************************************
 *  Test insert with many special character payload
 *  Insert record with many special character payload
 *  Insert record with many special character appname
 *  Both insert succeed
 ***********************************************************/
func TestCreateSpecialCharAppPayload(t *testing.T) {
	cvalue := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	app := "Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=."

	key := testutil.GenerateRandomKey(32)
	key1 := testutil.GenerateRandomKey(32)
	cfgShare.Namespace = "NS"
	cfgShare.Appname = "APP"
	var client1, client2 client.IClient
	var err error
	if client1, err = client.New(cfgShare); err != nil {
		t.Fatal(err)
	}
	cfgShare.Appname = app
	if client2, err = client.New(cfgShare); err != nil {
		t.Fatal(err)
	}

	glog.Debugln("request key is " + util.ToHexString(key) + " data is " + string(cvalue))

	if err := testutil.CreateAndValidate(client1, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(client2, key1, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	testutil.DestroyAndValidate(client1, key, nil)
	testutil.DestroyAndValidate(client2, key1, nil)
}

/***********************************************************
 *  Test client create with special character namespace
 ***********************************************************/
func TestCreateSpecialCharNS(t *testing.T) {
	NS := "Q:你好嗎？A:<?xml version=\"1.0en=\"UTF-8\"" + "id=\"1@@#$%^&*()_+?"
	key := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	value := []byte("value")

	cfgShare.Namespace = NS
	cfgShare.Appname = "APP1"
	if client1, err := client.New(cfgShare); err == nil {
		if err := testutil.CreateAndValidate(client1, key, value, 10, nil); err != nil {
			t.Error(err)
		}
		testutil.DestroyAndValidate(client1, key, nil)
	} else {
		t.Fatal(err)
	}
}

/******************************************************
 *  Test insert with null payload
 *  Insert record with payload = nil, create succeed
 ******************************************************/
func TestCreateNullPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)

	if err := testutil.CreateAndValidate(proxyClient, key, nil, 100, nil); err != nil {
		t.Error(err)
	}
}

/******************************************************
 *  Test insert with zero payload
 *  Insert record with payloadsize = 0, create succeed
 ******************************************************/
func TestCreateZeroPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(0)

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
}

/********************************************************
 *  Test insert with empty payload
 *  Insert record with empty payload, create succeed
 ********************************************************/
func TestCreateEmptyPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	if err := testutil.CreateAndValidate(proxyClient, key, []byte(""), 100, nil); err != nil {
		t.Error(err)
	}
}

/**********************************************************************
 *  Test insert with payload length = max allowed, wait proxy implement
 *  Insert record with payload length = max, create succeed
 **********************************************************************/
func TestCreateMaxLenPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(204800) //the number actually is not really the right max, anyway
	glog.Debug("length of key is " + strconv.Itoa(len(key)) + "length of value is" + strconv.Itoa(len(cvalue)))

	if rcerr := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, nil); rcerr != nil {
		t.Error(rcerr)
	}
}

/***********************************************************************
 *  Test insert with payload length > max allowed, wait proxy implement
 *  Insert record with payload length > max, get BadParam error
 ***********************************************************************/
func TestCreateExceedsMaxLenPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(204801)

	glog.Debug("length of key is " + strconv.Itoa(len(key)) + "length of value is" + strconv.Itoa(len(cvalue)))
	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test insert with lifetime = 0
 *  Test insert with lifetime = 0, insert succeed
 *  Read record with lifetime 1800?????
 ************************************************************/
func TestCreateDefaultLifeTime(t *testing.T) {
	defaultTTL := uint32(1800)
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("Haha, the max key length test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 0, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, defaultTTL, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/*****************************************************************
 *  Test insert with lifetime = max allowed, max lifetime is
 *  a configurable value and it is configured as one day for this
 *****************************************************************/
func TestCreateMaxLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("haha, max lifetime this time")

	//this is a configurate value
	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 86400, nil); err != nil {
		t.Error(err)
	}
}

/********************************************************************
 *  Test insert with max lifetime > max allowed, wait proxy implement
 *  Insert record with max lifetime > max, get BadParam error
 ********************************************************************/
func TestCreateExceedsMaxLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("test exceeds max lifetime")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 1296001, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************************
 *  Test insert with different key, value,  NS, Appname record
 *  Insert first record,
 *  Insert same NS, key, diff value, appname, gets DupKey error
 *  Insert same NS, diff key, same value, appname, insert succeed
 *  Insert diff NS, same key, value, appname, insert succeed
 *  Insert diff NS, same key, diff value, appname, insert succeed
 *  Insert diff NS, diff key, same value, appname, insert succeed
 ***********************************************************************/
func TestCreateDifferentRecord(t *testing.T) {
	key1 := testutil.GenerateRandomKey(8)
	key2 := testutil.GenerateRandomKey(8)
	key3 := testutil.GenerateRandomKey(8)
	cvalue1 := testutil.GenerateRandomKey(8)
	cvalue2 := testutil.GenerateRandomKey(8)
	cvalue3 := testutil.GenerateRandomKey(8)

	if err := testutil.CreateAndValidate(proxyClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}

	//Same NS, Key, diff value, diff Appname
	_, err := diffAppClient.Create(key1, cvalue2, client.WithTTL(30))
	if err != client.ErrUniqueKeyViolation {
		t.Error("should hit duplicate rc here, rcerr=", err)
	}

	//Same NS, diff key, Same Value, App Name
	if err := testutil.CreateAndValidate(proxyClient, key2, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key2, cvalue1, 30, 1, nil, 0); err != nil {
		t.Error(err)
	}

	//Diff NS, Same KEY, Value, Appname
	if err := testutil.CreateAndValidate(diffNSClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key1, cvalue1, 30, 1, nil, 0); err != nil {
		t.Error(err)
	}

	//Another diff NS, Same Key, diff value, diff Appname
	if err := testutil.CreateAndValidate(diffNSDiffAppClient, key1, cvalue3, 20, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSDiffAppClient, key1, cvalue3, 20, 1, nil, 0); err != nil {
		t.Error(err)
	}

	//Diff NS, diff key, Same Value, App Name
	if err := testutil.CreateAndValidate(diffNSClient, key3, cvalue1, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key3, cvalue1, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/****************************************************
 *  Test error code for NULL payload, dup key insert
 ****************************************************/
func TestCreateDupKeyNULLPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("test dupKey create")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 200, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(proxyClient, key, nil, 200, client.ErrUniqueKeyViolation); err != nil {
		t.Error(err)
	}
}

/*****************************************************
 *  Test insert after lifetime expire
 *  Insert record, sleep till liftime expire
 *  Insert the same record, record insert succeed
 *****************************************************/
func TestCreateLifetimeExpire(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("max lifetime expire test")
	var err error

	if err = testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Fatal(err)
	}
	if err = testutil.CreateAndValidate(proxyClient, key, cvalue, 10, client.ErrUniqueKeyViolation); err != nil {
		t.Fatal(err)
	}

	time.Sleep(12 * time.Second)
	cfgShare.Namespace = "NS1"
	cfgShare.Appname = "APP1"
	if proxyClient, err = client.New(cfgShare); err != nil {
		t.Fatal(err)
	}

	if err = testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Fatal(err)
	}
	if err = testutil.GetRecord(proxyClient, key, cvalue, 10, 1, nil, 0); err != nil {
		t.Fatal(err)
	}
}
