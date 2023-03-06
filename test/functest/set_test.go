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
	"juno/pkg/client"
	"juno/test/testutil"
	"strconv"
	"testing"
	"time"

	"juno/third_party/forked/golang/glog"
)

/*************************************************
 *  Test Normal Set
 *  Set normal record, record read successful
 *************************************************/
func TestSetNormal(t *testing.T) {
	key := testutil.GenerateRandomKey(32) //this is set by config prop file
	cvalue := []byte("Haha, the first Set test")

	creationTime := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Set(key, cvalue, client.WithTTL(10)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 1, 10, creationTime); err != nil {
			t.Error("set recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime1 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Set(key, []byte("value2"), client.WithTTL(20)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 2, 20, creationTime1); err != nil {
			t.Error("set2 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime3 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Set(key, []byte("value3"), client.WithTTL(15)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 3, 20, creationTime3); err != nil {
			t.Error("set3 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime4 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Set(key, []byte("value4"), client.WithTTL(30)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 4, 30, creationTime4); err != nil {
			t.Error("set4 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 40, nil); err != nil {
		//set one more time
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, []byte(" "), 10, nil); err != nil {
		//set one more time
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, []byte(" "), 40, 7, nil, 0); err != nil {
		t.Error(err)
	}
}

/***************************************************
 *  Test set NULL key record
 *  Set NULL key record, get BadParam error
 ***************************************************/
func TestSetNullKey(t *testing.T) {
	cvalue := []byte("Haha, the null key/ns/app Set test")

	if err := testutil.SetAndValidate(proxyClient, nil, cvalue, 10, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***************************************************
 *  Test set empty key/NS/Appname record
 *  Set empty key record, get BadParam error
 *  Set empty NS record, get BadParam error
 *  Set empty Appname record, get BadParam error
 ***************************************************/
func TestSetEmptyKeyNSAppname(t *testing.T) {
	nsKey := testutil.GenerateRandomKey(32)
	appKey := testutil.GenerateRandomKey(32)
	cvalue := []byte("Haha, the empty key Set test")
	cfgShare.Namespace = ""
	cfgShare.Appname = "APP1"
	if emptyNSClient, err := client.New(cfgShare); err == nil {
		cfgShare.Namespace = "NS2"
		cfgShare.Appname = ""
		if emptyAppClient, err := client.New(cfgShare); err == nil {
			///TODO emptyNSClient.ConnectTimeout.Duration = 500 * time.Millisecond
			///TODO emptyAppClient.ConnectTimeout.Duration = 500 * time.Millisecond

			if err := testutil.SetAndValidate(proxyClient, []byte(""), cvalue, 10, client.ErrBadParam); err != nil {
				t.Error(err)
			}

			if err := testutil.SetAndValidate(emptyNSClient, nsKey, cvalue, 10, client.ErrBadParam); err != nil {
				t.Error(err)
			}

			if err := testutil.SetAndValidate(emptyAppClient, appKey, cvalue, 10, nil); err != nil {
				t.Error(err)
			}
		}
	}
}

/****************************************************************
 *  Test Set with key length = max allowed (set in the config)
 *  Set record with key length = max, set successful
 ****************************************************************/
func TestSetMaxLenKey(t *testing.T) {
	key := testutil.GenerateRandomKey(128) //this is set by config prop file
	cvalue := []byte("Haha, the max key length test")
	glog.Debug("length of key is " + strconv.Itoa(len(key)) +
		", length of value is " + strconv.Itoa(len(cvalue)))

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 20, nil); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Set with key length > max allowed (set in the config)
 *  Set record with key length > max, get BadParam error
 ***********************************************************/
func TestSetExceedsMaxLenKey(t *testing.T) {
	key := testutil.GenerateRandomKey(257)
	cvalue := []byte("Haha, longer than max key length test")
	glog.Debug("length of key is " + strconv.Itoa(len(key)) +
		", length of value is " + strconv.Itoa(len(cvalue)))

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 20, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Set with many special character key or namespace
 *  Set record with many special character key
 *  Set record with many special character name space
 *  Both Set succeed
 ***********************************************************/
func TestSetSpecialCharNSKey(t *testing.T) {
	key := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	NS1 := "Q:你好嗎？A:<?xml version=\"1.0en=\"UTF-8\"" + "id=\"1@@#$%^&*()_+?"
	glog.Debug("length of key is " + strconv.Itoa(len(key)) +
		", length of NS1 is " + strconv.Itoa(len(NS1)))
	cvalue := []byte("Haha, the max key length test")

	cfgShare.Namespace = "NS"
	cfgShare.Appname = "APP"
	if client1, err := client.New(cfgShare); err == nil {
		cfgShare.Namespace = NS1
		cfgShare.Appname = "APP"
		if client2, err := client.New(cfgShare); err == nil {

			if err := testutil.SetAndValidate(client1, key, cvalue, 100, nil); err != nil {
				t.Error(err)
			}

			if err := testutil.SetAndValidate(client2, []byte("key haha"), cvalue, 100, nil); err != nil {
				t.Error(err)
			}
		}
	}
}

/***********************************************************
 *  Test Set with many special character payload
 *  Set record with many special character payload
 *  Set record with many special character appname
 *  Both Set succeed
 ***********************************************************/
func TestSetSpecialCharAppPayload(t *testing.T) {
	cvalue := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	app := "Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=."
	key := testutil.GenerateRandomKey(32)
	key1 := testutil.GenerateRandomKey(32)

	cfgShare.Namespace = "NS1"
	cfgShare.Appname = app
	if client2, err := client.New(cfgShare); err == nil {

		glog.Info("length of value is " + strconv.Itoa(len(cvalue)) +
			", length of app is " + strconv.Itoa(len(app)))

		if err := testutil.SetAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
			t.Error(err)
		}

		if err := testutil.SetAndValidate(client2, key1, cvalue, 100, nil); err != nil {
			t.Error(err)
		}
	}
}

/***********************************************************
 *  Test client Set with special character namespace
 ***********************************************************/
func TestSetSpecialCharNS(t *testing.T) {
	//MAX length of namespace is 64
	NS := "Q:你好嗎？A:<?xml version=\"1.0en=\"UTF-8\"" + "id=\"1@@#$%^&*()_+?"
	key := []byte("key")
	value := []byte("value")

	cfgShare.Namespace = NS
	cfgShare.Appname = "APP1"
	if client1, err := client.New(cfgShare); err == nil {

		if err := testutil.SetAndValidate(client1, key, value, 10, nil); err != nil {
			t.Error(err)
		}
	}
}

/***********************************************************
 *  Test Set with null payload
 *  Set record with payload = nil, return no error
 ***********************************************************/
func TestSetNullPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)

	if err := testutil.SetAndValidate(proxyClient, key, nil, 100, nil); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Set with empty payload
 *  Set record with empty payload, get BadParam error
 ***********************************************************/
func TestSetEmptyPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)

	if err := testutil.SetAndValidate(proxyClient, key, []byte(""), 100, nil); err != nil {
		t.Error(err)
	}
}

/**********************************************************************
 *  Test Set with payload length = max allowed, wait proxy implement
 *  Set record with payload length = max, return nil
 **********************************************************************/
func TestSetMaxLenPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(204800)
	glog.Debug("length of key is " + strconv.Itoa(len(key)) +
		", length of value is " + strconv.Itoa(len(cvalue)))

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
}

/***********************************************************************
 *  Test Set with payload length > max allowed, wait proxy implement
 *  Set record with payload length > max, get BadParam error
 ***********************************************************************/
func TestSetExceedsMaxLenPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	//currently, server side default max_payload_length is 204800, client side limit is missing
	cvalue := testutil.GenerateRandomKey(300000)
	glog.Debug("length of key is " + strconv.Itoa(len(key)) +
		", length of value is " + strconv.Itoa(len(cvalue)))

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 100, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/************************************************************
 *  Test Set with long/short different lifetime
 *  Set record with lifetime 0
 *  Read record, the lifetime got set 100, version is 1
 *  Set a shorter lifetime 50 and read record,
 * 	Lifetime got still 100, version is 2
 *  Set another rec with lifetime 10, set again with 0 lifetime
 *  Read record, version updated to 2, lifetime keeps at 10
 *  Set the same record again with lifetime 200, and 0 again
 *  Read record, version updated to 4, lifetime keeps at 200
 ************************************************************/
func TestSetDefaultLifeTime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	key1 := testutil.GenerateRandomKey(32)
	cvalue := []byte("Haha, lifetime test")
	cvalue1 := []byte("Default lifetime test value2")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 0, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 1800, 1, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 50, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 1800, 2, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key1, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key1, cvalue, 0, nil); err != nil {
		t.Error(err) //set again with default time
	}
	if err := testutil.GetRecord(proxyClient, key1, cvalue, 10, 2, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key1, cvalue1, 200, nil); err != nil {
		t.Error(err) //set again with longer time
	}
	if err := testutil.SetAndValidate(proxyClient, key1, cvalue1, 50, nil); err != nil {
		t.Error(err) ////set shorter time
	}

	if err := testutil.GetRecord(proxyClient, key1, cvalue1, 200, 4, nil, 0); err != nil {
		t.Error(err)
	}
}

/************************************************************
 *  Test Set with lifetime = max allowed which is one day
 *  Set record succeed
 ***********************************************************/
func TestSetMaxLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("haha, max lifetime this time")

	//1 day is configured as the max lifetime
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 86400, nil); err != nil {
		t.Error(err)
	}
}

/********************************************************************
 *  Test Set with max lifetime > max allowed, wait proxy implement
 *  Set record with max lifetime > max, get BadParam error
 ********************************************************************/
func TestSetExceedsMaxLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("test exceeds max lifetime")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 1296001, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************************
 *  Test Set with different key, value,  NS, Appname record
 *  Set first record,
 *  Set same NS, key, same value, appname, get version 2 record
 *  Set same NS, diff key, same value, appname, Set succeed with v1 rec
 *  Set diff NS, same key, value, appname, Set succeed
 *  Set diff NS, same key, diff value, appname, Set succeed with v1 rec
 *  Set diff NS, diff key, same value, appname, Set succeed
 ***********************************************************************/
func TestSetDifferentRecord(t *testing.T) {
	key1 := testutil.GenerateRandomKey(8)
	key2 := testutil.GenerateRandomKey(8)
	key3 := testutil.GenerateRandomKey(8)
	cvalue1 := testutil.GenerateRandomKey(8)
	cvalue3 := testutil.GenerateRandomKey(8)

	if err := testutil.SetAndValidate(proxyClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	//Set same NS, key, same value, appname
	if err := testutil.SetAndValidate(proxyClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key1, cvalue1, 30, 2, nil, 0); err != nil {
		t.Error(err)
	}

	//Same NS, diff key, Same Value, App Name
	if err := testutil.SetAndValidate(proxyClient, key2, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key2, cvalue1, 30, 1, nil, 0); err != nil {
		t.Error(err)
	}

	//Diff NS, Same KEY, Value, Appname
	if err := testutil.SetAndValidate(diffNSClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key1, cvalue1, 30, 1, nil, 0); err != nil {
		t.Error(err)
	}

	//Another diff NS, Same Key, diff value, diff Appname
	if err := testutil.SetAndValidate(diffNSDiffAppClient, key1, cvalue3, 20, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSDiffAppClient, key1, cvalue3, 20, 1, nil, 0); err != nil {
		t.Error(err)
	}

	//Diff NS, diff key, Same Value, App Name
	if err := testutil.SetAndValidate(diffNSClient, key3, cvalue1, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key3, cvalue1, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/**********************************************************
 *  Test set same key, NULL payload, succeed with v2 record
 **********************************************************/
func TestSetDupKeyNULLPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("test dupKey Set")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 200, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(proxyClient, key, nil, 100, client.ErrUniqueKeyViolation); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key, nil, 200, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, nil, 200, 2, nil, 0); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Set after lifetime expire
 *  Set record twice, sleep till liftime expire
 *  Set the same record, record Set succeed
 ***********************************************************/
func TestSetLifetimeExpire(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("max lifetime expire test")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	time.Sleep(11 * time.Second)
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, cvalue, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Set/Delete
 *  Set, delete, create,set, create, set and get.
 *  delete, set and get record
 *  Read record gets version 1 record
 ***********************************************************/
func TestSetCreateDelete(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("set/delete test")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.DestroyAndValidate(proxyClient, key, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, client.ErrUniqueKeyViolation); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 10, 3, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.DestroyAndValidate(proxyClient, key, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Set/Delete
 *  Set record, set the same key one with bigger lifetime
 *  Set the same key one with shorter lifetime
 *  Get record gets version 3 record
 ***********************************************************/
func TestSetVersionLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("Setversionlifetime")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 50, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 100, 3, nil, 0); err != nil {
		t.Error(err)
	}
}

/********************************************************************
 *  Test set record with different NS
 *  Set record with NS1,key1, set record with NS2, key2
 *  Read record with NS1, key2 and NS2, key1, both get no data error
 *  Set the record with NS1, key2 and set again
 *  Read record NS1/key2, NS2/key2 and NS1/key1
 *  Read all successful with v2, v1, v1 accordingly
 *******************************************************************/
func TestSetDiffNSCross(t *testing.T) {
	key1 := testutil.GenerateRandomKey(32)
	key2 := testutil.GenerateRandomKey(32)
	value := testutil.GenerateRandomKey(32)

	//create, get record with different NS, same key
	if err := testutil.SetAndValidate(proxyClient, key1, value, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(diffNSClient, key2, value, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key2, value, 10, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key1, value, 10, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key2, value, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.SetAndValidate(proxyClient, key2, value, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key2, value, 10, 2, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key2, value, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key1, value, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}
