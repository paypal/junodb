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
//  Package utility provides the utility interfaces for mux package
//  
package functest

import (
	"juno/pkg/client"
	"juno/pkg/util"
	"juno/test/testutil"
	"testing"
	"time"

	"juno/third_party/forked/golang/glog"
)

/***********************************************************************
 *  Test Normal get
 *  Insert record with liftime set as 10 secs
 *  Read record after sleep for 5 secs, read successful
 *  Read record after another sleep for 7 secs, read fail with no record
 ***********************************************************************/
func TestGetAfterSleep(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("Haha, the first get test")
	glog.Debug("request key is " + util.ToPrintableAndHexString(key) + " data is " + string(cvalue))

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	time.Sleep(5 * time.Second)

	if err := testutil.GetRecord(proxyClient, key, cvalue, 5, 1, nil, 0); err != nil {
		t.Error(err)
	}

	time.Sleep(6 * time.Second) //get expired record
	if err := testutil.GetRecord(proxyClient, key, cvalue, 5, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Null/Empty key get
 *  Read record with Null key, return bad param error
 *  Read record after empty key, return bad param error
 ***********************************************************/
func TestGetNullEmptyKey(t *testing.T) {
	cvalue := []byte("null/empty get test")

	if err := testutil.GetRecord(proxyClient, nil, cvalue, 10, 1, client.ErrBadParam, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, []byte(""), cvalue, 10, 1, client.ErrBadParam, 0); err != nil {
		t.Error(err)
	}
}

/**************************************************************
 *  Test Null/Empty payload get
 *  Insert and read record with Null payload, read successful
 *  Insert and read record with empty payload, read successful
 **************************************************************/
func TestGetNullEmptyPayload(t *testing.T) {
	key1 := testutil.GenerateRandomKey(32)
	key2 := testutil.GenerateRandomKey(32)

	if err := testutil.CreateAndValidate(proxyClient, key1, nil, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key1, nil, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(proxyClient, key2, []byte(""), 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key2, []byte(""), 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/**************************************************************
 *  Test non-exist record
 *  Read record with non-exist key, get no data error
 **************************************************************/
func TestGetNonExistingRecord(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := []byte("no value")

	if err := testutil.GetRecord(proxyClient, key, value, 10, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}
}

/**************************************************************
 *  Test get record with key length greater than 128
 *  Read record with key length greater than 128,
 *  Get Bad Param error
 **************************************************************/
func TestGetKeyLengthExceedsMax(t *testing.T) {
	key := testutil.GenerateRandomKey(257)
	value := []byte("no value")

	if err := testutil.GetRecord(proxyClient, key, value, 10, 1, client.ErrBadParam, 0); err != nil {
		t.Error(err)
	}
}

/**************************************************************
 *  Test get record with different NS
 *  Create record with NS1, create the same key record with NS2
 *  Read both record, both should read successful
 **************************************************************/
func TestGetDiffNS(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value1 := testutil.GenerateRandomKey(32)
	value2 := testutil.GenerateRandomKey(32)

	//create,get record with different NS, same key
	if err := testutil.CreateAndValidate(proxyClient, key, value1, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(diffNSClient, key, value2, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, value1, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(diffNSClient, key, value2, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/********************************************************************
 *  Test get record with different NS
 *  Create record with NS1,key1, create record with NS2, key2
 *  Read record with NS1, key2 and NS2, key1, both get no data error
 *******************************************************************/
func TestGetDiffNSCross(t *testing.T) {
	key1 := testutil.GenerateRandomKey(32)
	key2 := testutil.GenerateRandomKey(32)
	value := testutil.GenerateRandomKey(32)

	//create, get record with different NS, same key
	if err := testutil.CreateAndValidate(proxyClient, key1, value, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(diffNSClient, key2, value, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key2, value, 10, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(diffNSClient, key1, value, 10, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}
}

/**************************************************************
 *  Test get record via different appname
 *  Create record with key1,App1, read record with key1, app2
 *  Read record successful
 **************************************************************/
func TestGetDiffAppName(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := testutil.GenerateRandomKey(32)

	cfgShare.Namespace = "NS1"
	cfgShare.Appname = "APP2"
	if diffAppClient, err := client.New(cfgShare); err == nil {

		//create record in app1, retrieve record in app2
		if err := testutil.CreateAndValidate(proxyClient, key, value, 10, nil); err != nil {
			t.Error(err)
		}

		if err := testutil.GetRecord(proxyClient, key, value, 10, 1, nil, 0); err != nil {
			t.Error(err)
		}

		if err := testutil.GetRecord(diffAppClient, key, value, 10, 1, nil, 0); err != nil {
			t.Error(err)
		}
	}
}

/***********************************************************
*  Test get record TTL will update lifetime to new one
*  Create record, get record with new bigger lifetime
*  Get record again, check the lifetime got updated
*  Extending TTL by get does not change record version
***********************************************************/
func TestGetLifetimeUpdate(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := []byte("update lifetime")

	if err := testutil.CreateAndValidate(proxyClient, key, value, 100, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecordUpdateTTL(proxyClient, key, value, 300, 1); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, value, 300, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/* **********************************************************
 *  TODO:::Test get record shorten lifetime get error
 *  Might delete this depending on the implementation of get
 *  record update
 *  Create record, get record with new smaller lifetime
 *  Get record again, read original record still successful
 ************************************************************/
func TestGetShortLifetimeUpdate(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := []byte("shorten lifetime test")

	if err := testutil.SetAndValidate(proxyClient, key, value, 300, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecordUpdateTTL(proxyClient, key, value, 250, 1); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, value, 300, 1, nil, 0); err != nil {
		t.Error(err)
	}
}
