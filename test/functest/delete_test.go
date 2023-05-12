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
	//	"encoding/hex"
	"juno/pkg/client"
	"juno/test/testutil"
	"testing"
	"time"
)

/************************************************************************
 *  Insert and delete the record, read record, it should have no record
 *  Insert and read again, record read successfully
 ************************************************************************/
func TestDeleteBasic(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("Haha, the first delete test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.DestroyAndValidate(proxyClient, key, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, cvalue, 500, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 100, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/************************************************************************
 * Create record with same key, different namespace, appname, insert one
 * more time for one of the client, it should get dupKey error.
 * Delete one, read all three, two have noKey error, one read successfully
 ************************************************************************/
func TestDeleteDiffNSAPP(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("the second delete test")
	//fmt.Println("request key is " + string(key) + " data is " + string(cvalue))

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(diffAppClient, key, cvalue, 100, client.ErrUniqueKeyViolation); err != nil {
		t.Error(err)
	}
	//	"client2 has same NS and key, DupError should be throw here ")

	if err := testutil.CreateAndValidate(diffNSClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.CreateAndValidate(diffNSClient, key, cvalue, 100, client.ErrUniqueKeyViolation); err != nil {
		t.Error(err)
	}
	//	"client3 has record already, it should have dupKey error, rcerr=")

	if err := testutil.DestroyAndValidate(diffAppClient, key, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(diffAppClient, key, cvalue, 100, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, cvalue, 100, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(diffNSClient, key, cvalue, 100, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/**********************************************************************
 * Delete a non-existing record, delete return successful
 **********************************************************************/
func TestDeleteNonExisting(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	if err := testutil.DestroyAndValidate(proxyClient, key, nil); err != nil {
		t.Error(err)
	}
}

/**********************************************************************
 * Delete a NULL key or empty key record, delete return badParam
 **********************************************************************/
func TestDeleteNULLEmptyKey(t *testing.T) {
	var key1 []byte
	key1 = nil
	key2 := []byte("")

	if err := testutil.DestroyAndValidate(proxyClient, key1, client.ErrBadParam); err != nil {
		t.Error(err)
	}
	if err := testutil.DestroyAndValidate(proxyClient, key2, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/******************************************************************
 * Delete a record with a very long length, get badParam error
 ******************************************************************/
func TestDeleteLongLen(t *testing.T) {
	key := testutil.GenerateRandomKey(1500)
	if err := testutil.DestroyAndValidate(proxyClient, key, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/**********************************************************************
 * Delete expired record
 **********************************************************************/
func TestDeleteExpiredRecord(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("TestExpiredRecordDelete")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 3, nil); err != nil {
		t.Error(err)
		t.FailNow()
	}

	time.Sleep(9 * time.Second)
	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 3, nil); err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err := testutil.DestroyAndValidate(proxyClient, key, nil); err != nil {
		t.Error(err)
		t.FailNow()
	}
}
