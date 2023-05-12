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

	"juno/third_party/forked/golang/glog"

	"juno/pkg/client"
	"juno/test/testutil"
)

/****************************************************
 *  Test Normal Update
 *  Create two records with different key
 *  Update and set one of the record
 *  Get record, one has version 1, one has version 3
 ****************************************************/
func TestUpdateNormal(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	key1 := testutil.GenerateRandomKey(32)
	cvalue := []byte(" the first Update test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(proxyClient, key1, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	creationTime := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Update(key, cvalue, client.WithTTL(20)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 2, 20, creationTime); err != nil {
			t.Error("update1 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime1 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Update(key, []byte("value2"), client.WithTTL(15)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 3, 20, creationTime1); err != nil {
			t.Error("update2 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime3 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Update(key, []byte("value3"), client.WithTTL(30)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 4, 30, creationTime3); err != nil {
			t.Error("Update3 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	if err := testutil.UpdateAndValidate(proxyClient, key, []byte("update record"), 20, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 20, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, cvalue, 30, 6, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key1, cvalue, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

func TestUpdateCreateDupKey(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	key1 := testutil.GenerateRandomKey(32)
	cvalue := []byte(" the first Update test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(proxyClient, key1, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	creationTime := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Update(key, cvalue, client.WithTTL(20)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 2, 20, creationTime); err != nil {
			t.Error("update1 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime1 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Update(key, []byte("value2"), client.WithTTL(15)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 3, 20, creationTime1); err != nil {
			t.Error("update2 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	creationTime3 := uint32(time.Now().Unix())
	if recInfo, err := proxyClient.Update(key, []byte("value3"), client.WithTTL(30)); err == nil {
		if err = testutil.VerifyRecordInfo(recInfo, 4, 30, creationTime3); err != nil {
			t.Error("Update3 recInfo does not contain the right value", err)
		}
	} else {
		t.Error(err)
	}

	if err := testutil.UpdateAndValidate(proxyClient, key, []byte("update record"), 20, nil); err != nil {
		t.Error(err)
	}

	_, err := proxyClient.Create(key, cvalue, client.WithTTL(10))
	if err != client.ErrUniqueKeyViolation {
		t.Error("should hit duplicate rc here, rcerr=", err)
	}

	if err := testutil.GetRecord(proxyClient, key, []byte("update record"), 30, 5, nil, creationTime3); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 20, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, cvalue, 30, 6, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key1, cvalue, 10, 1, nil, 0); err != nil {
		t.Error(err)
	}
}

/***************************************************
 *  Test update NULL key record
 *  update NULL key record, get BadParam error
 ***************************************************/
func TestUpdateNullKey(t *testing.T) {
	cvalue := []byte("the null key/ns/app Update test")

	if err := testutil.UpdateAndValidate(proxyClient, nil, cvalue, 10, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***************************************************
 *  Test update empty key/NS/Appname record
 *  Update empty key record, get BadParam error
 *  Update empty NS record, get BadParam error
 *  Update empty Appname record, get BadParam error
 ***************************************************/
func TestUpdateEmptyKeyNSAppname(t *testing.T) {
	nsKey := testutil.GenerateRandomKey(32)
	appKey := testutil.GenerateRandomKey(32)
	cvalue := []byte(" the empty key Update test")

	cfgShare.Namespace = ""
	cfgShare.Appname = "APP1"
	if emptyNSClient, err := client.New(cfgShare); err == nil {
		cfgShare.Namespace = "NS2"
		cfgShare.Appname = ""
		if emptyAppClient, err := client.New(cfgShare); err == nil {

			if err := testutil.UpdateAndValidate(proxyClient, []byte(""), cvalue, 10, client.ErrBadParam); err != nil {
				t.Error(err)
			}

			if err := testutil.UpdateAndValidate(emptyNSClient, nsKey, cvalue, 10, client.ErrBadParam); err != nil {
				t.Error(err)
			}

			if err := testutil.CreateAndValidate(emptyAppClient, appKey, cvalue, 10, nil); err != nil {
				t.Error(err)
			}

			if err := testutil.UpdateAndValidate(emptyAppClient, appKey, cvalue, 20, nil); err != nil {
				t.Error(err)
			}
		}
	}
}

/****************************************************************
 *  Test Update with key length = max allowed (set in the config)
 *  Update record with key length = max, update successful
 ****************************************************************/
func TestUpdateMaxLenKey(t *testing.T) {
	key := testutil.GenerateRandomKey(128) //this is set by config prop file
	cvalue := []byte(" the max key length test")

	if err := testutil.SetAndValidate(proxyClient, key, cvalue, 20, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, []byte("updatevalue"), 20, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, []byte("updatevalue"), 20, 2, nil, 0); err != nil {
		t.Error(err)
	}
}

/****************************************************************
 *  Test Update with key length > max allowed (set in the config)
 *  Update record with key length > max, get BadParam error
 ****************************************************************/
func TestUpdateExceedsMaxLenKey(t *testing.T) {
	key := testutil.GenerateRandomKey(257)
	cvalue := []byte(" longer than max key length test")

	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 20, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Update with many special character key or namespace
 *  Update record with many special character key
 *  Update record with many special character name space
 *  Both Update succeed
 ***********************************************************/
func TestUpdateSpecialCharNSKey(t *testing.T) {
	key := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	NS := "Q:你好嗎？A:<?xml version=\"1.0en=\"UTF-8\"" + "id=\"1@@#$%^&*()_+?"
	cvalue := []byte(" the max key length test")

	cfgShare.Namespace = NS
	cfgShare.Appname = "APP1"
	if client1, err := client.New(cfgShare); err == nil {
		glog.Debug("length of key is " + strconv.Itoa(len(key)) +
			", length of NS is " + strconv.Itoa(len(NS)))

		if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
			t.Error(err)
		}
		if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 30, nil); err != nil {
			t.Error(err)
		}
		if err := testutil.GetRecord(proxyClient, key, cvalue, 30, 2, nil, 0); err != nil {
			t.Error(err)
		}

		if err := testutil.CreateAndValidate(client1, key, cvalue, 10, nil); err != nil {
			t.Error(err)
		}
		if err := testutil.UpdateAndValidate(client1, key, []byte("key "), 10, nil); err != nil {
			t.Error(err)
		}
		if err := testutil.GetRecord(client1, key, []byte("key "), 10, 2, nil, 0); err != nil {
			t.Error(err)
		}
	}
}

/***********************************************************
 *  Test Update with many special character payload
 *  Update record with many special character payload
 *  Update record with many special character appname
 *  Both Update succeed
 ***********************************************************/
func TestUpdateSpecialCharAppPayload(t *testing.T) {
	cvalue := []byte("Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=.")
	app := "Q:你好嗎？A:我很好.  <?xml version=\"1.0\" encoding=\"UTF-8\"?><Sample_A" +
		"ST id=\"1@@#$%^&*()_+?>,<|}{[]~abc780=."
	key := testutil.GenerateRandomKey(32)
	key1 := testutil.GenerateRandomKey(32)

	cfgShare.Namespace = "NS1"
	cfgShare.Appname = app
	if client1, err := client.New(cfgShare); err == nil {
		glog.Debug("length of value is " + strconv.Itoa(len(cvalue)) +
			"length of app is" + strconv.Itoa(len(app)))

		if err := testutil.SetAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
			t.Error(err)
		}
		if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
			t.Error(err)
		}

		if err := testutil.SetAndValidate(client1, key1, cvalue, 10, nil); err != nil {
			t.Error(err)
		}
		if err := testutil.UpdateAndValidate(client1, key1, cvalue, 100, nil); err != nil {
			t.Error(err)
		}
	}
}

/*****************************************************
 *  Test Update with null payload
 *  Create/Update record with payload = nil
 *  Record create/update successful
 *****************************************************/
func TestUpdateNullPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)

	if err := testutil.CreateAndValidate(proxyClient, key, nil, 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, nil, 100, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.SetAndValidate(proxyClient, key, []byte("set new value"), 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, nil, 100, nil); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Update with empty payload
 *  Update record with empty payload, get BadParam error
 ***********************************************************/
func TestUpdateEmptyPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)

	if err := testutil.CreateAndValidate(proxyClient, key, []byte(""), 50, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, []byte(""), 100, nil); err != nil {
		t.Error(err)
	}
}

/**********************************************************************
 *  Test Update with payload length = max allowed, wait proxy implement
 *  Update record with payload length = max, update successful
 **********************************************************************/
func TestUpdateMaxLenPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(1200) //max length here might not be right, anyway
	glog.Debugln("length of key is " + strconv.Itoa(len(key)) +
		", lengh of NS1 is " + strconv.Itoa(len(cvalue)))

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 50, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
}

/***********************************************************************
 *  Test Update with payload length > max allowed, wait proxy implement
 *  Update record with payload length > max, get BadParam error
 ***********************************************************************/
func TestUpdateExceedsMaxLenPayload(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(300000)
	glog.Debug("length of key is " + strconv.Itoa(len(key)) +
		"length of value is" + strconv.Itoa(len(cvalue)))

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 50, client.ErrBadParam); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 100, client.ErrBadParam); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 0, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Update with lifetime = 0
 *  Test Update with lifetime = 0, Update succeed
 ***********************************************************/
func TestUpdateDefaultLifeTime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte(" the max key length test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 100, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 0, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 100, 2, nil, 0); err != nil {
		t.Error(err)
	}

	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 60, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 100, 3, nil, 0); err != nil {
		t.Error(err)
	}
}

/*****************************************************************
 *  Test Update with lifetime = max allowed, wait proxy implement
 *  Update record with lifetime = max, Update succeed
 *****************************************************************/
func TestUpdateMaxLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte(" max lifetime this time")

	//1 day is the max lifetime here
	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 86400, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 86400, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 86400, 2, nil, 0); err != nil {
		t.Error(err)
	}
}

/********************************************************************
 *  Test Update with max lifetime > max allowed, wait proxy implement
 *  Update record with max lifetime > max, get BadParam error
 ********************************************************************/
func TestUpdateExceedsMaxLifetime(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("test exceeds max lifetime")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 1296001, client.ErrBadParam); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 1296001, client.ErrBadParam); err != nil {
		t.Error(err)
	}
}

/***********************************************************************
 *  Test Update with different key, value,  NS, Appname record
 *  Create records to prepare for different update,
 *  Update same NS, diff key, same value, appname, Update succeed
 *  Update diff NS, same key, value, appname, Update succeed
 *  Update diff NS, same key, diff value, appname, Update succeed
 *  Update diff NS, diff key, same value, appname, Update succeed
 ***********************************************************************/
func TestUpdateDifferentRecord(t *testing.T) {
	key1 := testutil.GenerateRandomKey(8)
	key2 := testutil.GenerateRandomKey(8)
	cvalue1 := testutil.GenerateRandomKey(8)
	cvalue2 := testutil.GenerateRandomKey(8)
	//fmt.Println("request key is " + string(key1) + " data is " + string(cvalue1))

	if err := testutil.CreateAndValidate(proxyClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(proxyClient, key2, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(diffNSClient, key1, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.UpdateAndValidate(proxyClient, key2, cvalue1, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(diffNSClient, key1, cvalue2, 30, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(diffNSClient, key2, cvalue2, 30, client.ErrNoKey); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key1, cvalue1, 30, 1, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(proxyClient, key2, cvalue1, 30, 2, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key1, cvalue2, 30, 2, nil, 0); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Update after lifetime expire
 *  Create/update record, sleep till liftime expire
 *  Update the same record, record Update fail
 ***********************************************************/
func TestUpdateLifetimeExpire(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("max lifetime expire test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(diffNSClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 20, nil); err != nil {
		t.Error(err)
	}

	time.Sleep(11 * time.Second)
	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(diffNSClient, key, cvalue, 10, client.ErrNoKey); err != nil {
		t.Error(err)
	}

	if err := testutil.GetRecord(proxyClient, key, cvalue, 10, 3, nil, 0); err != nil {
		t.Error(err)
	}
	if err := testutil.GetRecord(diffNSClient, key, cvalue, 10, 1, client.ErrNoKey, 0); err != nil {
		t.Error(err)
	}
}

/***********************************************************
 *  Test Update destroyed record
 *  Create two records with same key diff NS , destroy one
 *  Update both record, one fail, one succeed
 ***********************************************************/
func TestUpdateDeletedRecord(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := []byte("max lifetime expire test")

	if err := testutil.CreateAndValidate(proxyClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
	if err := testutil.CreateAndValidate(diffNSClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.DestroyAndValidate(proxyClient, key, nil); err != nil {
		t.Error(err)
	}

	if err := testutil.UpdateAndValidate(proxyClient, key, cvalue, 10, client.ErrNoKey); err != nil {
		t.Error(err)
	}
	if err := testutil.UpdateAndValidate(diffNSClient, key, cvalue, 10, nil); err != nil {
		t.Error(err)
	}
}

/************************************************************
 *  BELOW IS FOR CONDITIONAL UPDATE
 *	create/set and update record, then do multiple conditional
 *  update, update/set and expect things are all correct
 ************************************************************/
func TestConditionalUpdate(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	cvalue := testutil.GenerateRandomKey(32)

	r1v1Ctx, err := proxyClient.Create(key, cvalue, client.WithTTL(10))
	if err != nil {
		t.Error("Create record fail ", err)
	}
	if _, err := proxyClient.Set(key, []byte("value2"), client.WithTTL(20)); err != nil {
		t.Error("set record fail ", err)
	}
	r1v3Ctx, err := proxyClient.Update(key, cvalue, client.WithTTL(80))
	if err != nil {
		t.Error("Update key fail ", err)
	}

	if _, err = proxyClient.Update(key, []byte("cupdate1"), client.WithCond(r1v1Ctx)); err != client.ErrConditionViolation {
		t.Error("condition update should fail with version too old ", err)
	}
	recInfoCUpdate2, err := proxyClient.Update(key, []byte("cupdate2"), client.WithCond(r1v3Ctx), client.WithTTL(90))
	if err != nil {
		t.Error("condition update fail", err)
	}

	if _, err = proxyClient.Update(key, cvalue, client.WithCond(r1v3Ctx)); err != client.ErrConditionViolation {
		t.Error("condition update3 should fail with version too old ", err)
	}
	recInfoCUpdate3, err := proxyClient.Update(key, []byte("cupdate4"), client.WithCond(recInfoCUpdate2), client.WithTTL(100))
	if err != nil {
		t.Error("condition update4 fail", err)
	}
	if _, err := proxyClient.Create(key, cvalue, client.WithTTL(10)); err == nil {
		t.Error("Create record should fail ", err)
	}
	if _, err := proxyClient.Set(key, cvalue, client.WithTTL(10)); err != nil {
		t.Error("Set record fail after conditional update ", err)
	}

	if err := testutil.GetRecordUpdateTTL(proxyClient, key, cvalue, 150, 6); err != nil {
		t.Error(err)
	}
	if _, err := proxyClient.Update(key, cvalue, client.WithTTL(80)); err != nil {
		t.Error("Update key fail ", err)
	}
	if err := testutil.GetRecord(proxyClient, key, cvalue, 150, 7, nil, 0); err != nil {
		t.Error("get record fail ", err)
	}

	if err := proxyClient.Destroy(key); err != nil {
		t.Error("destroy fail ", err)
	}
	if _, err := proxyClient.Update(key, []byte("cupdate1"), client.WithCond(recInfoCUpdate3)); err != client.ErrNoKey {
		t.Error("condition update should fail with nokey ", err)
	}
	if _, err := proxyClient.Update(key, cvalue, client.WithTTL(80)); err != client.ErrNoKey {
		t.Error("Update should fail with no key ", err)
	}
}
