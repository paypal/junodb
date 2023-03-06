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
  
package unittest

import (
	"fmt"
	"juno/pkg/client"
	"juno/pkg/proto"
	"juno/test/testutil"
	"juno/test/testutil/mock"
	"strconv"
	"testing"

	"juno/third_party/forked/golang/glog"
)

var createStatusArray [7]uint8
var createAbortArray [3]uint8

func TestCreateNormal(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	testutil.RemoveLog(t, hostip, true)

	if _, err := Mockclient.Create(key, cvalue, 800, nil); err != nil {
		t.Error("create failed. error: ", err)
	}
	testutil.CheckCalLog(t, "API.*Create.*st=Ok.*ns=ns.*ttl=800", "1", hostip, true)
}

/***************************************************************
 * -- Timeout
 * looping each SS to simulate create gets timeout from each SS
 ***************************************************************/
func TestCreateOneSSTimeout(t *testing.T) {
	glog.SetAppName(" [TestCreateOneSSTimeout] ")
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	// note: delay is in microsecond
	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Delay = 1000000

		_, err := Mockclient.Create(key, cvalue, 180, params)
		if err != nil {
			t.Error("create failed at pos", i, err)
			t.Fatal("create failed at pos", err)
		}
		params.MockInfoList[i].Delay = 0
	}
	testutil.CheckCalLog(t, "SSReqTimeout.*Create.PrepareCreate", "3", hostip, true)
}

/*
 * looping SS to simulate create gets timeout from two SS
 */
func TestCreateTwoSSTimeout(t *testing.T) {
	glog.SetAppName(" [TestCreateTwoSSTimeout] ")
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
				params.Log(t)
				t.Error("create failed at pos", err)
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
}

/*
 * looping SS to simulate create gets timeout from three SS
 */
func TestCreateThreeSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Delay = 1000000
				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Errorf("create should fail with no storage server error: %s", err)
				}
				params.MockInfoList[k].Delay = 0
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
	testutil.CheckCalLog(t, "Create.*st=ErrBusy", "10", hostip, true)
}

/*******************************************************************
 * -- NoResponse
 * looping each SS to simulate create gets no response from each SS
 *******************************************************************/
func TestCreateOneSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].NoResponse = true

		if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
			params.Log(t)
			t.Error("create failed at pos", err)
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate create gets no response from two SS
 */
func TestCreateTwoSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].NoResponse = true

			if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
				params.Log(t)
				t.Error("create failed at pos", err)
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
	testutil.CheckCalLog(t, "SSReqTimeout.*Create.PrepareCreate", "15", hostip, true)
}

/*
 * looping SS to simulate create gets no response from three SS
 */
func TestCreateThreeSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].NoResponse = true

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Error("create should fail with no storage server error", err)
				}
				params.MockInfoList[k].NoResponse = false
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/******************************************************************
 * -- AllStatusErrorExceptDupKey
 * looping each SS to simulate create gets one error except dupkey
 ******************************************************************/
func TestCreateOneStatusError(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 5; i++ {
		for a := 0; a < len(createStatusArray)-2; a++ {
			params.MockInfoList[i].Status = createStatusArray[a]

			_, err := Mockclient.Create(key, cvalue, 180, params)
			if params.MockInfoList[i].Status != uint8(proto.OpStatusDupKey) {
				if err != nil {
					params.Log(t)
					t.Error("create failed at pos", err)
				}
			}
		}
	}
	testutil.CheckCalLog(t, "Create.*st=DupKey", "3", hostip, true)
	testutil.CheckCalLog(t, "Create.*st=Ok.*calls=P0:BadPar", "1", hostip, true)
	testutil.CheckCalLog(t, "Create.*st=Ok.*P1:RecLck", "1", hostip, true)
	testutil.CheckCalLog(t, "Create.*st=Ok.*P2:Done", "1", hostip, true)
	testutil.CheckCalLog(t, "Create.*st=Ok", "22", hostip, true)
}

/*
 * looping each SS to simulate create gets two errors except dupkey
 */
func TestCreateTwoStatusError(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		for a := 0; a < len(createStatusArray)-2; a++ {
			params.MockInfoList[i].Status = createStatusArray[a]

			for j := i + 1; j <= 4; j++ {
				for b := 0; b < len(createStatusArray)-2; b++ {
					params.MockInfoList[j].Status = createStatusArray[b]

					_, err := Mockclient.Create(key, cvalue, 180, params)
					if params.MockInfoList[i].Status != uint8(proto.OpStatusDupKey) &&
						params.MockInfoList[j].Status != uint8(proto.OpStatusDupKey) {
						if err != nil {
							params.Log(t)
							t.Error("create failed at pos", err)
						}
					}
					//printStatus("TestCreateTwoStatusError ", params, err)
				}
			}
		}
	}
}

/******************************************************************
 * -- RecordLocked (3SS)
 * looping SS to simulate create gets record lock from three SS
 ******************************************************************/
func TestCreateThreeRecordLocked(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("create failed at pos", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************************
 *-- AlreadyFulfilled (3SS)
 * looping each SS to simulate create gets AlreadyFulfilled msg from 3 SSs
 ***************************************************************************/
func TestCreateThreeAlreadyFulfilled(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusAlreadyFulfilled)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusAlreadyFulfilled)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusAlreadyFulfilled)

				if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
					params.Log(t)
					t.Error("create failed at pos", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, "Create.*st=Ok", "10", hostip, true)
}

/********************************************************************
 * -- BadParam (3SS)
 * looping SS to simulate create gets BadParam error from three SS
 ********************************************************************/
func TestCreateThreeBadParam(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadParam)

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrBadParam {
					params.Log(t)
					t.Error("create should fail with BadParam, pos", err)
				}
				//printStatus("create in TestCreateThreeBadParam ", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************************
 * -- BadMsg (3SS)
 * looping SS to simulate create gets BadMsg error from three SS
 ***************************************************************************/
func TestCreateThreeBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrBadMsg {
					params.Log(t)
					t.Error("create should fail with BadMessage error. pos", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, "Create.*st=BadMsg", "10", hostip, true)
}

/***************************************************************************
 * -- Three different status
 * looping each SS to simulate 3 different status for different SS
 ***************************************************************************/
func TestCreateThreeDifferentStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one dup, always return dup key error
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusDupKey)
				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrUniqueKeyViolation {
					params.Log(t)
					t.Error("loop 1 create should failed with dupKey error, pos ", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}

	//two error status + one dup, rc=OK only if dup located at the last position
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusAlreadyFulfilled)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[i+2].Status = uint8(proto.OpStatusDupKey)
				_, err := Mockclient.Create(key, cvalue, 180, params)

				//sample: 1{0},2{0},3{8},4{17},5{4}, rc= OK
				if params.MockInfoList[4].Status == uint8(proto.OpStatusDupKey) {
					if err != nil {
						params.Log(t)
						t.Error("create failed. error: ", err)
					}
				} else if err != client.ErrUniqueKeyViolation {
					params.Log(t)
					t.Error("loop 2 create should fail with Dupkey error,pos", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}

	//AlreadyFulfilled considered as OK
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusAlreadyFulfilled)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[i+2].Status = uint8(proto.OpStatusBadParam)
				if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
					params.Log(t)
					t.Error("create failed. error: ", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/****************************************************************************
 * -- OneTimeoutTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
func TestCreateOneTimeoutTwoDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one timeout, always return record lock error due to its position
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)
				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 1 create should fail with recordLock error, pos", err)
					t.Fatal("")
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Delay = 0
	}

	//two error status + one timeout, errorRecordLocked error
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Delay = 1000000

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 2 create should fail with recordLock error, pos", err)
				}
				//printTimeoutStatus("TestCreateOneTimeoutTwoDifferentStatus loop 2", params, err)
				params.MockInfoList[k].Delay = 0
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}

	//AlreadyFulfilled considered as OK, OK if dupkey at the last position
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusAlreadyFulfilled)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusDupKey)
				_, err := Mockclient.Create(key, cvalue, 180, params)

				if params.MockInfoList[4].Status == uint8(proto.OpStatusDupKey) {
					if err != nil {
						params.Log(t)
						t.Error("loop 3 create failed. error: ", err)
					}
				} else if err != client.ErrUniqueKeyViolation {
					params.Log(t)
					t.Fatal("loop 3 create should fail with dupKey error, pos", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************
 * Two SS timeout with one SS has different status
 * looping each SS to simulate create gets timeout for two SS
 ***************************************************************/
func TestCreateTwoTimeoutOneStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				for a := 0; a < len(createStatusArray)-2; a++ {
					params.MockInfoList[k].Status = createStatusArray[a]

					_, err := Mockclient.Create(key, cvalue, 180, params)
					if err != nil {
						if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							params.Log(t)
							t.Error("create shouldn't fail with noError and AlreadyFulfilled return code", err)
						}
						//printTimeoutStatus("TestCreateTwoTimeoutOneStatus 1,2,3rd SS timeout ", params, err)
					}
				}
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}

	/*******************************************************************************
	 * When simulate SS timeout at two positions, below iterations missing in above
	 * First SS iterate through 0,1,2 to set timeout, second SS iterate
	 *  through position 2,3 and loop through status code 0-6, third SS at position4
	 ********************************************************************************/
	params.MockInfoList[4].Delay = 1000000
	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			for a := 0; a < len(createStatusArray)-2; a++ {
				params.MockInfoList[j].Status = createStatusArray[a]

				if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
					if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
						params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
						params.Log(t)
						t.Error("create shouldn't fail with noError and AlreadyFulfilled return code", err)
					}
				}
			}
			//printTimeoutStatus("TestCreateTwoTimeoutOneStatus 4th SS timeout loop1 ", params, err)
		}
		params.MockInfoList[i].Delay = 0
	}

	/*******************************************************************************
	 * One more situation --
	 * First SS iterate through 0,1,2 and loop through status code 0-6, second SS
	 * at position 3 has timeout, third SS at position 4 has timeout
	 ********************************************************************************/
	params.MockInfoList[3].Delay = 1000000
	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 to set status code
		for a := 0; a < len(createStatusArray)-2; a++ {
			params.MockInfoList[i].Status = createStatusArray[a]

			if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
				if params.MockInfoList[i].Status == uint8(proto.OpStatusNoError) ||
					params.MockInfoList[i].Status == uint8(proto.OpStatusAlreadyFulfilled) {
					params.Log(t)
					t.Error("create shouldn't fail with noError and AlreadyFulfilled return code", err)
				}
			}
		}
		//printTimeoutStatus("TestCreateTwoTimeoutOneStatus 4th SS timeout loop2 ", params, err)
	}
	params.MockInfoList[3].Delay = 0 //recover back to normal, no timeout
	params.MockInfoList[4].Delay = 0
}

/****************************************************************************
 * -- OneNoResponseTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
func TestCreateOneNoResponseTwoDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one no response, always return record lock error due to its position
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)
				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 1 create should fail with recordLock error,pos", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].NoResponse = false
	}

	//two error status + one no response, errorRecordLocked error
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].NoResponse = true

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 2 create should fail with recordLock error,pos", err)
				}
				//printTimeoutStatus("TestCreateOneTimeoutTwoDifferentStatus loop 2", params, err)
				params.MockInfoList[k].NoResponse = false
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}

	//AlreadyFulfilled considered as OK, OK if dupkey at the last position
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusAlreadyFulfilled)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusDupKey)
				_, err := Mockclient.Create(key, cvalue, 180, params)

				if params.MockInfoList[4].Status == uint8(proto.OpStatusDupKey) {
					if err != nil {
						params.Log(t)
						t.Error("loop 3 create failed. error: ", err)
					}
				} else if err != client.ErrUniqueKeyViolation {
					params.Log(t)
					t.Error("loop 3 create should fail with dupKey error", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************
 * Two SS no response with one SS has different status
 * looping each SS to simulate create no response for two SS
 ***************************************************************/
func TestCreateTwoNoResponseOneStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				for a := 0; a < len(createStatusArray)-2; a++ {
					params.MockInfoList[k].Status = createStatusArray[a]

					if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
						if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							params.Log(t)
							t.Error("create should fail with two noresponse one error", i, j, "noresponse",
								params.MockInfoList[i].NoResponse, params.MockInfoList[j].NoResponse, "status",
								params.MockInfoList[k].Status, err)
						}
					}
				}
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/**************************************************************************
 * -- SecondFace commit one BadMsg
 * looping each SS to simulate one err, second face has bad request id err
 **************************************************************************/
func TestCreateOneCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 2; i++ {
		for a := 0; a < 3; a++ { //only set err for these ss so the commit can be valid for ss 4,5
			params.MockInfoList[i].Status = createStatusArray[a]

			for j := i + 1; j <= 4; j++ {
				params.MockInfoList[j].Opcode = proto.OpCodeCommit
				params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if params.MockInfoList[i].Status == uint8(proto.OpStatusDupKey) {
					if err == nil {
						params.Log(t)
						t.Error("create should fail with dupKey error", err)
					}
				} else if err != nil {
					if params.MockInfoList[4].Status != uint8(proto.OpStatusBadMsg) {
						params.Log(t)
						t.Error("create should fail with inconsistent status: BadMsg in commit", err)
					}
				}
				//printOpsCodeStatus("create in TestCreateOneCommitBadMsg ", params, err)
				params.MockInfoList[j].Opcode = proto.OpCodeNop
				params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
			}
		}
	}
	testutil.CheckCalLog(t, "Create.*st=InconsistentState", "12", hostip, true)
	testutil.CheckCalLog(t, "C.:BadRID", "12", hostip, true)
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- SecondFace commit two BadMsg
 * looping each SS to simulate two second face has bad request id errors
 **************************************************************************/
func TestCreateTwoCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 2; i++ {
		for a := 0; a < 3; a++ { //only set err for these ss so the commit can be valid for ss 4,5
			params.MockInfoList[i].Status = createStatusArray[a]

			for j := i + 1; j <= 4; j++ {
				params.MockInfoList[j].Opcode = proto.OpCodeCommit
				params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

				for k := j + 1; k <= 4; k++ {
					params.MockInfoList[k].Opcode = proto.OpCodeCommit
					params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

					_, err := Mockclient.Create(key, cvalue, 180, params)
					if params.MockInfoList[i].Status == uint8(proto.OpStatusDupKey) {
						if err == nil {
							params.Log(t)
							t.Error("create should fail with dupKey error", i, "status:",
								params.MockInfoList[i].Status, err)
						}
					} else if err != nil {
						params.Log(t)
						t.Error("create should fail with inconsistent status: BadMsg in commit ", err)
					}
					//printOpsCodeStatus("create in TestCreateTwoCommitBadMsg ", params, err)
					params.MockInfoList[k].Opcode = proto.OpCodeNop
					params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
				}
				params.MockInfoList[j].Opcode = proto.OpCodeNop
				params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
			}
		}
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- SecondFace commit three BadMsg
 * looping each SS to simulate three second face has bad request id errors
 **************************************************************************/
func TestCreateThreeCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 3; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodeCommit
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Opcode = proto.OpCodeCommit
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

				//TODO: XT: Return ErrInternal now, may change
				if _, err := Mockclient.Create(key, cvalue, 180, params); err != client.ErrWriteFailure {
					params.Log(t)
					t.Error("create should fail with 3 BadMsg in commit", err)
				}
				//printOpsCodeStatus("create in TestCreateThreeCommitBadMsg ", params, err)
				params.MockInfoList[k].Opcode = proto.OpCodeNop
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadParam)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)
	}
	testutil.CheckCalLog(t, "CommitFailure", "10", hostip, true)
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- commit with one NoUncommitted error
 * looping each SS to simulate one err, the second face has NoUncommit err
 **************************************************************************/
func TestCreateOneNoUncommit(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoUncommitted)

		for j := i + 1; j <= 4; j++ {
			for a := 0; a < len(createStatusArray)-2; a++ {
				params.MockInfoList[j].Status = createStatusArray[a]

				if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
					if params.MockInfoList[j].Status != uint8(proto.OpStatusDupKey) {
						params.Log(t)
						t.Error("create failed ", err)
					}
					//printOpsCodeStatus("create in TestCreateOneNoUncommit ", params, err)
				}
			}
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/******************************************************************
 * -- commit with two NoUncommitted error
 * looping each SS to simulate two NoUncommit errors at commit step
 ******************************************************************/
func TestCreateTwoNoUncommit(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 2; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoUncommitted)

		for j := i + 1; j < 3; j++ {
			for a := 0; a < 3; a++ { //only set err for these ss so the commit can be valid for ss 4,5
				params.MockInfoList[j].Status = createStatusArray[a]

				for k := j + 1; k <= 4; k++ {
					params.MockInfoList[k].Opcode = proto.OpCodeCommit
					params.MockInfoList[k].Status = uint8(proto.OpStatusNoUncommitted)

					if _, err := Mockclient.Create(key, cvalue, 180, params); err != nil {
						if params.MockInfoList[j].Status != uint8(proto.OpStatusDupKey) {
							params.Log(t)
							t.Error("create failed", err)
						}
					}
					//printOpsCodeStatus("create in TestCreateTwoNoUncommit ", params, err)
					params.MockInfoList[k].Opcode = proto.OpCodeNop
					params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
				}
			}
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- SecondFace commit three NoUnCommit error
 * looping each SS to simulate three second face had noUncommit errors
 **************************************************************************/
func TestCreateThreeNoUncommit(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoUncommitted)

		for j := i + 1; j <= 3; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodeCommit
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoUncommitted)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Opcode = proto.OpCodeCommit
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoUncommitted)

				_, err := Mockclient.Create(key, cvalue, 180, params)
				if err != client.ErrWriteFailure {
					params.Log(t)
					t.Error("Create should fail as 3 commit hits NoUncommit error ", err)
				}
				//printOpsCodeStatus("create in TestCreateThreeNoUncommit ", params, err)
				params.MockInfoList[k].Opcode = proto.OpCodeNop
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
	}
	testutil.CheckCalLog(t, "CommitFailure", "10", hostip, true)
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/*******************************************************************************
 * -- Repair OK
 * This is similar as the above test, use different way to simulate commit
 * gets noUnCommit error and if not all first 3 has noUncommit err, it return OK
 *******************************************************************************/
func TestCreateRepairNoUncommittedOK(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	params.SetStatusForAll(uint8(proto.OpStatusNoUncommitted))
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
	}

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		_, err := Mockclient.Create(key, cvalue, 180, params)
		if i >= 3 && err != client.ErrWriteFailure {
			params.Log(t)
			t.Error("create should fail with three commits fail", err)
		} else if i < 3 && err != nil { //first 2 has one noError in commit, return OK
			params.Log(t)
			t.Error("create should succeed with one commit OK and repair assume OK", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoUncommitted)
	}
	testutil.CheckCalLog(t, "Create.*st=Ok.*RR.:Ok.*RR.:Ok", "3", hostip, true)
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/*************************************************************************
 * -- BadMsg No repair
 * looping each SS to simulate commit fail due to BadMsg. return
 * error as BadMsg has no repair
 *************************************************************************/
func TestCreateBadMsgCommitErr(t *testing.T) {
	cvalue := []byte("Value to be stored for Create")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	params.SetStatusForAll(uint8(proto.OpStatusBadMsg))
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
	}

	//No need to loop to 5 because it returns CommitFailure when i>3
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		_, err := Mockclient.Create(key, cvalue, 180, params) //no repair for BadMsg
		if err != nil {
			params.Log(t)
			t.Error("create should fail with inconsistant error", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/***********************************************
 * -- abort with different return code
 * looping each SS to simulate abort case
 ***********************************************/
func TestCreateAbortDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//1,2,4 error state, 3 abort
	params.MockInfoList[0].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[1].Status = uint8(proto.OpStatusRecordLocked)
	params.MockInfoList[3].Status = uint8(proto.OpStatusRecordLocked)
	params.MockInfoList[2].Opcode = proto.OpCodeAbort
	for a := range createAbortArray {
		params.MockInfoList[2].Status = createAbortArray[a]
		_, err := Mockclient.Create(key, cvalue, 20, params)
		if err != client.ErrRecordLocked {
			t.Error("set should hit bad requestId error, err", err)
		}
		//printOpsCodeStatus("1st one in TestCreateAbortDiffStatus", params, err)
	}
	params.SetOpCodeForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))

	//2,4,5 error state, 1,3 abort
	params.MockInfoList[1].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[3].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[4].Status = uint8(proto.StatusNoCapacity)

	params.MockInfoList[0].Opcode = proto.OpCodeAbort
	params.MockInfoList[2].Opcode = proto.OpCodeAbort

	for a := range createAbortArray {
		params.MockInfoList[0].Status = createAbortArray[a]
		for b := range createAbortArray {
			params.MockInfoList[2].Status = createAbortArray[b]

			_, err := Mockclient.Create(key, cvalue, 20, params)
			if err != client.ErrInternal {
				t.Error("set should hit Internal error, err", err)
			}
			//printOpsCodeStatus("2nd one in TestCreateAbortDiffStatus", params, err)
		}
	}
	params.SetOpCodeForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

func init() {
	/********************************************************************
	 * test case status loop will loop from 0 to len(createStatusArray)-2
	 * as we want 0 to be the last error code so no need to reset 0 back
	 * for loop recovery
	 ********************************************************************/
	createStatusArray = [7]uint8{uint8(proto.OpStatusDupKey), //4
		uint8(proto.OpStatusBadParam),         //7
		uint8(proto.OpStatusRecordLocked),     //8
		uint8(proto.OpStatusAlreadyFulfilled), //17
		uint8(proto.OpStatusNoError),          //0
		//second face commit error
		uint8(proto.OpStatusNoUncommitted), //10
		uint8(proto.OpStatusBadMsg),        //1
	}

	createAbortArray = [3]uint8{uint8(proto.OpStatusNoUncommitted), //10
		uint8(proto.OpStatusBadMsg),  //1
		uint8(proto.OpStatusNoError), //0
	}
}

/********************************************
 * Print timeout and status info for each SS
 ********************************************/
func printTimeoutStatus(funcname string, params *mock.MockParams, err error) {
	fmt.Println("create in "+funcname+" "+
		"1{"+strconv.Itoa(int(params.MockInfoList[0].Status))+" <"+strconv.Itoa(int(params.MockInfoList[0].Delay))+">},"+
		"2{"+strconv.Itoa(int(params.MockInfoList[1].Status))+" <"+strconv.Itoa(int(params.MockInfoList[1].Delay))+">},"+
		"3{"+strconv.Itoa(int(params.MockInfoList[2].Status))+" <"+strconv.Itoa(int(params.MockInfoList[2].Delay))+">},"+
		"4{"+strconv.Itoa(int(params.MockInfoList[3].Status))+" <"+strconv.Itoa(int(params.MockInfoList[3].Delay))+">},"+
		"5{"+strconv.Itoa(int(params.MockInfoList[4].Status))+" <"+strconv.Itoa(int(params.MockInfoList[4].Delay))+">}, "+
		"rcerr=", err)
}

func printOpsCodeStatus(funcname string, params *mock.MockParams, err error) {
	fmt.Println("create in "+funcname+" "+
		"1{"+strconv.Itoa(int(params.MockInfoList[0].Status))+" <"+params.MockInfoList[0].Opcode.String()+","+strconv.Itoa(int(params.MockInfoList[0].Delay))+">},"+
		"2{"+strconv.Itoa(int(params.MockInfoList[1].Status))+" <"+params.MockInfoList[1].Opcode.String()+","+strconv.Itoa(int(params.MockInfoList[1].Delay))+">},"+
		"3{"+strconv.Itoa(int(params.MockInfoList[2].Status))+" <"+params.MockInfoList[2].Opcode.String()+","+strconv.Itoa(int(params.MockInfoList[2].Delay))+">},"+
		"4{"+strconv.Itoa(int(params.MockInfoList[3].Status))+" <"+params.MockInfoList[3].Opcode.String()+","+strconv.Itoa(int(params.MockInfoList[3].Delay))+">},"+
		"5{"+strconv.Itoa(int(params.MockInfoList[4].Status))+" <"+params.MockInfoList[4].Opcode.String()+","+strconv.Itoa(int(params.MockInfoList[4].Delay))+">},"+
		"rcerr=", err)
}

func printStatus(funcname string, params *mock.MockParams, err error) {
	var response [5]string

	for i := 0; i < 4; i++ {
		if params.MockInfoList[i].NoResponse == true {
			response[i] = "1"
		} else {
			response[i] = "0"
		}
	}
	fmt.Println("create in "+funcname+" "+
		"1{"+strconv.Itoa(int(params.MockInfoList[0].Status))+" <"+response[0]+">},"+
		"2{"+strconv.Itoa(int(params.MockInfoList[1].Status))+" <"+response[1]+">},"+
		"3{"+strconv.Itoa(int(params.MockInfoList[2].Status))+" <"+response[2]+">},"+
		"4{"+strconv.Itoa(int(params.MockInfoList[3].Status))+" <"+response[3]+">},"+
		"5{"+strconv.Itoa(int(params.MockInfoList[4].Status))+" <"+response[4]+">}, "+
		"rcerr=", err)
}

/*********************************************************************
 * -- StatusCode (complete loop)
 * This test will loop each SS (3 SS as unit) and defined status code.
 * It can simulate all the status code combination get from SS
 **********************************************************************/
//func TestCreateLoopProtoStatusThreeSS(t *testing.T) {
//	cvalue := []byte("Value to be stored for Create")
//	key := testutil.GenerateRandomKey(32)
//	params := mock.NewMockParams(5)
//
//	for i := 0; i < 3; i++ {
//		for a := 0; a < len(createStatusArray)-2; a++ { //loop through 6 status codes for first SS response
//			params.MockInfoList[i].Status = createStatusArray[a]
//			fmt.Println("len is ", len(createStatusArray), "a is", a, "i is", i, "status is", params.MockInfoList[i].Status)
//
//			for j := i + 1; j < 4; j++ {
//				for b := 0; b < len(createStatusArray)-2; b++ { //loop through 6 status codes for second SS response
//					params.MockInfoList[j].Status = createStatusArray[b]
//
//					for k := j + 1; k <= 4; k++ { //loop through 6 status codes for third SS response
//						for c := 0; c < len(createStatusArray)-2; c++ {
//							params.MockInfoList[k].Status = createStatusArray[c]
//							_, err := Mockclient.Create(key, cvalue, 180, params)
//							/*testutil.NEAssert(t, rc, 12, "create in TestCreateLoopProtoStatusThreeSS " +
//							strconv.Itoa(i) + "{" + strconv.Itoa(int(createStatusArray[a])) + "}," +
//							strconv.Itoa(j) + "{" + strconv.Itoa(int(createStatusArray[b])) + "}," +
//							strconv.Itoa(k) + "{" + strconv.Itoa(int(createStatusArray[c])) + "}," +
//							"should hit no storage server error, rc=" )
//							*/
//							printStatus("Tcreate in TestCreateLoopProtoStatusThreeSS ", params, err)
//						}
//					}
//				}
//			}
//		}
//	}
//	/****************************************************************************
//	 * When simulate SS at position 1 timeout, above loop misses below iteration
//	 * First SS at position 0 loop through status code 0-6, second SS iterate
//	 *  through position 2,3,4 and loop through status code 0-6
//	 ****************************************************************************/
//	params.MockInfoList[1].Delay = 1000000
//	for j := 0; j <= 0; j++ {
//		for a := 0; a < len(createStatusArray)-2; a++ {
//			params.MockInfoList[j].Status = createStatusArray[a]
//
//			for k := 2; k <= 4; k++ {
//				for b := 0; b < 7; b++ {
//					params.MockInfoList[k].Status = createStatusArray[b]
//
//					_, err := Mockclient.Create(key, cvalue, 180, params)
//					printTimeoutStatus("TestCreateOneTimeoutTwoStatus SS at p1 missing loop", params, err)
//				}
//			}
//		}
//	}
//	params.MockInfoList[1].Delay = 0
//
//	/****************************************************************************
//	 * When simulate SS at position 2 timeout, above loop misses below iteration
//	 * a> First SS at position 0 loop through status code 0-6, second SS iterate
//	 * 	  through position 1,3,4 and loop through status code 0-6
//	 * b> First SS at position 1 loop through status code 0-6, second SS iterate
//		  through position 3,4 and loop through status code 0-6
//	 ****************************************************************************/
//	params.MockInfoList[2].Delay = 1000000
//	secondSS := [3]int{1, 3, 4}
//	for j := 0; j <= 0; j++ {
//		for a := 0; a < len(createStatusArray)-2; a++ {
//			params.MockInfoList[j].Status = createStatusArray[a]
//
//			for k := 0; k <= 2; k++ {
//				for b := 0; b < 7; b++ {
//					fmt.Println("one SS at position " + strconv.Itoa(j) + ", " +
//						"one SS at position " + strconv.Itoa(secondSS[k]))
//					params.MockInfoList[secondSS[k]].Status = createStatusArray[b]
//
//					_, err := Mockclient.Create(key, cvalue, 180, params)
//					printTimeoutStatus("TestCreateOneTimeoutTwoStatus SS at p2 missing loop1", params, err)
//				}
//			}
//		}
//	}
//	secondSSP := [2]int{3, 4}
//	for j := 1; j <= 1; j++ {
//		for a := 0; a < len(createStatusArray)-2; a++ {
//			params.MockInfoList[j].Status = createStatusArray[a]
//
//			for k := 0; k <= 1; k++ {
//				for b := 0; b < len(createStatusArray)-2; b++ {
//					params.MockInfoList[secondSSP[k]].Status = createStatusArray[b]
//					fmt.Println("one SS at position " + strconv.Itoa(j) + ", " +
//						"one SS at position " + strconv.Itoa(secondSSP[k]))
//
//					_, err := Mockclient.Create(key, cvalue, 180, params)
//					printTimeoutStatus("TestCreateOneTimeoutTwoStatus SS at p2 missing loop2", params, err)
//				}
//			}
//		}
//	}
//	params.MockInfoList[2].Delay = 0
//
//	/************************************************
//	 * Timeout happens for SS in position 3
//	 ************************************************/
//	params.MockInfoList[3].Delay = 1000000
//	for i := 0; i < 2; i++ { //Loop through SS at position 0,1 to simulate set SS status code
//		for a := 0; a < len(createStatusArray)-2; a++ {
//			params.MockInfoList[i].Status = createStatusArray[a]
//
//			for j := i + 1; j <= 2; j++ { //Loop through SS at position 1,2 to simulate set SS status code
//				for b := 0; b < len(createStatusArray)-2; b++ {
//					params.MockInfoList[j].Status = createStatusArray[b]
//
//					_, err := Mockclient.Create(key, cvalue, 180, params)
//					/*testutil.NEAssert(t, rc, 12, "create in TestCreateOneTimeoutTwoStatus 2 " +
//					strconv.Itoa(j) + "{" + strconv.Itoa(int(createStatusArray[a])) + "}," +
//					strconv.Itoa(k) + "{" + strconv.Itoa(int(createStatusArray[b])) + "}," +
//					"3{timeout}, should hit no storage server error, rc=" ) */
//					printTimeoutStatus("printTestCreateOneTimeoutTwoStatus 4th SS loop1 ", params, err)
//				}
//			}
//		}
//	}
//	/****************************************************************************
//	 * Add one more situation -- set status code for one SS at position 4.
//	 * First SS iterate through position 0,1,2 loop through status code 0-6,
//	 *  second SS at position 4 loop through status code 0-6
//	 ****************************************************************************/
//	for j := 0; j < 3; j++ {
//		for a := 0; a < len(createStatusArray)-2; a++ {
//			params.MockInfoList[j].Status = createStatusArray[a]
//
//			for k := 4; k <= 4; k++ {
//				for b := 0; b < len(createStatusArray)-2; b++ {
//					params.MockInfoList[k].Status = createStatusArray[b]
//
//					_, err := Mockclient.Create(key, cvalue, 180, params)
//					/*testutil.NEAssert(t, rc, 12, "create in TestCreateOneTimeoutTwoStatus 3 " +
//					strconv.Itoa(j) + "{" + strconv.Itoa(int(createStatusArray[a])) + "}," + " 3{timeout}," +
//					strconv.Itoa(k) + "{" + strconv.Itoa(int(createStatusArray[b])) + "}," +
//					"should hit no storage server error, rc=" ) */
//					printTimeoutStatus("printTestCreateOneTimeoutTwoStatus 4th SS loop2 ", params, err)
//				}
//			}
//		}
//	}
//	params.MockInfoList[3].Delay = 0 //recover back to normal, no timeout
//
//	/************************************************
//	 * Timeout happens for SS in position 4
//	 ************************************************/
//	params.MockInfoList[4].Delay = 1000000
//	for j := 0; j < 3; j++ { //Loop through SS at position 0,1,2 to simulate set SS status code
//		for a := 0; a < len(createStatusArray)-2; a++ {
//			params.MockInfoList[j].Status = createStatusArray[a]
//
//			for k := j + 1; k < 4; k++ { //Loop through SS at position 1,2,3 to simulate set SS status code
//				for b := 0; b < 7; b++ {
//					params.MockInfoList[k].Status = createStatusArray[b]
//
//					_, err := Mockclient.Create(key, cvalue, 180, params)
//					/*testutil.NEAssert(t, rc, 12, "create in TestCreateOneTimeoutTwoStatus 4 " +
//					strconv.Itoa(j) + "{" + strconv.Itoa(int(createStatusArray[a])) + "}," +
//					strconv.Itoa(k) + "{" + strconv.Itoa(int(createStatusArray[b])) + "}," +
//					"4{timeout},should hit no storage server error, rc=" ) */
//					printTimeoutStatus("printTestCreateOneTimeoutTwoStatus 5th SS ", params, err)
//				}
//			}
//		}
//	}
//	params.MockInfoList[4].Delay = 0 //recover back to normal, no timeout
//}
