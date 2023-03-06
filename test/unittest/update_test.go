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
	//"juno/third_party/forked/golang/glog"
	"juno/pkg/client"
	"juno/pkg/proto"
	"juno/test/testutil"
	"juno/test/testutil/mock"
	"testing"
)

var updatePrepareArray [6]uint8
var updateCommitRepairableErr [3]uint8
var updateAbortArray [5]uint8

//var updateOKArray [3]uint8

func TestUpdateNormal(t *testing.T) {
	value := []byte("Value to be stored for Update")
	key := testutil.GenerateRandomKey(32)
	testutil.RemoveLog(t, hostip, true)

	params := mock.NewMockParams(5)
	params.SetOpCodeForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
	_, err := Mockclient.Update(key, value, 800, params)
	if err != nil {
		t.Error("create failed. error: ", err)
	}
	testutil.CheckCalLog(t, "API.*Update.*st=Ok.*ns=ns", "1", hostip, true)
}

func TestUpdatePrepareInserting(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := []byte("Value to be stored for Update")
	testutil.RemoveLog(t, hostip, true)

	params := mock.NewMockParams(5)
	params.SetVersionForAll(1)
	params.SetOpCodeForAll(proto.OpCodePrepareUpdate)
	params.SetStatusForAll(uint8(proto.OpStatusInserting))
	params.MockInfoList[1].Status = uint8(proto.OpStatusNoError)
	params.MockInfoList[1].Version = 3

	recInfo, err := Mockclient.Update(key, value, 800, params)
	if err != nil {
		t.Error("Update fail: ", err)
	}
	if recInfo.GetVersion() != 4 {
		t.Error("Wrong version, real version is ", recInfo.GetVersion())
	}
	testutil.CheckCalLog(t, "API.*Update.*st=Ok.*Insr.*Insr", "1", hostip, true)
}

/***************************************************************
 * -- Timeout
 * looping each SS to simulate update gets timeout from each SS
 ***************************************************************/
func TestUpdateOneSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	// note: delay is in microsecond
	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Delay = 1000000

		_, err := Mockclient.Update(key, cvalue, 100, params)
		if err != nil {
			params.Log(t)
			t.Error("update failed", err)
		}
		params.MockInfoList[i].Delay = 0
	}
}

/*
 * looping SS to simulate update gets timeout from two SS
 */
func TestUpdateTwoSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			if _, err := Mockclient.Update(key, cvalue, 100, params); err != nil {
				params.Log(t)
				t.Error("update failed ", err)
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
	testutil.CheckCalLog(t, ".*SSReqTimeout.*op=Update.PrepareUpdate", "15", hostip, true)
}

/*
 * looping SS to simulate update gets timeout from three SS
 */
func TestUpdateThreeSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Delay = 1000000
				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Errorf("update should fail with no storage server error. %s", err)
				}
				params.MockInfoList[k].Delay = 0
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
}

/*******************************************************************
 * -- NoResponse
 * looping each SS to simulate update gets no response from each SS
 *******************************************************************/
func TestUpdateOneSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].NoResponse = true

		if _, err := Mockclient.Update(key, cvalue, 100, params); err != nil {
			params.Log(t)
			t.Error("update failed", err)
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate update gets no response from two SS
 */
func TestUpdateTwoSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].NoResponse = true

			if _, err := Mockclient.Update(key, cvalue, 100, params); err != nil {
				params.Log(t)
				t.Error("update failed ", err)
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate update gets no response from three SS
 */
func TestUpdateThreeSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].NoResponse = true

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Error("update should fail with no storage server error", err)
				}
				params.MockInfoList[k].NoResponse = false
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/************************************************************
 * -- SetOneStatusError
 * looping each SS to simulate update gets one error from ss
 ************************************************************/
func TestUpdateOneStatusError(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetOpCodeForAll(proto.OpCodePrepareUpdate)

	for i := 0; i < 1; i++ {
		for a := range updatePrepareArray {
			params.MockInfoList[i].Status = updatePrepareArray[a]

                        if updatePrepareArray[a] == uint8(proto.OpStatusInserting) {
                                params.MockInfoList[i].Version = uint32(0)
                        }
			_, err := Mockclient.Update(key, cvalue, 100, params)
			if err != nil {
				params.Log(t)
				t.Error("update failed.", err)
			}
		}
	}
}

/*
 * looping each SS to simulate update gets two errors from ss
 */
func TestUpdateTwoStatusError(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetOpCodeForAll(proto.OpCodePrepareUpdate)
	insertingVersion := uint32(2) //the markdelete version
	okVersion := uint32(3)

	params.SetVersionForAll(okVersion)
	for i := 0; i < 4; i++ {
		for a := range updatePrepareArray {
			params.MockInfoList[i].Status = updatePrepareArray[a]
			if updatePrepareArray[a] == uint8(proto.OpStatusInserting) {
				params.MockInfoList[i].Version = insertingVersion
			}

			for j := i + 1; j <= 4; j++ {
				for b := range updatePrepareArray {
					params.MockInfoList[j].Status = updatePrepareArray[b]
					if updatePrepareArray[b] == uint8(proto.OpStatusInserting) {
						params.MockInfoList[j].Version = insertingVersion
					}

					_, err := Mockclient.Update(key, cvalue, 100, params)
					if err != nil {
						params.Log(t)
						t.Error("update failed ", err)
					}
					params.MockInfoList[j].Version = okVersion
				}
			}
			params.MockInfoList[i].Version = okVersion
		}
	}
}

/******************************************************************
 * -- ThreeStatusOK (3SS)
 * looping SS to simulate update gets record lock from three SS
 ******************************************************************/
func TestUpdateThreeStatusOKRecord(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetOpCodeForAll(proto.OpCodePrepareUpdate)
	testutil.RemoveLog(t, hostip, true)
	insertingVersion := uint32(2) //the markdelete version
	okVersion := uint32(3)

	for i := 0; i < 3; i++ {
		for a := 3; a <= 5; a++ {
			params.MockInfoList[i].Status = updatePrepareArray[a]
			if updatePrepareArray[a] == uint8(proto.OpStatusInserting) {
				params.MockInfoList[i].Version = insertingVersion
			}

			for j := i + 1; j < 4; j++ {
				for b := 3; b <= 5; b++ {
					params.MockInfoList[j].Status = updatePrepareArray[b]
					if updatePrepareArray[b] == uint8(proto.OpStatusInserting) {
						params.MockInfoList[j].Version = insertingVersion
					}

					for k := j + 1; k <= 4; k++ {
						for c := 3; c <= 5; c++ {
							params.MockInfoList[k].Status = updatePrepareArray[c]
							if updatePrepareArray[c] == uint8(proto.OpStatusInserting) {
								params.MockInfoList[k].Version = insertingVersion
							}

							_, err := Mockclient.Update(key, cvalue, 100, params)
							if params.MockInfoList[0].Status+params.MockInfoList[1].Status+
								params.MockInfoList[2].Status == 45 { //if first 3 are all inserting, return no key err
								if err != client.ErrNoKey {
									params.Log(t)
									t.Error("Should get no key err with three inserting status", err)
								}
							} else if err != nil {
								params.Log(t)
								t.Error("update failed ", err)
							}
							params.MockInfoList[k].Version = okVersion
						}
					}
					params.MockInfoList[j].Version = okVersion
				}
			}
			params.MockInfoList[i].Version = okVersion
		}
	}
	testutil.CheckCalLog(t, ".*API.Update.*st=Ok", "269", hostip, true)
	testutil.CheckCalLog(t, ".*API.Update.*st=NoKey", "1", hostip, true)
}

/******************************************************************
 * -- TwoRecordLockedOneBadParam (3SS)
 * looping SS to simulate update gets error for three SSs
 ******************************************************************/
func TestUpdateTwoRecordLockedOneBadParam(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("update should fail with RecordLock", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, ".*API.*Update.*st=RecordLocked", "10", hostip, true)
}

/***************************************************************************
 *-- TwoOutOfMemOneBadParam (3SS)
 * looping SS to simulate update gets error for three SSs
 * return either OutOfMem or BadParam
***************************************************************************/
func TestUpdateTwoOutOfMemOneBadParam(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBusy)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBusy)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadParam)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrBusy && err != client.ErrBadParam {
					params.Log(t)
					t.Error("update should fail with OutOfStorage", err)
				}
				//printStatus("update in TestUpdateTwoBadParamOneRecordLock ", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, ".*API.*Update.*st=BadParam", "9", hostip, true)
	testutil.CheckCalLog(t, ".*API.*Update.*st=OutOfMem", "1", hostip, true)
}

/*************************************************************
 * -- TwoBadParamOneRecordLock (3SS)
 * looping SS to simulate update gets error from three SSs
 *************************************************************/
func TestUpdateTwoBadParamOneRecordLock(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("update should fail with BadParam", err)
				}
				//printStatus("update in TestUpdateTwoBadParamOneRecordLock ", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/********************************************************************
 * -- ThreeMixOKErrorStatus
 * looping each SS to simulate 3 different status for different SS
 ********************************************************************/
//--- FAIL: TestUpdateThreeMixErrorOKStatus (0.04s)
//	common.go:102: MockParams being set {
//		SS[0] ns=ns,op=Nop,st=RecordLocked,del=0,ver=1
//		SS[1] ns=ns,op=Nop,st=Inserting,del=0,ver=1
//		SS[2] ns=ns,op=Nop,st=OutOfMem,del=0,ver=1
//		SS[3] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[4] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		}

func TestUpdateThreeMixErrorOKStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	//two error status + one AlreadyFulfilled, always return OK
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusAlreadyFulfilled)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadParam)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("loop 1 update failed", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}

	//two error status + one Inserting, always return inconsistent error
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusInserting)
			params.MockInfoList[j].Version = 0

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBusy)
				_, err := Mockclient.Update(key, cvalue, 100, params)
				//if err != client.ErrInternal { TODO: need further discuss, for now treat as ok
				if err != nil {
					params.Log(t)
					t.Error("loop 2 update should fail with inconsistent error", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
			params.MockInfoList[j].Version = mock.DEF_VER
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, "API.*Update.*st=Ok", "10", hostip, true)
	testutil.CheckCalLog(t, "API.*Update.*st=InconsistentState", "10", hostip, true)
}

/*********************************************************
 * -- ThreeMixPrepareInserting
 * looping each SS to simulate 3 different status
 * including  one inserting at prepareupdate
 *********************************************************/
func TestUpdateThreeMixPrepareInserting(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	//two error status + one Inserting at prepareUpdate, always return inconsistent error
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodePrepareUpdate
			params.MockInfoList[j].Status = uint8(proto.OpStatusInserting)
			params.MockInfoList[j].Version = 0

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.StatusNoCapacity)
				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("loop 2 update fail", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
			params.MockInfoList[j].Version = mock.DEF_VER
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, ".*API.*Update.*st=Ok", "10", hostip, true)
}

/****************************************************************************
 * -- OneTimeoutTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
//Why FIXME
//	common.go:102: MockParams being set {
//		SS[0] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[1] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[2] ns=ns,op=Nop,st=Ok,del=1000000,ver=1
//		SS[3] ns=ns,op=Nop,st=Inserting,del=0,ver=1
//		SS[4] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		}
//	update_test.go:486: update should succeed error: internal error
func FIXME_TestUpdateOneTimeoutTwoDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one timeout,
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			for a := range updatePrepareArray {
				params.MockInfoList[j].Status = updatePrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					for b := range updatePrepareArray {
						params.MockInfoList[k].Status = updatePrepareArray[b]
						_, err := Mockclient.Update(key, cvalue, 100, params)

						if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("update should succeed", err)
							}
						} else if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("update should succeed", err)
							}
						} else if params.MockInfoList[j].Status == uint8(proto.OpStatusInserting) {
							if err != client.ErrInternal {
								params.Log(t)
								t.Error("update should fail with inconsistent error", err)
							}
						} else {
							if err == nil {
								params.Log(t)
								t.Error("update should fail", err)
							}
						}
						//printTimeoutStatus("TestUpdateOneTimeoutTwoDiffStatus loop 1", params, err)
					}
				}
			}
		}
		params.MockInfoList[i].Delay = 0
	}
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrBadParam && err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 2 should fail with badParam ", err)
				}
				//printTimeoutStatus("TestUpdateOneTimeoutTwoDifferentStatus loop 2", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************
 * Two SS timeout with one SS has different status
 * looping each SS to simulate update gets timeout for two SS
 ***************************************************************/
func TestUpdateTwoTimeoutOneStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				for a := range updatePrepareArray {
					params.MockInfoList[k].Status = updatePrepareArray[a]
					params.MockInfoList[k].Version = 0

					_, err := Mockclient.Update(key, cvalue, 100, params)
					switch params.MockInfoList[k].Status {
					case uint8(proto.OpStatusNoError),
						uint8(proto.OpStatusAlreadyFulfilled):
						if err != nil {
							params.Log(t)
							t.Error("update fail ", err)
						}
					case uint8(proto.OpStatusBusy):
						if err != client.ErrBusy {
							t.Error("update should fail with OutOfStorage", err)
						}
					case uint8(proto.OpStatusBadParam):
						if err != client.ErrBadParam {
							t.Error("update should fail with badParam", err)
						}
					case uint8(proto.OpStatusRecordLocked):
						if err != client.ErrRecordLocked {
							t.Error("update should fail with recordLock", err)
						}
					case uint8(proto.OpStatusInserting):
						if err != nil {
							params.Log(t)
							t.Error("update inconsistent should be considered as ok", err)
						}
					}
					//printTimeoutStatus("TestUpdateTwoTimeoutOneStatus 1,2 SS timeout ", params, err)
					params.MockInfoList[k].Version = mock.DEF_VER
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
			for a := range updatePrepareArray {
				params.MockInfoList[j].Status = updatePrepareArray[a]
				params.MockInfoList[j].Version = 0

				_, err := Mockclient.Update(key, cvalue, 100, params)
				switch params.MockInfoList[j].Status {
				case uint8(proto.OpStatusNoError),
					uint8(proto.OpStatusAlreadyFulfilled):
					if err != nil {
						params.Log(t)
						t.Error("update should fail with two timeout one error ", err)
					}
				case uint8(proto.OpStatusBusy):
					if err != client.ErrBusy {
						t.Error("update loop2 should fail with OutOfStorage", err)
					}
				case uint8(proto.OpStatusBadParam):
					if err != client.ErrBadParam {
						t.Error("update loop2 should fail with badParam", err)
					}
				case uint8(proto.OpStatusRecordLocked):
					if err != client.ErrRecordLocked {
						t.Error("update loop2 should fail with recordLock", err)
					}
				case uint8(proto.OpStatusInserting):
					if err != nil {
						t.Error("update loop2 inconsistent error is considered as OK", err)
					}
				}
				//printTimeoutStatus("TestUpdateTwoTimeoutOneStatus 4th SS timeout loop1 ", params, err)
				params.MockInfoList[j].Version = mock.DEF_VER
			}
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
		for a := range updatePrepareArray {
			params.MockInfoList[i].Status = updatePrepareArray[a]
			params.MockInfoList[i].Version = 0

			_, err := Mockclient.Update(key, cvalue, 100, params)
			switch params.MockInfoList[i].Status {
			case uint8(proto.OpStatusNoError),
				uint8(proto.OpStatusAlreadyFulfilled):
				if err != nil {
					params.Log(t)
					t.Error("update should succeed with noError or AlreadyFulfilled", err)
				}
			case uint8(proto.OpStatusBusy):
				if err != client.ErrBusy {
					t.Error("update loop3 should fail with OutOfStorage", err)
				}
			case uint8(proto.OpStatusBadParam):
				if err != client.ErrBadParam {
					t.Error("update loop3 should fail with badParam", err)
				}
			case uint8(proto.OpStatusRecordLocked):
				if err != client.ErrRecordLocked {
					t.Error("update loop3 should fail with recordLock", err)
				}
			case uint8(proto.OpStatusInserting):
				if err != nil {
					t.Error("update loop3 inconsistent error is considered as Ok", err)
				}
			}
			//printTimeoutStatus("TestUpdateTwoTimeoutOneStatus 4th SS timeout loop2 ", params, err)
			params.MockInfoList[i].Version = mock.DEF_VER
		}
	}
	params.MockInfoList[3].Delay = 0 //recover back to normal, no timeout
	params.MockInfoList[4].Delay = 0
}

/****************************************************************************
 * -- OneNoResponseTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
//Why FIXME
//	common.go:102: MockParams being set {
//		SS[0] ns=ns,op=Nop,st=Ok,del=0,ver=1 no response
//		SS[1] ns=ns,op=Nop,st=Inserting,del=0,ver=1
//		SS[2] ns=ns,op=Nop,st=AlreadyFulfilled,del=0,ver=1
//		SS[3] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[4] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		}
//SS[1] return Inserting for Prepare (OK), commit(Failure) and Repair(Failure)

func FIXME_TestUpdateOneNoResponseTwoDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for update")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one noResponse,
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			for a := range updatePrepareArray {
				params.MockInfoList[j].Status = updatePrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					for b := range updatePrepareArray {
						params.MockInfoList[k].Status = updatePrepareArray[b]
						_, err := Mockclient.Update(key, cvalue, 100, params)

						if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("update should succeed with noError or AlreadyFulfilled", err)
							}
						} else if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("update should succeed with noError or AlreadyFulfilled", err)
							}
						} else if params.MockInfoList[j].Status == uint8(proto.OpStatusInserting) {
							if err != client.ErrInternal {
								params.Log(t)
								t.Error("update should fail with inconsistent error", err)
							}
						} else {
							if err == nil {
								params.Log(t)
								t.Error("update should fail ", err)
							}
						}
						//printTimeoutStatus("TestUpdateOneTimeoutTwoDiffStatus loop 1", params, err)
					}
				}
			}
		}
		params.MockInfoList[i].NoResponse = false
	}

	//we didn't do full loop, loop one type of error status and do validation
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.StatusNoCapacity)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 2 should fail with recordlock or outOfMem err", err)
				}
				//printStatus("SetOneNoResponseTwoDiffStatus loop 2", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/*********************************************************************************
 * -- OneNoResponseTwoPrepareUpdateStatus
 * looping each SS to simulate one timeout, two diff status at prepareupdate stage
 *********************************************************************************/
func TestUpdateOneNoResponseTwoPrepareUpdateStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for update")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one noResponse,
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			for a := range updatePrepareArray {
				params.MockInfoList[j].Opcode = proto.OpCodePrepareUpdate
				params.MockInfoList[j].Status = updatePrepareArray[a]
				params.MockInfoList[j].Version = 0

				for k := j + 1; k <= 4; k++ {
					for b := range updatePrepareArray {
						params.MockInfoList[k].Opcode = proto.OpCodePrepareUpdate
						params.MockInfoList[k].Status = updatePrepareArray[b]
						params.MockInfoList[k].Version = 0
						_, err := Mockclient.Update(key, cvalue, 100, params)

						if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusInserting) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("update should succeed with noError/AlreadyFulfilled or inserting", err)
							}
						} else if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusInserting) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("update should succeed with noError/AlreadyFulfilled or inserting", err)
							}
						} else {
							if err == nil {
								params.Log(t)
								t.Error("update should fail ", err)
							}
						}
						params.MockInfoList[k].Opcode = proto.OpCodeNop
						params.MockInfoList[k].Version = mock.DEF_VER
						//printTimeoutStatus("TestUpdateOneTimeoutTwoDiffStatus loop 1", params, err)
					}
				}
				params.MockInfoList[j].Opcode = proto.OpCodeNop
				params.MockInfoList[j].Version = mock.DEF_VER
			}
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/***************************************************************
 * Two SS no response with one SS has different status
 * looping each SS to simulate update no response for two SS
 ***************************************************************/
func TestUpdateTwoNoResponseOneStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				for a := range updatePrepareArray {
					params.MockInfoList[k].Status = updatePrepareArray[a]
					params.MockInfoList[k].Version = 0

					_, err := Mockclient.Update(key, cvalue, 100, params)
					switch params.MockInfoList[k].Status {
					case uint8(proto.OpStatusNoError),
						uint8(proto.OpStatusAlreadyFulfilled):
						if err != nil {
							params.Log(t)
							t.Error("update should succeed with noError/AlreadyFulfilled or Inserting", err)
						}
					case uint8(proto.OpStatusBusy):
						if err != client.ErrBusy {
							t.Error("update should fail with nostorageServ", err)
						}
					case uint8(proto.OpStatusBadParam):
						if err != client.ErrBadParam {
							t.Error("update should fail with badParam", err)
						}
					case uint8(proto.OpStatusRecordLocked):
						if err != client.ErrRecordLocked {
							t.Error("update should fail with recordLock", err)
						}
					case uint8(proto.OpStatusInserting):
						if err != nil {
							t.Error("update should pass", err)
						}

					}
					//printStatus("TestUpdateTwoNoResponseOneStatus 1,2 SS timeout ", params, err)
				}
				params.MockInfoList[k].Version = mock.DEF_VER
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/***************************************************************
 * -- update one commit status to repairable error
 * looping each SS to simulate one repairable error for commit
 ***************************************************************/
func TestUpdateOneCommitRepairableStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	recVersion := uint32(3)
	markDeleteRecVersion := recVersion + 1
	params.SetVersionForAll(recVersion)

	for i := 0; i < 3; i++ {
		for b := range updateCommitRepairableErr {
			params.MockInfoList[i].Opcode = proto.OpCodeCommit
			params.MockInfoList[i].Status = updateCommitRepairableErr[b]

			for j := i + 1; j <= 4; j++ {
				for a := range updatePrepareArray {
					params.MockInfoList[j].Opcode = proto.OpCodePrepareUpdate
					params.MockInfoList[j].Status = updatePrepareArray[a]
					params.MockInfoList[j].Version = 0

					_, err := Mockclient.Update(key, cvalue, 100, params)
					if err != nil {
						params.Log(t)
						t.Error("update failed", err)
					}
					//printOpsCodeStatus("set in TestUpdateOneCommitDiffStatus ", params, err)
				}
				//markdelete
				params.MockInfoList[j].Status = uint8(proto.OpStatusInserting)
				params.MockInfoList[j].Version = markDeleteRecVersion
				_, err := Mockclient.Update(key, cvalue, 100, params)
				var expectedErr error = nil
				if j < 3 {
					expectedErr = client.ErrNoKey
				}
				if err != expectedErr {
					params.Log(t)
					t.Error("set should failed with inconsistent error", i, "commit status:", params.MockInfoList[i].Status, j,
						"status:", params.MockInfoList[j].Status, err)
				}
				params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
				params.MockInfoList[j].Version = recVersion

			}
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		}
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- update two commit status to repairable error
 * looping each SS to simulate two repairable error for commit
 **************************************************************************/
func TestUpdateTwoCommitRepairableStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	recVersion := uint32(3)
	//1	markDeleteRecVersion := recVersion + 1
	params.SetVersionForAll(recVersion)

	for i := 0; i < 2; i++ {
		for b := range updateCommitRepairableErr {
			params.MockInfoList[i].Opcode = proto.OpCodeCommit
			params.MockInfoList[i].Status = updateCommitRepairableErr[b]

			for j := i + 1; j <= 3; j++ {
				for a := range updatePrepareArray {
					params.MockInfoList[j].Status = updatePrepareArray[a]
					params.MockInfoList[j].Opcode = proto.OpCodePrepareUpdate
					params.MockInfoList[j].Version = 0

					for k := j + 1; k <= 4; k++ {
						for c := range updateCommitRepairableErr {
							params.MockInfoList[k].Opcode = proto.OpCodeCommit
							params.MockInfoList[k].Status = updateCommitRepairableErr[c]
							params.MockInfoList[k].Version = recVersion

							_, err := Mockclient.Update(key, cvalue, 100, params)
							if err != nil {
								params.Log(t)
								t.Error("update failed", err)
							}
							//printOpsCodeStatus("set in TestUpdateTwoCommitDiffStatus ", params, err)
							params.MockInfoList[k].Opcode = proto.OpCodeNop
							params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
						}
					}
					params.MockInfoList[j].Opcode = proto.OpCodeNop
				}

				params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
				params.MockInfoList[j].Version = recVersion
			}
			params.MockInfoList[i].Opcode = proto.OpCodeNop
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		}
	}
}

/**************************************************************************
 * -- update three commit status to repairable error
 * looping each SS to simulate three repairable error for commit
 **************************************************************************/
func TestUpdateThreeCommitRepairableStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		for a := range updateCommitRepairableErr {
			params.MockInfoList[i].Opcode = proto.OpCodeCommit
			params.MockInfoList[i].Status = updateCommitRepairableErr[a]

			for j := i + 1; j <= 3; j++ {
				for b := range updateCommitRepairableErr {
					params.MockInfoList[j].Opcode = proto.OpCodeCommit
					params.MockInfoList[j].Status = updateCommitRepairableErr[b]

					for k := j + 1; k <= 4; k++ {
						for c := range updateCommitRepairableErr {
							params.MockInfoList[k].Opcode = proto.OpCodeCommit
							params.MockInfoList[k].Status = updateCommitRepairableErr[c]

							_, err := Mockclient.Update(key, cvalue, 100, params)
							if err == nil || err == client.ErrBusy {
								params.Log(t)
								t.Error("set should fail but error shouldn't be no storage error???? ", err)
							}
							//printOpsCodeStatus("set in TestUpdateThreeCommitRepairableStatus ", params, err)
							params.MockInfoList[k].Opcode = proto.OpCodeNop
							params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)
						}
					}
					params.MockInfoList[j].Opcode = proto.OpCodeNop
					params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)
				}
			}
			params.MockInfoList[i].Opcode = proto.OpCodeNop
			params.MockInfoList[i].Status = uint8(proto.StatusNoCapacity)
		}
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/***************************************************************
 * -- update one BadMsg status as commit error
 * looping each SS to simulate one BadMsg for commit error
 ***************************************************************/
func TestUpdateOneCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	recVersion := uint32(3)
	markDeleteRecVersion := recVersion + 1

	params.SetVersionForAll(recVersion)
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodePrepareUpdate
			for a := range updatePrepareArray {
				params.MockInfoList[j].Status = updatePrepareArray[a]
				params.MockInfoList[j].Version = 0

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("set should pass", err)
				}
				//printOpsCodeStatus("set in TestUpdateOneCommitDiffStatus ", params, err)
			}
			//markdelete
			params.MockInfoList[j].Status = uint8(proto.OpStatusInserting)
			params.MockInfoList[j].Version = markDeleteRecVersion
			_, err := Mockclient.Update(key, cvalue, 100, params)
			var expectedErr error = nil
			if j < 3 {
				expectedErr = client.ErrNoKey
			}
			if err != expectedErr {
				params.Log(t)
				t.Error("set should pass or have no key", err)
			}

			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
			params.MockInfoList[j].Version = recVersion
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/*******************************************************************
 * -- update two BadMsg status as commit error
 * looping each SS to simulate two BadMsg for commit error
 *******************************************************************/
func TestUpdateTwoCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 3; j++ {
			for a := range updatePrepareArray {
				params.MockInfoList[j].Status = updatePrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					params.MockInfoList[k].Opcode = proto.OpCodeCommit
					params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

					_, err := Mockclient.Update(key, cvalue, 100, params)
					if err != nil && err != client.ErrNoKey {
						params.Log(t)
						t.Error("set should pass or has no key error", err)
					}
					//printOpsCodeStatus("set in TestUpdateTwoCommitDiffStatus ", params, err)
					params.MockInfoList[k].Opcode = proto.OpCodeNop
					params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
				}
			}
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	testutil.CheckCalLog(t, ".*API.*Update.*st=InconsistentState", "59", hostip, true)
	testutil.CheckCalLog(t, ".*API.*Update.*st=CommitFailure", "59", hostip, true)
}

/********************************************************************
 * -- update three BadMsg status as commit error
 * looping each SS to simulate three BadMsg for commit error
 ********************************************************************/
func TestUpdateThreeCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 2; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodeCommit
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Opcode = proto.OpCodeCommit
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

				if _, err := Mockclient.Update(key, cvalue, 100, params); err != client.ErrWriteFailure && err != client.ErrInternal {
					params.Log(t)
					t.Error("create should fail with 3 BadMsg in commit", err)
				}
				//printOpsCodeStatus("create in TestUpdateThreeCommitBadMsg ", params, err)
				params.MockInfoList[k].Opcode = proto.OpCodeNop
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.StatusNoCapacity)
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- commit mix status
 * looping each SS to simulate three mix status error in commit
 **************************************************************************/
func TestUpdateCommitThreeMixError(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoUncommitted)

		for j := i + 1; j <= 2; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodeCommit
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Opcode = proto.OpCodeCommit
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoUncommitted)

				_, err := Mockclient.Update(key, cvalue, 100, params)
				if err == nil || err == client.ErrBusy {
					params.Log(t)
					t.Error("set should failed with inconsistent error", err)
				}
				//printOpsCodeStatus("set in TestUpdateCommitThreeMixError ", params, err)
				params.MockInfoList[k].Opcode = proto.OpCodeNop
				params.MockInfoList[k].Status = uint8(proto.StatusNoCapacity)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.StatusNoCapacity)
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/***********************************************
 * -- abort with different return code
 * looping each SS to simulate abort case
 ***********************************************/
func TestUpdateAbortDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//1,2,4 error state, 3 abort
	params.MockInfoList[0].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[1].Status = uint8(proto.OpStatusRecordLocked)
	params.MockInfoList[3].Status = uint8(proto.OpStatusRecordLocked)
	params.MockInfoList[2].Opcode = proto.OpCodeAbort
	for a := range updateAbortArray {
		params.MockInfoList[2].Status = updateAbortArray[a]
		_, err := Mockclient.Update(key, cvalue, 20, params)
		if err != client.ErrRecordLocked {
			t.Error("set should hit bad requestId error, err", err)
		}
		//printOpsCodeStatus("1st one in TestUpdateAbortDiffStatus", params, err)
	}
	params.SetOpCodeForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))

	//2,4,5 error state, 1,3 abort
	params.MockInfoList[1].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[3].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[4].Status = uint8(proto.OpStatusBusy)

	params.MockInfoList[0].Opcode = proto.OpCodeAbort
	params.MockInfoList[2].Opcode = proto.OpCodeAbort

	for a := range updateAbortArray {
		params.MockInfoList[0].Status = updateAbortArray[a]
		for b := range updateAbortArray {
			params.MockInfoList[2].Status = updateAbortArray[b]

			_, err := Mockclient.Update(key, cvalue, 20, params)
			if err != client.ErrBusy {
				t.Error("set should hit bad requestId error, err", err)
			}
			//printOpsCodeStatus("2nd one in TestUpdateAbortDiffStatus", params, err)
		}
	}
	params.SetOpCodeForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

func init() {
	/************************************************************
	 * We want 0 to be the last error code so no need to
	 * reset 0 back for loop recovery
	 ************************************************************/
	updatePrepareArray = [6]uint8{uint8(proto.StatusNoCapacity), //6
		uint8(proto.OpStatusBadParam),         //7
		uint8(proto.OpStatusRecordLocked),     //8
		uint8(proto.OpStatusInserting),        //15
		uint8(proto.OpStatusAlreadyFulfilled), //17
		uint8(proto.OpStatusNoError),          //0
	}
	updateCommitRepairableErr = [3]uint8{uint8(proto.StatusNoCapacity), //6
		uint8(proto.OpStatusBadParam),      //7
		uint8(proto.OpStatusNoUncommitted), //10
	}
	updateAbortArray = [5]uint8{uint8(proto.OpStatusNoKey), //3
		uint8(proto.OpStatusBadParam),      //7
		uint8(proto.OpStatusNoUncommitted), //10
		uint8(proto.OpStatusBadMsg),        //1
		uint8(proto.OpStatusNoError),       //0
	}
}
