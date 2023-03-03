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
package unittest

import (
	"juno/pkg/client"
	"juno/pkg/proto"
	"juno/test/testutil"
	"juno/test/testutil/mock"
	"testing"
)

var prepareDeleteArr [4]uint8

func TestDestroyNormal(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	if err := Mockclient.Destroy(key, params); err != nil {
		t.Error("Destroy failed ", err)
	}
	testutil.CheckCalLog(t, "API.*Destroy.*st=Ok.*ns=ns", "1", hostip, true)
}

/***************************************************************
 * -- Timeout
 * looping each SS to simulate delete gets timeout from each SS
 ***************************************************************/
func TestDestroyOneSSTimeout(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	// note: delay is in microsecond
	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Delay = 1000000

		if err := Mockclient.Destroy(key, params); err != nil {
			params.Log(t)
			t.Error("Destroy failed ", err)
		}

		params.MockInfoList[i].Delay = 0
	}
}

/*
 * looping SS to simulate delete gets timeout from two SS
 */
func TestDestroyTwoSSTimeout(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			if err := Mockclient.Destroy(key, params); err != nil {
				params.Log(t)
				t.Error("Destroy failed ", err)
			}

			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
	testutil.CheckCalLog(t, "API.*Destroy.*st=Ok.*ns=ns", "10", hostip, true)
	testutil.CheckCalLog(t, "SSReqTimeout", "20", hostip, true)
}

/*
 * looping SS to simulate delete gets timeout from three SS
 */
func TestDestroyThreeSSTimeout(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Delay = 1000000

				if err := Mockclient.Destroy(key, params); err != client.ErrBusy {
					params.Log(t)
					t.Errorf("Destroy failed: %s", err)
				}

				params.MockInfoList[k].Delay = 0
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
	testutil.CheckCalLog(t, "API.*Destroy.*st=NoStorageServer.*ns=ns", "10", hostip, true)
}

/*******************************************************************
 * -- NoResponse
 * looping each SS to simulate delete gets no response from each SS
 *******************************************************************/
func TestDestroyOneSSNoResponse(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].NoResponse = true

		if err := Mockclient.Destroy(key, params); err != nil {
			params.Log(t)
			t.Error("Destroy failed ", err)
		}

		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate delete gets no response from two SS
 */
func TestDestroyTwoSSNoResponse(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].NoResponse = true

			if err := Mockclient.Destroy(key, params); err != nil {
				params.Log(t)
				t.Error("Destroy failed ", err)
			}

			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate delete gets no response from three SS
 */
func TestDestroyThreeSSNoResponse(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].NoResponse = true

				if err := Mockclient.Destroy(key, params); err != client.ErrBusy {
					t.Errorf("Destroy %d,%d,%d，no response should fail with no server error. %s", i, j, k, err)
				}

				params.MockInfoList[k].NoResponse = false
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/**********************************************************************
 * -- one Status Error
 * looping each SS to simulate delete gets one error from each SS
 *********************************************************************/
func TestDestroyOneStatusError(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 5; i++ {
		for a := range prepareDeleteArr {
			params.MockInfoList[i].Status = prepareDeleteArr[a]

			if err := Mockclient.Destroy(key, params); err != nil {
				//				t.Error("Destroy failed ", err)
				t.Fatal("Destroy failed ", err)
			}
		}
	}
}

/**********************************************************************
 * -- two Status Error
 * looping each SS to simulate delete gets two errors from each SS
 *********************************************************************/
func TestDestroyTwoStatusError(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		for a := range prepareDeleteArr {
			params.MockInfoList[i].Opcode = proto.OpCodePrepareDelete
			params.MockInfoList[i].Status = prepareDeleteArr[a]

			for j := i + 1; j <= 4; j++ {
				for b := range prepareDeleteArr {
					params.MockInfoList[j].Opcode = proto.OpCodePrepareDelete
					params.MockInfoList[j].Status = prepareDeleteArr[b]
					if err := Mockclient.Destroy(key, params); err != nil {
						params.Log(t)
						t.Error("Destroy failed ", err)
					}
				}
			}
		}
	}
}

/**********************************************************************
 * -- three Status Error
 * looping each SS to simulate delete gets three errors from each SS
 *********************************************************************/
func TestDestroyThreeStatusError(t *testing.T) {
	//	if testConfig.ProxyConfig.TwoPhaseDestroyEnabled {
	//		t.Skip("skipping. two phase delete enabled.")
	//	}
	var s [5]uint8
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		for a := range prepareDeleteArr {
			params.MockInfoList[i].Status = prepareDeleteArr[a]
			params.MockInfoList[i].Opcode = proto.OpCodePrepareDelete

			for j := i + 1; j < 4; j++ {
				for b := range prepareDeleteArr {
					params.MockInfoList[j].Status = prepareDeleteArr[b]
					params.MockInfoList[j].Opcode = proto.OpCodePrepareDelete

					for k := j + 1; k <= 4; k++ {
						for c := range prepareDeleteArr {
							params.MockInfoList[k].Status = prepareDeleteArr[c]
							params.MockInfoList[k].Opcode = proto.OpCodePrepareDelete

							for w := 0; w <= 4; w++ {
								s[w] = params.MockInfoList[w].Status
							}
							status := s[0] + s[1] + s[2] + s[3] + s[4]

							err := Mockclient.Destroy(key, params)
							if status >= 21 {
								if s[0] == uint8(proto.OpStatusRecordLocked) || s[1] == uint8(proto.OpStatusRecordLocked) ||
									s[2] == uint8(proto.OpStatusRecordLocked) || s[3] == uint8(proto.OpStatusRecordLocked) ||
									s[4] == uint8(proto.OpStatusRecordLocked) {
									if err != client.ErrRecordLocked {
										params.Log(t)
										t.Error("Destroy should fail with error recorlock", err)
									}
								} else if err != client.ErrBadParam {
									params.Log(t)
									t.Error("Destroy should fail with error bad param", err)
								}
							} else if err != nil {
								params.Log(t)
								t.Error("Destroy fail", err)
							}
						}
					}
				}
			}
		}
	}
}

/****************************************************************************
 * -- OneTimeoutTwoDiffStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
func TestDestroyOneTimeoutTwoDiffStatus(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	var s [5]uint8
	var d [5]uint32

	for i := 0; i < 3; i++ {
		for a := range prepareDeleteArr {
			params.MockInfoList[i].Status = prepareDeleteArr[a]

			for j := i + 1; j < 4; j++ {
				for b := range prepareDeleteArr {
					params.MockInfoList[j].Status = prepareDeleteArr[b]

					for k := j + 1; k <= 4; k++ {
						params.MockInfoList[k].Delay = 1000000

						//if ss x timeout, we assign error status to ss x so it will be easy for
						//error calculation, but we don't change the real error status
						for w := 0; w <= 4; w++ {
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].Delay
							if d[w] != 0 {
								s[w] = uint8(proto.OpStatusBadParam)
							}
						}
						err := Mockclient.Destroy(key, params)

						if s[0]+s[1]+s[2]+s[3]+s[4] >= 21 {
							if err == nil {
								params.Log(t)
								t.Error("Destroy should fail as two error, one timeout occur", err)
							}
						} else if err != nil {
							params.Log(t)
							//							t.Error("Destroy fail", err)
							t.Fatal("Destroy fail", err)
						}
						params.MockInfoList[k].Delay = 0
					}
				}
			}
		}
	}
	/**** Below can be deleted later as it doesn't seem catch too many errors *****/
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			for a := range prepareDeleteArr {
				params.MockInfoList[j].Status = prepareDeleteArr[a]

				for k := j + 1; k <= 4; k++ {
					for b := range prepareDeleteArr {
						params.MockInfoList[k].Status = prepareDeleteArr[b]

						for w := 0; w <= 4; w++ {
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].Delay
							if d[w] != 0 {
								s[w] = uint8(proto.OpStatusBadParam)
							}
						}
						err := Mockclient.Destroy(key, params)

						if s[0]+s[1]+s[2]+s[3]+s[4] >= 21 {
							if err == nil {
								params.Log(t)
								t.Error("Destroy should fail as two error, one timeout occur", err)
							}
						} else if err != nil {
							params.Log(t)
							//							t.Error("Destroy fail", err)
							t.Fatal("Destroy fail", err)
						}
					}
				}
			}
		}
		params.MockInfoList[i].Delay = 0
	}

	for i := 0; i < 3; i++ {
		for a := range prepareDeleteArr {
			params.MockInfoList[i].Status = prepareDeleteArr[a]

			for j := i + 1; j < 4; j++ {
				params.MockInfoList[j].Delay = 1000000

				for k := j + 1; k <= 4; k++ {
					for b := range prepareDeleteArr {
						params.MockInfoList[k].Status = prepareDeleteArr[b]

						for w := 0; w <= 4; w++ {
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].Delay
							if d[w] != 0 {
								s[w] = uint8(proto.OpStatusBadParam)
							}
						}
						err := Mockclient.Destroy(key, params)

						if s[0]+s[1]+s[2]+s[3]+s[4] >= 21 {
							if err == nil {
								params.Log(t)
								t.Error("Destroy should fail as two error, one timeout occur", err)
							}
						} else if err != nil {
							params.Log(t)
							t.Error("Destroy fail", err)
						}
					}
				}
				params.MockInfoList[j].Delay = 0
			}
		}
	}
}

/****************************************************************************
* -- TwoTimeoutOneStatus
* looping each SS to simulate two timeout, one status from different SS
*****************************************************************************/
func TestDestroyTwoTimeoutOneStatus(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	var s [5]uint8
	var d [5]uint32

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				for a := range prepareDeleteArr {
					params.MockInfoList[k].Status = prepareDeleteArr[a]

					for w := 0; w <= 4; w++ {
						s[w] = params.MockInfoList[w].Status
						d[w] = params.MockInfoList[w].Delay
						if d[w] != 0 {
							s[w] = uint8(proto.OpStatusBadParam)
						}
					}

					err := Mockclient.Destroy(key, params)
					if s[0]+s[1]+s[2]+s[3]+s[4] >= 21 {
						if err == nil {
							params.Log(t)
							t.Error("Destroy should fail as two error, one timeout occur", err)
						}
					} else if err != nil {
						params.Log(t)
						t.Error("Destroy fail", err)
					}
				}
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}

	/**** Below can be deleted later as this pattern doesn't seem catch too many errors *****/
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			for a := range prepareDeleteArr {
				params.MockInfoList[j].Status = prepareDeleteArr[a]

				for k := j + 1; k <= 4; k++ {
					params.MockInfoList[k].Delay = 1000000

					for w := 0; w <= 4; w++ {
						s[w] = params.MockInfoList[w].Status
						d[w] = params.MockInfoList[w].Delay
						if d[w] != 0 {
							s[w] = uint8(proto.OpStatusBadParam)
						}
					}

					err := Mockclient.Destroy(key, params)
					if s[0]+s[1]+s[2]+s[3]+s[4] >= 21 {
						if err == nil {
							params.Log(t)
							t.Error("Destroy should fail as two error, one timeout occur", err)
						}
					} else if err != nil {
						params.Log(t)
						t.Error("Destroy fail", err)
					}
					params.MockInfoList[k].Delay = 0
				}
			}
		}
		params.MockInfoList[i].Delay = 0
	}

	params.MockInfoList[3].Delay = 1000000
	params.MockInfoList[4].Delay = 1000000
	for i := 0; i < 3; i++ {
		for a := range prepareDeleteArr {
			params.MockInfoList[i].Status = prepareDeleteArr[a]

			for w := 0; w <= 4; w++ {
				s[w] = params.MockInfoList[w].Status
				d[w] = params.MockInfoList[w].Delay
				if d[w] != 0 {
					s[w] = uint8(proto.OpStatusBadParam)
				}
			}

			err := Mockclient.Destroy(key, params)
			if s[0]+s[1]+s[2]+s[3]+s[4] >= 21 {
				if err == nil {
					params.Log(t)
					t.Error("Destroy should fail as two error, one timeout occur", err)
				}
			} else if err != nil {
				params.Log(t)
				t.Error("Destroy fail", err)
			}
		}
	}
	params.MockInfoList[3].Delay = 0
	params.MockInfoList[4].Delay = 0
}

/*************************************************************
 * BELOW TEST CASE ARE CREATED PARTICULARLLY for TWO PHASE
 * DELETE AND MAY WORK ONLY UNDER THE NEW DELETE, COMMENT
 * OUT BELOW TEST CASE IF RUNNING CODE SWITCHED TO OLD DELETE
**************************************************************/

/*********************************************
 * -- All respond NoKey during PrepareDelete
 *    Destroy should be successful
 *********************************************/
func TestDestroy2AllOK_NoKey(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	if err := Mockclient.Destroy(key, params); err != nil {
		t.Errorf("failed with error: %s", err)
	}
	params.SetOpCodeForAll(proto.OpCodePrepareDelete)
	params.SetStatusForAll(uint8(proto.OpStatusNoKey))
	if err := Mockclient.Destroy(key, params); err != nil {
		t.Errorf("failed with error: %s", err)
	}
}

/*******************************************************
 * -- All respond AlreadyFulfilled during PrepareDelete
 *    Destroy should be successful
 *******************************************************/
func TestDestroy2AllAlreadyFulfill(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	if err := Mockclient.Destroy(key, params); err != nil {
		t.Errorf("failed with error: %s", err)
	}
	params.SetOpCodeForAll(proto.OpCodePrepareDelete)
	params.SetStatusForAll(uint8(proto.OpStatusAlreadyFulfilled))
	if err := Mockclient.Destroy(key, params); err != nil {
		t.Errorf("failed with error: %s", err)
	}
}

/**********************************************
 * -- Get NoKey response During PrepareDelete
 *    from two SS, Destroy should be successful
 **********************************************/
func TestDestroy2TwoNoKey(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	if err := Mockclient.Destroy(key, params); err != nil {
		t.Errorf("failed with error: %s", err)
	}
	for j := 0; j < 5; j++ {
		params := mock.NewMockParams(5)
		params.SetOpCodeForAll(proto.OpCodePrepareDelete)
		params.MockInfoList[j].Status = uint8(proto.OpStatusNoKey)
		for i := j + 1; i < 5; i++ {
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoKey)
			if err := Mockclient.Destroy(key, params); err != nil {
				params.Log(t)
				t.Errorf("failed with error: %s", err)
			}
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		}
	}
}

/************************************************
 * -- Get NoKey response During PrepareDelete
 *    from three SS, Destroy should be successful
 ************************************************/
func TestDestroy2ThreeNoKey(t *testing.T) {
	key := []byte("aKey")

	for j := 0; j < 5; j++ {
		params := mock.NewMockParams(5)
		params.SetOpCodeForAll(proto.OpCodePrepareDelete)
		params.MockInfoList[j].Status = uint8(proto.OpStatusNoKey)
		for i := j + 1; i < 5; i++ {
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoKey)
			for k := i + 1; k < 5; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoKey)
				if err := Mockclient.Destroy(key, params); err != nil {
					params.Log(t)
					t.Errorf("failed with error: %s", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		}
	}
}

/************************************************
 * -- Get NoKey response During PrepareDelete
 *    from four SS, Destroy should be successful
 ************************************************/
func TestDestroy2FourNoKey(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	params.SetOpCodeForAll(proto.OpCodePrepareDelete)
	params.SetStatusForAll(uint8(proto.OpStatusNoKey))

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		if err := Mockclient.Destroy(key, params); err != nil {
			params.Log(t)
			t.Errorf("failed with error: %s", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoKey)
	}
}

/***************************************************
 * -- Get BadParam response During PrepareDeletefrom
 *    Four SS, Destroy should get badParam error
 ***************************************************/
func TestDestroy2_p1_1OK(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	params.SetOpCodeForAll(proto.OpCodePrepareDelete)
	params.SetStatusForAll(uint8(proto.OpStatusBadParam))

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		if err := Mockclient.Destroy(key, params); err != client.ErrBadParam {
			params.Log(t)
			t.Errorf("failed with error: %s", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
	}
}

/*****************************************************
 * -- Get BadParam response During PrepareDelete
 *    from Three SS,Destroy should get badParam error
 *****************************************************/
func TestDestroy2_p1_2OK(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	params.SetOpCodeForAll(proto.OpCodePrepareDelete)
	params.SetStatusForAll(uint8(proto.OpStatusBadParam))

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)

		for j := i + 1; j < 5; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
			if err := Mockclient.Destroy(key, params); err != client.ErrBadParam {
				params.Log(t)
				t.Errorf("failed with error: %s", err)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
	}
}

/************************************
 * -- Get CommitFailure from one SS
 *    Destroy should be successful
 ************************************/
func TestDestroy2_fail_to_commit(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	params.SetOpCodeForAll(proto.OpCodeCommit)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusCommitFailure)

		if err := Mockclient.Destroy(key, params); err != nil {
			params.Log(t)
			t.Fatalf("failed with error: %s", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/********************************************
 * -- Get BadParam during prepareDelete from
 *    one SS, Destroy should be successful
 ********************************************/
func TestDestroy2_1PrepareErr(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	params.SetOpCodeForAll(proto.OpCodePrepareDelete)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		if err := Mockclient.Destroy(key, params); err != nil {
			params.Log(t)
			t.Fatalf("failed with error: %s", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***********************************************
 * -- Get BadParam during markDelete from
 *    one SS, Destroy should fail with badParam
 ***********************************************/
func TestDestroy2_1PrepareErr_1MarkDeleteErr(t *testing.T) {
	key := []byte("aKey")

	params := mock.NewMockParams(5)

	for i := 0; i < 5; i++ {
		params.SetOpCodeForAll(proto.OpCodePrepareDelete)
		params.SetStatusForAll(uint8(proto.OpStatusNoError))
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
		if i == 0 {
			params.MockInfoList[1].Opcode = proto.OpCodeMarkDelete
			params.MockInfoList[1].Status = uint8(proto.OpStatusBadParam)
		} else {
			params.MockInfoList[0].Opcode = proto.OpCodeMarkDelete
			params.MockInfoList[0].Status = uint8(proto.OpStatusBadParam)
		}

		if err := Mockclient.Destroy(key, params); err != nil {
			params.Log(t)
			t.Fatalf("failed with error: %s", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

func init() {
	/***********************************************************
	 * 0 -- keep response code 0 as the last one so the error
	 * 		code assign can be recovered to 0 as the last step
	 ***********************************************************/
	prepareDeleteArr = [4]uint8{uint8(proto.OpStatusBadParam), //7
		uint8(proto.OpStatusRecordLocked), //8
		uint8(proto.OpStatusNoKey),        //3
		uint8(proto.OpStatusNoError),
	}
}
