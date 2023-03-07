package unittest

import (
	"juno/pkg/client"
	"juno/pkg/proto"
	"juno/test/testutil"
	"juno/test/testutil/mock"
	"testing"
)

var setPrepareArray [7]uint8
var setCommitRepairableErr [3]uint8
var setAbortArray [5]uint8

func TestSetNormal(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)

	params := mock.NewMockParams(5)
	if _, err := Mockclient.Set(key, cvalue, 800, params); err != nil {
		t.Error("SetNormal fail", err)
	}
}

func TestSetInserting(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := []byte("Value to be stored for Update")
	params := mock.NewMockParams(5)
	params.SetStatusForAll(uint8(proto.OpStatusInserting))
	params.SetVersion(1)
	testutil.RemoveLog(t, hostip, true)

	_, err := Mockclient.Set(key, value, 800, params)
	if err != client.ErrWriteFailure {
		params.Log(t)
		t.Error("Set Inserting should fail with ErrWriteFailure: ", err)
	}
	testutil.CheckCalLog(t, "API.*Set.*st=CommitFailure.*ttl=800.*Insr.*Insr.*Insr.*Insr.*Insr.*Insr", "1", hostip, true)
}

func TestSetPrepareInserting(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	value := []byte("Value to be stored for Update")
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	params.SetOpCodeForAll(proto.OpCodePrepareSet)
	params.MockInfoList[0].Status = uint8(proto.OpStatusInserting)
	params.MockInfoList[1].Status = uint8(proto.OpStatusInserting)
	params.MockInfoList[2].Status = uint8(proto.OpStatusInserting)

	params.SetVersion(1)
	recInfo, err := Mockclient.Set(key, value, 800, params)
	if err != nil {
		t.Error("SetInserting fail: ", err)
	}
	if recInfo.GetVersion() != 1 {
		t.Error("Wrong version, real version is ", recInfo.GetVersion())
	}
	testutil.CheckCalLog(t, "API.*Set.*st=Ok", "1", hostip, true)
}

/***************************************************************
 * -- Timeout
 * looping each SS to simulate set gets timeout from each SS
 ***************************************************************/
func TestSetOneSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	// note: delay is in microsecond
	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Delay = 1000000

		_, err := Mockclient.Set(key, cvalue, 100, params)
		if err != nil {
			params.Log(t)
			t.Error("set failed", err)
		}
		params.MockInfoList[i].Delay = 0
	}
	testutil.CheckCalLog(t, "SSReqTimeout.*Set.PrepareSet", "3", hostip, true)
}

/*
 * looping SS to simulate set gets timeout from two SS
 */
func TestSetTwoSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			if _, err := Mockclient.Set(key, cvalue, 100, params); err != nil {
				params.Log(t)
				t.Error("set failed ", err)
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
}

/*
 * looping SS to simulate set gets timeout from three SS
 */
func TestSetThreeSSTimeout(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Delay = 1000000
				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Errorf("set should fail with no storage server error. %s", err)
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
 * looping each SS to simulate set gets no response from each SS
 *******************************************************************/
func TestSetOneSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].NoResponse = true

		if _, err := Mockclient.Set(key, cvalue, 100, params); err != nil {
			params.Log(t)
			t.Error("set failed", err)
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate set gets no response from two SS
 */
func TestSetTwoSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].NoResponse = true

			if _, err := Mockclient.Set(key, cvalue, 100, params); err != nil {
				params.Log(t)
				t.Error("set failed", err)
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate set gets no response from three SS
 */
func TestSetThreeSSNoResponse(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].NoResponse = true

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Error("set should fail with no storage server error", err)
				}
				params.MockInfoList[k].NoResponse = false
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
	testutil.CheckCalLog(t, "API.*Set.*st=NoStorageServ", "10", hostip, true)
}

/************************************************************
 * -- SetOneStatusError
 * looping each SS to simulate set gets one error from ss
 ************************************************************/
func TestSetOneStatusError(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 1; i++ {
		for a := range setPrepareArray {
			st := setPrepareArray[a]
			params.MockInfoList[i].Status = st

			_, err := Mockclient.Set(key, cvalue, 100, params)
			if proto.OpStatus(st) == proto.OpStatusInserting {
				if err != nil {
					//because OpCode is not defined in the params, SS returns the given OpStatus for
					//all types of requests
					//Inserting OpStatus is considered as GOOD (Success) for Prepare, but as Failure for Commit and Repair
					//proxy talks to the first 3 SSs only. prepare successful, but the both commit and repair fail
					//for the first SS
					params.Log(t)
					t.Error("set failed", err)

				}
			} else {
				if err != nil {
					params.Log(t)
					t.Error("set failed", err)
				}
			}
		}
	}
}

/*
 * looping each SS to simulate set gets two errors from ss
 */
func TestSetTwoStatusError(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetOpCodeForAll(proto.OpCodePrepareSet)

	for i := 0; i < 4; i++ {
		for a := range setPrepareArray {
			params.MockInfoList[i].Status = setPrepareArray[a]

			for j := i + 1; j <= 4; j++ {
				for b := range setPrepareArray {
					params.MockInfoList[j].Status = setPrepareArray[b]

					_, err := Mockclient.Set(key, cvalue, 100, params)
					if err != nil {
						params.Log(t)
						t.Error("set failed", err)
					}
				}
				//printStatus("TestSetTwoStatusError ", params, err)
			}
		}
	}
}

/******************************************************************
 * -- ThreeStatusOK (3SS)
 * looping SS to simulate set gets record lock from three SS
 ******************************************************************/
func TestSetThreeStatusOKRecord(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetOpCodeForAll(proto.OpCodePrepareSet)

	for i := 0; i < 3; i++ {
		for a := 3; a <= 5; a++ {
			params.MockInfoList[i].Status = setPrepareArray[a]

			for j := i + 1; j < 4; j++ {
				for b := 3; b <= 5; b++ {
					params.MockInfoList[j].Status = setPrepareArray[b]

					for k := j + 1; k <= 4; k++ {
						for c := 3; c <= 5; c++ {
							params.MockInfoList[k].Status = setPrepareArray[c]

							_, err := Mockclient.Set(key, cvalue, 100, params)
							if err != nil {
								params.Log(t)
								t.Error("set failed ", err)
							}
						}
					}
				}
			}
		}
	}
}

/******************************************************************
 * -- TwoRecordLockedOneBadParam (3SS)
 * looping SS to simulate set gets error for three SSs
 ******************************************************************/
func TestSetTwoRecordLockedOneBadParam(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("set should fail with RecordLock", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************************
 *-- TwoOutOfMemOneBadParam (3SS)
 * looping SS to simulate set gets error for three SSs
 * return either OutOfMem or BadParam
***************************************************************************/
func TestSetTwoOutOfMemOneBadParam(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBusy)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBusy)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadParam)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrBusy && err != client.ErrBadParam {
					params.Log(t)
					t.Error("set should fail with OutOfStorage error", err)
				}
				//printStatus("set in TestSetTwoBadParamOneRecordLock ", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/*************************************************************
 * -- TwoBadParamOneRecordLock (3SS)
 * looping SS to simulate set gets error from three SSs
 *************************************************************/
func TestSetTwoBadParamOneRecordLock(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadParam)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("set should fail with BadParam", err)
				}
				//printStatus("set in TestSetTwoBadParamOneRecordLock ", params, err)
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
func TestSetThreeMixErrorOKStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one AlreadyFulfilled, always return OK
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusAlreadyFulfilled)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Status = uint8(proto.OpStatusRecordLocked)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadParam)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("loop 1 set failed", err)
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

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoStorageServer)
				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("loop 2 set should fail with inconsistent error", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}

		//This is almost the same as above except inserting only happens
		//specifically at prepareset step. This actually is the main real
		//case and it should return succeed
		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodePrepareSet
			params.MockInfoList[j].Status = uint8(proto.OpStatusInserting)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoStorageServer)
				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("loop 2 set failed", err)
				}
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.OpStatusNoError)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/****************************************************************************
 * -- OneTimeoutTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
//Why FIXME
// Considering
//	common.go:102: MockParams being set {
//		SS[0] ns=ns,op=Nop,st=Ok,del=1000000,ver=1
//		SS[1] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[2] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[3] ns=ns,op=Nop,st=Inserting,del=0,ver=1
//		SS[4] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		}
//	Prepare succeeds for SS[1], SS[2], SS[3]
//	Commit to SS[1] and SS[2] OK, but to SS[3] Fails
//	Repair to SS[3] Fails
func FIXME_TestSetOneTimeoutTwoDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one timeout,
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			for a := range setPrepareArray {
				params.MockInfoList[j].Status = setPrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					for b := range setPrepareArray {
						params.MockInfoList[k].Status = setPrepareArray[b]
						_, err := Mockclient.Set(key, cvalue, 100, params)

						if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("set should succeed ", err)
							}
						} else if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("set should succeed ", err)
							}
						} else if params.MockInfoList[j].Status == uint8(proto.OpStatusInserting) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusInserting) {
							if err != client.ErrInternal {
								params.Log(t)
								t.Error("update should fail with inconsistent error", err)
							}
						} else {
							if err == nil {
								params.Log(t)
								t.Error("set should fail ", err)
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
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.StatusNoCapacity)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrBusy && err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 2 should fail with recordlock or outOfMem", err)
				}
				//printTimeoutStatus("TestSetOneTimeoutTwoDifferentStatus loop 2", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/******************************************************************************
 * -- OneTimeoutTwoPrepareSetStatus
 * looping each SS to simulate one timeout, two diff status at prepareset stage
 ******************************************************************************/
func TestSetOneTimeoutTwoPrepareSetStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one timeout,
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			for a := range setPrepareArray {
				params.MockInfoList[j].Opcode = proto.OpCodePrepareSet
				params.MockInfoList[j].Status = setPrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					for b := range setPrepareArray {
						params.MockInfoList[k].Opcode = proto.OpCodePrepareSet
						params.MockInfoList[k].Status = setPrepareArray[b]

						_, err := Mockclient.Set(key, cvalue, 100, params)
						if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusInserting) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("set should succeed ", err)
							}
						} else if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusInserting) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("set should succeed ", err)
							}
						} else if err == nil {
							params.Log(t)
							t.Error("set should fail ", err)
						}
						params.MockInfoList[k].Opcode = proto.OpCodeNop
					}
					//printTimeoutStatus("TestSetOneTimeoutTwoPrepareSetStatus", params, err)
				}
				params.MockInfoList[j].Opcode = proto.OpCodeNop
			}
		}
		params.MockInfoList[i].Delay = 0
	}
}

/***************************************************************
 * Two SS timeout with one SS has different status
 * looping each SS to simulate set gets timeout for two SS
 ***************************************************************/
func TestSetTwoTimeoutOneStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				for a := range setPrepareArray {
					params.MockInfoList[k].Status = setPrepareArray[a]

					_, err := Mockclient.Set(key, cvalue, 100, params)
					switch params.MockInfoList[k].Status {
					case uint8(proto.OpStatusNoError),
						uint8(proto.OpStatusAlreadyFulfilled):
						if err != nil {
							params.Log(t)
							t.Error("set fail", err)
						}
					case uint8(proto.OpStatusBusy):
						if err != client.ErrBusy {
							t.Error("set should fail with OutOfStorage", err)
						}
					case uint8(proto.OpStatusBadParam):
						if err != client.ErrBadParam {
							t.Error("set should fail with badParam", err)
						}
					case uint8(proto.OpStatusRecordLocked):
						if err != client.ErrRecordLocked {
							t.Error("set should fail with recordLock", err)
						}
					case uint8(proto.OpStatusInserting):
						if err != nil {
							t.Error("set should fail with inconsistent error", err)
						}
					}
					//printTimeoutStatus("TestSetTwoTimeoutOneStatus 1,2 SS timeout ", params, err)
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
			for a := range setPrepareArray {
				params.MockInfoList[j].Status = setPrepareArray[a]

				_, err := Mockclient.Set(key, cvalue, 100, params)
				switch params.MockInfoList[j].Status {
				case uint8(proto.OpStatusNoError),
					uint8(proto.OpStatusAlreadyFulfilled):
					if err != nil {
						params.Log(t)
						t.Error("set shouldn't fail with noError, AlreadyFulfilled or Inserting", err)
					}
				case uint8(proto.OpStatusBusy):
					if err != client.ErrBusy {
						t.Error("set loop2 should fail with OutOfStorage", err)
					}
				case uint8(proto.OpStatusBadParam):
					if err != client.ErrBadParam {
						t.Error("set loop2 should fail with badParam", err)
					}
				case uint8(proto.OpStatusRecordLocked):
					if err != client.ErrRecordLocked {
						t.Error("set loop2 should fail with recordLock", err)
					}
				case uint8(proto.OpStatusInserting):
					if err != nil {
						t.Error("update should fail with inconsistent error", err)
					}
				}
				//printTimeoutStatus("TestSetTwoTimeoutOneStatus 4th SS timeout loop1 ", params, err)
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
		for a := range setPrepareArray {
			params.MockInfoList[i].Status = setPrepareArray[a]

			_, err := Mockclient.Set(key, cvalue, 100, params)
			switch params.MockInfoList[i].Status {
			case uint8(proto.OpStatusNoError),
				uint8(proto.OpStatusAlreadyFulfilled):
				if err != nil {
					params.Log(t)
					t.Error("set shouldn't fail with noError,AlreadyFulfilled,Inserting", err)
				}
			case uint8(proto.OpStatusBusy):
				if err != client.ErrBusy {
					t.Error("set loop3 should fail with OutOfStorage", err)
				}
			case uint8(proto.OpStatusBadParam):
				if err != client.ErrBadParam {
					t.Error("set loop3 should fail with badParam", err)
				}
			case uint8(proto.OpStatusRecordLocked):
				if err != client.ErrRecordLocked {
					t.Error("set loop3 should fail with recordLock", err)
				}
			case uint8(proto.OpStatusInserting):
				if err != nil {
					t.Error("set should fail with inconsistent error", err)
				}
			}
			//printTimeoutStatus("TestSetTwoTimeoutOneStatus 4th SS timeout loop2 ", params, err)
		}
	}
	params.MockInfoList[3].Delay = 0 //recover back to normal, no timeout
	params.MockInfoList[4].Delay = 0
}

/*********************************************************************
 * Two SS timeout with one SS has different status at prepareset stage
 * looping each SS to simulate set gets timeout for two SS
 *********************************************************************/
func TestSetTwoTimeoutOnePrepareSetStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				for a := range setPrepareArray {
					params.MockInfoList[k].Opcode = proto.OpCodePrepareSet
					params.MockInfoList[k].Status = setPrepareArray[a]

					_, err := Mockclient.Set(key, cvalue, 100, params)
					switch params.MockInfoList[k].Status {
					case uint8(proto.OpStatusNoError),
						uint8(proto.OpStatusInserting),
						uint8(proto.OpStatusAlreadyFulfilled):
						if err != nil {
							params.Log(t)
							t.Error("set fail", err)
						}
					case uint8(proto.OpStatusBusy):
						if err != client.ErrBusy {
							t.Error("set should fail with OutOfStorage", err)
						}
					case uint8(proto.OpStatusBadParam):
						if err != client.ErrBadParam {
							t.Error("set should fail with badParam", err)
						}
					case uint8(proto.OpStatusRecordLocked):
						if err != client.ErrRecordLocked {
							t.Error("set should fail with recordLock", err)
						}
					}
					params.MockInfoList[k].Opcode = proto.OpCodeNop
					//printTimeoutStatus("TestSetTwoTimeoutOneStatus 1,2 SS timeout ", params, err)
				}
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
}

/****************************************************************************
 * -- OneNoResponseTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
//Why FIXME
//	common.go:102: MockParams being set {
//		SS[0] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[1] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[2] ns=ns,op=Nop,st=Ok,del=0,ver=1 no response
//		SS[3] ns=ns,op=Nop,st=Inserting,del=0,ver=1
//		SS[4] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		}
// SS[0], SS[1] and SS[3] Prepare OK
// SS[0] and SS[1] commit OK, SS[3] commit fail
// SS[3] Repare Fail
func FIXME_TestSetOneNoResponseTwoDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//two error status + one noResponse,
	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			for a := range setPrepareArray {
				params.MockInfoList[j].Status = setPrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					for b := range setPrepareArray {
						params.MockInfoList[k].Status = setPrepareArray[b]
						_, err := Mockclient.Set(key, cvalue, 100, params)

						if params.MockInfoList[j].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[j].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("set should succeed ", err)
							}
						} else if params.MockInfoList[k].Status == uint8(proto.OpStatusNoError) ||
							params.MockInfoList[k].Status == uint8(proto.OpStatusAlreadyFulfilled) {
							if err != nil {
								params.Log(t)
								t.Error("set should succeed ", err)
							}
						} else if params.MockInfoList[j].Status == uint8(proto.OpStatusInserting) {
							if err != client.ErrInternal {
								params.Log(t)
								t.Error("set should fail with inconsistent error", err)
							}
						} else {
							if err == nil {
								params.Log(t)
								t.Error("set should fail ", err)
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
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != client.ErrRecordLocked {
					params.Log(t)
					t.Error("loop 2 should fail with badParam or recordLock error", err)
				}
				//printStatus("SetOneNoResponseTwoDiffStatus loop 2", params, err)
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/***************************************************************
 * Two SS no response with one SS has different status
 * looping each SS to simulate set no response for two SS
 ***************************************************************/
func TestSetTwoNoResponseOneStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ { //Loop through SS at position 0,1,2 for timeout
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ { //Loop through SS at position 1,2,3 for timeout
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				for a := range setPrepareArray {
					params.MockInfoList[k].Status = setPrepareArray[a]

					_, err := Mockclient.Set(key, cvalue, 100, params)
					switch params.MockInfoList[k].Status {
					case uint8(proto.OpStatusNoError),
						uint8(proto.OpStatusAlreadyFulfilled):
						if err != nil {
							params.Log(t)
							t.Error("set should succeed with noError or AlreadyFulfilled", err)
						}
					case uint8(proto.OpStatusBusy):
						if err != client.ErrBusy {
							t.Error("set should fail with nostorageServ", err)
						}
					case uint8(proto.OpStatusBadParam):
						if err != client.ErrBadParam {
							t.Error("set should fail with badParam", err)
						}
					case uint8(proto.OpStatusRecordLocked):
						if err != client.ErrRecordLocked {
							t.Error("set should fail with recordLock", err)
						}
					case uint8(proto.OpStatusInserting):
						if err != nil {
							t.Error("set should fail with inconsistent error", err)
						}
					}
					//printStatus("TestSetTwoNoResponseOneStatus 1,2 SS timeout ", params, err)
				}
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/***************************************************************
 * -- set one commit status to repairable error
 * looping each SS to simulate one repairable error for commit
 ***************************************************************/
//Why FIXME:
//  consider the following param
//	common.go:102: MockParams being set {
//		SS[0] ns=ns,op=Commit,st=OutOfMem,del=0,ver=1
//		SS[1] ns=ns,op=Nop,st=Inserting,del=0,ver=1
//		SS[2] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[3] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		SS[4] ns=ns,op=Nop,st=Ok,del=0,ver=1
//		}
// 	For prepare, SS[0], SS[1], SS[2] return OK, Inserting, and Ok respectively. Successful
//  For commit, SS[0], SS[1], SS[2] return OutOfMem, Inserting, and OK. One Ok, two failures. SS[0] and SS[1] need
//		to be repaired
//  For repair, SS[0] and SS[1] return Ok, Inserting. One Success, one Failure. So in InconsistentState, return to client
//  ErrInternal

func FIXME_TestSetOneCommitRepairableStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		for b := range setCommitRepairableErr {
			params.MockInfoList[i].Opcode = proto.OpCodeCommit
			params.MockInfoList[i].Status = setCommitRepairableErr[b]

			for j := i + 1; j <= 4; j++ {
				for a := range setPrepareArray {
					params.MockInfoList[j].Status = setPrepareArray[a]

					_, err := Mockclient.Set(key, cvalue, 100, params)
					if err != nil {
						params.Log(t)
						t.Error("set failed ", err)
					}
					//printOpsCodeStatus("set in TestSetOneCommitDiffStatus ", params, err)
				}
			}
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		}
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

/**************************************************************************
 * -- set two commit status to repairable error
 * looping each SS to simulate two repairable error for commit
 **************************************************************************/
//Why FIXME: same reason as FIXME_TestSetOneCommitRepairableStatus
func FIXME_TestSetTwoCommitRepairableStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 2; i++ {
		for b := range setCommitRepairableErr {
			params.MockInfoList[i].Opcode = proto.OpCodeCommit
			params.MockInfoList[i].Status = setCommitRepairableErr[b]

			for j := i + 1; j <= 3; j++ {
				for a := range setPrepareArray {
					params.MockInfoList[j].Status = setPrepareArray[a]

					for k := j + 1; k <= 4; k++ {
						for c := range setCommitRepairableErr {
							params.MockInfoList[k].Opcode = proto.OpCodeCommit
							params.MockInfoList[k].Status = setCommitRepairableErr[c]

							_, err := Mockclient.Set(key, cvalue, 100, params)
							if err != nil {
								params.Log(t)
								t.Error("set failed ", err)
							}
							//printOpsCodeStatus("set in TestSetTwoCommitDiffStatus ", params, err)
							params.MockInfoList[k].Opcode = proto.OpCodeNop
							params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
						}
					}
				}
			}
			params.MockInfoList[i].Opcode = proto.OpCodeNop
			params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		}
	}
}

/**************************************************************************
 * -- set three commit status to repairable error
 * looping each SS to simulate three repairable error for commit
 **************************************************************************/
func TestSetThreeCommitRepairableStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		for a := range setCommitRepairableErr {
			params.MockInfoList[i].Opcode = proto.OpCodeCommit
			params.MockInfoList[i].Status = setCommitRepairableErr[a]

			for j := i + 1; j <= 3; j++ {
				for b := range setCommitRepairableErr {
					params.MockInfoList[j].Opcode = proto.OpCodeCommit
					params.MockInfoList[j].Status = setCommitRepairableErr[b]

					for k := j + 1; k <= 4; k++ {
						for c := range setCommitRepairableErr {
							params.MockInfoList[k].Opcode = proto.OpCodeCommit
							params.MockInfoList[k].Status = setCommitRepairableErr[c]

							_, err := Mockclient.Set(key, cvalue, 100, params)
							if err == nil || err == client.ErrBusy {
								params.Log(t)
								t.Error("set should fail but error shouldn't be no storage error???? ", err)
							}
							//printOpsCodeStatus("set in TestSetThreeCommitRepairableStatus ", params, err)
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
 * -- set one BadMsg status as commit error
 * looping each SS to simulate one BadMsg for commit error
 ***************************************************************/
func TestSetOneCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 4; j++ {
			for a := range setPrepareArray {
				params.MockInfoList[j].Status = setPrepareArray[a]

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("set should failed with inconsistent error", err)
				}
				//printOpsCodeStatus("set in TestSetOneCommitDiffStatus ", params, err)
			}
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
	testutil.CheckCalLog(t, "API.*Set.*st=InconsistentState", "63", hostip, true)
}

/*******************************************************************
 * -- set two BadMsg status as commit error
 * looping each SS to simulate two BadMsg for commit error
 *******************************************************************/
func TestSetTwoCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 3; j++ {
			for a := range setPrepareArray {
				params.MockInfoList[j].Opcode = proto.OpCodePrepareSet
				params.MockInfoList[j].Status = setPrepareArray[a]

				for k := j + 1; k <= 4; k++ {
					params.MockInfoList[k].Opcode = proto.OpCodeCommit
					params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

					_, err := Mockclient.Set(key, cvalue, 100, params)
					if err != nil {
						params.Log(t)
						t.Error("set should pass", err)
					}
					//printOpsCodeStatus("set in TestSetTwoCommitDiffStatus ", params, err)
					params.MockInfoList[k].Opcode = proto.OpCodeNop
					params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)
				}
			}
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
	}
}

/********************************************************************
 * -- set three BadMsg status as commit error
 * looping each SS to simulate three BadMsg for commit error
 ********************************************************************/
func TestSetThreeCommitBadMsg(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadMsg)

		for j := i + 1; j <= 2; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodeCommit
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Opcode = proto.OpCodeCommit
				params.MockInfoList[k].Status = uint8(proto.OpStatusBadMsg)

				///TODO: XT: may change the returned status later
				if _, err := Mockclient.Set(key, cvalue, 100, params); err != client.ErrWriteFailure && err != client.ErrInternal {
					params.Log(t)
					t.Error("create should fail with CommitFailure", err)
				}
				//printOpsCodeStatus("create in TestSetThreeCommitBadMsg ", params, err)
				params.MockInfoList[k].Opcode = proto.OpCodeNop
				params.MockInfoList[k].Status = uint8(proto.OpStatusRecordLocked)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.OpStatusBusy)
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
	testutil.CheckCalLog(t, "API.*Set.*st=CommitFailure", "7", hostip, true)
}

/**************************************************************************
 * -- SecondFace commit mix status
 * looping each SS to simulate three second face has mix status error
 **************************************************************************/
func TestSetCommitThreeMixError(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Opcode = proto.OpCodeCommit
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoUncommitted)

		for j := i + 1; j <= 2; j++ {
			params.MockInfoList[j].Opcode = proto.OpCodeCommit
			params.MockInfoList[j].Status = uint8(proto.OpStatusBadMsg)

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Opcode = proto.OpCodeCommit
				params.MockInfoList[k].Status = uint8(proto.OpStatusNoError)

				_, err := Mockclient.Set(key, cvalue, 100, params)
				if err != nil {
					params.Log(t)
					t.Error("set should succeed", err)
				}
				//printOpsCodeStatus("set in TestSetCommitThreeMixError ", params, err)
				params.MockInfoList[k].Opcode = proto.OpCodeNop
				params.MockInfoList[k].Status = uint8(proto.OpStatusBusy)
			}
			params.MockInfoList[j].Opcode = proto.OpCodeNop
			params.MockInfoList[j].Status = uint8(proto.OpStatusBusy)
		}
		params.MockInfoList[i].Opcode = proto.OpCodeNop
		params.MockInfoList[i].Status = uint8(proto.OpStatusRecordLocked)
	}
	params.SetOpCodeForAll(proto.OpCodeNop)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
	testutil.CheckCalLog(t, "API.*Set.*st=InconsistentState", "7", hostip, true)
}

/***********************************************
 * -- abort with different return code
 * looping each SS to simulate abort case
 ***********************************************/
func TestSetAbortDiffStatus(t *testing.T) {
	cvalue := []byte("Value to be stored for Set")
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	//1,2,4 error state, 3 abort
	params.MockInfoList[0].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[1].Status = uint8(proto.OpStatusRecordLocked)
	params.MockInfoList[3].Status = uint8(proto.OpStatusRecordLocked)
	params.MockInfoList[2].Opcode = proto.OpCodeAbort
	for a := range setAbortArray {
		params.MockInfoList[2].Status = setAbortArray[a]
		_, err := Mockclient.Set(key, cvalue, 20, params)
		if err != client.ErrRecordLocked {
			t.Error("set should hit bad requestId error, err", err)
		}
		//printOpsCodeStatus("1st one in TestSetAbortDiffStatus", params, err)
	}
	params.SetOpCodeForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))

	//2,4,5 error state, 1,3 abort
	params.MockInfoList[1].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[3].Status = uint8(proto.OpStatusBadParam)
	params.MockInfoList[4].Status = uint8(proto.OpStatusBusy)

	params.MockInfoList[0].Opcode = proto.OpCodeAbort
	params.MockInfoList[2].Opcode = proto.OpCodeAbort

	for a := range setAbortArray {
		params.MockInfoList[0].Status = setAbortArray[a]
		for b := range setAbortArray {
			params.MockInfoList[2].Status = setAbortArray[b]

			_, err := Mockclient.Set(key, cvalue, 20, params)
			if err != client.ErrBusy {
				t.Error("set should hit bad requestId error, err", err)
			}
			//printOpsCodeStatus("2nd one in TestSetAbortDiffStatus", params, err)
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
	setPrepareArray = [7]uint8{uint8(proto.StatusNoCapacity), //6
		uint8(proto.OpStatusBadParam),         //7
		uint8(proto.OpStatusRecordLocked),     //8
		uint8(proto.OpStatusInserting),        //15
		uint8(proto.OpStatusAlreadyFulfilled), //17
		uint8(proto.OpStatusNoError),          //0
	}
	setCommitRepairableErr = [3]uint8{uint8(proto.StatusNoCapacity), //6
		uint8(proto.OpStatusBadParam),      //7
		uint8(proto.OpStatusNoUncommitted), //10
	}
	setAbortArray = [5]uint8{uint8(proto.OpStatusNoKey), //3
		uint8(proto.OpStatusBadParam),      //7
		uint8(proto.OpStatusNoUncommitted), //10
		uint8(proto.OpStatusBadMsg),        //11
		uint8(proto.OpStatusNoError),       //0
	}
}
