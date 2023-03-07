package unittest

import (
	"juno/pkg/client"
	"juno/pkg/proto"
	"juno/test/testutil"
	"juno/test/testutil/mock"
	"testing"
)

var getStatusArray [5]uint8

func TestGetNormal(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersionForAll(1)
	testutil.RemoveLog(t, hostip, true)

	_, recInfo, err := Mockclient.Get(key, params)
	if err != nil {
		t.Error("Normal get failed. error:", err)
	}
	if recInfo.GetVersion() != 1 {
		t.Errorf("Wrong version: %d", recInfo.GetVersion())
	}
}

func TestGetNoKey(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetStatusForAll(uint8(proto.OpStatusNoKey))
	testutil.RemoveLog(t, hostip, true)

	_, _, err := Mockclient.Get(key, params)
	if err != client.ErrNoKey {
		t.Error("failed. error: ", err)
	}
}

func TestGetOneHasRecordOthersNoKey(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersionForAll(1)
	params.SetStatusForAll(uint8(proto.OpStatusNoKey))
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)
		params.MockInfoList[i].Version = 5

		_, recInfo, err := Mockclient.Get(key, params)
		if i >= 3 {
			if err != client.ErrNoKey {
				t.Error("get should fail as first 3 ss all has no key", err)
			}
		} else if recInfo.GetVersion() != 5 {
			t.Error("version get should be the highest version", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoKey)
		params.MockInfoList[i].Version = 1
	}
	params.SetVersionForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

func TestGetOneHasRecordOthersBadParam(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersionForAll(1)
	params.SetStatusForAll(uint8(proto.OpStatusBadParam))
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Status = uint8(proto.OpStatusNoError)

		if _, _, err := Mockclient.Get(key, params); err == nil {
			params.Log(t)
			t.Error("get should fail as >= 3 ss get bad params", err)
		}
		params.MockInfoList[i].Status = uint8(proto.OpStatusBadParam)
		params.MockInfoList[i].Version = 1
	}
	params.SetVersionForAll(0)
	params.SetStatusForAll(uint8(proto.OpStatusNoError))
}

func TestGetSSReturnDifferent(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersionForAll(1)
	params.MockInfoList[1].Version = 2
	params.MockInfoList[2].Version = 3

	_, recInfo, err := Mockclient.Get(key, params)
	if err != nil {
		t.Error("Get failed. error", err)
	}
	if recInfo.GetVersion() != 3 {
		t.Error("Wrong version: ", recInfo.GetVersion())
	}
}

/***************************************************************
 * -- GetTimeout
 * looping each SS to simulate get gets timeout from each SS
 ***************************************************************/
func TestGetVersionValueOneSSTimeout(t *testing.T) {
	params := mock.NewMockParams(5)
	key := testutil.GenerateRandomKey(32)
	params.SetVersion(1, 2, 3, 2, 1)
	params.SetValue([]byte("v1"), []byte("v2"), []byte("v3"), []byte("v2"), []byte("v1"))

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].Delay = 1000000
		value, recInfo, err := Mockclient.Get(key, params)

		if err != nil {
			params.Log(t)
			t.Error("Get ", i, " failed. error: ", err)
		} else {
			if i == 2 { //only when timeout happened at pos 2, version won't be 3, will be 2
				if recInfo.GetVersion() != 2 || string(value) != "v2" {
					params.Log(t)
					t.Error("Wrong version/value get", recInfo.GetVersion(), "string value is ", string(value[:]), "expect version 2", err)
				}
			} else {
				if recInfo.GetVersion() != 3 || string(value) != "v3" {
					params.Log(t)
					t.Error("Wrong version/value get", recInfo.GetVersion(), "string value is ", string(value[:]), "expect version 3", err)
				}
			}
		}
		params.MockInfoList[i].Delay = 0
	}
}

/*
 * looping SS to simulate get gets timeout from two SS
 */
func TestGetVersionValueTwoSSTimeout(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	params.SetValue([]byte("v1"), []byte("v2"), []byte("v3"), []byte("v2"), []byte("v1"))
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			value, recInfo, err := Mockclient.Get(key, params)
			if err != nil {
				params.Log(t)
				t.Error("Get ", i, j, " failed. error: ", err)
			} else {
				if i == 2 || j == 2 { //only when timeout happened at pos 2, version won't be 3, will be 2
					if recInfo.GetVersion() != 2 || string(value) != "v2" {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
					}
				} else {
					if recInfo.GetVersion() != 3 || string(value) != "v3" {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
					}
				}
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}
}

/*
 * looping SS to simulate get gets timeout from three SS
 */
func TestGetThreeSSTimeout(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	testutil.RemoveLog(t, hostip, true)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].Delay = 1000000

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].Delay = 1000000

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].Delay = 1000000
				_, _, err := Mockclient.Get(key, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Error("get should fail with no storage server error", err)
				}
				params.MockInfoList[k].Delay = 0
			}
			params.MockInfoList[j].Delay = 0
		}
		params.MockInfoList[i].Delay = 0
	}

	params.MockInfoList[0].Delay = 1000000
	params.MockInfoList[2].Delay = 1000000
	params.MockInfoList[4].Delay = 1000000
	_, _, err := Mockclient.Get(key, params)
	if err != client.ErrBusy {
		params.Log(t)
		t.Error("get should fail with no storage server error", err)
	}
	params.MockInfoList[0].Delay = 0
	params.MockInfoList[2].Delay = 0
	params.MockInfoList[4].Delay = 0
}

/***************************************************************
 * -- NoResponse
 * looping each SS to simulate get gets no response from each SS
 ***************************************************************/
func TestGetVersionValueOneSSNoResponse(t *testing.T) {
	params := mock.NewMockParams(5)
	key := testutil.GenerateRandomKey(32)
	params.SetVersion(1, 2, 3, 2, 1)
	params.SetValue([]byte("v1"), []byte("v2"), []byte("v3"), []byte("v2"), []byte("v1"))

	for i := 0; i < 5; i++ {
		params.MockInfoList[i].NoResponse = true
		value, recInfo, err := Mockclient.Get(key, params)
		if err != nil {
			params.Log(t)
			t.Error("Get ", i, " failed. error: ", err)
		} else {
			if i == 2 {
				if recInfo.GetVersion() != 2 || string(value) != "v2" {
					params.Log(t)
					t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
				}
			} else {
				if recInfo.GetVersion() != 3 || string(value) != "v3" {
					params.Log(t)
					t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
				}
			}
		}
		params.MockInfoList[i].NoResponse = false
	}
}

/*
 * looping SS to simulate get gets no response from two SS
 */
func TestGetVersionValueTwoSSNoResponse(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	params.SetValue([]byte("v1"), []byte("v2"), []byte("v3"), []byte("v2"), []byte("v1"))

	for i := 0; i < 4; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j <= 4; j++ {
			params.MockInfoList[j].NoResponse = true

			value, recInfo, err := Mockclient.Get(key, params)
			if err != nil {
				params.Log(t)
				t.Error("Get failed ", err)
			} else {
				if i == 2 || j == 2 {
					if recInfo.GetVersion() != 2 || string(value) != "v2" {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
					}
				} else {
					if recInfo.GetVersion() != 3 || string(value) != "v3" {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
					}
				}
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
	//we don't do full loop, just add one more case on top of above loop
	params.MockInfoList[1].NoResponse = true
	params.MockInfoList[3].NoResponse = true

	value, recInfo, err := Mockclient.Get(key, params)
	if err != nil {
		params.Log(t)
		t.Error("Get failed ", err)
	} else {
		if recInfo.GetVersion() != 3 || string(value) != "v3" {
			params.Log(t)
			t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
		}
	}
	params.MockInfoList[1].NoResponse = false
	params.MockInfoList[3].NoResponse = false
}

/*
 * looping SS to simulate get gets no response from three SS
 */
func TestGetThreeSSNoResponse(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				params.MockInfoList[k].NoResponse = true

				_, _, err := Mockclient.Get(key, params)
				if err != client.ErrBusy {
					params.Log(t)
					t.Errorf("Get should fail with no storageserv error. %s", err)
				}
				params.MockInfoList[k].NoResponse = false
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
	//One more condition check on top of above loop
	params.MockInfoList[0].NoResponse = true
	params.MockInfoList[1].NoResponse = true
	params.MockInfoList[3].NoResponse = true
	_, _, err := Mockclient.Get(key, params)
	if err != client.ErrBusy {
		params.Log(t)
		t.Error("Get should fail with no storageserver error", err)
	}
	params.MockInfoList[0].NoResponse = false
	params.MockInfoList[1].NoResponse = false
	params.MockInfoList[3].NoResponse = false
}

/**************************************************
 * -- AllStatusError
 * looping each SS to simulate get gets one error
 *************************************************/
func TestGetOneStatusError(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(2, 3, 1, 2, 3)
	params.SetValue([]byte("v2"), []byte("v3"), []byte("v1"), []byte("v2"), []byte("v3"))

	for i := 0; i < 5; i++ {
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			value, recInfo, err := Mockclient.Get(key, params)
			if err != nil {
				params.Log(t)
				t.Error("Get at pos ", i, " failed. error: ", err)
			} else {
				if i == 1 && params.MockInfoList[i].Status != uint8(proto.OpStatusNoError) {
					if recInfo.GetVersion() != 2 || string(value) != "v2" {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
					}
				} else {
					if recInfo.GetVersion() != 3 || string(value) != "v3" {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
					}
				}
			}
		}
	}
}

/*
 * looping each SS to simulate get gets two errors
 */
func TestGetTwoStatusError(t *testing.T) { //code has bug
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)

	var s [5]uint8

	for i := 0; i < 4; i++ {
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			for j := i + 1; j <= 4; j++ {
				for b := range getStatusArray {
					params.MockInfoList[j].Status = getStatusArray[b]

					for w := 0; w <= 4; w++ { //only to short the printout string, no real usage
						s[w] = params.MockInfoList[w].Status
					}

					_, recInfo, err := Mockclient.Get(key, params)
					if err != nil {
						params.Log(t)
						t.Error("Get fail", err)
					} else {
						if params.MockInfoList[2].Status == uint8(proto.OpStatusNoError) { //x,x,0,x,x
							if recInfo.GetVersion() != 3 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
							}
						} else if params.MockInfoList[1].Status == uint8(proto.OpStatusNoError) { //x,0,x,x,x
							if recInfo.GetVersion() != 2 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
							}
						} else if s[0] == uint8(proto.OpStatusNoError) && s[3] == uint8(proto.OpStatusNoError) {
							if s[1]+s[2] == 6 || s[1]+s[2] == 8 || (s[1] == 5 && s[2] == 5) { //sample: ok,nokey,dataExpire,ok,x
								if recInfo.GetVersion() != 1 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
								}
							} else {
								if recInfo.GetVersion() != 2 { //sample: ok, badParam,nokey,ok,x
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
							}
						}
					}
				}
				//printStatus("TestGetTwoStatusError", params, err)
			}
		}
	}

}

/********************************************************************
 * looping SS to simulate get gets different errors from three SS
 ********************************************************************/
func TestGetThreeStatusError(t *testing.T) { //code has bug
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	var s [5]uint8
	var nokeyCount int

	for i := 0; i < 3; i++ {
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			for j := i + 1; j < 4; j++ {
				for b := range getStatusArray {
					params.MockInfoList[j].Status = getStatusArray[b]

					for k := j + 1; k <= 4; k++ {
						for c := range getStatusArray {
							params.MockInfoList[k].Status = getStatusArray[c]

							for w := 0; w <= 4; w++ { //only to short the printout string, no real usage
								s[w] = params.MockInfoList[w].Status
							}

							_, recInfo, err := Mockclient.Get(key, params)

							if err == nil { //err=nil means it hits W PrepareEndState
								if s[2] == uint8(proto.OpStatusNoError) { //x,x,ok,x,x
									if recInfo.GetVersion() != 3 {
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
									}
								} else if s[1] == uint8(proto.OpStatusNoError) { //x,ok,x,x,x
									if recInfo.GetVersion() != 2 {
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
									}
									//} else if s[0]+s[1]+s[2] > 14 {
								} else if s[0] != uint8(proto.OpStatusNoKey) && s[1] != uint8(proto.OpStatusNoKey) &&
									s[2] != uint8(proto.OpStatusNoKey) && s[0]+s[1]+s[2] >= 18 { //first 3 all hit outofMem or badParam
									params.Log(t)
									t.Error("Get should fail as first 3 all get error", err)
								} else if s[0] == uint8(proto.OpStatusNoError) && s[3] != uint8(proto.OpStatusNoError) {
									if recInfo.GetVersion() != 1 { //sample: ok,nokey,bad,nokey,ok|ok,nokey,bad,bad,ok
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
									}
								} else if s[0] == uint8(proto.OpStatusNoError) && s[3] == uint8(proto.OpStatusNoError) {
									if s[1]+s[2] == 6 || s[1]+s[2] == 8 || (s[1] == 5 && s[2] == 5) { //sample: ok,nokey,dataExpire,ok,x
										if recInfo.GetVersion() != 1 {
											params.Log(t)
											t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
										}
									} else {
										if recInfo.GetVersion() != 2 { //sample: ok, badParam,nokey,ok,x
											params.Log(t)
											t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
										}
									}
									//no ok for first 3 ss, as long as it has badParam or outofMem. sample: badParam,nokey,nokey,ok,ok
								} else if s[0]+s[1] >= 9 || s[0]+s[2] >= 9 || s[1]+s[2] >= 9 { //no ok for first 3 ss
									if recInfo.GetVersion() != 2 {
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
									}
								}
							} else { //err != nil
								if err == client.ErrNoKey {
									nokeyCount++
								}
							}
							//printStatus("TestGetThreeStatusError", params, err)
						}
					}
				}
			}
		}

		/*******************************************************************
		 * We should get total 8 noKey errors : nokey|nokey|nokey;
		 * nokey|nokey|exp; nokey|exp|nokey; exp|nokey|nokey; nokey|exp|exp;
		 * exp|nokey|exp; exp|exp|nokey; exp|exp|exp
		 *******************************************************************/
		if nokeyCount < 8 { //TODO: need check later as don't know for noKey,badparam,nokey,x,x what it will return
			t.Error("We should get at least eight times noKey error, please check ")
		}
	}
}

/****************************************************************************
* -- OneTimeoutTwoStatus
* looping each SS to simulate one timeout, two diff status from different SS
****************************************************************************/
func TestGetOneTimeoutTwoDiffStatus(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	params.SetValue([]byte("v1"), []byte("v2"), []byte("v3"), []byte("v2"), []byte("v1"))
	var s [5]uint8
	var d [5]uint32

	for i := 0; i < 3; i++ {
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			for j := i + 1; j < 4; j++ {
				for b := range getStatusArray {
					params.MockInfoList[j].Status = getStatusArray[b]

					for k := j + 1; k <= 4; k++ {
						params.MockInfoList[k].Delay = 1000000

						//We set s status the same as badParam number because timeout is also a retryable
						//error. With this, we can use the same calucation as error code for timeout, but we don't
						//change the real timeout value in params. In this way,validation don't need to seperate out
						//timeout and error condition and have less checking factor.
						for w := 0; w <= 4; w++ { //short printout string and assign error status no for timeout ss
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].Delay
							if d[w] != 0 {
								s[w] = uint8(proto.StatusNoCapacity)
							}
						}
						_, recInfo, err := Mockclient.Get(key, params)

						if err == nil {
							if s[2] == uint8(proto.OpStatusNoError) { //x,x,ok,x,x
								if recInfo.GetVersion() != 3 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
								}
							} else if s[1] == uint8(proto.OpStatusNoError) { //x,ok,x,x,x
								if recInfo.GetVersion() != 2 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
								//} else if s[0]+s[1]+s[2] > 14 {
							} else if s[0] != uint8(proto.OpStatusNoKey) && s[1] != uint8(proto.OpStatusNoKey) &&
								s[2] != uint8(proto.OpStatusNoKey) && s[0]+s[1]+s[2] >= 18 { //first 3 all hit outofMem or badParam
								params.Log(t)
								t.Error("Get should fail as first 3 all get error", err)
							} else if s[0] == uint8(proto.OpStatusNoError) && s[3] != uint8(proto.OpStatusNoError) {
								if recInfo.GetVersion() != 1 { //sample: ok,nokey,bad,nokey,ok|ok,nokey,bad,bad,ok
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
								}
							} else if s[0] == uint8(proto.OpStatusNoError) && s[3] == uint8(proto.OpStatusNoError) {
								if s[1]+s[2] == 6 || s[1]+s[2] == 8 || (s[1] == 5 && s[2] == 5) { //sample: ok,dataExpire,dataExpire,ok,x
									if recInfo.GetVersion() != 1 {
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
									}
								} else {
									if recInfo.GetVersion() != 2 { //sample: ok, badParam,nokey,ok,x
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
									}
								}
								//no ok for first 3 ss, as long as it has badParam or outofMem. sample: badParam,nokey,nokey,ok,ok
							} else if s[0]+s[1] >= 9 || s[0]+s[2] >= 9 || s[1]+s[2] >= 9 { //no ok for first 3 ss
								if recInfo.GetVersion() != 2 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
							}
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
			for a := range getStatusArray {
				params.MockInfoList[j].Status = getStatusArray[a]

				for k := j + 1; k <= 4; k++ {
					for b := range getStatusArray {
						params.MockInfoList[k].Status = getStatusArray[b]

						for w := 0; w <= 4; w++ {
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].Delay
							if d[w] != 0 {
								s[w] = uint8(proto.StatusNoCapacity)
							}
						}
						value, recInfo, err := Mockclient.Get(key, params)

						if err == nil {
							if s[2] == uint8(proto.OpStatusNoError) {
								if recInfo.GetVersion() != 3 || string(value) != "v3" {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
								}
							} else if s[1] == uint8(proto.OpStatusNoError) || s[3] == uint8(proto.OpStatusNoError) {
								if recInfo.GetVersion() != 2 || string(value) != "v2" {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
							} else {
								if recInfo.GetVersion() != 1 || string(value) != "v1" {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
								}
							}
						}
					}
				}
			}
		}
		params.MockInfoList[i].Delay = 0
	}

	for i := 0; i < 3; i++ {
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			for j := i + 1; j < 4; j++ {
				params.MockInfoList[j].Delay = 1000000

				for k := j + 1; k <= 4; k++ {
					for b := range getStatusArray {
						params.MockInfoList[k].Status = getStatusArray[b]

						for w := 0; w <= 4; w++ {
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].Delay
							if d[w] != 0 {
								s[w] = uint8(proto.StatusNoCapacity)
							}
						}
						_, recInfo, err := Mockclient.Get(key, params)

						if err == nil {
							if s[2] == uint8(proto.OpStatusNoError) { //x,x,ok,x,x
								if recInfo.GetVersion() != 3 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
								}
							} else if s[1] == uint8(proto.OpStatusNoError) { //x,ok,x,x,x
								if recInfo.GetVersion() != 2 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
								//} else if s[0]+s[1]+s[2] > 14 {
							} else if s[0] != uint8(proto.OpStatusNoKey) && s[1] != uint8(proto.OpStatusNoKey) &&
								s[2] != uint8(proto.OpStatusNoKey) && s[0]+s[1]+s[2] >= 18 { //first 3 all hit outofMem or badParam
								params.Log(t)
								t.Error("Get should fail as first 3 all get error", err)
							} else if s[0] == uint8(proto.OpStatusNoError) && s[3] != uint8(proto.OpStatusNoError) {
								if recInfo.GetVersion() != 1 { //sample: ok,nokey,bad,nokey,ok|ok,nokey,bad,bad,ok
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
								}
							} else if s[0] == uint8(proto.OpStatusNoError) && s[3] == uint8(proto.OpStatusNoError) {
								if s[1]+s[2] == 6 || s[1]+s[2] == 8 || (s[1] == 5 && s[2] == 5) { //sample: ok,nokey,dataExpire,ok,x
									if recInfo.GetVersion() != 1 {
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
									}
								} else {
									if recInfo.GetVersion() != 2 { //sample: ok, badParam,nokey,ok,x
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
									}
								}
								//no ok for first 3 ss, as long as it has badParam or outofMem. sample: badParam,nokey,nokey,ok,ok
							} else if s[0]+s[1] >= 9 || s[0]+s[2] >= 9 || s[1]+s[2] >= 9 { //no ok for first 3 ss
								if recInfo.GetVersion() != 2 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
							}
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
func TestGetTwoTimeoutOneStatus(t *testing.T) {
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
				for a := range getStatusArray {
					params.MockInfoList[k].Status = getStatusArray[a]

					for w := 0; w <= 4; w++ {
						s[w] = params.MockInfoList[w].Status
						d[w] = params.MockInfoList[w].Delay
						if d[w] != 0 {
							s[w] = uint8(proto.StatusNoCapacity)
						}
					}

					_, recInfo, err := Mockclient.Get(key, params)
					if err == nil {
						if s[2] == uint8(proto.OpStatusNoError) {
							if recInfo.GetVersion() != 3 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
							}
						} else if s[1] == uint8(proto.OpStatusNoError) || s[3] == uint8(proto.OpStatusNoError) {
							if recInfo.GetVersion() != 2 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
							}
						}
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
			for a := range getStatusArray {
				params.MockInfoList[j].Status = getStatusArray[a]

				for k := j + 1; k <= 4; k++ {
					params.MockInfoList[k].Delay = 1000000

					for w := 0; w <= 4; w++ {
						s[w] = params.MockInfoList[w].Status
						d[w] = params.MockInfoList[w].Delay
						if d[w] != 0 {
							s[w] = uint8(proto.StatusNoCapacity)
						}
					}

					_, recInfo, err := Mockclient.Get(key, params)

					if err == nil {
						if s[2] == uint8(proto.OpStatusNoError) {
							if recInfo.GetVersion() != 3 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
							}
						} else if s[1] == uint8(proto.OpStatusNoError) || s[3] == uint8(proto.OpStatusNoError) {
							if recInfo.GetVersion() != 2 {
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)

							}
						}
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
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			for w := 0; w <= 2; w++ {
				s[w] = params.MockInfoList[w].Status
			}
			_, recInfo, err := Mockclient.Get(key, params)
			if err == nil {
				if s[2] == uint8(proto.OpStatusNoError) {
					if recInfo.GetVersion() != 3 {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
					}
				} else if s[1] == uint8(proto.OpStatusNoError) {
					if recInfo.GetVersion() != 2 {
						params.Log(t)
						t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
					}
				}
			}
		}
	}
	params.MockInfoList[3].Delay = 0
	params.MockInfoList[4].Delay = 0
}

/****************************************************************************
 * -- OneNoResponseTwoStatus
 * looping each SS to simulate one timeout, two diff status from different SS
 ****************************************************************************/
func TestGetOneNoResponseTwoDiffStatus(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	params.SetValue([]byte("v1"), []byte("v2"), []byte("v3"), []byte("v2"), []byte("v1"))
	var s [5]uint8
	var d [5]bool

	for i := 0; i < 3; i++ {
		for a := range getStatusArray {
			params.MockInfoList[i].Status = getStatusArray[a]

			for j := i + 1; j < 4; j++ {
				for b := range getStatusArray {
					params.MockInfoList[j].Status = getStatusArray[b]

					for k := j + 1; k <= 4; k++ {
						params.MockInfoList[k].NoResponse = true

						//We set s status the same as badParam number because timeout is also a retryable
						//error. With this, we can use the same calucation as error code for timeout, but we don't
						//change the real timeout value in params. In this way,validation don't need to seperate out
						//timeout and error condition and have less checking factor.
						for w := 0; w <= 4; w++ { //short printout string and assign error status no for timeout ss
							s[w] = params.MockInfoList[w].Status
							d[w] = params.MockInfoList[w].NoResponse
							if d[w] == true {
								s[w] = uint8(proto.StatusNoCapacity)
							}
						}
						_, recInfo, err := Mockclient.Get(key, params)

						if err == nil {
							if s[2] == uint8(proto.OpStatusNoError) { //x,x,ok,x,x
								if recInfo.GetVersion() != 3 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
								}
							} else if s[1] == uint8(proto.OpStatusNoError) { //x,ok,x,x,x
								if recInfo.GetVersion() != 2 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
								//} else if s[0]+s[1]+s[2] > 14 {
							} else if s[0] != uint8(proto.OpStatusNoKey) && s[1] != uint8(proto.OpStatusNoKey) &&
								s[2] != uint8(proto.OpStatusNoKey) && s[0]+s[1]+s[2] >= 18 { //first 3 all hit outofMem or badParam
								params.Log(t)
								t.Error("Get should fail as first 3 all get error", err)
							} else if s[0] == uint8(proto.OpStatusNoError) && s[3] != uint8(proto.OpStatusNoError) {
								if recInfo.GetVersion() != 1 { //sample: ok,nokey,bad,nokey,ok|ok,nokey,bad,bad,ok
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
								}
							} else if s[0] == uint8(proto.OpStatusNoError) && s[3] == uint8(proto.OpStatusNoError) {
								if s[1]+s[2] == 6 || s[1]+s[2] == 8 || (s[1] == 5 && s[2] == 5) { //sample: ok,dataExpire,dataExpire,ok,x
									if recInfo.GetVersion() != 1 {
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 1", err)
									}
								} else {
									if recInfo.GetVersion() != 2 { //sample: ok, badParam,nokey,ok,x
										params.Log(t)
										t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
									}
								}
								//no ok for first 3 ss, as long as it has badParam or outofMem. sample: badParam,nokey,nokey,ok,ok
							} else if s[0]+s[1] >= 9 || s[0]+s[2] >= 9 || s[1]+s[2] >= 9 { //no ok for first 3 ss
								if recInfo.GetVersion() != 2 {
									params.Log(t)
									t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
								}
							}
						}
						params.MockInfoList[k].NoResponse = false
					}
				}
			}
		}
	}
}

/****************************************************************************
* -- TwoNoResponseOneStatus
* looping each SS to simulate two timeout, one status from different SS
*****************************************************************************/
func TestGetTwoNoResponseOneStatus(t *testing.T) {
	key := testutil.GenerateRandomKey(32)
	params := mock.NewMockParams(5)
	params.SetVersion(1, 2, 3, 2, 1)
	var s [5]uint8
	var d [5]bool

	for i := 0; i < 3; i++ {
		params.MockInfoList[i].NoResponse = true

		for j := i + 1; j < 4; j++ {
			params.MockInfoList[j].NoResponse = true

			for k := j + 1; k <= 4; k++ {
				for a := range getStatusArray {
					params.MockInfoList[k].Status = getStatusArray[a]

					for w := 0; w <= 4; w++ {
						s[w] = params.MockInfoList[w].Status
						d[w] = params.MockInfoList[w].NoResponse
						if d[w] == true {
							s[w] = uint8(proto.StatusNoCapacity)
						}
					}

					_, recInfo, err := Mockclient.Get(key, params)
					if err == nil {
						if s[2] == uint8(proto.OpStatusNoError) {
							if recInfo.GetVersion() != 3 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 3", err)
							}
						} else if s[1] == uint8(proto.OpStatusNoError) || s[3] == uint8(proto.OpStatusNoError) {
							if recInfo.GetVersion() != 2 {
								params.Log(t)
								t.Error("Wrong version/value get", recInfo.GetVersion(), "expect version 2", err)
							}
						}
					}
				}
			}
			params.MockInfoList[j].NoResponse = false
		}
		params.MockInfoList[i].NoResponse = false
	}
}

func init() {
	/***********************************************************
	 * 0 -- keep response code 0 as the last one so the error
	 * 		code assign can be recovered to 0 as the last step
	 ***********************************************************/
	getStatusArray = [5]uint8{uint8(proto.StatusNoCapacity), //6
		uint8(proto.OpStatusBadParam), //7
		uint8(proto.OpStatusNoKey),    //3
		uint8(proto.OpStatusNoKey),    //5
		uint8(proto.OpStatusNoError),
	}
}
