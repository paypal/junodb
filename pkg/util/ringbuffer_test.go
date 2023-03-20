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

//
// Yaping shi
// Dec 20, 2016
//
// lock-free RingBuffer
//
package util

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var TotalProcessed uint32 = 0
var TotalDropped uint32 = 0
var TotalExpired uint32 = 0
var TotalRetry uint32 = 0
var TotalCleanup uint32 = 0

func GetTotal() uint32 {
	return TotalProcessed + TotalDropped + TotalExpired + TotalCleanup
}

type MyQueItem struct {
	QueItemBase
	Id       uint32
	OrigId   uint32
	deadline time.Time
	timeout  time.Duration
	retryCnt uint32
	retryCh  chan *MyQueItem
}

func (item *MyQueItem) OnCleanup() {
	fmt.Printf("cleanup id=%d\n", item.Id)
	TotalCleanup++
}

func (item *MyQueItem) OnExpiration() {
	fmt.Printf("Expired, id=%d\n", item.Id)
	if item.retryCnt < 3 {
		item.retryCnt++
		TotalRetry++
		if item.retryCh != nil {
			select {
			case item.retryCh <- item:
				item.ResetDeadline()
				fmt.Printf("retry, %d\n", item.Id)
			default:
				fmt.Printf("queue full, drop %d\n", item.Id)
			}
		}
	} else {
		fmt.Printf("max retry reached, drop %d\n", item.Id)
		TotalExpired++
	}
}

func TestQueueSize(t *testing.T) {
	timeout := time.Duration(100 * time.Millisecond)
	N := 100
	rb := NewRingBufferWithExtra(100, 200)
	for i := 0; i < N+2; i++ {
		item := &MyQueItem{Id: uint32(i)}
		item.SetQueTimeout(timeout)
		id, err := rb.EnQueue(item)
		if err != nil {
			fmt.Printf("i=%d, id=%d, err=%v\n", i, id, err)
		}
	}
}

func TestQueueSizeWithHoles(t *testing.T) {

	timeout := time.Duration(1000 * time.Millisecond)
	N := 100

	rb := NewRingBufferWithExtra(100, 200)
	for i := 0; i < N+2; i++ {
		item := &MyQueItem{Id: uint32(i)}
		item.SetQueTimeout(timeout)
		id, err := rb.EnQueue(item)
		if err != nil {
			fmt.Printf("i=%d, id=%d, err=%v\n", i, id, err)
		}
	}

	for i := 1; i < N; i++ {
		_, err := rb.Remove(uint32(i))
		if err != nil {
			fmt.Printf("i=%d, err=%v\n", i, err)
		}
	}

	// the item 199 will fail as there is the item 0, 100-198, which is 100 already
	for i := N; i < N+N; i++ {
		if N%10 == 0 {
			timeout = time.Duration(1000*time.Millisecond) + time.Duration(i-N)*time.Millisecond
		} else {
			timeout = time.Duration(1000 * time.Millisecond)

		}
		item := &MyQueItem{Id: uint32(i)}
		item.SetQueTimeout(timeout)
		id, err := rb.EnQueue(item)
		if err != nil {
			fmt.Printf("i=%d, id=%d, err=%v\n", i, id, err)
		}
	}

	rb.CleanUp()
	item := &MyQueItem{Id: uint32(199)}
	item.SetQueTimeout(timeout)
	_, err := rb.EnQueue(item)
	if err != nil {
		fmt.Printf("err=%v\n", err)
	}

	time.Sleep(1000 * time.Millisecond)
	rb.CleanUp()

	// try again
	item.SetQueTimeout(time.Duration(10 * time.Millisecond))
	_, err = rb.EnQueue(item)
	if err != nil {
		fmt.Printf("err=%v\n", err)
	}

	success_cnt := 0
	loop_cnt := 1
	start_id := 2 * N
	for success_cnt < N {
		fmt.Printf("loop: %d\n", loop_cnt)
		loop_cnt++
		time.Sleep(10 * time.Millisecond)
		rb.CleanUp()
		for i := start_id; i < 3*N; i++ {
			item := &MyQueItem{Id: uint32(i)}
			item.SetQueTimeout(timeout)
			id, err := rb.EnQueue(item)
			if err != nil {
				fmt.Printf("i=%d, id=%d, err=%v\n", i, id, err)
				break
			} else {
				success_cnt++
				start_id++
			}
		}
	}
}

func TestRingSameRateLockLess(t *testing.T) {
	N := 10000
	idCh := make(chan uint32, N)
	rb := NewRingBuffer(1023)

	var wg sync.WaitGroup
	wg.Add(2)

	// writer go routine
	go func(ch chan uint32, rb *RingBuffer) {
		timeout := time.Duration(10000 * time.Millisecond)
		defer wg.Done()
		start := time.Now()
		for i := 0; i < N; i++ {
			item := &MyQueItem{Id: uint32(i)}
			item.SetQueTimeout(timeout)
			//deadline: time.Now().Add(timeout)}
			id, err := rb.EnQueue(item)

			if err != nil {
				fmt.Printf("enque failed %d, err=%s\n", i, err)
			} else {
				item.Id = id // it's safe to set as the reader hasn't received it
				ch <- uint32(id)
				//	fmt.Printf("enque i: %d, id=%d\n", i, id)
			}
			time.Sleep(10 * time.Microsecond)
		}
		close(ch)
		elapsed := time.Since(start)
		fmt.Printf("writer took %s\n", elapsed)
	}(idCh, rb)

	// reader go routine
	go func(ch chan uint32, rb *RingBuffer) {
		defer wg.Done()
		start := time.Now()

		for id := range ch {
			//fmt.Printf("remove id=%d \n", id)
			item, err := rb.Remove(id)
			if item != nil {
				myitem, ok := item.(*MyQueItem)
				if ok {
					if id != myitem.Id {
						fmt.Printf("id does not match: %d!=%d\n", id, myitem.Id)
						t.Fatalf("id does not match: %d!=%d\n", id, myitem.Id)
					}
				}
				//fmt.Printf("remove id=%d\n", id)
			} else {
				fmt.Printf("remove id=%d returns nil, err=%s\n", id, err)
			}
			time.Sleep(10 * time.Microsecond)

		}
		elapsed := time.Since(start)
		fmt.Printf("reader took %s\n", elapsed)
	}(idCh, rb)

	wg.Wait()
}

/*
func TestRingSameRateSync(t *testing.T) {
	N := 10000
	idCh := make(chan uint32, N)
	rb := NewSyncRingBuffer(1023)

	var wg sync.WaitGroup
	wg.Add(2)

	// writer go routine
	go func(ch chan uint32, rb *SyncRingBuffer)() {
		defer wg.Done()
		start := time.Now()
		timeout := time.Duration(100*time.Millisecond)
		for i:=0; i<N; i++ {
			item := &MyQueItem{Id: uint32(i), deadline: time.Now().Add(timeout)}
			id, err := rb.EnQueue(item)

			if err !=nil {
				fmt.Printf("enque failed %d, err=%s\n", i, err)
			} else {
				item.Id=id // it's safe to set as the reader hasn't received it
				ch <- uint32(id)
				//fmt.Printf("enque i: %d, id=%d\n", i, id)
			}
		}
		close(ch)
		elapsed := time.Since(start)
		fmt.Printf("writer took %s\n", elapsed)
	} (idCh, rb)

	// reader go routine
	go func(ch chan uint32, rb *SyncRingBuffer)() {
		defer wg.Done()
		start := time.Now()

		for id := range ch {
			//fmt.Printf("remove id=%d \n", id)
			item, err := rb.Remove(id)
			if item != nil {
				myitem, ok := item.(*MyQueItem)
				if ok {
					if id != myitem.Id {
						fmt.Printf("id does not match: %d!=%d\n", id, myitem.Id)
						t.Fatalf("id does not match: %d!=%d\n", id, myitem.Id)
					}
				}
			} else {
				fmt.Printf("remove id=%d returns nil, err=%s\n", id, err)
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("reader took %s\n", elapsed)
	} (idCh, rb)

	wg.Wait()
}

func TestRingSameRateMap(t *testing.T) {
	N := 10000
	idCh := make(chan uint32, N)
	rb := NewMapBuffer(1023)

	var wg sync.WaitGroup
	wg.Add(2)

	// writer go routine
	go func(ch chan uint32, rb *MapBuffer)() {
		defer wg.Done()
		start := time.Now()
		timeout := time.Duration(100*time.Millisecond)
		for i:=0; i<N; i++ {
			item := &MyQueItem{Id: uint32(i), deadline: time.Now().Add(timeout)}
			id, err := rb.EnQueue(item)

			if err !=nil {
				fmt.Printf("enque failed %d, err=%s\n", i, err)
			} else {
				item.Id=id // it's safe to set as the reader hasn't received it
				ch <- uint32(id)
				//fmt.Printf("enque i: %d, id=%d\n", i, id)
			}
		}
		close(ch)
		elapsed := time.Since(start)
		fmt.Printf("writer took %s\n", elapsed)
	} (idCh, rb)

	// reader go routine
	go func(ch chan uint32, rb *MapBuffer)() {
		defer wg.Done()
		start := time.Now()

		for id := range ch {
			//fmt.Printf("remove id=%d \n", id)
			item, err := rb.Remove(id)
			if item != nil {
				myitem, ok := item.(*MyQueItem)
				if ok {
					if id != myitem.Id {
						fmt.Printf("id does not match: %d!=%d\n", id, myitem.Id)
						t.Fatalf("id does not match: %d!=%d\n", id, myitem.Id)
					}
				}
			} else {
				fmt.Printf("remove id=%d returns nil, err=%s\n", id, err)
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("reader took %s\n", elapsed)
	} (idCh, rb)

	wg.Wait()
}
*/

func SendReq(item *MyQueItem, rb *RingBuffer, ch chan uint32) {
	//item.ResetDeadline()
	id, err := rb.EnQueue(item)
	if err != nil {
		fmt.Printf("enque failed %d, err=%s\n", item.OrigId, err)
		TotalDropped++
	} else {
		item.Id = id // it's safe to set as the reader hasn't received it
		ch <- uint32(id)
	}
}

func TestRingSlowConsumer(t *testing.T) {
	N := 10000
	idCh := make(chan uint32, 5*N)
	retryCh := make(chan *MyQueItem, 5*N)
	rb := NewRingBufferWithExtra(1000, 20)
	start := time.Now()

	var wg sync.WaitGroup

	wg.Add(2)
	// writer go routine
	go func(ch chan uint32, rb *RingBuffer) {
		defer wg.Done()

		timeout := time.Duration(10 * time.Millisecond)
		for i := 0; i < N; i++ {
			item := &MyQueItem{Id: uint32(i),
				retryCnt: 0, retryCh: retryCh, OrigId: uint32(i)}
			item.SetQueTimeout(timeout)
			SendReq(item, rb, ch)

			if !rb.IsFull() {
				select {
				case req, ok := <-retryCh:
					if ok {
						SendReq(req, rb, ch)
					}
				default:
				}
			}
			time.Sleep(100 * time.Microsecond)
		}

		i := 0
		for GetTotal() < uint32(N) {
			i++
			if !rb.IsFull() {
				select {
				case req, ok := <-retryCh:
					if ok {
						SendReq(req, rb, ch)

					}
				default:
				}
			}
			//if i%1000 == 0 {
			//	fmt.Printf("i=%d total processed %d, dropped: %d, expired: %d, retried: %d\n",
			//		i, TotalProcessed, TotalDropped, TotalExpired, TotalRetry)
			//}
		}
		close(ch)
		fmt.Printf("writer exit\n")
		fmt.Printf("total processed %d, dropped: %d, expired: %d, retried: %d\n",
			TotalProcessed, TotalDropped, TotalExpired, TotalRetry)
	}(idCh, rb)

	// reader go routine
	go func(ch chan uint32, rb *RingBuffer) {
		defer wg.Done()
		reader_i := 0
		for GetTotal() < uint32(N) {
			reader_i++
			fmt.Printf("read_i=%d\n", reader_i)
			for id := range ch {
				item, err := rb.Remove(id)
				if item != nil {
					myitem, ok := item.(*MyQueItem)
					if ok {
						if id != myitem.Id {
							fmt.Printf("id does not match: %d!=%d\n", id, myitem.Id)
							t.Fatalf("id does not match: %d!=%d\n", id, myitem.Id)
						} else {
							TotalProcessed++
						}
					}
				} else {
					fmt.Printf("remove id=%d returns nil, err=%s\n", id, err)
					//TotalProcessed++
				}
				time.Sleep(150 * time.Microsecond)
			}
		}
		fmt.Printf("reader exit\n")
		fmt.Printf("total processed %d, dropped: %d, expired: %d, retried: %d\n",
			TotalProcessed, TotalDropped, TotalExpired, TotalRetry)
	}(idCh, rb)

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Took %s\n", elapsed)
	fmt.Printf("total processed %d, dropped: %d, expired: %d, retried: %d\n",
		TotalProcessed, TotalDropped, TotalExpired, TotalRetry)
}

func TestOutlier(t *testing.T) {
	N := 5000
	idCh := make(chan uint32, N)
	rb := NewRingBuffer(1000)

	var wg sync.WaitGroup

	wg.Add(2)
	// writer go routine
	go func(ch chan uint32, rb *RingBuffer) {
		defer wg.Done()

		timeout := time.Duration(100 * time.Millisecond)
		for i := 0; i < N; i++ {
			item := &MyQueItem{Id: uint32(i), deadline: time.Now().Add(timeout)}
			item.SetQueTimeout(timeout)
			id, err := rb.EnQueue(item)
			if err != nil {
				fmt.Printf("enque failed %d, err=%s\n", i, err)
			} else {
				item.Id = id // it's safe to set as the reader hasn't received it
				ch <- uint32(id)
				//fmt.Printf("enque i: %d, id=%d\n", i, id)
			}
			time.Sleep(1 * time.Millisecond)
		}
		close(ch)
	}(idCh, rb)

	// reader go routine
	go func(ch chan uint32, rb *RingBuffer) {
		defer wg.Done()
		for id := range ch {
			if id%10 == 0 {
				continue
			}

			item, err := rb.Remove(id)
			if item != nil {
				myitem, ok := item.(*MyQueItem)
				if ok {
					if id != myitem.Id {
						fmt.Printf("id does not match: %d!=%d\n", id, myitem.Id)
						t.Fatalf("id does not match: %d!=%d\n", id, myitem.Id)
					}
				}
			} else {
				fmt.Printf("remove id=%d returns nil, err=%s\n", id, err)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}(idCh, rb)

	wg.Wait()
}

func TestCleanAll(t *testing.T) {
	N := 10
	rb := NewRingBuffer(1000)
	timeout := time.Duration(100 * time.Millisecond)
	for i := 0; i < N; i++ {
		item := &MyQueItem{Id: uint32(i), deadline: time.Now().Add(timeout)}
		id, err := rb.EnQueue(item)
		fmt.Printf("id=%d\n", id)
		if err != nil {
			fmt.Printf("enque failed %d, %d,  err=%s\n", i, id, err)
		}
		time.Sleep(1 * time.Millisecond)
	}
	rb.Remove(4)
	rb.CleanAll()
}
