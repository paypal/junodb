package util

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

//
// Yaping shi
// Dec 20, 2016
//
// lock-free RingBuffer for single producer - single consumer
// - This RingBuffer can be used for communication between maximum two go routines without locking
//
// - producer can only update tail
// - conusmer can only update head
//
//
const (
	EXTRA_PERCENT = 20
)

type QueItem interface {
	OnCleanup()
	OnExpiration()
	Deadline() (deadline time.Time)
	ResetDeadline()
	SetId(id uint32)
	GetId() uint32
	SetInUse(flag bool)
	SetQueTimeout(t time.Duration)
	GetQueTimeout() (t time.Duration)
	IsInUse() bool
}

type QueItemBase struct {
	id       uint32
	flag     uint32
	timeout  time.Duration
	deadline time.Time
}

func (q *QueItemBase) SetId(id uint32) {
	q.id = id
}

func (q *QueItemBase) GetId() uint32 {
	return q.id
}

func (q *QueItemBase) SetQueTimeout(t time.Duration) {
	q.timeout = t
}

func (q *QueItemBase) GetQueTimeout() time.Duration {
	return q.timeout
}

func (q *QueItemBase) Deadline() (deadline time.Time) {
	return q.deadline
}

func (q *QueItemBase) SetDeadline(d time.Time) {
    q.deadline = d
}

func (q *QueItemBase) ResetDeadline() {
	if q.GetQueTimeout() != 0 {
		q.deadline = time.Now().Add(q.GetQueTimeout())
	}
}

func (q *QueItemBase) SetInUse(flag bool) {
	if flag {
		atomic.StoreUint32(&q.flag, 1)
	} else {
		atomic.StoreUint32(&q.flag, 0)
	}
}

func (q *QueItemBase) IsInUse() bool {
	flag := atomic.LoadUint32(&q.flag)
	return flag != 0
}

type RingBuffer struct {
	head     uint32 // Atomic access, updated by reader, used
	tail     uint32 // Atomic access, updated by writer, unused
	capacity uint32 // qsize + 10% extra + 1
	buf      []QueItem
	seqId    uint32
	qsize    uint32 // qsize exposed to user
	extra    uint32
	cursize  int32
}

func NewRingBuffer(size uint32) *RingBuffer {
	return NewRingBufferWithExtra(size, 20)
}

func NewRingBufferWithExtra(size uint32, extra_pct uint32) *RingBuffer {
	extra := size * extra_pct / 100
	capacity := size + extra + 1
	rb := &RingBuffer{
		head:     0,
		tail:     0,
		capacity: capacity,
		buf:      make([]QueItem, capacity, capacity),
		seqId:    0,
		qsize:    size, // max queue size
		extra:    extra,
		cursize:  0,
	}

	return rb
}

func (rb *RingBuffer) EnQueue(item QueItem) (id uint32, err error) {
	item.ResetDeadline()

	curTail := atomic.LoadUint32(&rb.tail)
	cursize := atomic.LoadInt32(&rb.cursize)
	nextTail := (curTail + 1) % rb.capacity
	if (nextTail != atomic.LoadUint32(&rb.head)) && (cursize < int32(rb.qsize)) {
		item.SetId(rb.seqId)
		rb.buf[curTail] = item
		atomic.AddInt32(&rb.cursize, 1)
		atomic.StoreUint32(&rb.tail, nextTail)
		id = rb.seqId
		atomic.AddUint32(&rb.seqId, 1)
		return id, nil
	}
	return 0, errors.New("Queue full")
}

func (rb *RingBuffer) DeQueue() (item QueItem, err error) {
	curHead := atomic.LoadUint32(&rb.head) // 1
	if curHead == atomic.LoadUint32(&rb.tail) {
		return nil, errors.New("Queue empty") // empty queue
	}

	item = rb.buf[curHead]
	if item != nil {
		rb.buf[curHead] = nil
		atomic.AddInt32(&rb.cursize, -1)
	}
	atomic.StoreUint32(&rb.head, (curHead+1)%rb.capacity)
	return item, nil
}

func (rb *RingBuffer) Remove(id uint32) (item QueItem, err error) {
	curHead := atomic.LoadUint32(&rb.head)
	curTail := atomic.LoadUint32(&rb.tail)
	if curHead == curTail {
		return nil, errors.New("Queue empty") // empty queue
	}

	pos := id % rb.capacity
	if (id+rb.capacity <= atomic.LoadUint32(&rb.seqId)) ||
		(curHead < curTail && (pos < curHead || curTail <= pos)) ||
		(curTail < curHead && (curTail <= pos && pos < curHead)) {
		rb.CleanUp()
		return nil, errors.New("Id Out of valid Range") //
	}

	// remove
	item = rb.buf[pos]
	if item != nil {
		rb.buf[pos] = nil
		atomic.AddInt32(&rb.cursize, -1)
	}

	// move head if needed
	if pos == rb.head {
		atomic.StoreUint32(&rb.head, (curHead+1)%rb.capacity)
	}
	rb.CleanUp()

	if item.GetId() != id {
		// this should never happen because of the above check
		return nil, errors.New("Id does not match")
	}
	return item, nil
}

func (rb *RingBuffer) GetSize() uint32 {
	return uint32(atomic.LoadInt32(&rb.cursize))
}

func (rb *RingBuffer) IsEmpty() bool {
	return atomic.LoadUint32(&rb.head) == atomic.LoadUint32(&rb.tail)
}

func (rb *RingBuffer) IsFull() bool {
	idx := atomic.LoadUint32(&rb.tail)
	nextTail := (idx + 1) % rb.capacity
	cursize := atomic.LoadInt32(&rb.cursize)
	return nextTail == atomic.LoadUint32(&rb.head) || cursize >= int32(rb.qsize)
}

// only called by reader (head)
func (rb *RingBuffer) updateHead() {
	curTail := atomic.LoadUint32(&rb.tail)
	for curTail != rb.head && rb.buf[rb.head] == nil {
		atomic.StoreUint32(&rb.head, (rb.head+1)%rb.capacity)
	}
}

// only called by reader (head)
// less than size * 10% available for writer
func (rb *RingBuffer) overSize() bool {
	curTail := atomic.LoadUint32(&rb.tail)
	if rb.head <= curTail { // not wrapped case
		if rb.capacity-(curTail-rb.head) < rb.extra {
			return true
		}
	} else { // wrapped, tail < head
		if rb.head-curTail < rb.extra {
			return true
		}
	}
	return false
}

func (rb *RingBuffer) topExpired(now time.Time) bool {
	curHead := atomic.LoadUint32(&rb.head) // 1
	if curHead == atomic.LoadUint32(&rb.tail) {
		return false
	}

	item := rb.buf[curHead]
	if item == nil {
		return true
	}

	return ((!item.IsInUse()) && item.Deadline().Before(now))
}

// only called by reader (head)
// two cases:
// if full, clean up size/10, dequeue if not nil
// if head is free (nil), move head till not nil
func (rb *RingBuffer) CleanUp() bool {
	now := time.Now()
	//for rb.overSize() || rb.topExpired(now) {
	for rb.topExpired(now) {
		if rb.buf[rb.head] != nil {
			item, _ := rb.DeQueue()
			if item != nil {
				item.OnExpiration()
			}
		} else {
			rb.updateHead()
		}
	}
	rb.updateHead()
	return true
}

func (rb *RingBuffer) WriteStats(w io.Writer) {
	fmt.Fprintf(w, "head:%d, tail:%d, capacity:%d, seqId:%d, qsize:%d, extra:%d", rb.head, rb.tail, rb.capacity, rb.seqId, rb.qsize, rb.extra)
}

// drain everything in the ringbuffer
func (rb *RingBuffer) CleanAll() {
	for !rb.IsEmpty() {
		if rb.buf[rb.head] != nil {
			item, _ := rb.DeQueue()
			if item != nil {
				item.OnCleanup()
			}
		}
		rb.updateHead()
	}
}
