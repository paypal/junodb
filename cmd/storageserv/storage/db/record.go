/*
Package db implements Juno storage interfaces with gorocksdb.

Record Encoding Format

   Offset | Field                           | Size
  --------+---------------------------------+---------------
        0 | encoding version                | 1 byte
  --------+---------------------------------+---------------
        1 | flag                            | 1 byte
  --------+---------------------------------+---------------
        2 | reserved                        | 2 bytes
  --------+---------------------------------+---------------
        4 | expiration time                 | 4 bytes
  --------+---------------------------------+---------------
        8 | version                         | 4 bytes
  --------+---------------------------------+---------------
       12 | creation time                   | 4 bytes
  --------+---------------------------------+---------------
       16 | last modification time          | 8 bytes
  --------+---------------------------------+---------------
       24 | request Id of the last modifier | 16 bytes
  --------+---------------------------------+---------------
       40 | request Id of the originator    | 16 bytes
  --------+---------------------------------+---------------
       56 | encapsulating payload           | ...

  Record Flag
    bit |           0|           1|           2|           3|           4|           5|           6|           7
  ------+------------+------------+------------+------------+------------+------------+------------+------------+
        | MarkDelete |


Storage Key Format

  ----------------------------+----------- +--------
    namespace length (1 byte) |  namespace |  key
  ----------------------------+----------- +--------
*/
package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/proto"
	"juno/pkg/shard"
	"juno/pkg/util"
)

const (
	kEncVersion byte = 0x01

	kSzEncVersion            = 1
	kSzFlag                  = 1
	kSzReserved              = 2
	kSzExpirationTime        = 4
	kSzVersion               = 4
	kSzCreationTime          = 4
	kSzLastModificationTime  = 8
	kSzLastModifierRequestId = 16
	kSzOriginatorRequestId   = 16
	kSzHeader                = 56

	kOffEncodingVersion       = 0
	kOffFlag                  = kOffEncodingVersion + kSzEncVersion
	kOffReserved              = kOffFlag + kSzFlag
	kOffExpirationTime        = kOffReserved + kSzReserved
	kOffVersion               = kOffExpirationTime + kSzExpirationTime
	kOffCreationTime          = kOffVersion + kSzVersion
	kOffLastModificationTime  = kOffCreationTime + kSzCreationTime
	kOffLastModifierRequestId = kOffLastModificationTime + kSzLastModificationTime
	kOffOriginatorRequestId   = kOffLastModifierRequestId + kSzLastModifierRequestId
)

const (
	MAX_NAMESPACE_LENGTH int = 255
)

type (
	compactionFilter struct {
		shardFilter *ShardFilter
	}
	recordFlagT byte

	valueHolderI interface {
		Data() []byte
		Size() int
		Free()
	}

	RecordHeader struct {
		Version              uint32
		CreationTime         uint32
		LastModificationTime uint64
		ExpirationTime       uint32

		OriginatorRequestId proto.RequestId
		RequestId           proto.RequestId
		flag                recordFlagT
	}
	Record struct {
		RecordHeader
		Payload proto.Payload
		holder  valueHolderI
	}
)

func (m *compactionFilter) Name() string {
	return "JunoCompactionFilter\x00"
}

func (m *compactionFilter) Filter(level int, key, value []byte) (expired bool, newCalue []byte) {
	if len(value) < kOffExpirationTime+kSzExpirationTime {
		glog.Warningf("invalid value length. Key:%X len=%d. return as expired.", key, len(value))
		return true, nil
	}

	expirationTime := int64(binary.BigEndian.Uint32(value[kOffExpirationTime : kOffExpirationTime+kSzExpirationTime]))
	expired = expirationTime < time.Now().Unix()

	if expired {
		if glog.LOG_VERBOSE {
			glog.Verbosef("Key:%X is expired.", key)
		}
		return true, nil
	}

	if (m.shardFilter != nil) && m.shardFilter.matchShardNum(key) {
		return true, nil
	}

	return false, nil
}

func (f recordFlagT) isMarkedDelete() bool {
	return (f & 0x1) != 0
}

func (f *recordFlagT) markDelete() {
	(*f) |= 0x1
}

func (f *recordFlagT) clearMarkDelete() {
	(*f) &^= 0x1
}

func (recId RecordID) Key() uint32 {
	return util.Murmur3Hash(recId)
}

func (recId RecordID) String() string {
	return util.ToHexString(recId[:])
}

func (b *Record) String() string {
	return fmt.Sprintf("{ReqId: %X, ExpTime:%d, CreTime:%d, Ver:%d, Vlen:%d}",
		b.RequestId, b.ExpirationTime, b.CreationTime, b.Version, b.Payload.GetLength())
}

func (b *Record) ResetRecord() { //changed from Reset() to ResetRecord() to make it easy to seach for the callers
	if b.holder != nil {
		onFreeValue(b.holder)
		b.holder.Free()
		b.holder = nil
	}
	*b = Record{}
}

func (rec *Record) EncodingSize() int {
	return kSzHeader + int(rec.Payload.GetLength())
}

func (rec *Record) EncodeToBuffer(buffer *bytes.Buffer) error {
	var buf [kSzHeader]byte
	buf[0] = kEncVersion
	buf[1] = byte(rec.flag)
	buf[2] = 0
	buf[3] = 0

	//	if !rec.OriginatorRequestId.IsSet() {
	//		panic("")
	//	}
	if rec.LastModificationTime == 0 {
		glog.Warning("Last Modifition time is not set")
		rec.LastModificationTime = uint64(time.Now().UnixNano())
	}
	binary.BigEndian.PutUint32(
		buf[kOffExpirationTime:kOffExpirationTime+kSzExpirationTime],
		rec.ExpirationTime)
	binary.BigEndian.PutUint32(
		buf[kOffVersion:kOffVersion+kSzVersion],
		rec.Version)
	binary.BigEndian.PutUint32(
		buf[kOffCreationTime:kOffCreationTime+kSzCreationTime],
		rec.CreationTime)
	binary.BigEndian.PutUint64(
		buf[kOffLastModificationTime:kOffLastModificationTime+kSzLastModificationTime],
		rec.LastModificationTime)
	copy(buf[kOffLastModifierRequestId:kOffLastModifierRequestId+kSzLastModifierRequestId],
		rec.RequestId.Bytes())
	copy(buf[kOffOriginatorRequestId:kOffOriginatorRequestId+kSzOriginatorRequestId],
		rec.OriginatorRequestId.Bytes())
	buffer.Write(buf[:])

	rec.Payload.EncodeToBuffer(buffer)

	return nil
}

///TODO validation. the slices
func (rec *Record) Decode(data []byte) error {
	if data == nil || len(data) < kSzHeader {
		return errors.New("Decoding error: empty")
	}
	// TODO add more validation here !
	szData := len(data)
	encodingVersion := data[kOffEncodingVersion]
	if encodingVersion != kEncVersion {
		return fmt.Errorf("unsupported encoding version %d", encodingVersion)
	}
	rec.flag = recordFlagT(data[kOffFlag])
	rec.ExpirationTime = binary.BigEndian.Uint32(
		data[kOffExpirationTime : kOffExpirationTime+kSzExpirationTime])
	rec.Version = binary.BigEndian.Uint32(
		data[kOffVersion : kOffVersion+kSzVersion])
	rec.CreationTime = binary.BigEndian.Uint32(
		data[kOffCreationTime : kOffCreationTime+kSzCreationTime])
	rec.LastModificationTime = binary.BigEndian.Uint64(
		data[kOffLastModificationTime : kOffLastModificationTime+kSzLastModificationTime])
	rec.RequestId.SetFromBytes(data[kOffLastModifierRequestId : kOffLastModifierRequestId+kSzLastModifierRequestId])
	rec.OriginatorRequestId.SetFromBytes(data[kOffOriginatorRequestId : kOffOriginatorRequestId+kSzOriginatorRequestId])
	rec.Payload.Decode(data[kSzHeader:szData], false)
	if glog.LOG_VERBOSE {
		b := logging.NewKVBufferForLog()
		b.AddRequestID(rec.RequestId).AddVersion(rec.Version).AddExpirationTime(rec.ExpirationTime).
			AddCreationTime(rec.CreationTime).AddOriginator(rec.OriginatorRequestId)
		glog.Verbosef("Record: %v", b)
	}
	//	if !rec.OriginatorRequestId.IsSet() {
	//		panic(rec.OriginatorRequestId.String())
	//	}
	return nil
}

func (rec *Record) DecodeFrom(holder valueHolderI) error {
	data := holder.Data()
	rec.holder = holder
	onAllocValue(holder)

	if data == nil || len(data) < kSzHeader {
		return errors.New("Decoding error: empty")
	}

	// TODO add more validation here !
	szData := len(data)
	encodingVersion := data[kOffEncodingVersion]
	if encodingVersion != kEncVersion {
		return fmt.Errorf("unsupported encoding version %d", encodingVersion)
	}
	rec.flag = recordFlagT(data[kOffFlag])
	rec.ExpirationTime = binary.BigEndian.Uint32(
		data[kOffExpirationTime : kOffExpirationTime+kSzExpirationTime])
	rec.Version = binary.BigEndian.Uint32(
		data[kOffVersion : kOffVersion+kSzVersion])
	rec.CreationTime = binary.BigEndian.Uint32(
		data[kOffCreationTime : kOffCreationTime+kSzCreationTime])
	rec.LastModificationTime = binary.BigEndian.Uint64(
		data[kOffLastModificationTime : kOffLastModificationTime+kSzLastModificationTime])
	rec.RequestId.SetFromBytes(data[kOffLastModifierRequestId : kOffLastModifierRequestId+kSzLastModifierRequestId])
	rec.OriginatorRequestId.SetFromBytes(data[kOffOriginatorRequestId : kOffOriginatorRequestId+kSzOriginatorRequestId])
	rec.Payload.Decode(data[kSzHeader:szData], false)
	if glog.LOG_VERBOSE {
		b := logging.NewKVBufferForLog()
		b.AddRequestID(rec.RequestId).AddVersion(rec.Version).AddExpirationTime(rec.ExpirationTime).
			AddCreationTime(rec.CreationTime).AddOriginator(rec.OriginatorRequestId)
		glog.Verbosef("Record: %v", b)
	}
	//	if !rec.OriginatorRequestId.IsSet() {
	//		panic(rec.OriginatorRequestId.String())
	//	}
	return nil
}

func (rec *Record) IsExpired() (expired bool) {
	expired = int64(rec.ExpirationTime) < time.Now().Unix()
	return
}

func (rec *Record) EncodeRedistMsg(shardId shard.ID, ns []byte, key []byte, row *proto.RawMessage) (err error) {
	msg := &proto.OperationalMessage{}
	ttl := rec.ExpirationTime - uint32(time.Now().Unix())
	msg.SetRequest(proto.OpCodeClone, key, ns, &rec.Payload, ttl)
	msg.SetShardId(shardId.Uint16())
	msg.SetRequestID(rec.RequestId)
	msg.SetCreationTime(rec.CreationTime)
	msg.SetLastModificationTime(rec.LastModificationTime)
	msg.SetVersion(rec.Version)
	msg.SetExpirationTime(rec.ExpirationTime)
	msg.SetOriginatorRequestID(rec.OriginatorRequestId)
	return msg.Encode(row)
}

func (rec *Record) IsMarkedDelete() bool {
	return rec.flag.isMarkedDelete()
}

func (rec *Record) ClearMarkedDelete() {
	rec.flag.clearMarkDelete()
}

func (rec *Record) MarkDelete() {
	rec.flag.markDelete()
	///TODO clear value, bump version and adjust lifetime?
}

func (rec *Record) PrettyPrint(w io.Writer) {
	if rec.IsMarkedDelete() {
		fmt.Fprintln(w, "MarkedDelete")
	}
	fmt.Fprintf(w, "Version               : %d\n", rec.Version)
	fmt.Fprintf(w, "Creation Time         : %d\n", rec.CreationTime)
	fmt.Fprintf(w, "Last Modification Time: %d\n", rec.LastModificationTime)
	fmt.Fprintf(w, "Expiration Time       : %d\n", rec.ExpirationTime)
	fmt.Fprintf(w, "Originator Request Id : %s\n", rec.OriginatorRequestId.String())
	fmt.Fprintf(w, "Request Id            : %s\n", rec.RequestId.String())

	rec.Payload.PrettyPrint(w)
}
