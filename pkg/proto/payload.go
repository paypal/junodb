package proto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/util"

	"github.com/golang/snappy"
)

const (
	PayloadTypeClear = iota
	PayloadTypeEncryptedByClient
	PayloadTypeEncryptedByProxy
	PayloadTypecompressedByClient
)

const (
	SnappyCompression string = "Snappy"
)

type (
	PayloadType uint8

	Payload struct {
		tag  PayloadType
		data []byte
	}
)

var (
	ErrClientKeyStoreNotAvailable = fmt.Errorf("client encryption key store not avaliable")
	ErrServerKeyStoreNotAvailable = fmt.Errorf("server encryption key store not avaliable")
	ErrUnsupportedPayloadType     = fmt.Errorf("unsupported payload type")
	ErrPayloadNoEncryptionHeader  = fmt.Errorf("no encryption header")
	ErrUnsupportedCompressionType = fmt.Errorf("unsupported compression type")
)

func (t PayloadType) String() string {
	switch t {
	case PayloadTypeClear:
		return "clear payload"
	case PayloadTypeEncryptedByClient:
		return "encrypted by client"
	case PayloadTypeEncryptedByProxy:
		return "encrypted by proxy"
	default:
		return fmt.Sprintf("unsupported payload type: %d", t)
	}
}

func (p *Payload) GetPayloadType() PayloadType {
	return p.tag
}

func (p *Payload) GetData() []byte {
	return p.data
}

func (p *Payload) GetValueLength() uint32 {
	return uint32(len(p.data))
}

func (p *Payload) GetLength() uint32 {
	szPayload := len(p.data)
	if szPayload == 0 {
		return 0
	}
	return uint32(1 + szPayload)
}

func (p *Payload) SetPayload(tag PayloadType, data []byte) {
	p.tag = tag
	p.data = data
}

func (p *Payload) SetWithClearValue(value []byte) {
	p.tag = PayloadTypeClear
	p.data = value
}

///TODO
func (p *Payload) GetClearValue() (value []byte, err error) {
	if p.GetLength() == 0 {
		return
	}
	pl := *p
	if pl.tag == PayloadTypecompressedByClient {
		//Get compression type
		compTypeSize := uint8(pl.data[0])
		if len(pl.data[1:]) > int(compTypeSize) {
			compType := string(pl.data[1 : compTypeSize+1])
			if compType == SnappyCompression {
				err = nil
				if value, err = snappy.Decode(nil, pl.data[compTypeSize+1:]); err != nil {
					glog.Error("Error while uncompressing :", err)
				}
			} else {
				err = ErrUnsupportedCompressionType
			}
		}
	} else {
		if err = pl.Decrypt(); err == nil {
			value = pl.data
		}
	}
	return
}

func (p *Payload) Encrypt(pType PayloadType) (err error) {
	if p.GetLength() == 0 {
		return nil
	}
	if p.tag == PayloadTypeClear {
		var ks IEncryptionKeyStore
		if ks, err = getKeyStore(pType); err == nil {
			var key []byte
			var version uint32
			var block cipher.Block
			var gcm cipher.AEAD
			if key, version, err = ks.GetEncryptionKey(); err == nil {
				if block, err = aes.NewCipher(key); err == nil {

					if gcm, err = cipher.NewGCM(block); err == nil {
						var nonce [12]byte
						if _, err = io.ReadFull(rand.Reader, nonce[:]); err == nil {

							encryptedData := gcm.Seal(nil, nonce[:], p.GetData(), nil)
							newData := make([]byte, 4+12+len(encryptedData))
							binary.BigEndian.PutUint32(newData[:4], version)
							copy(newData[4:4+12], nonce[:])
							copy(newData[4+12:], encryptedData)
							p.tag = pType
							p.data = newData
						}
					}
				}
			}
		}
	} else {
		err = fmt.Errorf("already encrypted")
	}

	return
}

//	if p.GetPayloadType() == proto.PayloadTypeClear {
//		var key []byte
//		var version uint32
//		var block cipher.Block
//		var gcm cipher.AEAD
//
//		ks := &testKeyStoreT{}
//
//		if key, version, err = ks.GetEncryptionKey(); err == nil {
//			fmt.Printf("key: %X version: %d", key, version)
//			if block, err = aes.NewCipher(key); err == nil {
//
//				if gcm, err = cipher.NewGCM(block); err == nil {
//					var nonce [12]byte
//					if _, err = io.ReadFull(rand.Reader, nonce[:]); err == nil {
//
//						encryptedData := gcm.Seal(nil, nonce[:], p.GetData(), nil)
//						newData := make([]byte, 4+12+len(encryptedData))
//						binary.BigEndian.PutUint32(newData[:4], version)
//						copy(newData[4:4+12], nonce[:])
//						copy(newData[4+12:], encryptedData)
//						p.SetPayload(proto.PayloadTypeEncryptedByProxy, newData)
//					}
//				}
//			}
//		}
//		//		util.HexDump(p.GetData())
//	} else {
//		err = fmt.Errorf("tag not 0")
//	}

func (p *Payload) Decrypt() (err error) {
	if p.GetLength() == 0 || p.tag == PayloadTypeClear {
		return nil
	}

	var ks IEncryptionKeyStore

	if ks, err = getKeyStore(p.tag); err == nil {

		data := p.GetData()
		if len(data) < 4+12+16 {
			return ErrPayloadNoEncryptionHeader
		}
		version := binary.BigEndian.Uint32(data[:4])
		var block cipher.Block
		var gcm cipher.AEAD
		var key []byte
		if key, err = ks.GetDecryptionKey(version); err == nil {
			if block, err = aes.NewCipher(key); err == nil {
				if gcm, err = cipher.NewGCM(block); err == nil {
					nonce := data[4 : 4+12]
					var newData []byte
					if newData, err = gcm.Open(nil, nonce, data[16:], nil); err == nil {
						p.SetPayload(PayloadTypeClear, newData)
					}
				}
			}
		}

	}

	return
}

func (p *Payload) Equal(other *Payload) bool {
	if other == nil {
		return false
	}
	if p.tag != other.tag { ///*********************************************************
		return false
	}
	return bytes.Equal(p.data, other.data)
}

func (p *Payload) EncodeToBuffer(buffer *bytes.Buffer) {
	if p.GetLength() != 0 {
		buffer.WriteByte(byte(p.tag))
		buffer.Write(p.data)
	}
}

func (p *Payload) Clear() {
	p.tag = PayloadTypeClear
	p.data = nil
}

func (p *Payload) Set(payload *Payload) {
	if payload != nil {
		*p = *payload
	} else {
		p.Clear()
	}
}

func (p *Payload) Clone() (PayloadType, []byte) {
	if p.data == nil {
		return p.tag, p.data
	}

	buf := make([]byte, len(p.data))
	copy(buf, p.data)
	p.data = buf
	return p.tag, p.data
}

func (p *Payload) Decode(raw []byte, copyData bool) {
	szRaw := len(raw)

	if szRaw > 1 {
		p.tag = PayloadType(raw[0])
		if copyData {
			p.data = make([]byte, szRaw-1)
			copy(p.data, raw[1:szRaw])
		} else {
			p.data = raw[1:]
		}
	} else {
		p.Clear()
		if szRaw == 1 {
			glog.Warning("size of payload is 1")
		}
	}
}

func (p *Payload) PrettyPrint(w io.Writer) {
	szValue := len(p.data)

	fmt.Fprintf(w, "Payload         %X: ", p.tag)
	if szValue == 0 {
		fmt.Fprint(w, "[]\n")
	} else if szValue < 24 {
		fmt.Fprintf(w, "%s\n", util.ToPrintableAndHexString(p.data))
	} else {
		fmt.Fprintf(w, "(first 24 bytes) %s\n", util.ToPrintableAndHexString(p.data[:24]))
	}
}
