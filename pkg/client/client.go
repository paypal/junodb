/*
package client implements Juno client API.

possible returned error if client successfully received the response from Proxy

  Create
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrUniqueKeyViolation
  * ErrRecordLocked
  * ErrWriteFailure

  Get
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrNoKey
  * ErrTTLExtendFailure

  Update
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrRecordLocked
  * ErrConditionViolation
  * ErrWriteFailure

  Set
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrRecordLocked
  * ErrWriteFailure

  Destroy
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrRecordLocked
  * ErrWriteFailure

*/
package client

import (
	"io"
)

type IContext interface {
	GetVersion() uint32
	GetCreationTime() uint32
	GetTimeToLive() uint32
	PrettyPrint(w io.Writer)
}

///TODO check API input arguments

type IClient interface {
	Create(key []byte, value []byte, opts ...IOption) (IContext, error)
	Get(key []byte, opts ...IOption) ([]byte, IContext, error)
	Update(key []byte, value []byte, opts ...IOption) (IContext, error)
	Set(key []byte, value []byte, opts ...IOption) (IContext, error)
	Destroy(key []byte, opts ...IOption) (err error)
	UDFGet(key []byte, fname []byte, params []byte, opts ...IOption) ([]byte, IContext, error)
	UDFSet(key []byte, fname []byte, params []byte, opts ...IOption) (IContext, error)
}

//type IResult interface {
//	Get()
//	GetWithTimeout()
//	Poll()
//}
//type IValueResult interface {
//	IResult
//}
//
//type AsyncClient interface {
//	Create(key []byte, value []byte, opts ...IOption) IResult
//	Update(key []byte, value []byte, opts ...IOption) IResult
//	Set(key []byte, value []byte, opts ...IOption) IResult
//	Get(key []byte, value []byte, opts ...IOption) IResult
//	Destroy(key []byte) IResult
//}
