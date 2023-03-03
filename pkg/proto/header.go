package proto

import (
	"fmt"
)

type (
	opMsgFlagT uint8

	messageHeaderT struct {
		magic    uint16
		version  uint8
		typeFlag messageTypeFlagT
		msgSize  uint32
		opaque   uint32
	}
	operationalHeaderT struct {
		opCode          OpCode
		flags           opMsgFlagT
		shardIdOrStatus shardIdOrStatusT
	}

	componentHeaderT struct {
		szComp        uint32
		tagComp       uint8
		szCompPadding uint8
	}

	metaComponentHeaderT struct {
		componentHeaderT
		numFields       uint8
		szHeaderPadding uint8
	}

	payloadComponentHeaderT struct {
		componentHeaderT
		szNamespace uint8
		szKey       uint16
		szValue     uint32
	}
)

func (f opMsgFlagT) IsFlagReplicationSet() bool {
	return (f & 1) != 0
}

func (f *opMsgFlagT) SetReplicationFlag() {
	(*f) |= 1
}

func (f opMsgFlagT) IsFlagDeleteReplicationSet() bool {
	return (f & 0x5) == 0x5
}

func (f *opMsgFlagT) SetDeleteReplicationFlag() {
	(*f) |= 0x5
}

func (f opMsgFlagT) IsFlagMarkDeleteSet() bool {
	return (f & 0x2) != 0
}

func (f *opMsgFlagT) SetMarkDeleteFlag() {
	(*f) |= 0x2
}

func (h *messageHeaderT) reset() {
	h.magic = kMessageMagic
	h.version = kCurrentVersion
	h.typeFlag = 0
	h.msgSize = 0
	h.opaque = 0
}

func (h *messageHeaderT) SetAsResponse() {
	h.typeFlag.setAsResponse()
}

func (h *messageHeaderT) IsSupported() bool {
	if h.magic == kMessageMagic && h.version == kCurrentVersion {
		if h.typeFlag.getMessageType() == kOperationalMessageType {
			return true
		}
	}
	return false
}

func (h *messageHeaderT) GetMsgSize() uint32 {
	return h.msgSize
}

func (h *messageHeaderT) GetOpaque() uint32 {
	return h.opaque
}

func (h *messageHeaderT) SetOpaque(opaque uint32) {
	h.opaque = opaque
}

func (h *messageHeaderT) getMsgType() uint8 {
	return h.typeFlag.getMessageType()
}

func (h *messageHeaderT) PrettyPrint() {
	fmt.Println("\nHeader:")
	fmt.Printf("  Magic\t\t:%#X\n", h.magic)
	fmt.Printf("  Version\t:%d\n", h.version)
	fmt.Printf("  MessageType\t:%d\n", h.getMsgType())
	fmt.Printf("  MessageSize\t:%d\n", h.msgSize)
	fmt.Printf("  OPaque\t:%#X\n", h.opaque)
}
