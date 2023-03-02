package proto

// Compoment Tag
const (
	_               = iota
	kCompTagPayload = iota
	kCompTagMeta
)

type metaComponentT struct {
	metaComponentHeaderT
}

type payloadComponentT struct {
	payloadComponentHeaderT
	key           []byte
	namespace     []byte
	payload       Payload
	szCompPadding uint8
}
