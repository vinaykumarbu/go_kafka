package types

import (
	"encoding/binary"
)

var ErrorCodes = map[string]ResponseError{
	"UNSUPPORTED_VERSION":        {35, "UNSUPPORTED_VERSION", false},
	"UNKNOWN_TOPIC_OR_PARTITION": {3, "UNKNOWN_TOPIC_OR_PARTITION", true},
	"UNKNOWN_TOPIC":              {100, "UNKNOWN_TOPIC", true},
}

type RequestHeader struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          NullableString
	TAG_BUFFER        byte
}

type APIInfo struct {
	Name       string
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type BaseResponse struct {
	CorrelationId int32
	TAG_BUFFER    byte
}

type RequestData interface{}
type ResponseData interface{}

func (r *BaseResponse) Serialize(apiRes []byte) []byte {
	bytes := make([]byte, 8)

	MessageSize := 4 + len(apiRes)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(MessageSize))
	binary.BigEndian.PutUint32(bytes[4:8], uint32(r.CorrelationId))
	// bytes[8] = r.TAG_BUFFER

	finalBytes := append(bytes, apiRes...)

	return finalBytes
}

type ResponseError struct {
	Code      int16
	Name      string
	Retriable bool
}

// RequestMessage wraps a parsed header plus a typed request body.
type RequestMessage struct {
	Header RequestHeader
	Body   interface{}
}
