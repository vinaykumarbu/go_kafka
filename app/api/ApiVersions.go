package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/types"
)

type APIVersionV4Request struct {
}

type APIVersionV4Response struct {
	ErrorCode      int16
	NumOfApiKeys   int8
	Apis           []types.APIInfo
	ThrottleTimeMs int32
	TaggedFields2  byte
}

var SupportedAPIs = map[int16]types.APIInfo{
	18: {
		Name:       "ApiVersions",
		ApiKey:     18,
		MinVersion: 0,
		MaxVersion: 4,
	},
	75: {
		Name:       "DescribeTopicPartitions",
		ApiKey:     75,
		MinVersion: 0,
		MaxVersion: 0,
	},
	1: {
		Name:       "Fetch",
		ApiKey:     1,
		MinVersion: 0,
		MaxVersion: 16,
	},
}

func ParseApiVersionV4Request(body []byte) (APIVersionV4Request, error) {
	return APIVersionV4Request{}, nil
}

func GenerateApiVersionV4Response(request types.RequestHeader) ([]byte, error) {
	requestedApi, ok := SupportedAPIs[request.RequestApiKey]
	if !ok {
		requestedApi = SupportedAPIs[18]
	}

	var ErrorCode int16 = 0

	if request.RequestApiVersion < requestedApi.MinVersion || request.RequestApiVersion > requestedApi.MaxVersion {
		ErrorCode = int16(types.ErrorCodes["UNSUPPORTED_VERSION"].Code)
	}

	apis := []types.APIInfo{
		SupportedAPIs[18],
	}

	for key, api := range SupportedAPIs {
		if key != 18 {
			apis = append(apis, api)
		}
	}

	res := APIVersionV4Response{
		ErrorCode:      ErrorCode,
		NumOfApiKeys:   int8(len(apis) + 1),
		Apis:           apis,
		ThrottleTimeMs: 0,
		TaggedFields2:  0,
	}

	resBytes := res.Serialize()

	return resBytes, nil
}

func (r *APIVersionV4Response) Serialize() []byte {
	size := 2 + 1 + (7 * len(r.Apis)) + 4 + 1

	bytes := make([]byte, size)

	offset := 0

	binary.BigEndian.PutUint16(bytes[offset:], uint16(r.ErrorCode))
	offset += 2

	bytes[offset] = byte(r.NumOfApiKeys)
	offset++

	for _, api := range r.Apis {
		binary.BigEndian.PutUint16(bytes[offset:], uint16(api.ApiKey))
		binary.BigEndian.PutUint16(bytes[offset+2:], uint16(api.MinVersion))
		binary.BigEndian.PutUint16(bytes[offset+4:], uint16(api.MaxVersion))
		bytes[offset+6] = 0
		offset += 7
	}

	binary.BigEndian.PutUint32(bytes[offset:], uint32(r.ThrottleTimeMs))
	offset += 4

	bytes[offset] = r.TaggedFields2

	return bytes
}
