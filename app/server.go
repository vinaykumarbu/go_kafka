package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/api"
	"github.com/codecrafters-io/kafka-starter-go/app/types"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		panic(fmt.Sprintf("Failed to bind to port 9092: %v", err))
	}
	fmt.Println("Listening on: 9092")
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(fmt.Sprintf("Error accepting connection: %v", err))
		}

		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	for {
		req, err := readRequest(conn)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client closed the connection")
				return
			}
			panic(fmt.Sprintf("Error reading from connection: %v", err))
		}

		responseBytes, err := generateResponse(*req)
		if err != nil {
			panic(fmt.Sprintf("Error generating response: %v", err))
		}

		// fmt.Println("Response Bytes: ", responseBytes)

		_, err = conn.Write(responseBytes)
		if err != nil {
			if err == net.ErrClosed || err.Error() == "write: broken pipe" {
				fmt.Println("Client closed the connection before writing response")
				return
			}
			panic(fmt.Sprintf("Error writing to connection: %v", err))
		}
	}
}

func readRequest(conn net.Conn) (*types.RequestMessage, error) {
	var data = make([]byte, 1024)
	n, err := conn.Read(data)
	if err != nil {
		return &types.RequestMessage{}, err
	}

	if n < 12 {
		return &types.RequestMessage{}, fmt.Errorf("types.RequestHeader too short")
	}

	header, offset, err := readHeader(data)
	if err != nil {
		return nil, err
	}

	var body interface{}

	switch header.RequestApiKey {
	case 18:
		body, err = api.ParseApiVersionV4Request(data[offset:])
		if err != nil {
			return nil, err
		}

	case 75:
		body, err = api.ParseDescribeTopicPartitionsV0Request(data[offset:])
		if err != nil {
			return nil, err
		}
	case 1:
		body, err = api.ParseFetchV16Request(data[offset:])
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported API key: %d", header.RequestApiKey)
	}

	return &types.RequestMessage{
		Header: header,
		Body:   body,
	}, nil
}

func readHeader(data []byte) (types.RequestHeader, int, error) {

	var header types.RequestHeader

	offset := 0
	header.MessageSize = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4 // 4
	header.RequestApiKey = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2 // 6
	header.RequestApiVersion = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2 // 8
	header.CorrelationId = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4 // 12
	clientIdLength := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2 // 14

	if clientIdLength > 0 {
		val := string(data[offset : offset+int(clientIdLength)])
		header.ClientId = types.NullableString(val)
		offset += int(clientIdLength)
	} else {
		header.ClientId = ""
	}

	header.TAG_BUFFER = data[offset]
	offset++

	return header, offset, nil
}

func generateResponse(request types.RequestMessage) ([]byte, error) {
	headers, _ := request.Header, request.Body

	res := types.BaseResponse{
		CorrelationId: headers.CorrelationId,
		TAG_BUFFER:    headers.TAG_BUFFER,
	}

	var apiResBytes []byte
	var err error

	switch headers.RequestApiKey {
	case 18:
		apiResBytes, err = api.GenerateApiVersionV4Response(headers)
		if err != nil {
			return nil, err
		}

	case 75:
		apiResBytes, err = api.GenerateDescribeTopicPartitionsV0Response(request)
		if err != nil {
			return nil, err
		}

	case 1:
		apiResBytes, err = api.GenerateFetchResponse(request)
		if err != nil {
			return nil, err
		}
	}

	resBytes := res.Serialize(apiResBytes)

	return resBytes, nil
}
