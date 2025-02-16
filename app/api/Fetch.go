package api

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/types"
	"github.com/google/uuid"
)

func ParseFetchV16Request(body []byte) (FetchV16Request, error) {
	var req FetchV16Request

	offset := 0

	req.MaxWaitMs = int32(binary.BigEndian.Uint32(body[offset:]))
	offset += 4

	req.MinBytes = int32(binary.BigEndian.Uint32(body[offset:]))
	offset += 4

	req.MaxBytes = int32(binary.BigEndian.Uint32(body[offset:]))
	offset += 4

	req.IsolationLevel = int8(body[offset])
	offset++

	req.SessionId = int32(binary.BigEndian.Uint32(body[offset:]))
	offset += 4

	req.SessionEpoch = int32(binary.BigEndian.Uint32(body[offset:]))
	offset += 4

	var topics []FetchRequestTopic
	topicsCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
	offset += numOfBytes

	for i := 0; i < int(topicsCount); i++ {
		var topic FetchRequestTopic

		topic.TopicId = uuid.UUID(body[offset : offset+16])
		offset += 16

		var partitions []FetchRequestTopicPartition

		partitionsCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
		offset += numOfBytes

		for j := 0; j < int(partitionsCount); j++ {
			var partition FetchRequestTopicPartition

			partition.Partition = int32(binary.BigEndian.Uint32(body[offset:]))
			offset += 4

			partition.CurrentLeaderEpoch = int32(binary.BigEndian.Uint32(body[offset:]))
			offset += 4

			partition.FetchOffset = int64(binary.BigEndian.Uint64(body[offset:]))
			offset += 8

			partition.LastFetchedEpoch = int32(binary.BigEndian.Uint32(body[offset:]))
			offset += 4

			partition.LogStartOffset = int64(binary.BigEndian.Uint64(body[offset:]))
			offset += 8

			partition.PartitionMaxBytes = int32(binary.BigEndian.Uint32(body[offset:]))
			offset += 4

			partitions = append(partitions, partition)
		}

		topic.Partitions = partitions
		topic.TAG_BUFFER = byte(body[offset])
		offset++

		topics = append(topics, topic)
	}

	req.Topics = topics

	var forgottenTopicsData []FetchRequestForgottenTopicsData

	forgottenTopicsDataCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
	offset += numOfBytes

	for i := 0; i < int(forgottenTopicsDataCount); i++ {
		var forgottenTopicData FetchRequestForgottenTopicsData

		forgottenTopicData.TopicId = uuid.UUID(body[offset : offset+16])
		offset += 16

		var partitions []int32

		partitionsCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
		offset += numOfBytes

		for j := 0; j < int(partitionsCount); j++ {
			partition := int32(binary.BigEndian.Uint32(body[offset:]))
			offset += 4

			partitions = append(partitions, partition)
		}

		forgottenTopicData.Partitions = partitions
		forgottenTopicData.TAG_BUFFER = byte(body[offset])
		offset++

		forgottenTopicsData = append(forgottenTopicsData, forgottenTopicData)
	}

	req.ForgottenTopicsData = forgottenTopicsData

	rackIdLength, numOfBytes, _ := DecodeSignedVarint(body[offset:])
	offset += numOfBytes

	if rackIdLength > 0 {
		req.RackId = string(body[offset : offset+int(rackIdLength-1)])
		offset += int(rackIdLength - 1)
	}

	req.TAG_BUFFER = byte(body[offset])
	offset++

	return req, nil
}

func GenerateFetchResponse(request types.RequestMessage) ([]byte, error) {
	req := request.Body.(FetchV16Request)

	var res FetchV16Response

	res.ThrottleTimeMs = 0
	res.ErrorCode = 0
	res.SessionId = req.SessionId

	var responses []FetchResponseTopic

	for _, topic := range req.Topics {
		var topicRes FetchResponseTopic

		topicRes.TopicId = topic.TopicId

		partitionErrorCode := int16(0)

		found, _ := GetTopicById(topic.TopicId)
		if found.ErrorCode != 0 {
			partitionErrorCode = types.ErrorCodes["UNKNOWN_TOPIC"].Code
		}

		var partitions []FetchResponseTopicPartitions

		for _, partition := range topic.Partitions {
			var partitionRes FetchResponseTopicPartitions

			partitionRes.PartitionIndex = partition.Partition
			partitionRes.ErrorCode = partitionErrorCode
			partitionRes.HighWatermark = 0
			partitionRes.LastStableOffset = 0
			partitionRes.LogStartOffset = 0
			partitionRes.AbortedTransactions = []FetchResponseAbortedTransactions{}
			partitionRes.PreferredReadReplica = -1

			fetchedRecords, err := ReadTopicMessages(string(found.Name), partitionRes.PartitionIndex)
			if err != nil {
				fmt.Println("Error reading topic messages: ", err.Error())
			}
			partitionRes.Records = []FetchResponseRecord{*fetchedRecords}

			partitions = append(partitions, partitionRes)
		}

		topicRes.Partitions = partitions
		topicRes.TAG_BUFFER = 0

		responses = append(responses, topicRes)
	}

	res.Responses = responses
	res.TAG_BUFFER = 0

	bytes := res.Serialize()

	return bytes, nil
}

func (req FetchV16Response) Serialize() []byte {
	body := make([]byte, 1024)

	offset := 0

	body[offset] = 0
	offset++

	binary.BigEndian.PutUint32(body[offset:], uint32(req.ThrottleTimeMs))
	offset += 4

	binary.BigEndian.PutUint16(body[offset:], uint16(req.ErrorCode))
	offset += 2

	binary.BigEndian.PutUint32(body[offset:], uint32(req.SessionId))
	offset += 4

	responsesLength := EncodeSignedVarint(len(req.Responses))
	copy(body[offset:], responsesLength)
	offset += len(responsesLength)

	for _, topic := range req.Responses {
		copy(body[offset:], topic.TopicId[:])
		offset += 16

		partitionsLength := EncodeSignedVarint(len(topic.Partitions))
		copy(body[offset:], partitionsLength)
		offset += len(partitionsLength)

		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint32(body[offset:], uint32(partition.PartitionIndex))
			offset += 4

			binary.BigEndian.PutUint16(body[offset:], uint16(partition.ErrorCode))
			offset += 2

			binary.BigEndian.PutUint64(body[offset:], uint64(partition.HighWatermark))
			offset += 8

			binary.BigEndian.PutUint64(body[offset:], uint64(partition.LastStableOffset))
			offset += 8

			binary.BigEndian.PutUint64(body[offset:], uint64(partition.LogStartOffset))
			offset += 8

			abortedTransactionsLength := EncodeSignedVarint(len(partition.AbortedTransactions))
			copy(body[offset:], abortedTransactionsLength)
			offset += len(abortedTransactionsLength)

			for _, transaction := range partition.AbortedTransactions {
				binary.BigEndian.PutUint64(body[offset:], uint64(transaction.ProducerId))
				offset += 8

				binary.BigEndian.PutUint64(body[offset:], uint64(transaction.FirstOffset))
				offset += 8

				body[offset] = transaction.TAG_BUFFER
				offset++

			}

			binary.BigEndian.PutUint32(body[offset:], uint32(partition.PreferredReadReplica))
			offset += 4

			recordsLength := EncodeSignedVarint(len(partition.Records))
			copy(body[offset:], recordsLength)
			offset += len(recordsLength)

			if len(partition.Records) > 0 {
				for _, record := range partition.Records {
					copy(body[offset:], record.Content)
					offset += len(record.Content)

				}
			}

			body[offset] = partition.TAG_BUFFER
			offset++

		}

		body[offset] = topic.TAG_BUFFER
		offset++

	}

	body[offset] = req.TAG_BUFFER
	offset++

	return body[:offset]
}

func ReadTopicMessages(topicName string, partitionId int32) (*FetchResponseRecord, error) {
	fileName := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionId)
	fmt.Println("Reading log file: ", fileName)
	data, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("Error reading log file: ", err.Error())
	}

	return &FetchResponseRecord{
		Length:  uint32(len(data)),
		Content: data,
	}, nil
}

type FetchV16Request struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionId           int32
	SessionEpoch        int32
	Topics              []FetchRequestTopic
	ForgottenTopicsData []FetchRequestForgottenTopicsData
	RackId              string
	TAG_BUFFER          byte
}

type FetchRequestTopic struct {
	TopicId    uuid.UUID
	Partitions []FetchRequestTopicPartition
	TAG_BUFFER byte
}

type FetchRequestTopicPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
	TAG_BUFFER         byte
}

type FetchRequestForgottenTopicsData struct {
	TopicId    uuid.UUID
	Partitions []int32
	TAG_BUFFER byte
}

type FetchV16Response struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Responses      []FetchResponseTopic
	TAG_BUFFER     byte
}

type FetchResponseTopic struct {
	TopicId    uuid.UUID
	Partitions []FetchResponseTopicPartitions
	TAG_BUFFER byte
}

type FetchResponseTopicPartitions struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []FetchResponseAbortedTransactions
	PreferredReadReplica int32
	Records              []FetchResponseRecord
	TAG_BUFFER           byte
}

type FetchResponseRecord struct {
	Length  uint32
	Content []byte
}

type FetchResponseAbortedTransactions struct {
	ProducerId  int64
	FirstOffset int64
	TAG_BUFFER  byte
}
