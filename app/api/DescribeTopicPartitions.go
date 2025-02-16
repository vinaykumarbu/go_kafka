package api

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"

	"github.com/codecrafters-io/kafka-starter-go/app/types"
	"github.com/google/uuid"
)

var metadataFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

func ParseDescribeTopicPartitionsV0Request(data []byte) (DescribeTopicPartitionsv0Request, error) {
	var body DescribeTopicPartitionsv0Request

	offset := 0

	topicsCount := int8(data[offset])
	offset++

	var allTopics []topics
	for i := 0; i < int(topicsCount-1); i++ {
		topicNameLength := int8(data[offset])
		offset++

		topicName := string(data[offset : offset+int(topicNameLength-1)])
		offset += int(topicNameLength - 1)

		topicTagBuffer := data[offset]
		offset++

		allTopics = append(allTopics, topics{
			Name:       types.CompactString(topicName),
			TAG_BUFFER: topicTagBuffer,
		})
	}

	body.Topics = allTopics

	body.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// If next byte is 0xff, then there is no cursor meaning null
	var cursorData cursor

	nextByte := data[offset]
	offset++

	if nextByte != 0xff {
		topicNameLength := int8(data[offset])
		offset++

		topicName := string(data[offset : offset+int(topicNameLength-1)])
		offset += int(topicNameLength - 1)

		partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		cursorData = cursor{
			TopicName:      types.CompactString(topicName),
			PartitionIndex: partitionIndex,
			TAG_BUFFER:     data[offset],
		}
	}

	body.Cursor = cursorData

	body.TAG_BUFFER = data[offset]
	offset++

	return body, nil
}

func GenerateDescribeTopicPartitionsV0Response(request types.RequestMessage) ([]byte, error) {
	body := request.Body.(DescribeTopicPartitionsv0Request)

	var topics []ResponseTopic

	for _, topicToFind := range body.Topics {
		topic, err := GetTopicByName(strings.TrimSpace(string(topicToFind.Name)))
		if err != nil {
			topic.ErrorCode = 3
			fmt.Println(err)
		}

		topics = append(topics, topic)
	}

	res := DescribeTopicPartitionsv0Response{
		ThrottleTimeMs: 0,
		Topics:         types.CompactArray[ResponseTopic](topics),
		NextCursor:     NextCursor{},
		TAG_BUFFER:     0,
	}

	resBytes := res.Serialize()

	return resBytes, nil
}

func (r *DescribeTopicPartitionsv0Response) Serialize() []byte {
	resBytes := make([]byte, 1024) // Adjust the size as needed

	offset := 0

	// Tag Buffer
	resBytes[offset] = 0
	offset++

	// Throttle Time
	binary.BigEndian.PutUint32(resBytes[offset:], uint32(r.ThrottleTimeMs))
	offset += 4

	// Topics Length byte
	topicsLength := len(r.Topics) + 1
	resBytes[offset] = byte(topicsLength)
	offset++

	for _, topic := range r.Topics {
		binary.BigEndian.PutUint16(resBytes[offset:], uint16(topic.ErrorCode))
		offset += 2

		topicNameBytes, _ := topic.Name.Serialize()
		copy(resBytes[offset:], topicNameBytes)
		offset += len(topicNameBytes)

		topicIdBytes := topic.TopicId[:]
		copy(resBytes[offset:], topicIdBytes)
		offset += 16

		if topic.IsInternal {
			resBytes[offset] = 1
		} else {
			resBytes[offset] = 0
		}
		offset++

		// Partitions Length byte
		resBytes[offset] = byte(len(topic.Partitions) + 1)
		offset++

		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint16(resBytes[offset:], uint16(partition.ErrorCode))
			offset += 2
			binary.BigEndian.PutUint32(resBytes[offset:], uint32(partition.PartitionIndex))
			offset += 4
			binary.BigEndian.PutUint32(resBytes[offset:], uint32(partition.LeaderId))
			offset += 4
			binary.BigEndian.PutUint32(resBytes[offset:], uint32(partition.LeaderEpoch))
			offset += 4

			resBytes[offset] = byte(len(partition.ReplicaNodes) + 1)
			offset++
			for _, node := range partition.ReplicaNodes {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.IsrNodes) + 1)
			offset++
			for _, node := range partition.IsrNodes {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.EligibleLeaderReplicas) + 1)
			offset++
			for _, node := range partition.EligibleLeaderReplicas {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.LastKnownElr) + 1)
			offset++
			for _, node := range partition.LastKnownElr {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.OfflineReplicas) + 1)
			offset++
			for _, node := range partition.OfflineReplicas {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = partition.TAG_BUFFER
			offset++
		}

		binary.BigEndian.PutUint32(resBytes[offset:], uint32(topic.TopicAuthorizedOperations))
		offset += 4

		resBytes[offset] = topic.TAG_BUFFER
		offset++
	}

	if r.NextCursor.TopicName == "" {
		resBytes[offset] = 0xff
		offset++
	} else {
		nextCursorTopicNameBytes, _ := r.NextCursor.TopicName.Serialize()

		copy(resBytes[offset:], nextCursorTopicNameBytes)
		offset += len(nextCursorTopicNameBytes)

		binary.BigEndian.PutUint32(resBytes[offset:], uint32(r.NextCursor.PartitionIndex))
		offset += 4
	}

	resBytes[offset] = r.TAG_BUFFER
	offset++

	return resBytes[:offset]
}

func ReadClusterMetadata(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

func ParseClusterMetadata(data []byte) ([]RecordBatch, error) {
	var batches []RecordBatch
	offset := 0

	for offset < len(data) {
		batch, newOffset, err := ParseRecordBatch(data, offset)
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
		offset = newOffset
	}

	return batches, nil
}

func ParseRecordBatch(data []byte, offset int) (RecordBatch, int, error) {
	var batch RecordBatch

	batch.BaseOffset = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.BatchLength = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.MagicByte = int8(data[offset])
	offset++

	batch.CRC = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.Attributes = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	batch.LastOffsetDelta = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.BaseTimestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.ProducerId = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.ProducerEpoch = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	batch.BaseSequence = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.RecordsLength = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.Records = make([]Record, 0)
	for i := 0; i < int(batch.RecordsLength); i++ {
		record, newOffset, err := ParseRecord(data, offset)
		if err != nil {
			return batch, offset, err
		}

		batch.Records = append(batch.Records, record)
		offset = newOffset
	}

	return batch, offset, nil
}

func ParseRecord(data []byte, offset int) (Record, int, error) {
	var record Record

	var numOfBytes int
	record.Length, numOfBytes, _ = DecodeSignedVarint(data[offset:])
	offset = offset + numOfBytes

	record.Attributes = int8(data[offset])
	offset++

	record.TimestampDelta = int8(data[offset])
	offset++

	record.OffsetDelta = int8(data[offset])
	offset++

	record.KeyLength, numOfBytes, _ = DecodeSignedVarint(data[offset:])
	offset = offset + numOfBytes

	if record.KeyLength-1 > 0 {
		record.Key = data[offset : offset+int(record.KeyLength)]
		offset += int(record.KeyLength)
	}

	record.ValueLength, numOfBytes, _ = DecodeSignedVarint(data[offset:])
	offset = offset + numOfBytes

	recordType := int8(data[offset+1])

	switch recordType {
	case 12:
		var value FeatureLevelRecordValue
		value.FrameVersion = int8(data[offset])
		offset++
		value.Type = int8(data[offset])
		offset++
		value.Version = int8(data[offset])
		offset++
		value.NameLength, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.Name = string(data[offset : offset+int(value.NameLength-1)])
		offset += int(value.NameLength - 1)
		value.FeatureLevel = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		value.TaggedFieldsCount = int8(data[offset])
		offset++
		record.Value = &value
	case 2:
		var value TopicRecordValue
		value.FrameVersion = int8(data[offset])
		offset++
		value.Type = int8(data[offset])
		offset++
		value.Version = int8(data[offset])
		offset++
		value.NameLength, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.TopicName = string(data[offset : offset+int(value.NameLength-1)])
		offset += int(value.NameLength - 1)
		value.TopicUUID = uuid.UUID(data[offset : offset+16])
		offset += 16
		value.TaggedFieldsCount = int8(data[offset])
		offset++
		record.Value = &value
	case 3:
		var value PartitionRecordValue
		value.FrameVersion = int8(data[offset])
		offset++
		value.Type = int8(data[offset])
		offset++
		value.Version = int8(data[offset])
		offset++
		value.PartitionId = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		value.TopicUUID = uuid.UUID(data[offset : offset+16])
		offset += 16
		value.LengthOfReplicaArray, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.ReplicaArray = make([]int32, value.LengthOfReplicaArray-1)
		for i := 0; i < int(value.LengthOfReplicaArray-1); i++ {
			value.ReplicaArray[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
		}
		value.LengthOfInSyncReplicaArray, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.InSyncReplicaArray = make([]int32, value.LengthOfInSyncReplicaArray-1)
		for i := 0; i < int(value.LengthOfInSyncReplicaArray-1); i++ {
			value.InSyncReplicaArray[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
		}
		value.LengthOfRemovingReplicasArray, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.RemovingReplicasArray = make([]int32, value.LengthOfRemovingReplicasArray-1)
		for i := 0; i < int(value.LengthOfRemovingReplicasArray-1); i++ {
			value.RemovingReplicasArray[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
		}
		value.LengthOfAddingReplicasArray, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.AddingReplicasArray = make([]int32, value.LengthOfAddingReplicasArray-1)
		for i := 0; i < int(value.LengthOfAddingReplicasArray-1); i++ {
			value.AddingReplicasArray[i] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
		}
		value.Leader = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		value.LeaderEpoch = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		value.PartitionEpoch = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		value.LengthOfDirectoriesArray, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes
		value.DirectoriesArray = make([]uuid.UUID, value.LengthOfDirectoriesArray-1)
		for i := 0; i < int(value.LengthOfDirectoriesArray-1); i++ {
			value.DirectoriesArray[i] = uuid.UUID(data[offset : offset+16])
			offset += 16
		}
		value.TaggedFieldsCount, numOfBytes, _ = DecodeUnsignedVarint(data[offset:])
		offset = offset + numOfBytes

		record.Value = &value
	default:
		return record, offset, fmt.Errorf("unknown record value type: %d", record.ValueLength)
	}

	record.HeadersArrayCount = int8(data[offset])
	offset++

	return record, offset, nil
}

func GetTopicByName(name string) (ResponseTopic, error) {
	rawMetadata, err := ReadClusterMetadata(metadataFilePath)
	if err != nil {
		fmt.Println("Error reading cluster metadata: ", err)
		return ResponseTopic{}, err
	}

	// Hexdump(rawMetadata)

	topic := ResponseTopic{
		ErrorCode:                 3,
		Name:                      types.CompactNullableString(name),
		IsInternal:                false,
		Partitions:                types.CompactArray[ResponsePartition]{},
		TopicAuthorizedOperations: 3576,
		TAG_BUFFER:                0,
	}

	batches, err := ParseClusterMetadata(rawMetadata)
	if err != nil {
		fmt.Println("Error parsing cluster metadata: ", err)
		return topic, err
	}

	for _, batch := range batches {
		for _, record := range batch.Records {
			if record.Value != nil {
				switch v := record.Value.(type) {
				case *TopicRecordValue:
					if string(v.TopicName) == string(name) {
						topic.ErrorCode = 0
						topic.Name = types.CompactNullableString(v.TopicName)
						topic.TopicId = v.TopicUUID
					}
				}
			}
		}
	}

	if topic.ErrorCode == 0 {
		var partitions []ResponsePartition
		for _, batch := range batches {
			for _, record := range batch.Records {
				if record.Value != nil {
					switch v := record.Value.(type) {
					case *PartitionRecordValue:
						if v.TopicUUID == topic.TopicId {
							partition := ResponsePartition{
								ErrorCode:      0,
								PartitionIndex: v.PartitionId,
								LeaderId:       v.Leader,
								LeaderEpoch:    v.LeaderEpoch,
								ReplicaNodes:   types.CompactArray[int32](v.ReplicaArray),
								IsrNodes:       types.CompactArray[int32](v.InSyncReplicaArray),
								TAG_BUFFER:     0,
							}

							partitions = append(partitions, partition)
						}
					}
				}
			}
		}

		topic.Partitions = types.CompactArray[ResponsePartition](partitions)
	}

	return topic, nil
}

func GetTopicById(topicId uuid.UUID) (ResponseTopic, error) {
	rawMetadata, err := ReadClusterMetadata(metadataFilePath)
	if err != nil {
		fmt.Println("Error reading cluster metadata: ", err)
		return ResponseTopic{}, err
	}

	topic := ResponseTopic{
		ErrorCode:                 3,
		IsInternal:                false,
		Partitions:                types.CompactArray[ResponsePartition]{},
		TopicAuthorizedOperations: 3576,
		TAG_BUFFER:                0,
	}

	batches, err := ParseClusterMetadata(rawMetadata)
	if err != nil {
		fmt.Println("Error parsing cluster metadata: ", err)
		return topic, err
	}

	for _, batch := range batches {
		for _, record := range batch.Records {
			if record.Value != nil {
				switch v := record.Value.(type) {
				case *TopicRecordValue:
					if v.TopicUUID == topicId {
						topic.ErrorCode = 0
						topic.Name = types.CompactNullableString(v.TopicName)
						topic.TopicId = v.TopicUUID
					}
				}
			}
		}
	}

	if topic.ErrorCode == 0 {
		var partitions []ResponsePartition
		for _, batch := range batches {
			for _, record := range batch.Records {
				if record.Value != nil {
					switch v := record.Value.(type) {
					case *PartitionRecordValue:
						if v.TopicUUID == topic.TopicId {
							partition := ResponsePartition{
								ErrorCode:      0,
								PartitionIndex: v.PartitionId,
								LeaderId:       v.Leader,
								LeaderEpoch:    v.LeaderEpoch,
								ReplicaNodes:   types.CompactArray[int32](v.ReplicaArray),
								IsrNodes:       types.CompactArray[int32](v.InSyncReplicaArray),
								TAG_BUFFER:     0,
							}

							partitions = append(partitions, partition)
						}
					}
				}
			}
		}

		topic.Partitions = types.CompactArray[ResponsePartition](partitions)
	}

	return topic, nil
}

func EncodeUnsignedVarint(value uint) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	length := binary.PutUvarint(buf, uint64(value))
	return buf[:length]
}

func DecodeUnsignedVarint(data []byte) (uint, int, error) {
	value, length := binary.Uvarint(data)
	if length <= 0 {
		return 0, 0, errors.New("failed to decode varint")
	}
	return uint(value), length, nil
}

func EncodeSignedVarint(value int) []byte {
	// Zigzag encoding
	uValue := uint((value << 1) ^ (value >> 31))
	return EncodeUnsignedVarint(uValue)
}

func DecodeSignedVarint(data []byte) (int, int, error) {
	uValue, length, err := DecodeUnsignedVarint(data)
	if err != nil {
		return 0, 0, err
	}
	// Zigzag decoding
	value := int((uValue >> 1) ^ uint(-(uValue & 1)))
	return value, length, nil
}

type DescribeTopicPartitionsv0Request struct {
	Topics                 []topics
	ResponsePartitionLimit int32
	Cursor                 cursor
	TAG_BUFFER             byte
}

type topics struct {
	Name       types.CompactString
	TAG_BUFFER byte
}

type cursor struct {
	TopicName      types.CompactString
	PartitionIndex int32
	TAG_BUFFER     byte
}

type DescribeTopicPartitionsv0Response struct {
	ThrottleTimeMs int32
	Topics         types.CompactArray[ResponseTopic]
	NextCursor     NextCursor
	TAG_BUFFER     byte
}

type ResponseTopic struct {
	ErrorCode                 int16
	Name                      types.CompactNullableString
	TopicId                   uuid.UUID
	IsInternal                bool
	Partitions                types.CompactArray[ResponsePartition]
	TopicAuthorizedOperations int32
	TAG_BUFFER                byte
}

type ResponsePartition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           types.CompactArray[int32]
	IsrNodes               types.CompactArray[int32]
	EligibleLeaderReplicas types.CompactArray[int32]
	LastKnownElr           types.CompactArray[int32]
	OfflineReplicas        types.CompactArray[int32]
	TAG_BUFFER             byte
}

type NextCursor struct {
	TopicName      types.CompactString
	PartitionIndex int32
}

type RecordBatch struct {
	// offset of the first record in this batch.
	BaseOffset int64

	// length of the entire record batch in bytes.
	// excludes the Base Offset (8 bytes) and the Batch Length (4 bytes) itself, but includes all other bytes in the record batch.
	BatchLength int32

	// epoch of the leader for this partition. It is a monotonically increasing number that is incremented by 1 whenever the partition leader changes. This value is used to detect out of order writes.
	PartitionLeaderEpoch int32

	// version of the record batch format. This value is used to evolve the record batch format in a backward-compatible way.
	MagicByte int8

	// 	CRC is a 4-byte big-endian integer indicating the CRC32-C checksum of the record batch.
	// The CRC is computed over the data following the CRC field to the end of the record batch. The CRC32-C (Castagnoli) polynomial is used for the computation.
	CRC int32

	// 	the attributes of the record batch.

	// Attributes is a bitmask of the following flags:

	// bit 0~2:
	// 0: no compression
	// 1: gzip
	// 2: snappy
	// 3: lz4
	// 4: zstd
	// bit 3: timestampType
	// bit 4: isTransactional (0 means not transactional)
	// bit 5: isControlBatch (0 means not a control batch)
	// bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
	// bit 7~15: unused
	Attributes int16

	// difference between the last offset of this record batch and the base offset.
	LastOffsetDelta int32

	// timestamp of the first record in this batch.
	BaseTimestamp int64

	// maximum timestamp of the records in this batch.
	MaxTimestamp int64

	// ID of the producer that produced the records in this batch.
	// -1 means that this batch is not associated with a producer.
	ProducerId int64

	// epoch of the producer that produced the records in this batch.
	// -1 means that this batch is not associated with a producer.
	ProducerEpoch int16

	// the sequence number of the first record in a batch. It is used to ensure the correct ordering and deduplication of messages produced by a Kafka producer.
	BaseSequence int32

	// the number of records in this batch.
	RecordsLength int32

	Records []Record
}

type Record struct {
	// Length is a signed variable size integer indicating the length of the record, the length is calculated from the attributes field to the end of the record.
	Length int

	// attributes of the record. Currently, this field is unused in the protocol.
	Attributes int8

	// Timestamp Delta is a signed variable size integer indicating the difference between the timestamp of the record and the base timestamp of the record batch.
	TimestampDelta int8

	// Offset Delta is a signed variable size integer indicating the difference between the offset of the record and the base offset of the record batch.
	OffsetDelta int8

	// Key Length is a signed variable size integer indicating the length of the key of the record.
	// If the key is null, the key length is -1.
	KeyLength int

	Key []byte

	// Value Length is a signed variable size integer indicating the length of the value of the record.
	ValueLength int

	Value interface{}

	// Header array count is an unsigned variable size integer indicating the number of headers present.
	HeadersArrayCount int8
}

type FeatureLevelRecordValue struct {
	// version of the format of the record.
	FrameVersion int8

	// type of the record.
	// 12 -> Feature level record
	Type int8

	// version of the feature level record.
	Version int8

	// Name length is a unsigned variable size integer indicating the length of the name. But, as name is a compact string, the length of the name is always length - 1.
	NameLength uint

	// Name is a compact string representing the name of the feature.
	Name string

	// level of the feature.
	FeatureLevel int16

	// Tagged Field count is an unsigned variable size integer indicating the number of tagged fields.
	TaggedFieldsCount int8
}

type TopicRecordValue struct {
	// version of the format of the record.
	FrameVersion int8

	// type of the record.
	// 2 -> Topic record
	Type int8

	// version of the feature level record.
	Version int8

	// Name length is a unsigned variable size integer indicating the length of the name. But, as name is a compact string, the length of the name is always length - 1.
	NameLength uint

	TopicName string

	TopicUUID uuid.UUID

	TaggedFieldsCount int8
}

type PartitionRecordValue struct {
	// version of the format of the record.
	FrameVersion int8

	// type of the record.
	// 3 -> Partition record
	Type int8

	// version of the feature level record.
	Version int8

	// ID of the partition
	PartitionId int32

	TopicUUID uuid.UUID

	LengthOfReplicaArray uint
	ReplicaArray         []int32

	LengthOfInSyncReplicaArray uint
	InSyncReplicaArray         []int32

	LengthOfRemovingReplicasArray uint
	RemovingReplicasArray         []int32

	LengthOfAddingReplicasArray uint
	AddingReplicasArray         []int32

	Leader         int32
	LeaderEpoch    int32
	PartitionEpoch int32

	LengthOfDirectoriesArray uint
	DirectoriesArray         []uuid.UUID

	TaggedFieldsCount uint
}

func Hexdump(data []byte) {
	const bytesPerLine = 16

	for i := 0; i < len(data); i += bytesPerLine {
		// Print the offset
		fmt.Printf("%08x  ", i)

		// Print the hex values
		for j := 0; j < bytesPerLine; j++ {
			if i+j < len(data) {
				fmt.Printf("%02x ", data[i+j])
			} else {
				fmt.Print("   ")
			}
		}

		// Print the ASCII characters
		fmt.Print(" |")
		for j := 0; j < bytesPerLine; j++ {
			if i+j < len(data) {
				if unicode.IsPrint(rune(data[i+j])) {
					fmt.Printf("%c", data[i+j])
				} else {
					fmt.Print(".")
				}
			} else {
				fmt.Print(" ")
			}
		}
		fmt.Println("|")
	}
}
