package types

import (
	"encoding/binary"
)

type CompactString string

func (cs CompactString) Serialize() ([]byte, error) {
	actualLength := len(cs)
	if actualLength == 0 {
		return []byte{1}, nil
	}

	length := actualLength + 1

	data := []byte{byte(length)}

	return append(data, []byte(cs)...), nil
}

type NullableString string

func (ns *NullableString) Serialize() ([]byte, error) {
	if ns == nil {
		// Null => length is -1
		return []byte{0xFF, 0xFF}, nil
	}

	strBytes := []byte(*ns)
	actualLength := len(strBytes)
	length := int16(actualLength)

	// Store length as 2 bytes
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, uint16(length))

	return append(data, strBytes...), nil
}

type CompactNullableString string

func (cns *CompactNullableString) Serialize() ([]byte, error) {
	if cns == nil {
		// Null => length is 0
		return []byte{0}, nil
	}

	strBytes := []byte(*cns)
	actualLength := len(strBytes)
	length := actualLength + 1

	data := []byte{byte(length)}

	return append(data, strBytes...), nil
}

type Array[T any] struct {
	Elements []T
}
type CompactArray[T any] []T
