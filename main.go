package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/api"
)

// WriteBinaryDataToFile writes the given binary data to the specified file.
func WriteBinaryDataToFile(filePath string) error {
	data := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 79, 0, 0, 0, 1, 2, 176, 105, 69, 124, 0, 0, 0, 0, 0, 0, 0, 0, 1, 145, 224, 90, 248, 24, 0, 0, 1, 145, 224, 90, 248, 24, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 58, 0, 0, 0, 1, 46, 1, 12, 0, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111, 110, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 228, 0, 0, 0, 1, 2, 36, 219, 18, 221, 0, 0, 0, 0, 0, 2, 0, 0, 1, 145, 224, 91, 45, 21, 0, 0, 1, 145, 224, 91, 45, 21, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 60, 0, 0, 0, 1, 48, 1, 2, 0, 4, 115, 97, 122, 0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 145, 0, 0, 144, 1, 0, 0, 2, 1, 130, 1, 1, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 145, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1, 0, 0, 144, 1, 0, 0, 4, 1, 130, 1, 1, 3, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 145, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1, 0, 0}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Write the binary data to the file
	byteData := make([]byte, len(data))
	for i, v := range data {
		byteData[i] = byte(v)
	}
	_, err = file.Write(byteData)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %w", err)
	}

	return nil
}

func main() {
	a := api.EncodeSignedVarint(3)
	b, c, _ := api.DecodeSignedVarint(a)
	fmt.Println(a, b, c)
	// filePath := "tmps/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

	// // Write binary data to file
	// if err := WriteBinaryDataToFile(filePath); err != nil {
	// 	log.Fatalf("Error writing binary data to file: %v", err)
	// }

	// data, err := api.ReadClusterMetadata(filePath)
	// if err != nil {
	// 	log.Fatalf("Error writing binary data to file: %v", err)
	// }

	// // fmt.Println(data)

	// parse, err := api.ParseClusterMetadata(data)

	// if err != nil {
	// 	log.Fatalf("Error parsing binary data: %v", err)
	// }

	// fmt.Println(parse)
}
