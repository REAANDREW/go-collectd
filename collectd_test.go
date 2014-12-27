package collectd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

const (
	HOSTNAME      uint16 = 0x0000
	HIGH_DEF_TIME uint16 = 0x0008
)

type Header struct {
	Type   uint16
	Length uint16
}

type StringPart struct {
	Header  Header
	Content string
}

type NumericPart struct {
	Header  Header
	Content int64
}

var packetBytes []byte
var err error
var parsers map[uint16]ParsePart

func init() {
	parsers = map[uint16]ParsePart{
		HOSTNAME:      ParseStringPart,
		HIGH_DEF_TIME: ParseHighDefNumericPart,
	}
}

func TestMain(m *testing.M) {
	packetBytes, err = ioutil.ReadFile("cpu_disk_packet.dat")
	if err != nil {
		fmt.Errorf("error encountered %v", err)
	}

	os.Exit(m.Run())
}

type ParsePart func(header Header, buffer *bytes.Buffer) (part interface{}, err error)

func ParseHeader(buffer *bytes.Buffer) (header Header, err error) {
	var partType uint16
	var partLength uint16

	err = binary.Read(buffer, binary.BigEndian, &partType)
	if err != nil {
		return Header{}, err
	}

	err = binary.Read(buffer, binary.BigEndian, &partLength)
	if err != nil {
		return Header{}, err
	}
	return Header{partType, partLength}, nil
}

func ParseStringPart(header Header, buffer *bytes.Buffer) (part interface{}, err error) {
	contentBytes := buffer.Next(int(header.Length - 4))
	//Trim the null terminating byte from the string
	content := string(contentBytes[0 : len(contentBytes)-1])
	return StringPart{header, content}, nil
}

func ParseNumericPart(header Header, buffer *bytes.Buffer) (part interface{}, err error) {
	var content int64
	err = binary.Read(buffer, binary.BigEndian, &content)
	if err != nil {
		return NumericPart{}, err
	}
	return NumericPart{header, content}, nil
}

func ParseHighDefNumericPart(header Header, buffer *bytes.Buffer) (part interface{}, err error) {
	parsedPart, err := ParseNumericPart(header, buffer)
	numericPart := parsedPart.(NumericPart)
	if err != nil {
		return NumericPart{}, err
	}
	return NumericPart{numericPart.Header, numericPart.Content >> 30}, nil
}

func parseParts(buffer *bytes.Buffer) []interface{} {
	var partsSlice []interface{}
	for buffer.Len() > 0 {
		header, err := ParseHeader(buffer)
		if err != nil {
			fmt.Errorf("err encountered %v", err)
		}
		if parser, ok := parsers[header.Type]; ok {
			part, err := parser(header, buffer)
			if err != nil {
				fmt.Errorf("err encountered %v", err)
			}
			partsSlice = append(partsSlice, part)
		}
	}
	return partsSlice
}

func FindStringPart(partTypeId uint16, parts []interface{}) (stringPart StringPart, err error) {
	var part StringPart
	for _, value := range parts {
		switch value := value.(type) {
		case StringPart:
			if value.Header.Type == partTypeId {
				part = value
				return part, nil
			}
		}
	}
	return StringPart{}, nil
}

func FindNumericPart(partTypeId uint16, parts []interface{}) (numericPart NumericPart, err error) {
	var part NumericPart
	for _, value := range parts {
		switch value := value.(type) {
		case NumericPart:
			if value.Header.Type == partTypeId {
				part = value
				return part, nil
			}
		}
	}
	return NumericPart{}, nil
}

func Test_parsesTheHostname(t *testing.T) {
	buffer := bytes.NewBuffer(packetBytes)
	parts := parseParts(buffer)
	hostname_part, _ := FindStringPart(HOSTNAME, parts)
	assert.Equal(t, hostname_part.Content, "localhost", "contents does not equal localhost")
}

func Test_parsesTheHighDefinitionTime(t *testing.T) {
	buffer := bytes.NewBuffer(packetBytes)
	parts := parseParts(buffer)
	time_part, _ := FindNumericPart(HIGH_DEF_TIME, parts)
	assert.Equal(t, time_part.Content, 1419415668, "contents does not equal expected")
}
