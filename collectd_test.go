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
	PLUGIN        uint16 = 0x0002
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
		PLUGIN:        ParseStringPart,
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
		} else {
			buffer.Next(int(header.Length - 4))
		}
	}
	return partsSlice
}

func FindStringParts(partTypeId uint16, parts []interface{}) (stringParts []StringPart, err error) {
	var returnParts []StringPart
	for _, value := range parts {
		switch value := value.(type) {
		case StringPart:
			if value.Header.Type == partTypeId {
				returnParts = append(returnParts, value)
			}
		}
	}
	return returnParts, nil
}

func FindNumericParts(partTypeId uint16, parts []interface{}) (numericParts []NumericPart, err error) {
	var returnParts []NumericPart
	for _, value := range parts {
		switch value := value.(type) {
		case NumericPart:
			if value.Header.Type == partTypeId {
				returnParts = append(returnParts, value)
			}
		}
	}
	return returnParts, nil
}

func Test_parsesTheHostname(t *testing.T) {
	buffer := bytes.NewBuffer(packetBytes)
	parts := parseParts(buffer)
	hostname_parts, _ := FindStringParts(HOSTNAME, parts)
	assert.Equal(t, len(hostname_parts), 1, "number of parts is not equal to 1")
	assert.Equal(t, hostname_parts[0].Content, "localhost", "contents does not equal localhost")
}

func Test_parsesTheHighDefinitionTime(t *testing.T) {
	buffer := bytes.NewBuffer(packetBytes)
	parts := parseParts(buffer)
	time_parts, _ := FindNumericParts(HIGH_DEF_TIME, parts)
	assert.Equal(t, len(time_parts), 24, "number of parts is not equal to 1")
	assert.Equal(t, time_parts[0].Content, 1419415668, "contents does not equal expected")
}

func Test_parsesThePlugin(t *testing.T) {
	buffer := bytes.NewBuffer(packetBytes)
	parts := parseParts(buffer)
	plugin_parts, _ := FindStringParts(PLUGIN, parts)
	assert.Equal(t, len(plugin_parts), 1, "number of parts is not equal to 1")
	assert.Equal(t, plugin_parts[0].Content, "disk", "plugin content does not equal expected")
}
