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

func TestMain(m *testing.M) {
	packetBytes, err = ioutil.ReadFile("cpu_disk_packet.dat")
	if err != nil {
		fmt.Errorf("error encountered %v", err)
	}

	os.Exit(m.Run())
}

func parseParts(buffer *bytes.Buffer) []interface{} {
	var partsSlice []interface{}
	for buffer.Len() > 0 {
		var partType uint16
		var partLength uint16
		var content string

		err = binary.Read(buffer, binary.BigEndian, &partType)
		if err != nil {
			fmt.Errorf("error encountered %v", err)
		}

		err = binary.Read(buffer, binary.BigEndian, &partLength)
		if err != nil {
			fmt.Errorf("error encountered %v", err)
		}

		switch partType {
		case HOSTNAME:
			contentBytes := buffer.Next(int(partLength - 4))
			//Trim the null terminating byte from the string
			content = string(contentBytes[0 : len(contentBytes)-1])
			partsSlice = append(partsSlice, StringPart{Header{partType, partLength}, content})

		case HIGH_DEF_TIME:
			var content int64
			err = binary.Read(buffer, binary.BigEndian, &content)
			if err != nil {
				fmt.Errorf("error encountered %v", err)
			}
			content = content >> 30
			partsSlice = append(partsSlice, NumericPart{Header{partType, partLength}, content})
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
