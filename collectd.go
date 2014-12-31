package collectd

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	HOSTNAME          uint16 = 0x0000
	HIGH_DEF_TIME     uint16 = 0x0008
	PLUGIN            uint16 = 0x0002
	PLUGIN_INSTANCE   uint16 = 0x0003
	TYPE              uint16 = 0x0004
	TYPE_INSTANCE     uint16 = 0x0005
	HIGH_DEF_INTERVAL uint16 = 0x0009
	MESSAGE           uint16 = 0x0100
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

var parsers map[uint16]parsePart

func init() {
	parsers = map[uint16]parsePart{
		HOSTNAME:          parseStringPart,
		HIGH_DEF_TIME:     parseHighDefNumericPart,
		PLUGIN:            parseStringPart,
		PLUGIN_INSTANCE:   parseStringPart,
		TYPE:              parseStringPart,
		TYPE_INSTANCE:     parseStringPart,
		HIGH_DEF_INTERVAL: parseHighDefNumericPart,
		MESSAGE:           parseStringPart,
	}
}

type parsePart func(header Header, buffer *bytes.Buffer) (part interface{}, err error)

func parseHeader(buffer *bytes.Buffer) (header Header, err error) {
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

func parseStringPart(header Header, buffer *bytes.Buffer) (part interface{}, err error) {
	contentBytes := buffer.Next(int(header.Length - 4))
	//Trim the null terminating byte from the string
	content := string(contentBytes[0 : len(contentBytes)-1])
	return StringPart{header, content}, nil
}

func parseNumericPart(header Header, buffer *bytes.Buffer) (part interface{}, err error) {
	var content int64
	err = binary.Read(buffer, binary.BigEndian, &content)
	if err != nil {
		return NumericPart{}, err
	}
	return NumericPart{header, content}, nil
}

func parseHighDefNumericPart(header Header, buffer *bytes.Buffer) (part interface{}, err error) {
	parsedPart, err := parseNumericPart(header, buffer)
	numericPart := parsedPart.(NumericPart)
	if err != nil {
		return NumericPart{}, err
	}
	return NumericPart{numericPart.Header, numericPart.Content >> 30}, nil
}

func parseParts(buffer *bytes.Buffer) []interface{} {
	var partsSlice []interface{}
	for buffer.Len() > 0 {
		header, err := parseHeader(buffer)
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
