// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package json

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/hack"
)

func keys(m map[string]interface{}) []string {
	kSlice := make([]string, len(m))
	i := 0
	for k := range m {
		kSlice[i] = k
		i++
	}
	sort.Strings(kSlice)
	return kSlice
}

func jsonTypeCodeAndLength(in interface{}) (code byte, len int, err error) {
	switch x := in.(type) {
	case map[string]interface{}:
		return 0x01, -1, nil
	case []interface{}:
		return 0x03, -1, nil
	case nil, bool:
		return 0x04, 1, nil
	case int16:
		return 0x05, 2, nil
	case uint16:
		return 0x06, 2, nil
	case int32:
		return 0x07, 2, nil
	case uint32:
		return 0x08, 4, nil
	case int64:
		return 0x09, 8, nil
	case uint64:
		return 0x0a, 8, nil
	case float64:
		return 0x0b, 8, nil
	case string:
		return 0x0c, -1, nil
	default:
		return 0x00, 0, errors.New("Unsupported type")
	}
}

func append_object(buffer *bytes.Buffer, object map[string]interface{}) {
	var countAndSize = make([]uint32, 2)
	var countAndSizeLen = len(countAndSize) * 4

	var keyEntryBuffer, keyBuffer bytes.Buffer
	keySlice := keys(object)
	for _, key := range keySlice {
		var keyOffset uint32 = uint32(buffer.Len() + countAndSizeLen)
		var keyLength uint16 = uint16(len(hack.Slice(key)))
		binary.Write(&keyEntryBuffer, binary.LittleEndian, keyOffset)
		binary.Write(&keyEntryBuffer, binary.LittleEndian, keyLength)
		keyBuffer.Write(hack.Slice(key))
	}

	var valueEntryBytes []byte = make([]byte, len(keySlice)*(1+4))
	var valueBuffer bytes.Buffer
	for i, key := range keySlice {
		value, _ := object[key]
		var jsonTypeLen int
		valueEntryBytes[i*(1+4)], jsonTypeLen, _ = jsonTypeCodeAndLength(value)
		if jsonTypeLen > 0 && jsonTypeLen <= 4 {
			buf := bytes.NewBuffer(valueEntryBytes[i*(1+4)+1 : i*(1+4)+1+jsonTypeLen])
			binary.Write(buf, binary.LittleEndian, value)
		} else {
			var valueOffset = uint32(buffer.Len() + countAndSizeLen + keyEntryBuffer.Len() + keyBuffer.Len() + len(valueEntryBytes) + valueBuffer.Len())
			buf := bytes.NewBuffer(valueEntryBytes[i*(1+4)+1 : i*(1+4)+5])
			binary.Write(buf, binary.LittleEndian, valueOffset)
			valueBuffer.Write(serialize(value))
		}
	}
	countAndSize[1] = uint32(buffer.Len() + countAndSizeLen + keyEntryBuffer.Len() + keyBuffer.Len() + len(valueEntryBytes) + valueBuffer.Len())
	for _, v := range countAndSize {
		binary.Write(buffer, binary.LittleEndian, v)
	}
	buffer.Write(keyEntryBuffer.Bytes())
	buffer.Write(valueEntryBytes)
	buffer.Write(keyBuffer.Bytes())
	buffer.Write(valueBuffer.Bytes())
}

func pop_object(reader *bytes.Reader) map[string]interface{} {
	return map[string]interface{}{}
}

func serialize(in interface{}) []byte {
	var bytesBuffer bytes.Buffer
	var typeCode byte

	typeCode, _, _ = jsonTypeCodeAndLength(in)
	bytesBuffer.WriteByte(typeCode)

	switch x := in.(type) {
	case nil:
		bytesBuffer.WriteByte(0x00)
	case bool:
		if x {
			bytesBuffer.WriteByte(0x01)
		} else {
			bytesBuffer.WriteByte(0x02)
		}
	case int16, uint16, int32, uint32, int64, uint64, float64:
		binary.Write(&bytesBuffer, binary.LittleEndian, x)
	case string:
		var varIntBuf [7]byte
		var varIntLen = binary.PutUvarint(varIntBuf[0:], uint64(len(hack.Slice(x))))
		bytesBuffer.Write(varIntBuf[0:varIntLen])
	case map[string]interface{}:
		append_object(&bytesBuffer, x)
	}
	return bytesBuffer.Bytes()
}

func deserialize(data []byte, out *interface{}) (err error) {
	var reader *bytes.Reader = bytes.NewReader(data)
	typeCode, err := reader.ReadByte()
	if err != nil {
		return err
	}
	switch typeByte {
	case 0x04:
		literal = reader.ReadByte()
		switch literal {
		case 0x00:
			*out = nil
		case 0x01:
			*out = true
		case 0x02:
			*out = false
		}
	case 0x05:
		buf = make([]byte, 2)
		reader.Read(buf)
		var i16 int16
		binary.Read(buf, binary.LittleEndian, &i16)
		*out = i16
	case 0x06:
		buf = make([]byte, 2)
		reader.Read(buf)
		var u16 uint16
		binary.Read(buf, binary.LittleEndian, &u16)
		*out = u16
	case 0x07:
		var i32 int32
		binary.Read(buf, binary.LittleEndian, &i32)
		*out = i32
	case 0x08:
		var u32 uint32
		binary.Read(buf, binary.LittleEndian, &u32)
		*out = u32
	case 0x09:
		var i64 int64
		binary.Read(buf, binary.LittleEndian, &i64)
		*out = i64
	case 0x0a:
		var u64 uint64
		binary.Read(buf, binary.LittleEndian, &u64)
		*out = u64
	case 0x0b:
		var f64 float64
		binary.Read(buf, binary.LittleEndian, &f64)
		*out = f64
	case 0x01:
		var object map[string]interface{} = pop_object(reader)
		*out = object
	}
}
