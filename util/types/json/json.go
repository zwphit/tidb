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

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Json is for MySQL Json type.
type Json interface {
	// ParseFromString parses a json from string.
	ParseFromString(s string) error
	// DumpToString dumps itself to string.
	DumpToString() string
	// Serialize means serialize itself into bytes.
	Serialize() []byte
	// Deserialize means deserialize a json from bytes.
	Deserialize(bytes []byte)

	// Extract is used for json_extract function.
	Extract(pathExpr string) (Json, error)
	// Unquote is used for json_unquote function.
	Unquote() (string, error)
}

// CreateJson will create a json with bson as serde format and nil as data.
func CreateJson(j interface{}) Json {
	return &jsonImpl{
		json: j,
	}
}

// CompareJson compares two json object.
func CompareJson(j1 Json, j2 Json) (int, error) {
	s1 := j1.Serialize()
	s2 := j2.Serialize()
	return bytes.Compare(s1, s2), nil
}

var (
	// Error codes and messages about json.
	ErrInvalidJSONText = terror.ClassJSON.New(mysql.ErrInvalidJSONText, mysql.MySQLErrName[mysql.ErrInvalidJSONText])
	ErrInvalidJSONPath = terror.ClassJSON.New(mysql.ErrInvalidJSONPath, mysql.MySQLErrName[mysql.ErrInvalidJSONPath])
	ErrInvalidJSONData = terror.ClassJSON.New(mysql.ErrInvalidJSONData, mysql.MySQLErrName[mysql.ErrInvalidJSONData])
)

func init() {
	terror.ErrClassToMySQLCodes[terror.ClassJSON] = map[terror.ErrCode]uint16{
		mysql.ErrInvalidJSONText: mysql.ErrInvalidJSONText,
		mysql.ErrInvalidJSONPath: mysql.ErrInvalidJSONPath,
		mysql.ErrInvalidJSONData: mysql.ErrInvalidJSONData,
	}
}
