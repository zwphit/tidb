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
	"fmt"

	"github.com/juju/errors"
)

// Type returns type of JSON as string.
func (j JSON) Type() string {
	switch j.typeCode {
	case typeCodeObject:
		return "OBJECT"
	case typeCodeArray:
		return "ARRAY"
	case typeCodeLiteral:
		switch byte(j.i64) {
		case jsonLiteralNil:
			return "NULL"
		default:
			return "BOOLEAN"
		}
	case typeCodeInt64:
		return "INTEGER"
	case typeCodeFloat64:
		return "DOUBLE"
	case typeCodeString:
		return "STRING"
	default:
		msg := fmt.Sprintf(unknownTypeCodeErrorMsg, j.typeCode)
		panic(msg)
	}
}

// Extract receives several path expressions as arguments, matches them in j, and returns:
//  ret: target JSON matched any path expressions. maybe autowrapped as an array.
//  found: true if any path expressions matched.
func (j JSON) Extract(pathExprList []PathExpression) (ret JSON, found bool) {
	elemList := make([]JSON, 0, len(pathExprList))
	for _, pathExpr := range pathExprList {
		elemList = append(elemList, extract(j, pathExpr)...)
	}
	if len(elemList) == 0 {
		found = false
	} else if len(pathExprList) == 1 && len(elemList) == 1 {
		found = true
		ret = elemList[0]
	} else {
		found = true
		ret.typeCode = typeCodeArray
		ret.array = append(ret.array, elemList...)
	}
	return
}

// Unquote is for JSON_UNQUOTE.
func (j JSON) Unquote() string {
	switch j.typeCode {
	case typeCodeString:
		return j.str
	default:
		return j.String()
	}
}

// extract is used by Extract.
// NOTE: the return value will share something with j.
func extract(j JSON, pathExpr PathExpression) (ret []JSON) {
	if len(pathExpr.legs) == 0 {
		return []JSON{j}
	}
	var currentLeg = pathExpr.legs[0]
	pathExpr.legs = pathExpr.legs[1:]
	if currentLeg.isArrayIndex && j.typeCode == typeCodeArray {
		if currentLeg.arrayIndex == -1 {
			for _, child := range j.array {
				ret = append(ret, extract(child, pathExpr)...)
			}
		} else if currentLeg.arrayIndex < len(j.array) {
			childRet := extract(j.array[currentLeg.arrayIndex], pathExpr)
			ret = append(ret, childRet...)
		}
	} else if !currentLeg.isArrayIndex && j.typeCode == typeCodeObject {
		var key = pathExpr.raw[currentLeg.start+1 : currentLeg.end]
		if len(key) == 1 && key[0] == '*' {
			for _, child := range j.object {
				ret = append(ret, extract(child, pathExpr)...)
			}
		} else if child, ok := j.object[key]; ok {
			childRet := extract(child, pathExpr)
			ret = append(ret, childRet...)
		}
	}
	return
}

// Merge merges suffixes into j according the following rules:
// 1) adjacent arrays are merged to a single array;
// 2) adjacent object are merged to a single object;
// 3) a scalar value is autowrapped as an array before merge;
// 4) an adjacent array and object are merged by autowrapping the object as an array.
func (j *JSON) Merge(suffixes []JSON) {
	switch j.typeCode {
	case typeCodeArray, typeCodeObject:
	default:
		firstElem := *j
		*j = CreateJSON(nil)
		j.typeCode = typeCodeArray
		j.array = []JSON{firstElem}
	}
	for i := 0; i < len(suffixes); i++ {
		suffix := suffixes[i]
		switch j.typeCode {
		case typeCodeArray:
			if suffix.typeCode == typeCodeArray {
				// rule (1)
				for _, elem := range suffix.array {
					j.array = append(j.array, elem)
				}
			} else {
				// rule (3), (4)
				j.array = append(j.array, suffix)
			}
		case typeCodeObject:
			if suffix.typeCode == typeCodeObject {
				// rule (2)
				for key := range suffix.object {
					j.object[key] = suffix.object[key]
				}
			} else {
				// rule (4)
				firstElem := *j
				*j = CreateJSON(nil)
				j.typeCode = typeCodeArray
				j.array = []JSON{firstElem}
				i--
			}
		}
	}
	return
}

// Set inserts or updates data in j. All path expressions cannot contains * or ** wildcard.
func (j *JSON) Set(pathExprList []PathExpression, values []JSON) (err error) {
	if len(pathExprList) != len(values) {
		// TODO should return 1582(42000)
		return errors.New("Incorrect parameter count")
	}
	for i := 0; i < len(pathExprList); i++ {
		pathExpr := pathExprList[i]
		if pathExpr.flags.containsAnyAsterisk() {
			// TODO should return 3149(42000)
			return errors.New("Invalid path expression")
		}
		value := values[i]
		*j = set(*j, pathExpr, value)
	}
	return
}

func set(j JSON, pathExpr PathExpression, value JSON) JSON {
	if len(pathExpr.legs) == 0 {
		return value
	}
	currentLeg := pathExpr.legs[0]
	pathExpr.legs = pathExpr.legs[1:]
	if currentLeg.isArrayIndex && j.typeCode == typeCodeArray {
		var index = currentLeg.arrayIndex
		if len(j.array) > index {
			j.array[index] = set(j.array[index], pathExpr, value)
		} else if len(pathExpr.legs) == 0 {
			j.array = append(j.array, value)
		}
	} else if !currentLeg.isArrayIndex && j.typeCode == typeCodeObject {
		var key = pathExpr.raw[currentLeg.start+1 : currentLeg.end]
		if child, ok := j.object[key]; ok {
			j.object[key] = set(child, pathExpr, value)
		} else if len(pathExpr.legs) == 0 {
			j.object[key] = value
		}
	}
	return j
}
