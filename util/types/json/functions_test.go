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

	. "github.com/pingcap/check"
)

func (s *testJSONSuite) TestJSONType(c *C) {
	j1 := parseFromStringPanic(`{"a": "b"}`)
	j2 := parseFromStringPanic(`["a", "b"]`)
	j3 := parseFromStringPanic(`3`)
	j4 := parseFromStringPanic(`3.0`)
	j5 := parseFromStringPanic(`null`)
	j6 := parseFromStringPanic(`true`)
	var jList = []struct {
		In  JSON
		Out string
	}{
		{j1, "OBJECT"},
		{j2, "ARRAY"},
		{j3, "INTEGER"},
		{j4, "DOUBLE"},
		{j5, "NULL"},
		{j6, "BOOLEAN"},
	}
	for _, j := range jList {
		c.Assert(j.In.Type(), Equals, j.Out)
	}
}

func (s *testJSONSuite) TestJSONExtract(c *C) {
	j1 := parseFromStringPanic(`{"a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"]}`)
	j2 := parseFromStringPanic(`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	var caseList = []struct {
		j               JSON
		pathExprStrings []string
		expected        JSON
		found           bool
		err             error
	}{
		// test extract with only one path expression.
		{j1, []string{"$.a"}, j1.object["a"], true, nil},
		{j2, []string{"$.a"}, CreateJSON(nil), false, nil},
		{j1, []string{"$[0]"}, CreateJSON(nil), false, nil},
		{j2, []string{"$[0]"}, j2.array[0], true, nil},
		{j1, []string{"$.a[2].aa"}, CreateJSON("bb"), true, nil},
		{j1, []string{"$.a[*].aa"}, parseFromStringPanic(`["bb", "cc"]`), true, nil},
		{j1, []string{"$.*[0]"}, parseFromStringPanic(`[1, "d"]`), true, nil},

		// test extract with multi path expressions.
		{j1, []string{"$.a", "$[0]"}, parseFromStringPanic(`[[1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}]]`), true, nil},
		{j2, []string{"$.a", "$[0]"}, parseFromStringPanic(`[{"a": 1, "b": true}]`), true, nil},
	}

	for _, caseItem := range caseList {
		var pathExprList = make([]PathExpression, 0)
		for _, peStr := range caseItem.pathExprStrings {
			pe, err := ParseJSONPathExpr(peStr)
			c.Assert(err, IsNil)
			pathExprList = append(pathExprList, pe)
		}

		expected, found := caseItem.j.Extract(pathExprList)
		c.Assert(found, Equals, caseItem.found)
		if found {
			b1 := Serialize(expected)
			b2 := Serialize(caseItem.expected)
			c.Assert(bytes.Compare(b1, b2), Equals, 0)
		}
	}
}

func (s *testJSONSuite) TestJSONUnquote(c *C) {
	var caseList = []struct {
		j        JSON
		unquoted string
	}{
		{j: parseFromStringPanic(`3`), unquoted: "3"},
		{j: parseFromStringPanic(`"3"`), unquoted: "3"},
		{j: parseFromStringPanic(`true`), unquoted: "true"},
		{j: parseFromStringPanic(`null`), unquoted: "null"},
		{j: parseFromStringPanic(`{"a": [1, 2]}`), unquoted: `{"a":[1,2]}`},
	}
	for _, caseItem := range caseList {
		c.Assert(caseItem.j.Unquote(), Equals, caseItem.unquoted)
	}
}

func (s *testJSONSuite) TestJSONMerge(c *C) {
	jstr1 := `{"a": 1}`
	jstr2 := `{"b": 2}`
	jstr3 := `[1]`
	jstr4 := `[2]`
	jstr5 := `4`
	var caseList = []struct {
		base     JSON
		suffixes []JSON
		expected JSON
	}{
		{parseFromStringPanic(jstr1), []JSON{parseFromStringPanic(jstr2)}, parseFromStringPanic(`{"a": 1, "b": 2}`)},
		{parseFromStringPanic(jstr3), []JSON{parseFromStringPanic(jstr4)}, parseFromStringPanic(`[1, 2]`)},
		{parseFromStringPanic(jstr1), []JSON{parseFromStringPanic(jstr3)}, parseFromStringPanic(`[{"a": 1}, 1]`)},
		{parseFromStringPanic(jstr3), []JSON{parseFromStringPanic(jstr1)}, parseFromStringPanic(`[1, {"a": 1}]`)},
		{parseFromStringPanic(jstr1), []JSON{parseFromStringPanic(jstr5)}, parseFromStringPanic(`[{"a": 1}, 4]`)},
		{parseFromStringPanic(jstr3), []JSON{parseFromStringPanic(jstr5)}, parseFromStringPanic(`[1, 4]`)},
		{parseFromStringPanic(jstr5), []JSON{parseFromStringPanic(jstr1)}, parseFromStringPanic(`[4, {"a": 1}]`)},
		{parseFromStringPanic(jstr5), []JSON{parseFromStringPanic(jstr3)}, parseFromStringPanic(`[4, 1]`)},
	}

	for _, caseItem := range caseList {
		caseItem.base.Merge(caseItem.suffixes)
		cmp, err := CompareJSON(caseItem.base, caseItem.expected)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testJSONSuite) TestJSONSetIsertReplace(c *C) {
	var base = parseFromStringPanic(`null`)
	var caseList = []struct {
		setField string
		setValue JSON
		mt       ModifyType
		expected JSON
	}{
		{"$", parseFromStringPanic(`{}`), ModifySet, parseFromStringPanic(`{}`)},
		{"$.a", parseFromStringPanic(`[]`), ModifySet, parseFromStringPanic(`{"a": []}`)},
		{"$.a[1]", parseFromStringPanic(`3`), ModifySet, parseFromStringPanic(`{"a": [3]}`)},

		// won't modify base because path doesn't exist.
		{"$.b[1]", parseFromStringPanic(`3`), ModifySet, parseFromStringPanic(`{"a": [3]}`)},
		{"$.a[2].b", parseFromStringPanic(`3`), ModifySet, parseFromStringPanic(`{"a": [3]}`)},

		// won't modify because of modify type.
		{"$.a[1]", parseFromStringPanic(`30`), ModifyInsert, parseFromStringPanic(`{"a": [3]}`)},
		{"$.a[2]", parseFromStringPanic(`30`), ModifyReplace, parseFromStringPanic(`{"a": [3]}`)},
	}
	for _, caseItem := range caseList {
		pathExpr, err := ParseJSONPathExpr(caseItem.setField)
		c.Assert(err, IsNil)

		err = base.SetInsertReplace([]PathExpression{pathExpr}, []JSON{caseItem.setValue}, caseItem.mt)
		c.Assert(err, IsNil)

		cmp, err := CompareJSON(base, caseItem.expected)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}
