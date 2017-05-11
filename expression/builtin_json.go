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

package expression

import (
	"github.com/juju/errors"
	//"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
	"github.com/pingcap/tipb/go-tipb"
)

func argDatumToJSON(jsond types.Datum, errfmt string, position int) (jsonObj json.JSON, err error) {
	switch kind := jsond.Kind(); kind {
	case types.KindString, types.KindBytes:
		jsonObj = json.CreateJSON(nil)
		jsonObj.ParseFromString(jsond.GetString())
	case types.KindMysqlJSON:
		jsonObj = jsond.GetMysqlJSON()
	case types.KindNull:
		jsonObj = nil
		return
	default:
		err = json.ErrInvalidJSONData.Gen(errfmt, position)
		return
	}
	return
}

func argDatumToString(pathd types.Datum, errfmt string, position int) (path string, err error) {
	switch kind := pathd.Kind(); kind {
	case types.KindString, types.KindBytes:
		path = pathd.GetString()
	default:
		err = json.ErrInvalidJSONPath.Gen(errfmt, position)
		return
	}
	return
}

// JSONExtract do really json_extract(jsond, pathd) work.
func JSONExtract(jsond, pathd types.Datum) (d types.Datum, err error) {
	check_err := func() {
		if err != nil {
			return
		}
	}

	jsonObj, err := argDatumToJSON(jsond, "invalid json data in argument %d", 0)
	check_err()
	path, err := argDatumToString(pathd, "invalid json path in argument %d", 1)
	check_err()

	if jsonObj == nil {
		return
	}

	jsonObj, err = jsonObj.Extract(path)
	check_err()
	d.SetValue(jsonObj)
	return
}

// JSONUnquote do really json_unquote(jsond) work.
func JSONUnquote(jsond types.Datum) (d types.Datum, err error) {
	check_err := func() {
		if err != nil {
			return
		}
	}
	jsonObj, err := argDatumToJSON(jsond, "invalid json data in argument %d", 0)
	check_err()
	if jsonObj == nil {
		return
	}
	unquoted, err := jsonObj.Unquote()
	check_err()
	d.SetValue(unquoted)
	return
}

var (
	_ functionClass = &jsonExtractFunctionClass{}
)

var jsonFunctionNameToPB = map[string]tipb.ExprType{
//ast.JSONType:     tipb.ExprType_JSONType,
//ast.JSONExtract:  tipb.ExprType_JSONExtract,
//ast.JSONValid:    tipb.ExprType_JSONValid,
//ast.JSONObject:   tipb.ExprType_JSONObject,
//ast.JSONArray:    tipb.ExprType_JSONArray,
//ast.JSONMerge:    tipb.ExprType_JSONMerge,
//ast.JSONSet:      tipb.ExprType_JSONSet,
//ast.JSONInsert:   tipb.ExprType_JSONInsert,
//ast.JSONReplace:  tipb.ExprType_JSONReplace,
//ast.JSONRemove:   tipb.ExprType_JSONRemove,
//ast.JSONContains: tipb.ExprType_JSONContains,
//ast.JSONUnquote:  tipb.ExprType_JSONUnquote,
}

type jsonExtractFunctionClass struct {
	baseFunctionClass
}

func (c *jsonExtractFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONExtractSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinJSONExtractSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONExtractSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return JSONExtract(args[0], args[1])
}

type jsonUnquoteFunctionClass struct {
	baseFunctionClass
}

func (c *jsonUnquoteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinJSONUnquoteSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinJSONUnquoteSig struct {
	baseBuiltinFunc
}

func (b *builtinJSONUnquoteSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return JSONUnquote(args[0])
}
