// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
	"golang.org/x/text/transform"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &asciiFunctionClass{}
	_ functionClass = &concatFunctionClass{}
	_ functionClass = &concatWSFunctionClass{}
	_ functionClass = &leftFunctionClass{}
	_ functionClass = &repeatFunctionClass{}
	_ functionClass = &lowerFunctionClass{}
	_ functionClass = &reverseFunctionClass{}
	_ functionClass = &spaceFunctionClass{}
	_ functionClass = &upperFunctionClass{}
	_ functionClass = &strcmpFunctionClass{}
	_ functionClass = &replaceFunctionClass{}
	_ functionClass = &convertFunctionClass{}
	_ functionClass = &substringFunctionClass{}
	_ functionClass = &substringIndexFunctionClass{}
	_ functionClass = &locateFunctionClass{}
	_ functionClass = &hexFunctionClass{}
	_ functionClass = &unhexFunctionClass{}
	_ functionClass = &trimFunctionClass{}
	_ functionClass = &lTrimFunctionClass{}
	_ functionClass = &rTrimFunctionClass{}
	_ functionClass = &rpadFunctionClass{}
	_ functionClass = &bitLengthFunctionClass{}
	_ functionClass = &charFunctionClass{}
	_ functionClass = &charLengthFunctionClass{}
	_ functionClass = &findInSetFunctionClass{}
	_ functionClass = &fieldFunctionClass{}
	_ functionClass = &makeSetFunctionClass{}
	_ functionClass = &octFunctionClass{}
	_ functionClass = &ordFunctionClass{}
	_ functionClass = &quoteFunctionClass{}
	_ functionClass = &binFunctionClass{}
	_ functionClass = &eltFunctionClass{}
	_ functionClass = &exportSetFunctionClass{}
	_ functionClass = &formatFunctionClass{}
	_ functionClass = &fromBase64FunctionClass{}
	_ functionClass = &toBase64FunctionClass{}
	_ functionClass = &insertFuncFunctionClass{}
	_ functionClass = &instrFunctionClass{}
	_ functionClass = &loadFileFunctionClass{}
	_ functionClass = &lpadFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinASCIISig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinConcatWSSig{}
	_ builtinFunc = &builtinLeftSig{}
	_ builtinFunc = &builtinRepeatSig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinReverseSig{}
	_ builtinFunc = &builtinSpaceSig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinStrcmpSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinConvertSig{}
	_ builtinFunc = &builtinSubstring2ArgsSig{}
	_ builtinFunc = &builtinSubstring3ArgsSig{}
	_ builtinFunc = &builtinSubstringIndexSig{}
	_ builtinFunc = &builtinLocateSig{}
	_ builtinFunc = &builtinHexStrArgSig{}
	_ builtinFunc = &builtinHexIntArgSig{}
	_ builtinFunc = &builtinUnHexSig{}
	_ builtinFunc = &builtinTrim1ArgSig{}
	_ builtinFunc = &builtinTrim2ArgsSig{}
	_ builtinFunc = &builtinTrim3ArgsSig{}
	_ builtinFunc = &builtinLTrimSig{}
	_ builtinFunc = &builtinRTrimSig{}
	_ builtinFunc = &builtinRpadSig{}
	_ builtinFunc = &builtinBitLengthSig{}
	_ builtinFunc = &builtinCharSig{}
	_ builtinFunc = &builtinCharLengthSig{}
	_ builtinFunc = &builtinFindInSetSig{}
	_ builtinFunc = &builtinMakeSetSig{}
	_ builtinFunc = &builtinOctSig{}
	_ builtinFunc = &builtinOrdSig{}
	_ builtinFunc = &builtinQuoteSig{}
	_ builtinFunc = &builtinBinSig{}
	_ builtinFunc = &builtinEltSig{}
	_ builtinFunc = &builtinExportSetSig{}
	_ builtinFunc = &builtinFormatSig{}
	_ builtinFunc = &builtinFromBase64Sig{}
	_ builtinFunc = &builtinToBase64Sig{}
	_ builtinFunc = &builtinInsertFuncSig{}
	_ builtinFunc = &builtinInstrSig{}
	_ builtinFunc = &builtinLoadFileSig{}
	_ builtinFunc = &builtinLpadSig{}
)

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 10
	sig := &builtinLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 3
	sig := &builtinASCIISig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinASCIISig struct {
	baseIntBuiltinFunc
}

// eval evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if len(val) == 0 {
		return 0, false, nil
	}
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	argTps := make([]evalTp, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, tpString)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := range args {
		argType := args[i].GetType()
		if types.IsBinaryStr(argType) {
			bf.tp.Flag |= mysql.BinaryFlag
		}

		if argType.Flen < 0 {
			bf.tp.Flen = mysql.MaxBlobWidth
			log.Warningf("Not Expected: `Flen` of arg[%v] in CONCAT is -1.", i)
		}
		bf.tp.Flen += argType.Flen
	}
	if bf.tp.Flen >= mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinConcatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinConcatSig struct {
	baseStringBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(row, b.ctx.GetSessionVars().StmtCtx)
		if isNull || err != nil {
			return d, isNull, errors.Trace(err)
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	argTps := make([]evalTp, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, tpString)
	}

	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i := range args {
		argType := args[i].GetType()
		if types.IsBinaryStr(argType) {
			bf.tp.Flag |= mysql.BinaryFlag
		}

		// skip seperator param
		if i != 0 {
			if argType.Flen < 0 {
				bf.tp.Flen = mysql.MaxBlobWidth
				log.Warningf("Not Expected: `Flen` of arg[%v] in CONCAT_WS is -1.", i)
			}
			bf.tp.Flen += argType.Flen
		}
	}

	// add seperator
	argsLen := len(args) - 1
	bf.tp.Flen += argsLen - 1

	if bf.tp.Flen >= mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}

	sig := &builtinConcatWSSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinConcatWSSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinConcatWSSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) evalString(row []types.Datum) (string, bool, error) {
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	for i, arg := range args {
		val, isNull, err := arg.EvalString(row, b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return val, isNull, errors.Trace(err)
		}

		if isNull {
			// If the separator is NULL, the result is NULL.
			if i == 0 {
				return val, isNull, nil
			}
			// CONCAT_WS() does not skip empty strings. However,
			// it does skip any NULL values after the separator argument.
			continue
		}

		if i == 0 {
			sep = val
			continue
		}
		strs = append(strs, val)
	}

	// TODO: check whether the length of result is larger than Flen
	return strings.Join(strs, sep), false, nil
}

type leftFunctionClass struct {
	baseFunctionClass
}

func (c *leftFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = args[0].GetType().Flen
	sig := &builtinLeftSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLeftSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinLeftSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var left int64

	sc := b.ctx.GetSessionVars().StmtCtx
	d, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	left, isNull, err = b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	l := int(left)
	if l < 0 {
		l = 0
	} else if l > len(d) {
		l = len(d)
	}
	return d[:l], false, nil
}

type rightFunctionClass struct {
	baseFunctionClass
}

func (c *rightFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = args[0].GetType().Flen
	sig := &builtinRightSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRightSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinRightSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var right int64

	sc := b.ctx.GetSessionVars().StmtCtx
	d, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	right, isNull, err = b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	r := int(right)
	length := len(d)
	if r < 0 {
		r = 0
	} else if r > length {
		r = length
	}
	return d[length-r:], false, nil
}

type repeatFunctionClass struct {
	baseFunctionClass
}

func (c *repeatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = mysql.MaxBlobWidth
	if mysql.HasBinaryFlag(args[0].GetType().Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	sig := &builtinRepeatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRepeatSig struct {
	baseStringBuiltinFunc
}

// eval evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	num, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if num < 1 {
		return "", false, nil
	}
	if num > math.MaxInt32 {
		num = math.MaxInt32
	}

	if int64(len(str)) > int64(b.tp.Flen)/num {
		return "", true, nil
	}
	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	if mysql.HasBinaryFlag(argTp.Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	sig := &builtinLowerSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLowerSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	if types.IsBinaryStr(b.args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToLower(d), false, nil
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinReverseSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinReverseSig struct {
	baseBuiltinFunc
}

// eval evals a builtinReverseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	x := args[0]
	switch x.Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := x.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetString(stringutil.Reverse(s))
		return d, nil
	}
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = mysql.MaxBlobWidth
	sig := &builtinSpaceSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSpaceSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var x int64

	x, isNull, err = b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	if x > mysql.MaxBlobWidth {
		return d, true, nil
	}
	if x < 0 {
		x = 0
	}
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	if mysql.HasBinaryFlag(argTp.Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	sig := &builtinUpperSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinUpperSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	if types.IsBinaryStr(b.args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToUpper(d), false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 2
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStrcmpSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinStrcmpSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row []types.Datum) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	left, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	right, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	res := types.CompareString(left, right)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = c.fixLength(args)
	for _, a := range args {
		if mysql.HasBinaryFlag(a.GetType().Flag) {
			types.SetBinChsClnFlag(bf.tp)
			break
		}
	}
	sig := &builtinReplaceSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

// fixLength calculate the Flen of the return type.
func (c *replaceFunctionClass) fixLength(args []Expression) int {
	charLen := args[0].GetType().Flen
	oldStrLen := args[1].GetType().Flen
	diff := args[2].GetType().Flen - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	oldStr, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	newStr, isNull, err = b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.Replace(str, oldStr, newStr, -1), false, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinConvertSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinConvertSig struct {
	baseBuiltinFunc
}

// eval evals a builtinConvertSig.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// Casting nil to any type returns nil
	if args[0].Kind() != types.KindString {
		return d, nil
	}

	str := args[0].GetString()
	Charset := args[1].GetString()

	if strings.ToLower(Charset) == "ascii" {
		d.SetString(str)
		return d, nil
	} else if strings.ToLower(Charset) == "utf8mb4" {
		d.SetString(str)
		return d, nil
	}

	encoding, _ := charset.Lookup(Charset)
	if encoding == nil {
		return d, errors.Errorf("unknown encoding: %s", Charset)
	}

	target, _, err := transform.String(encoding.NewDecoder(), str)
	if err != nil {
		log.Errorf("Convert %s to %s with error: %v", str, Charset, err)
		return d, errors.Trace(err)
	}
	d.SetString(target)
	return d, nil
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	var (
		bf  baseBuiltinFunc
		err error
	)

	hasLen := len(args) == 3
	if hasLen {
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt, tpInt)
	} else {
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	if mysql.HasBinaryFlag(argType.Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	if hasLen {
		sig := &builtinSubstring3ArgsSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
	}
	sig := &builtinSubstring2ArgsSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSubstring2ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSubstring2ArgsSig, corresponding to substr(str, pos).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var (
		str string
		pos int64
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	pos, isNull, err = b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	strLen := int64(len(str))
	if pos < 0 {
		pos += strLen
	} else {
		pos--
	}
	if pos > strLen || pos < 0 {
		pos = strLen
	}
	return str[pos:], false, nil
}

type builtinSubstring3ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSubstring3ArgsSig, corresponding to substr(str, pos, len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var (
		str         string
		pos, length int64
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	pos, isNull, err = b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	length, isNull, err = b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	strLen := int64(len(str))
	if pos < 0 {
		pos += strLen
	} else {
		pos--
	}
	if pos > strLen || pos < 0 {
		pos = strLen
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < strLen {
		return str[pos:end], false, nil
	}
	return str[pos:], false, nil
}

type substringIndexFunctionClass struct {
	baseFunctionClass
}

func (c *substringIndexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	if mysql.HasBinaryFlag(argType.Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	sig := &builtinSubstringIndexSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSubstringIndexSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var (
		str, delim string
		count      int64
	)
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	delim, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	count, isNull, err = b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	if len(delim) == 0 {
		return "", false, nil
	}

	strs := strings.Split(str, delim)
	start, end := int64(0), int64(len(strs))
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < end {
			start = end - count
		}
	}
	substrs := strs[start:end]
	return strings.Join(substrs, delim), false, nil
}

type locateFunctionClass struct {
	baseFunctionClass
}

func (c *locateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLocateSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLocateSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLocateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// The meaning of the elements of args.
	// args[0] -> SubStr
	// args[1] -> Str
	// args[2] -> Pos

	// eval str
	if args[1].IsNull() {
		return d, nil
	}
	str, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	// eval substr
	if args[0].IsNull() {
		return d, nil
	}
	subStr, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	// eval pos
	pos := int64(0)
	if len(args) == 3 {
		p, err := args[2].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		pos = p - 1
	}
	var ret, subStrLen, sentinel int64
	caseSensitive := false
	if args[0].Kind() == types.KindBytes || args[1].Kind() == types.KindBytes {
		caseSensitive = true
		subStrLen = int64(len(subStr))
		sentinel = int64(len(str)) - subStrLen
	} else {
		subStrLen = int64(len([]rune(subStr)))
		sentinel = int64(len([]rune(strings.ToLower(str)))) - subStrLen
	}

	if pos < 0 || pos > sentinel {
		d.SetInt64(0)
		return d, nil
	} else if subStrLen == 0 {
		d.SetInt64(pos + 1)
		return d, nil
	} else if caseSensitive {
		slice := str[pos:]
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			ret = pos + int64(idx) + 1
		}
	} else {
		slice := string([]rune(strings.ToLower(str))[pos:])
		idx := strings.Index(slice, strings.ToLower(subStr))
		if idx != -1 {
			ret = pos + int64(utf8.RuneCountInString(slice[:idx])) + 1
		}
	}
	d.SetInt64(ret)
	return d, nil
}

type hexFunctionClass struct {
	baseFunctionClass
}

func (c *hexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	switch t := args[0].GetTypeClass(); t {
	case types.ClassString:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Use UTF-8 as default
		bf.tp.Flen = args[0].GetType().Flen * 3 * 2
		sig := &builtinHexStrArgSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	case types.ClassInt, types.ClassReal, types.ClassDecimal:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		bf.tp.Flen = args[0].GetType().Flen * 2
		sig := &builtinHexIntArgSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	default:
		return nil, errors.Errorf("Hex invalid args, need int or string but get %T", t)
	}
}

type builtinHexStrArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) evalString(row []types.Datum) (string, bool, error) {
	d, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.ToUpper(hex.EncodeToString(hack.Slice(d))), false, nil
}

type builtinHexIntArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinHexIntArgSig, corresponding to hex(N)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexIntArgSig) evalString(row []types.Datum) (string, bool, error) {
	x, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	return strings.ToUpper(fmt.Sprintf("%x", uint64(x))), false, nil
}

type unhexFunctionClass struct {
	baseFunctionClass
}

func (c *unhexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	var retFlen int

	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	switch t := args[0].GetTypeClass(); t {
	case types.ClassString:
		// Use UTF-8 as default charset, so there're (Flen * 3 + 1) / 2 byte-pairs
		retFlen = (argType.Flen*3 + 1) / 2

	case types.ClassInt, types.ClassReal, types.ClassDecimal:
		// For number value, there're (Flen + 1) / 2 byte-pairs
		retFlen = (argType.Flen + 1) / 2

	default:
		return nil, errors.Errorf("Unhex invalid args, need int or string but get %T", t)
	}

	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = retFlen
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUnHexSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUnHexSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) evalString(row []types.Datum) (string, bool, error) {
	var bs []byte

	d, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	// Add a '0' to the front, if the length is not the multiple of 2
	if len(d)%2 != 0 {
		d = "0" + d
	}
	bs, err = hex.DecodeString(d)
	if err != nil {
		return "", true, nil
	}
	return string(bs), false, nil
}

const spaceChars = "\n\t\r "

type trimFunctionClass struct {
	baseFunctionClass
}

// The syntax of trim in mysql is 'TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)',
// but we wil convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
func (c *trimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	switch len(args) {
	case 1:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
		if err != nil {
			return nil, errors.Trace(err)
		}
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		if mysql.HasBinaryFlag(argType.Flag) {
			types.SetBinChsClnFlag(bf.tp)
		}
		sig := &builtinTrim1ArgSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	case 2:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
		if err != nil {
			return nil, errors.Trace(err)
		}
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		if mysql.HasBinaryFlag(argType.Flag) {
			types.SetBinChsClnFlag(bf.tp)
		}
		sig := &builtinTrim2ArgsSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	case 3:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		if mysql.HasBinaryFlag(argType.Flag) {
			types.SetBinChsClnFlag(bf.tp)
		}
		sig := &builtinTrim3ArgsSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	default:
		return nil, errors.Trace(c.verifyArgs(args))
	}
}

type builtinTrim1ArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim1ArgSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.Trim(d, spaceChars), false, nil
}

type builtinTrim2ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var str, remstr string

	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	remstr, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	d = trimLeft(str, remstr)
	d = trimRight(d, remstr)
	return d, false, nil
}

type builtinTrim3ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim3ArgsSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var (
		str, remstr  string
		x            int64
		direction    ast.TrimDirectionType
		isRemStrNull bool
	)
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	remstr, isRemStrNull, err = b.args[1].EvalString(row, sc)
	if err != nil {
		return d, isNull, errors.Trace(err)
	}
	x, isNull, err = b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	direction = ast.TrimDirectionType(x)
	if direction == ast.TrimLeading {
		if isRemStrNull {
			d = strings.TrimLeft(str, spaceChars)
		} else {
			d = trimLeft(str, remstr)
		}
	} else if direction == ast.TrimTrailing {
		if isRemStrNull {
			d = strings.TrimRight(str, spaceChars)
		} else {
			d = trimRight(str, remstr)
		}
	} else {
		if isRemStrNull {
			d = strings.Trim(str, spaceChars)
		} else {
			d = trimLeft(str, remstr)
			d = trimRight(d, remstr)
		}
	}
	return d, false, nil
}

type lTrimFunctionClass struct {
	baseFunctionClass
}

func (c *lTrimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	if mysql.HasBinaryFlag(argType.Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	sig := &builtinLTrimSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLTrimSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.TrimLeft(d, spaceChars), false, nil
}

type rTrimFunctionClass struct {
	baseFunctionClass
}

func (c *rTrimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	if mysql.HasBinaryFlag(argType.Flag) {
		types.SetBinChsClnFlag(bf.tp)
	}
	sig := &builtinRTrimSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRTrimSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.TrimRight(d, spaceChars), false, nil
}

func trimLeft(str, remstr string) string {
	for {
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func trimRight(str, remstr string) string {
	for {
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

type rpadFunctionClass struct {
	baseFunctionClass
}

func (c *rpadFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRpadSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRpadSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRpadSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// RPAD(str,len,padstr)
	// args[0] string, args[1] int, args[2] string
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	length, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	l := int(length)

	padStr, err := args[2].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	if l < 0 || (len(str) < l && padStr == "") {
		d.SetNull()
		return d, nil
	}

	tailLen := l - len(str)
	if tailLen > 0 {
		repeatCount := tailLen/len(padStr) + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	d.SetString(str[:l])

	return d, nil
}

type bitLengthFunctionClass struct {
	baseFunctionClass
}

func (c *bitLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 10
	sig := &builtinBitLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinBitLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evaluates a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return int64(len(val) * 8), false, nil
}

type charFunctionClass struct {
	baseFunctionClass
}

func (c *charFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCharSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCharSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCharSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char
func (b *builtinCharSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// The kinds of args are int or string, and the last one represents charset.
	var resultStr string
	var intSlice = make([]int64, 0, len(args)-1)

	for _, datum := range args[:len(args)-1] {
		switch datum.Kind() {
		case types.KindNull:
			continue
		case types.KindString:
			i, err := datum.ToInt64(b.ctx.GetSessionVars().StmtCtx)
			if err != nil {
				d.SetString(resultStr)
				return d, nil
			}
			intSlice = append(intSlice, i)
		case types.KindInt64, types.KindUint64, types.KindMysqlHex, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			x, err := datum.Cast(b.ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeLonglong))
			if err != nil {
				return d, errors.Trace(err)
			}
			intSlice = append(intSlice, x.GetInt64())
		default:
			return d, errors.Errorf("Char invalid args, need int or string but get %T", args[0].GetValue())
		}
	}

	resultStr = string(convertInt64ToBytes(intSlice))

	// The last argument represents the charset name after "using".
	// If it is nil, the default charset utf8 is used.
	argCharset := args[len(args)-1]
	if !argCharset.IsNull() {
		char, err := argCharset.ToString()
		if err != nil {
			return d, errors.Trace(err)
		}

		if strings.ToLower(char) == "ascii" || strings.HasPrefix(strings.ToLower(char), "utf8") {
			d.SetString(resultStr)
			return d, nil
		}

		encoding, _ := charset.Lookup(char)
		if encoding == nil {
			return d, errors.Errorf("unknown encoding: %s", char)
		}

		resultStr, _, err = transform.String(encoding.NewDecoder(), resultStr)
		if err != nil {
			log.Errorf("Convert %s to %s with error: %v", resultStr, char, err)
			return d, errors.Trace(err)
		}
	}
	d.SetString(resultStr)
	return d, nil
}

func convertInt64ToBytes(ints []int64) []byte {
	var buf bytes.Buffer
	for i := len(ints) - 1; i >= 0; i-- {
		var count int
		v := ints[i]
		for count < 4 {
			buf.WriteByte(byte(v & 0xff))
			v = v >> 8
			if v == 0 {
				break
			}
			count++
		}
	}
	s := buf.Bytes()
	reverseByteSlice(s)
	return s
}

func reverseByteSlice(slice []byte) {
	var start int
	var end = len(slice) - 1
	for start < end {
		slice[start], slice[end] = slice[end], slice[start]
		start++
		end--
	}
}

type charLengthFunctionClass struct {
	baseFunctionClass
}

func (c *charLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCharLengthSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCharLengthSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCharLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch args[0].Kind() {
	case types.KindNull:
		return d, nil
	default:
		s, err := args[0].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		r := []rune(s)
		d.SetInt64(int64(len(r)))
		return d, nil
	}
}

type findInSetFunctionClass struct {
	baseFunctionClass
}

func (c *findInSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFindInSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFindInSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFindInSetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
// TODO: This function can be optimized by using bit arithmetic when the first argument is
// a constant string and the second is a column of type SET.
func (b *builtinFindInSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// args[0] -> Str
	// args[1] -> StrList
	if args[0].IsNull() || args[1].IsNull() {
		return
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	strlst, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetInt64(0)
	if len(strlst) == 0 {
		return
	}
	for i, s := range strings.Split(strlst, ",") {
		if s == str {
			d.SetInt64(int64(i + 1))
			return
		}
	}
	return
}

type fieldFunctionClass struct {
	baseFunctionClass
}

func (c *fieldFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFieldSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFieldSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFieldSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
// Returns the index (position) of arg0 in the arg1, arg2, arg3, ... list.
// Returns 0 if arg0 is not found.
// If arg0 is NULL, the return value is 0 because NULL fails equality comparison with any value.
// If all arguments are strings, all arguments are compared as strings.
// If all arguments are numbers, they are compared as numbers.
// Otherwise, the arguments are compared as double.
func (b *builtinFieldSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d.SetInt64(0)
	if args[0].IsNull() {
		return
	}
	var (
		pos                  int64
		allString, allNumber bool
		newArgs              []types.Datum
	)
	allString, allNumber = true, true
	for i := 0; i < len(args) && (allString || allNumber); i++ {
		switch args[i].Kind() {
		case types.KindInt64, types.KindUint64, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			allString = false
		case types.KindString, types.KindBytes:
			allNumber = false
		default:
			allString, allNumber = false, false
		}
	}
	newArgs, err = argsToSpecifiedType(args, allString, allNumber, b.ctx)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg0, sc := newArgs[0], b.ctx.GetSessionVars().StmtCtx
	for i, curArg := range newArgs[1:] {
		cmpResult, _ := arg0.CompareDatum(sc, curArg)
		if cmpResult == 0 {
			pos = int64(i + 1)
			break
		}
	}
	d.SetInt64(pos)
	return d, errors.Trace(err)
}

// argsToSpecifiedType converts the type of all arguments in args into string type or double type.
func argsToSpecifiedType(args []types.Datum, allString bool, allNumber bool, ctx context.Context) (newArgs []types.Datum, err error) {
	if allNumber { // If all arguments are numbers, they can be compared directly without type converting.
		return args, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	newArgs = make([]types.Datum, len(args))
	for i, arg := range args {
		if allString {
			str, err := arg.ToString()
			if err != nil {
				return newArgs, errors.Trace(err)
			}
			newArgs[i] = types.NewStringDatum(str)
		} else {
			// If error occurred when convert arg to float64, ignore it and set f as 0.
			f, _ := arg.ToFloat64(sc)
			newArgs[i] = types.NewFloat64Datum(f)
		}
	}
	return
}

type makeSetFunctionClass struct {
	baseFunctionClass
}

func (c *makeSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinMakeSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinMakeSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinMakeSetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetNull()
		return
	}
	var (
		arg0 int64
		sets []string
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, err = args[0].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			continue
		}
		if arg0&(1<<uint(i-1)) > 0 {
			str, err1 := args[i].ToString()
			if err1 != nil {
				return d, errors.Trace(err1)
			}
			sets = append(sets, str)
		}
	}

	d.SetString(strings.Join(sets, ","))
	return d, errors.Trace(err)
}

type octFunctionClass struct {
	baseFunctionClass
}

func (c *octFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinOctSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinOctSig struct {
	baseBuiltinFunc
}

// eval evals a builtinOctSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		negative bool
		overflow bool
	)
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	n, err := arg.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	n = getValidPrefix(strings.TrimSpace(n), 10)
	if len(n) == 0 {
		d.SetString("0")
		return d, nil
	}
	if n[0] == '-' {
		negative = true
		n = n[1:]
	}
	val, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		if numError, ok := err.(*strconv.NumError); ok {
			if numError.Err == strconv.ErrRange {
				overflow = true
			} else {
				return d, errors.Trace(err)
			}
		} else {
			return d, errors.Trace(err)
		}
	}

	if negative && !overflow {
		val = -val
	}
	str := strconv.FormatUint(val, 8)
	d.SetString(str)
	return d, nil
}

type ordFunctionClass struct {
	baseFunctionClass
}

func (c *ordFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 10
	sig := &builtinOrdSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinOrdSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) evalInt(row []types.Datum) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if len(str) == 0 {
		return 0, false, nil
	}

	_, size := utf8.DecodeRuneInString(str)
	leftMost := str[:size]
	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		result += int64(leftMost[i]) * factor
		factor *= 256
	}

	return result, false, nil
}

type quoteFunctionClass struct {
	baseFunctionClass
}

func (c *quoteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinQuoteSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinQuoteSig struct {
	baseBuiltinFunc
}

// eval evals a builtinQuoteSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	var (
		str    string
		buffer bytes.Buffer
	)
	str, err = args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	runes := []rune(str)
	buffer.WriteRune('\'')
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\\', '\'':
			buffer.WriteRune('\\')
			buffer.WriteRune(runes[i])
		case 0:
			buffer.WriteRune('\\')
			buffer.WriteRune('0')
		case '\032':
			buffer.WriteRune('\\')
			buffer.WriteRune('Z')
		default:
			buffer.WriteRune(runes[i])
		}
	}
	buffer.WriteRune('\'')
	d.SetString(buffer.String())
	return d, errors.Trace(err)
}

type binFunctionClass struct {
	baseFunctionClass
}

func (c *binFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinBinSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinBinSig struct {
	baseBuiltinFunc
}

// eval evals a builtinBinSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_bin
func (b *builtinBinSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg := args[0]
	sc := b.ctx.GetSessionVars().StmtCtx
	if arg.IsNull() || (arg.Kind() == types.KindString && arg.GetString() == "") {
		return d, nil
	}

	num, err := arg.ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	bits := fmt.Sprintf("%b", uint64(num))
	d.SetString(bits)
	return d, nil
}

type eltFunctionClass struct {
	baseFunctionClass
}

func (c *eltFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinEltSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinEltSig struct {
	baseBuiltinFunc
}

// eval evals a builtinEltSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_elt
func (b *builtinEltSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	index, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}

	argsLength := int64(len(args))
	if index < 1 || index > (argsLength-1) {
		return d, nil
	}

	result, err := args[index].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(result)

	return d, nil
}

type exportSetFunctionClass struct {
	baseFunctionClass
}

func (c *exportSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinExportSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinExportSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinExportSetSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_export-set
func (b *builtinExportSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		bits         uint64
		on           string
		off          string
		separator    = ","
		numberOfBits = 64
	)
	switch len(args) {
	case 5:
		var arg int64
		arg, err = args[4].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		if arg >= 0 && arg < 64 {
			numberOfBits = int(arg)
		}
		fallthrough
	case 4:
		separator, err = args[3].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		fallthrough
	case 3:
		arg, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		bits = uint64(arg)
		on, err = args[1].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		off, err = args[2].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
	}
	var result string
	for i := 0; i < numberOfBits; i++ {
		if bits&1 > 0 {
			result += on
		} else {
			result += off
		}
		bits >>= 1
		if i < numberOfBits-1 {
			result += separator
		}
	}
	d.SetString(result)
	return d, nil
}

type formatFunctionClass struct {
	baseFunctionClass
}

func (c *formatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFormatSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFormatSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFormatSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_format
func (b *builtinFormatSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetNull()
		return
	}
	arg0, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	arg1, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	var arg2 string

	if len(args) == 2 {
		arg2 = "en_US"
	} else if len(args) == 3 {
		arg2, err = args[2].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
	}

	formatString, err := mysql.GetLocaleFormatFunction(arg2)(arg0, arg1)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetString(formatString)
	return d, nil
}

type fromBase64FunctionClass struct {
	baseFunctionClass
}

func (c *fromBase64FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinFromBase64Sig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinFromBase64Sig struct {
	baseBuiltinFunc
}

// eval evals a builtinFromBase64Sig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	str = strings.Replace(str, "\t", "", -1)
	str = strings.Replace(str, " ", "", -1)
	result, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return d, errors.Trace(err)
	}
	// Set the result to be of type []byte
	d.SetBytes(result)
	return d, nil

}

type toBase64FunctionClass struct {
	baseFunctionClass
}

func (c *toBase64FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = base64NeededEncodedLength(bf.args[0].GetType().Flen)
	sig := &builtinToBase64Sig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinToBase64Sig struct {
	baseStringBuiltinFunc
}

// base64NeededEncodedLength return the base64 encoded string length.
func base64NeededEncodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 {
		// len(arg)            -> len(to_base64(arg))
		// 6827690988321067803 -> 9223372036854775804
		// 6827690988321067804 -> -9223372036854775808
		if n > 6827690988321067803 {
			return -1
		}
	} else {
		// len(arg)   -> len(to_base64(arg))
		// 1589695686 -> 2147483645
		// 1589695687 -> -2147483646
		if n > 1589695686 {
			return -1
		}
	}

	length := (n + 2) / 3 * 4
	return length + (length-1)/76
}

// evalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	if b.tp.Flen == -1 || b.tp.Flen > mysql.MaxBlobWidth {
		return "", true, nil
	}

	//encode
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	//A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
	count := len(result)
	if count > 76 {
		resultArr := splitToSubN(result, 76)
		result = strings.Join(resultArr, "\n")
	}

	return result, false, nil
}

// splitToSubN splits a string every n runes into a string[]
func splitToSubN(s string, n int) []string {
	subs := make([]string, 0, len(s)/n+1)
	for len(s) > n {
		subs = append(subs, s[:n])
		s = s[n:]
	}
	subs = append(subs, s)
	return subs
}

type insertFuncFunctionClass struct {
	baseFunctionClass
}

func (c *insertFuncFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinInsertFuncSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinInsertFuncSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInsertFuncSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertFuncSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	// Returns NULL if any argument is NULL.
	if args[0].IsNull() || args[1].IsNull() || args[2].IsNull() || args[3].IsNull() {
		return
	}

	str0, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	str := []rune(str0)
	strLen := len(str)

	posInt64, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	pos := int(posInt64)

	lenInt64, err := args[2].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	length := int(lenInt64)

	newstr, err := args[3].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	var s string
	if pos < 1 || pos > strLen {
		s = str0
	} else if length > strLen-pos+1 || length < 0 {
		s = string(str[0:pos-1]) + newstr
	} else {
		s = string(str[0:pos-1]) + newstr + string(str[pos+length-1:])
	}

	d.SetString(s)
	return d, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinInstrSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinInstrSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInstrSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_instr
func (b *builtinInstrSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	// INSTR(str, substr)
	if args[0].IsNull() || args[1].IsNull() {
		return d, nil
	}

	var str, substr string
	if str, err = args[0].ToString(); err != nil {
		return d, errors.Trace(err)
	}
	if substr, err = args[1].ToString(); err != nil {
		return d, errors.Trace(err)
	}

	// INSTR performs case **insensitive** search by default, while at least one argument is binary string
	// we do case sensitive search.
	var caseSensitive bool
	if args[0].Kind() == types.KindBytes || args[1].Kind() == types.KindBytes {
		caseSensitive = true
	}

	var pos, idx int
	if caseSensitive {
		idx = strings.Index(str, substr)
	} else {
		idx = strings.Index(strings.ToLower(str), strings.ToLower(substr))
	}
	if idx == -1 {
		pos = 0
	} else {
		if caseSensitive {
			pos = idx + 1
		} else {
			pos = utf8.RuneCountInString(str[:idx]) + 1
		}
	}
	d.SetInt64(int64(pos))
	return d, nil
}

type loadFileFunctionClass struct {
	baseFunctionClass
}

func (c *loadFileFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLoadFileSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLoadFileSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLoadFileSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_load-file
func (b *builtinLoadFileSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("load_file")
}

type lpadFunctionClass struct {
	baseFunctionClass
}

func (c *lpadFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLpadSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLpadSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLpadSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lpad
func (b *builtinLpadSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// LPAD(str,len,padstr)
	// args[0] string, args[1] int, args[2] string
	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	length, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	l := int(length)

	padStr, err := args[2].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	if l < 0 || (len(str) < l && padStr == "") {
		return d, nil
	}

	tailLen := l - len(str)
	if tailLen > 0 {
		repeatCount := tailLen/len(padStr) + 1
		str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
	}
	d.SetString(str[:l])

	return d, nil
}
