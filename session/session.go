// Copyright 2017 The ql Authors. All rights reserved.
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

package session

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util"
)

// Session context
type Session interface {
	context.Context
	Status() uint16                              // Flag of current status, such as autocommit.
	LastInsertID() uint64                        // LastInsertID is the last inserted auto_increment ID.
	AffectedRows() uint64                        // Affected rows by latest executed stmt.
	Execute(sql string) ([]ast.RecordSet, error) // Execute a sql statement.
	String() string                              // String is used to debug.
	CommitTxn() error
	RollbackTxn() error
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(stmtID uint32, param ...interface{}) (ast.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	SetSessionManager(util.SessionManager)
	Close()
	Auth(user string, auth []byte, salt []byte) bool
	// Cancel the execution of current transaction.
	Cancel()
	ShowProcess() util.ProcessInfo
	// PrePareTxnCtx is exported for test.
	PrepareTxnCtx()
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	return nil, nil
}

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}
