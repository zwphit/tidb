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

package tidb

import (
	"net/url"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util"
)

// Engine prefix name
const (
	defaultMaxRetries        = 30
	retryInterval     uint64 = 500
)

var (
	stores = make(map[string]kv.Driver)

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	SchemaLease = 1 * time.Second

	// statsLease is the time for reload stats table.
	StatsLease = 3 * time.Second

	// The maximum number of retries to recover from retryable errors.
	commitRetryLimit = 10
)

// RegisterStore registers a kv storage with unique name and its associated Driver.
func RegisterStore(name string, driver kv.Driver) error {
	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// RegisterLocalStore registers a local kv storage with unique name and its associated engine Driver.
func RegisterLocalStore(name string, driver engine.Driver) error {
	d := localstore.Driver{Driver: driver}
	return RegisterStore(name, d)
}

// NewStore creates a kv Storage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// tidb.Open() but with the dbname cut off.
// Examples:
//    goleveldb://relative/path
//    boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func NewStore(path string) (kv.Storage, error) {
	return newStoreWithRetry(path, defaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (kv.Storage, error) {
	url, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	name := strings.ToLower(url.Scheme)
	d, ok := stores[name]
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s kv.Storage
	util.RunWithRetry(maxRetries, retryInterval, func() (bool, error) {
		s, err = d.Open(path)
		return kv.IsRetryableError(err), err
	})
	return s, errors.Trace(err)
}

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	SchemaLease = lease
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	StatsLease = lease
}

// SetCommitRetryLimit setups the maximum number of retries when trying to recover
// from retryable errors.
// Retryable errors are generally refer to temporary errors that are expected to be
// reinstated by retry, including network interruption, transaction conflicts, and
// so on.
func SetCommitRetryLimit(limit int) {
	commitRetryLimit = limit
}
