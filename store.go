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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	stores = make(map[string]kv.Driver)
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
