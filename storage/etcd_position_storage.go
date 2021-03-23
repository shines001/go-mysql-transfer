/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package storage

import (
	"encoding/json"

	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/etcds"
)

type etcdPositionStorage struct {
}

func (s *etcdPositionStorage) Initialize() error {
	data, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err
	}

	err = etcds.CreateIfNecessary(global.Cfg().ZkPositionDir(), string(data), _etcdOps)
	if err != nil {
		return err
	}

	data, err = json.Marshal(mysql.MysqlGTIDSet{})
	if err != nil {
		return err
	}

	err = etcds.CreateIfNecessary(global.Cfg().ZkGtidDir(), string(data), _etcdOps)
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdPositionStorage) Save(pos mysql.Position, gtid mysql.MysqlGTIDSet) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	err = etcds.Save(global.Cfg().ZkPositionDir(), string(data), _etcdOps)
	if err != nil {
		return err
	}

	data, err = json.Marshal(gtid)
	if err != nil {
		return err
	}
	return etcds.Save(global.Cfg().ZkGtidDir(), string(data), _etcdOps)
}

func (s *etcdPositionStorage) Get() (mysql.Position, mysql.MysqlGTIDSet, error) {
	var pos mysql.Position
	var gtid mysql.MysqlGTIDSet

	data, _, err := etcds.Get(global.Cfg().ZkPositionDir(), _etcdOps)
	if err != nil {
		return pos, gtid, err
	}

	err = json.Unmarshal(data, &pos)
	if err != nil {
		return pos, gtid, err
	}

	data, _, err = etcds.Get(global.Cfg().ZkGtidDir(), _etcdOps)
	if err != nil {
		return pos, gtid, err
	}

	err = json.Unmarshal(data, &gtid)

	return pos, gtid, err
}
