// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gamedb

type CacheConf struct {
	Tag  string `json:"tag"`
	Addr string `json:"addr"`
	Pwd  string `json:"pwd"`
	Db   int    `json:"db"`
}

type DBConf struct {
	Tag     string `json:"tag"`
	Addr    string `json:"addr"`
	Port    uint16 `json:"port"`
	DbName  string `json:"name"`
	Acc     string `json:"acc"`
	Pwd     string `json:"pwd"`
	Charset string `json:"charset"`
}

type WorkerConf struct {
	Tag                   string            `json:"tag"`
	CacheKey              string            `json:"cache_key"`
	MapCacheField2DbField map[string]string `json:"field_map"`
	TableName             string            `json:"table_name"`
	RowObj                string            `json:"row_obj"`
	InsertTag             string            `json:"insert_tag"`
	SelectKeyTag          string            `json:"select_key_tag"`
	UpdateKeyTag          string            `json:"update_key_tag"`
	UpdateIgnoreTag       string            `json:"update_ignore_tag"`
}

type StorageConf struct {
	Cache   *CacheConf    `json:"cache"`
	Db      *DBConf       `json:"db"`
	Workers []*WorkerConf `json:"workers"`
}

type Config struct {
	SaveIntv  int            `json:"save_interval"`
	ClearIntv int            `json:"clear_interval"`
	Storages  []*StorageConf `json:"storages"`
}
