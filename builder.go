// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import "github.com/yxlib/yx"

type builder struct {
	logger *yx.Logger
}

var Builder = &builder{
	logger: yx.NewLogger("odb.Builder"),
}

func (b *builder) Build(dc *DataCenter, cfg *Config) {
	var err error = nil
	for _, storageCfg := range cfg.Storages {
		// cache
		var cd *CacheDriver = nil
		cacheCfg := storageCfg.Cache
		if cacheCfg != nil {
			cd = NewCacheDriver()
			err = cd.Open(cacheCfg.Addr, cacheCfg.Pwd, cacheCfg.Db)
			if err != nil {
				b.logger.E("open cache ", cacheCfg.Tag, "err: ", err)
				continue
			}

			dc.AddCacheDriver(cacheCfg.Tag, cd)
		}

		// db
		var dd *DbDriver = nil
		dbCfg := storageCfg.Db
		if dbCfg != nil {
			dd = NewDbDriver(RowObjFactory)
			err = dd.Open(dbCfg.Acc, dbCfg.Pwd, dbCfg.Addr, dbCfg.Port, dbCfg.DbName, dbCfg.Charset)
			if err != nil {
				b.logger.E("open db ", dbCfg.Tag, " err: ", err)
				continue
			}

			dc.AddDbDriver(dbCfg.Tag, dd)
		}

		// workers
		for _, workerCfg := range storageCfg.Workers {
			dw := NewDataWorker(cd, workerCfg.CacheKey, workerCfg.MapCacheField2DbField, dd, workerCfg.TableName)
			err = dw.Init(workerCfg.RowObj, workerCfg.InsertTag, workerCfg.SelectTag, workerCfg.SelectKeyTag, workerCfg.UpdateTag, workerCfg.UpdateKeyTag)
			if err != nil {
				b.logger.E("init data worker ", workerCfg.TableName, " err: ", err)
				continue
			}

			if dw.HasCache() || dw.HasDb() {
				dc.AddWorker(workerCfg.Tag, dw)
			}
		}
	}
}
