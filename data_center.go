// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import (
	"database/sql"
	"errors"
	"time"

	"github.com/yxlib/yx"
)

var (
	ErrNoDataWorker  = errors.New("no data worker")
	ErrNoDbDriver    = errors.New("no db driver")
	ErrAffectRowZero = errors.New("affect row is zero")
)

var RowObjFactory = yx.NewObjectFactory()

type DataCenter struct {
	mapTag2CacheDriver map[string]*CacheDriver
	mapTag2DbDriver    map[string]*DbDriver
	mapTag2Worker      map[string]*DataWorker
}

func NewDataCenter() *DataCenter {
	return &DataCenter{
		mapTag2CacheDriver: make(map[string]*CacheDriver),
		mapTag2DbDriver:    make(map[string]*DbDriver),
		mapTag2Worker:      make(map[string]*DataWorker),
	}
}

func (c *DataCenter) AddCacheDriver(tag string, driver *CacheDriver) {
	_, ok := c.mapTag2CacheDriver[tag]
	if !ok {
		c.mapTag2CacheDriver[tag] = driver
	}
}

func (c *DataCenter) GetCacheDriver(tag string) (*CacheDriver, bool) {
	driver, ok := c.mapTag2CacheDriver[tag]
	return driver, ok
}

func (c *DataCenter) CloseAllCacheDriver() {
	for _, driver := range c.mapTag2CacheDriver {
		driver.Close()
	}
}

func (c *DataCenter) AddDbDriver(tag string, driver *DbDriver) {
	_, ok := c.mapTag2DbDriver[tag]
	if !ok {
		c.mapTag2DbDriver[tag] = driver
	}
}

func (c *DataCenter) GetDbDriver(tag string) (*DbDriver, bool) {
	driver, ok := c.mapTag2DbDriver[tag]
	return driver, ok
}

func (c *DataCenter) CloseAllDbDriver() {
	for _, driver := range c.mapTag2DbDriver {
		driver.Close()
	}
}

func (c *DataCenter) AddWorker(tag string, w *DataWorker) {
	_, ok := c.mapTag2Worker[tag]
	if !ok {
		c.mapTag2Worker[tag] = w
	}
}

func (c *DataCenter) GetWorker(tag string) (*DataWorker, bool) {
	w, ok := c.mapTag2Worker[tag]
	return w, ok
}

func (c *DataCenter) Start(saveIntv time.Duration, clearExpireIntv time.Duration) {
	for _, worker := range c.mapTag2Worker {
		if !worker.HasCache() || !worker.HasDb() || !worker.IsOpenAutoSave() {
			continue
		}

		go worker.Start(saveIntv, clearExpireIntv)
	}
}

func (c *DataCenter) Stop() {
	for _, worker := range c.mapTag2Worker {
		worker.Stop()
	}
}

func (c *DataCenter) Insert(tag string, mapper interface{}, extraCond string) (int64, error) {
	dw, ok := c.GetWorker(tag)
	if !ok {
		return 0, ErrNoDataWorker
	}

	if len(extraCond) == 0 {
		tid, err := dw.InsertToDb(mapper)
		return tid, err
	} else {
		tid, err := dw.InsertToDbEx(mapper, extraCond)
		return tid, err
	}
}

func (c *DataCenter) ReplaceToDb(tag string, mapper interface{}, extraCond string) error {
	dw, ok := c.GetWorker(tag)
	if !ok {
		return ErrNoDataWorker
	}

	var affectRow int64
	var err error
	if len(extraCond) == 0 {
		affectRow, err = dw.ReplaceToDb(mapper)
	} else {
		affectRow, err = dw.ReplaceToDbEx(mapper, extraCond)
	}

	if err != nil {
		return err
	}

	if affectRow == 0 {
		return ErrAffectRowZero
	}

	return nil
}

func (c *DataCenter) SelectRowsFromDb(tag string, mapper interface{}, extraCond string, limitCnt int) ([]DBTableRow, error) {
	dw, ok := c.GetWorker(tag)
	if !ok {
		return nil, ErrNoDataWorker
	}

	if len(extraCond) == 0 {
		dbRows, err := dw.SelectRowsFromDb(mapper, limitCnt)
		return dbRows, err
	} else {
		dbRows, err := dw.SelectRowsFromDbEx(mapper, extraCond, limitCnt)
		return dbRows, err
	}
}

func (c *DataCenter) SelectRowFromDb(tag string, mapper interface{}, extraCond string) (DBTableRow, error) {
	dw, ok := c.GetWorker(tag)
	if !ok {
		return nil, ErrNoDataWorker
	}

	if len(extraCond) == 0 {
		dbRow, err := dw.SelectFromDb(mapper)
		return dbRow, err
	} else {
		dbRow, err := dw.SelectFromDbEx(mapper, extraCond)
		return dbRow, err
	}
}

func (c *DataCenter) Count(tag string, mapper interface{}, extraCond string) (int64, error) {
	dw, ok := c.GetWorker(tag)
	if !ok {
		return 0, ErrNoDataWorker
	}

	if len(extraCond) == 0 {
		cnt, err := dw.Count(mapper)
		return cnt, err
	} else {
		cnt, err := dw.CountEx(mapper, extraCond)
		return cnt, err
	}
}

func (c *DataCenter) UpdateToDb(tag string, mapper interface{}, extraCond string) error {
	dw, ok := c.GetWorker(tag)
	if !ok {
		return ErrNoDataWorker
	}

	var affectRow int64
	var err error
	if len(extraCond) == 0 {
		affectRow, err = dw.UpdateToDb(mapper)
	} else {
		affectRow, err = dw.UpdateToDbEx(mapper, extraCond)
	}

	if err != nil {
		return err
	}

	if affectRow == 0 {
		return ErrAffectRowZero
	}

	return nil
}

func (c *DataCenter) BeginTransaction(dbTag string) (*sql.Tx, error) {
	dbDriver, ok := c.GetDbDriver(dbTag)
	if !ok {
		return nil, ErrNoDbDriver
	}

	tx, err := dbDriver.Begin()
	return tx, err
}

func InsertTransaction(tx *sql.Tx, query string, args ...interface{}) (int64, error) {
	r, err := tx.Exec(query, args...)
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	affectRow, err := r.RowsAffected()
	if err != nil || affectRow == 0 {
		tx.Rollback()
		return 0, ErrAffectRowZero
	}

	recordId, err := r.LastInsertId()
	if err != nil {
		tx.Rollback()
		return 0, err
	}

	return recordId, nil
}

func ExecTransaction(tx *sql.Tx, query string, args ...interface{}) error {
	r, err := tx.Exec(query, args...)
	if err != nil {
		tx.Rollback()
		return err
	}

	affectRow, err := r.RowsAffected()
	if err != nil || affectRow == 0 {
		tx.Rollback()
		return ErrAffectRowZero
	}

	return nil
}
