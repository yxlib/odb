// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import (
	"time"

	"github.com/yxlib/yx"
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
