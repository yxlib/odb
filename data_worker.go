// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gamedb

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/yxlib/yx"
)

var (
	ErrWorkerObjectIsNil        = errors.New("object is nil")
	ErrWorkerRowObjIsNil        = errors.New("row object is nil")
	ErrWorkerInsertFieldNotFind = errors.New("insert field not find")
	ErrWorkerKeyFieldNotDefine  = errors.New("key field not define")
	ErrWorkerCacheNotExist      = errors.New("cache data no exist")
	ErrWorkerDBNotExist         = errors.New("db data no data")
	ErrWorkerFieldNotExist      = errors.New("some field not exist")
	ErrWorkerCacheKeyNotFind    = errors.New("cache key not define")
	ErrWorkerCacheKeyNotString  = errors.New("cache key not string")
)

const (
	CACHE_FIELD_UPDATE_TIME = "cache_update_time"
)

type Cacheable interface {
	FromCache(mapField2Val map[string]string) error
	ToCache(fields []string) (map[string]interface{}, error)
}

type DBTableRow interface {
	yx.Reuseable
	Cacheable
}

type DataWorker struct {
	cacheDriver           *CacheDriver
	mapCacheField2DbField map[string]string
	cacheFields           []string
	cacheKeyName          string
	dbDriver              *DbDriver
	tableName             string
	rowReflectName        string
	keyField              string
	insertSql             string
	selectSql             string
	updateSql             string
	setUpdatedCacheKeys   *yx.Set
	lckUpdatedCacheKeys   *sync.Mutex
	evtStop               *yx.Event
	evtExit               *yx.Event
	logger                *yx.Logger
	ec                    *yx.ErrCatcher
}

func NewDataWorker(cacheDriver *CacheDriver, cacheKeyName string, mapCacheField2DbField map[string]string, dbDriver *DbDriver, tableName string) *DataWorker {
	w := &DataWorker{
		cacheDriver:           cacheDriver,
		cacheKeyName:          cacheKeyName,
		mapCacheField2DbField: mapCacheField2DbField,
		cacheFields:           make([]string, 0),
		dbDriver:              dbDriver,
		tableName:             tableName,
		rowReflectName:        "",
		keyField:              "",
		insertSql:             "",
		selectSql:             "",
		updateSql:             "",
		setUpdatedCacheKeys:   yx.NewSet(yx.SET_TYPE_OBJ),
		lckUpdatedCacheKeys:   &sync.Mutex{},
		evtStop:               yx.NewEvent(),
		evtExit:               yx.NewEvent(),
		logger:                yx.NewLogger("DataWorker"),
		ec:                    yx.NewErrCatcher("DataWorker"),
	}

	for cacheField := range mapCacheField2DbField {
		w.cacheFields = append(w.cacheFields, cacheField)
	}

	return w
}

func (w *DataWorker) Init(reflectName string, insertTag string, selectKeyTag string, updateKeyTag string, updateIgnoreTag string) error {
	var err error = nil
	defer w.ec.DeferThrow("Init", &err)

	w.rowReflectName = reflectName

	rowObj, err := w.dbDriver.CreateTableRow(reflectName)
	if err != nil {
		return err
	}

	defer w.dbDriver.ReuseTableRow(rowObj, reflectName)

	err = w.initInsertSql(rowObj, insertTag)
	if err != nil {
		return err
	}

	err = w.initSelectSql(rowObj, selectKeyTag)
	if err != nil {
		return err
	}

	err = w.initUpdateSql(rowObj, updateKeyTag, updateIgnoreTag)
	if err != nil {
		return err
	}

	return nil
}

func (w *DataWorker) Start(saveIntv time.Duration, clearExpireIntv time.Duration) {
	saveTicker := time.NewTicker(saveIntv)
	clearExpireTick := time.NewTicker(clearExpireIntv)

	for {
		select {
		case <-clearExpireTick.C:
			w.ClearExpireCaches()

		case <-saveTicker.C:
			w.SaveCaches()

		case <-w.evtStop.C:
			w.SaveCaches()
			goto Exit0
		}
	}

Exit0:
	saveTicker.Stop()
	clearExpireTick.Stop()
	w.evtExit.Send()
}

func (w *DataWorker) Stop() {
	w.evtStop.Send()
	w.evtExit.Wait()
}

func (w *DataWorker) LoadFromCache(obj Cacheable, key string) error {
	cacheKey := w.getCacheKey(key)
	err := w.loadFromCacheImpl(obj, cacheKey)
	return w.ec.Throw("LoadFromCache", err)
}

func (w *DataWorker) GetAllCacheKeys() ([]string, error) {
	keys, err := w.cacheDriver.Keys(w.tableName + "_" + "*")
	return keys, w.ec.Throw("GetAllCacheKeys", err)
}

func (w *DataWorker) GetCacheData(obj Cacheable, key string, fields ...string) error {
	var err error = nil
	defer w.ec.DeferThrow("GetCacheData", &err)

	if obj == nil {
		err = ErrWorkerObjectIsNil
		return err
	}

	mapField2Val, err := w.GetCacheDataToMap(key, fields...)
	if err != nil {
		return err
	}

	err = obj.FromCache(mapField2Val)
	return err
}

func (w *DataWorker) GetCacheDataToMap(key string, fields ...string) (map[string]string, error) {
	var err error = nil
	defer w.ec.DeferThrow("GetCacheDataToMap", &err)

	cacheKey := w.getCacheKey(key)
	datas, err := w.cacheDriver.HMGet(cacheKey, fields...)
	if err != nil {
		return nil, err
	}

	if len(datas) != len(fields) {
		err = ErrWorkerFieldNotExist
		return nil, err
	}

	mapField2Val := make(map[string]string)
	for idx, key := range fields {
		val := datas[idx]
		if val == nil {
			mapField2Val[key] = ""
		} else {
			mapField2Val[key] = val.(string)
		}
	}

	return mapField2Val, nil
}

func (w *DataWorker) SetCacheData(obj Cacheable, key string, fields ...string) error {
	var err error = nil
	defer w.ec.DeferThrow("SetCacheData", &err)

	if obj == nil {
		err = ErrWorkerObjectIsNil
		return err
	}

	mapField2Val, err := obj.ToCache(fields)
	if err != nil {
		return err
	}

	mapField2Val[CACHE_FIELD_UPDATE_TIME] = time.Now().UnixNano()
	err = w.SetCacheDataByMap(key, mapField2Val)
	return err
}

func (w *DataWorker) SetCacheDataByMap(key string, mapField2Val map[string]interface{}) error {
	cacheKey := w.getCacheKey(key)
	err := w.cacheDriver.HMSet(cacheKey, mapField2Val)
	if err != nil {
		return w.ec.Throw("SetCacheDataByMap", err)
	}

	w.setCacheUpdatedImpl(cacheKey)
	return nil
}

func (w *DataWorker) SetCacheExpire(key string, expiration time.Duration) error {
	cacheKey := w.getCacheKey(key)
	_, err := w.cacheDriver.Expire(cacheKey, expiration)
	return w.ec.Throw("SetCacheExpire", err)
}

func (w *DataWorker) ClearExpireCaches() error {
	var err error = nil
	defer w.ec.DeferThrow("ClearExpireCaches", &err)

	keys, err := w.cacheDriver.Keys(w.tableName + "_" + "*")
	if err != nil {
		return err
	}

	expireKeys := make([]string, 0)
	for _, key := range keys {
		// expire data
		t, err := w.cacheDriver.TTL(key)
		if err != nil {
			w.logger.E("ClearExpireCaches TTL key: ", key, ", err: ", err)
			continue
		}

		if t != (-1*time.Second) && t <= time.Second {
			expireKeys = append(expireKeys, key)
		}
	}

	w.cacheDriver.Del(expireKeys...)
	return nil
}

func (w *DataWorker) InsertToDb(mapper interface{}) (int64, error) {
	lastId, err := w.dbDriver.NameInsert(w.insertSql, mapper)
	if err != nil {
		return 0, w.ec.Throw("InsertToDb", err)
	}

	return lastId, nil
}

func (w *DataWorker) SelectFromDb(mapper interface{}) (DBTableRow, error) {
	var err error = nil
	defer w.ec.DeferThrow("SelectFromDb", &err)

	results, err := w.dbDriver.NameQuery(w.rowReflectName, w.selectSql, mapper)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		err = ErrWorkerDBNotExist
		return nil, err
	}

	return results[0], nil
}

func (w *DataWorker) UpdateToDb(mapper interface{}) error {
	err := w.dbDriver.NameExec(w.updateSql, mapper)
	return w.ec.Throw("UpdateToDb", err)
}

func (w *DataWorker) PreloadData(obj Cacheable, mapper interface{}) error {
	var err error = nil
	defer w.ec.DeferThrow("PreloadData", &err)

	// load from db
	rowObj, err := w.SelectFromDb(mapper)
	if err != nil {
		return err
	}

	defer w.dbDriver.ReuseTableRow(rowObj, w.rowReflectName)

	// key
	mapField2Val, err := rowObj.ToCache([]string{w.cacheKeyName})
	if err != nil {
		return err
	}

	keyInterface, ok := mapField2Val[w.cacheKeyName]
	if !ok {
		err = ErrWorkerCacheKeyNotFind
		return err
	}

	key, ok := keyInterface.(string)
	if !ok {
		err = ErrWorkerCacheKeyNotString
		return err
	}

	// cache
	err = w.SetCacheData(rowObj, key, w.cacheFields...)
	if err != nil {
		return err
	}

	// object
	data := make(map[string]string)
	for k, v := range mapField2Val {
		data[k] = v.(string)
	}

	err = obj.FromCache(data)
	return err
}

func (w *DataWorker) SetCacheUpdated(key string) {
	cacheKey := w.getCacheKey(key)
	w.setCacheUpdatedImpl(cacheKey)
}

func (w *DataWorker) SaveCaches() error {
	var err error = nil

	cacheKeys := w.popUpdatedCacheKeys()
	for _, cacheKey := range cacheKeys {
		err = w.saveCache(cacheKey)
		if err != nil {
			w.setCacheUpdatedImpl(cacheKey)
			w.ec.Catch("SaveCaches", &err)
		}
	}

	return nil
}

func (w *DataWorker) getCacheKey(key string) string {
	return w.tableName + "_" + key
}

func (w *DataWorker) setCacheUpdatedImpl(cacheKey string) {
	w.lckUpdatedCacheKeys.Lock()
	defer w.lckUpdatedCacheKeys.Unlock()

	w.setUpdatedCacheKeys.Add(cacheKey)
}

func (w *DataWorker) popUpdatedCacheKeys() []string {
	w.lckUpdatedCacheKeys.Lock()
	defer w.lckUpdatedCacheKeys.Unlock()

	cacheKeys := w.setUpdatedCacheKeys.GetElements()
	w.setUpdatedCacheKeys = yx.NewSet(yx.SET_TYPE_OBJ)

	updatedCacheKeys := make([]string, 0, len(cacheKeys))
	for _, key := range cacheKeys {
		keyStr, ok := key.(string)
		if !ok {
			continue
		}

		updatedCacheKeys = append(updatedCacheKeys, keyStr)
	}

	return updatedCacheKeys
}

func (w *DataWorker) loadFromCacheImpl(obj Cacheable, cacheKey string) error {
	var err error = nil
	defer w.ec.DeferThrow("loadFromCacheImpl", &err)

	if obj == nil {
		err = ErrWorkerObjectIsNil
		return err
	}

	count, err := w.cacheDriver.Exists(cacheKey)
	if err != nil {
		return err
	}

	if count == 0 {
		err = ErrWorkerCacheNotExist
		return err
	}

	_, err = w.cacheDriver.Persist(cacheKey)
	if err != nil {
		return err
	}

	mapField2Val, err := w.cacheDriver.HGetAll(cacheKey)
	if err != nil {
		return err
	}

	if len(mapField2Val) == 0 {
		err = ErrWorkerCacheNotExist
		return err
	}

	err = obj.FromCache(mapField2Val)
	return err
}

func (w *DataWorker) saveCache(cacheKey string) error {
	var err error = nil
	defer w.ec.DeferThrow("saveCache", &err)

	rowObj, err := w.dbDriver.CreateTableRow(w.rowReflectName)
	if err != nil {
		return err
	}

	defer w.dbDriver.ReuseTableRow(rowObj, w.rowReflectName)

	err = w.loadFromCacheImpl(rowObj, cacheKey)
	if err != nil {
		return err
	}

	err = w.UpdateToDb(rowObj)
	return err
}

func (w *DataWorker) initInsertSql(tableObj interface{}, insertTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	fieldSql := ""
	valSql := ""

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(insertTag) == "" {
			continue
		}

		if fieldSql != "" {
			fieldSql += ", "
			valSql += ", "
		}

		fieldSql += name
		valSql += ":" + name
	}

	if fieldSql == "" {
		return w.ec.Throw("initInsertSql", ErrWorkerInsertFieldNotFind)
	}

	w.insertSql = "INSERT INTO " + w.tableName + " (" + fieldSql + ") VALUES (" + valSql + ")"
	return nil
}

func (w *DataWorker) initSelectSql(tableObj interface{}, keyTag string) error {
	keyField, err := w.findKeyField(tableObj, keyTag)
	if err != nil {
		return w.ec.Throw("initSelectSql", err)
	}

	w.selectSql = "SELECT * FROM " + w.tableName + " WHERE " + keyField + "=:" + keyField + " LIMIT 1"
	return nil
}

func (w *DataWorker) initUpdateSql(tableObj interface{}, keyTag string, ignoreTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	sql := ""
	keyField := ""

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(keyTag) != "" { // key field
			keyField = name
			continue
		}

		if name == ignoreTag {
			continue
		}

		if sql != "" {
			sql += ", "
		}

		sql += name + "=:" + name
	}

	if keyField == "" {
		return w.ec.Throw("initUpdateSql", ErrWorkerKeyFieldNotDefine)
	}

	w.updateSql = "UPDATE " + w.tableName + " SET " + sql + " WHERE " + keyField + "=:" + keyField
	return nil
}

func (w *DataWorker) findKeyField(tableObj interface{}, keyTag string) (string, error) {
	v := reflect.TypeOf(tableObj).Elem()
	keyField := ""

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(keyTag) != "" { // key field
			keyField = name
			break
		}
	}

	if keyField == "" {
		return "", w.ec.Throw("findKeyField", ErrWorkerKeyFieldNotDefine)
	}

	return keyField, nil
}
