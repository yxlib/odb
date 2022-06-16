// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/yxlib/yx"
)

var (
	ErrNoCache                   = errors.New("no cache")
	ErrNoDb                      = errors.New("no db")
	ErrWorkerObjectIsNil         = errors.New("object is nil")
	ErrWorkerRowObjIsNil         = errors.New("row object is nil")
	ErrWorkerInsertFieldNotFind  = errors.New("insert field not find")
	ErrWorkerReplaceFieldNotFind = errors.New("insert field and update field not find")
	ErrWorkerKeyFieldNotDefine   = errors.New("key field not define")
	ErrWorkerCacheNotExist       = errors.New("cache data no exist")
	ErrWorkerDBNotExist          = errors.New("db data no data")
	ErrWorkerFieldNotExist       = errors.New("some field not exist")
	ErrWorkerCacheKeyNotFind     = errors.New("cache key not define")
	ErrWorkerCacheKeyNotString   = errors.New("cache key not string")
)

const (
	CACHE_FIELD_UPDATE_TIME = "cache_update_time"
)

type Cacheable interface {
	FromCache(mapField2Val map[string]string) error
	ToCache(fields []string) (map[string]interface{}, error)
}

type DBTableRow interface {
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
	replaceSql            string
	selectSql             string
	updateSql             string
	setUpdatedCacheKeys   *yx.Set
	lckUpdatedCacheKeys   *sync.Mutex
	bAutoSave             bool
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
		replaceSql:            "",
		selectSql:             "",
		updateSql:             "",
		setUpdatedCacheKeys:   yx.NewSet(yx.SET_TYPE_OBJ),
		lckUpdatedCacheKeys:   &sync.Mutex{},
		bAutoSave:             false,
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

func (w *DataWorker) Init(reflectName string, insertTag string, selectTag string, selectKeyTag string, updateTag string, updateKeyTag string) error {
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
		w.ec.Catch("Init", &err)
	}

	err = w.initReplaceSql(rowObj, insertTag, updateTag)
	if err != nil {
		w.ec.Catch("Init", &err)
	}

	err = w.initSelectSql(rowObj, selectTag, selectKeyTag)
	if err != nil {
		w.ec.Catch("Init", &err)
	}

	err = w.initUpdateSql(rowObj, updateTag, updateKeyTag)
	if err != nil {
		w.ec.Catch("Init", &err)
	}

	return nil
}

func (w *DataWorker) HasCache() bool {
	return w.cacheDriver != nil
}

func (w *DataWorker) HasDb() bool {
	return w.dbDriver != nil
}

func (w *DataWorker) Start(saveIntv time.Duration, clearExpireIntv time.Duration) {
	w.logger.I("Auto save for table: ", w.tableName)

	w.bAutoSave = true
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
	w.bAutoSave = false
}

func (w *DataWorker) Stop() {
	if !w.bAutoSave {
		return
	}

	w.evtStop.Send()
	w.evtExit.Wait()
}

func (w *DataWorker) LoadFromCache(obj Cacheable, key string) error {
	if !w.HasCache() {
		return w.ec.Throw("LoadFromCache", ErrNoCache)
	}

	cacheKey := w.getCacheKey(key)
	err := w.loadFromCacheImpl(obj, cacheKey)
	return w.ec.Throw("LoadFromCache", err)
}

func (w *DataWorker) GetAllCacheKeys() ([]string, error) {
	if !w.HasCache() {
		return nil, w.ec.Throw("GetAllCacheKeys", ErrNoCache)
	}

	keys, err := w.cacheDriver.Keys(w.tableName + "_" + "*")
	return keys, w.ec.Throw("GetAllCacheKeys", err)
}

func (w *DataWorker) GetCacheData(obj Cacheable, key string, fields ...string) error {
	var err error = nil
	defer w.ec.DeferThrow("GetCacheData", &err)

	if !w.HasCache() {
		err = ErrNoCache
		return err
	}

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

	if !w.HasCache() {
		err = ErrNoCache
		return nil, err
	}

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

	if !w.HasCache() {
		err = ErrNoCache
		return err
	}

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
	if !w.HasCache() {
		return w.ec.Throw("SetCacheDataByMap", ErrNoCache)
	}

	cacheKey := w.getCacheKey(key)
	err := w.cacheDriver.HMSet(cacheKey, mapField2Val)
	if err != nil {
		return w.ec.Throw("SetCacheDataByMap", err)
	}

	w.setCacheUpdatedImpl(cacheKey)
	return nil
}

func (w *DataWorker) SetCacheExpire(key string, expiration time.Duration) error {
	if !w.HasCache() {
		return w.ec.Throw("SetCacheExpire", ErrNoCache)
	}

	cacheKey := w.getCacheKey(key)
	_, err := w.cacheDriver.Expire(cacheKey, expiration)
	return w.ec.Throw("SetCacheExpire", err)
}

func (w *DataWorker) ClearExpireCaches() error {
	var err error = nil
	defer w.ec.DeferThrow("ClearExpireCaches", &err)

	if !w.HasCache() {
		err = ErrNoCache
		return err
	}

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
	if !w.HasDb() {
		return 0, w.ec.Throw("InsertToDb", ErrNoDb)
	}

	lastId, err := w.dbDriver.NameInsert(w.insertSql, mapper)
	if err != nil {
		return 0, w.ec.Throw("InsertToDb", err)
	}

	return lastId, nil
}

func (w *DataWorker) ReplaceToDb(mapper interface{}) error {
	if !w.HasDb() {
		return w.ec.Throw("ReplaceToDb", ErrNoDb)
	}

	err := w.dbDriver.NameExec(w.replaceSql, mapper)
	if err != nil {
		return w.ec.Throw("ReplaceToDb", err)
	}

	return nil
}

func (w *DataWorker) SelectRowsFromDb(mapper interface{}, limitCnt int) ([]DBTableRow, error) {
	if !w.HasDb() {
		return nil, w.ec.Throw("SelectRowsFromDb", ErrNoDb)
	}

	rows, err := w.selectImpl(mapper, limitCnt)
	return rows, w.ec.Throw("SelectRowsFromDb", err)
}

func (w *DataWorker) SelectFromDb(mapper interface{}) (DBTableRow, error) {
	if !w.HasDb() {
		return nil, w.ec.Throw("SelectFromDb", ErrNoDb)
	}

	rows, err := w.selectImpl(mapper, 1)
	if err != nil {
		return nil, w.ec.Throw("SelectFromDb", err)
	}

	if len(rows) == 0 {
		return nil, w.ec.Throw("SelectFromDb", ErrWorkerDBNotExist)
	}

	return rows[0], nil
}

func (w *DataWorker) UpdateToDb(mapper interface{}) error {
	if !w.HasDb() {
		return w.ec.Throw("UpdateToDb", ErrNoDb)
	}

	err := w.dbDriver.NameExec(w.updateSql, mapper)
	return w.ec.Throw("UpdateToDb", err)
}

func (w *DataWorker) PreloadData(obj Cacheable, mapper interface{}) error {
	var err error = nil
	defer w.ec.DeferThrow("PreloadData", &err)

	if !w.HasDb() {
		err = ErrNoDb
		return err
	}

	// load from db
	rowObj, err := w.SelectFromDb(mapper)
	if err != nil {
		return err
	}

	defer w.dbDriver.ReuseTableRow(rowObj, w.rowReflectName)

	// key
	mapField2Val, err := rowObj.ToCache(w.cacheFields)
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
	if !w.HasCache() || !w.HasDb() {
		return
	}

	cacheKey := w.getCacheKey(key)
	w.setCacheUpdatedImpl(cacheKey)
}

func (w *DataWorker) SaveCaches() error {
	if !w.HasCache() || !w.HasDb() {
		return ErrNoCache
	}

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

	insertFields := make([]string, 0)
	insertValues := make([]string, 0)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(insertTag) != "" { // tag field
			insertFields = append(insertFields, name)
			insertValues = append(insertValues, ":"+name)
		}
	}

	if len(insertFields) == 0 {
		return w.ec.Throw("initInsertSql", ErrWorkerInsertFieldNotFind)
	}

	fieldStr := strings.Join(insertFields, ",")
	valueStr := strings.Join(insertValues, ",")
	w.insertSql = "INSERT INTO " + w.tableName + " (" + fieldStr + ") VALUES (" + valueStr + ")"
	w.logger.D("Insert SQL: ", w.insertSql)
	return nil
}

func (w *DataWorker) initReplaceSql(tableObj interface{}, insertTag string, updateTag string) error {
	v := reflect.TypeOf(tableObj).Elem()

	replaceFields := make([]string, 0)
	replaceValues := make([]string, 0)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(insertTag) != "" || field.Tag.Get(updateTag) != "" { // tag field
			replaceFields = append(replaceFields, name)
			replaceValues = append(replaceValues, ":"+name)
		}
	}

	if len(replaceFields) == 0 {
		return w.ec.Throw("initReplaceSql", ErrWorkerReplaceFieldNotFind)
	}

	fieldStr := strings.Join(replaceFields, ",")
	valueStr := strings.Join(replaceValues, ",")
	w.replaceSql = "REPLACE INTO " + w.tableName + " (" + fieldStr + ") VALUES (" + valueStr + ")"
	w.logger.D("Replace SQL: ", w.replaceSql)
	return nil
}

func (w *DataWorker) initSelectSql(tableObj interface{}, selectTag string, keyTag string) error {
	v := reflect.TypeOf(tableObj).Elem()

	selectFields := make([]string, 0)
	condFields := make([]string, 0)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(selectTag) != "" { // tag field
			selectFields = append(selectFields, name)
		}

		if field.Tag.Get(keyTag) != "" { // key field
			condFields = append(condFields, name+"=:"+name)
		}
	}

	// conditions
	if len(condFields) == 0 {
		return w.ec.Throw("initSelectSql", ErrWorkerKeyFieldNotDefine)
	}

	condStr := strings.Join(condFields, " AND ")

	// fields
	fieldStr := ""
	if len(selectFields) == 0 {
		fieldStr = "*"
	} else {
		fieldStr = strings.Join(selectFields, ",")
	}

	w.selectSql = "SELECT " + fieldStr + " FROM " + w.tableName + " WHERE " + condStr
	w.logger.D("Select SQL: ", w.selectSql)
	return nil
}

func (w *DataWorker) initUpdateSql(tableObj interface{}, updateTag string, keyTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	// sql := ""
	// keyField := ""

	updateFields := make([]string, 0)
	condFields := make([]string, 0)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			continue
		}

		if field.Tag.Get(updateTag) != "" { // tag field
			updateFields = append(updateFields, name+"=:"+name)
			continue
		}

		if field.Tag.Get(keyTag) != "" { // key field
			condFields = append(condFields, name+"=:"+name)
		}
	}

	// conditions
	if len(condFields) == 0 {
		return w.ec.Throw("initUpdateSql", ErrWorkerKeyFieldNotDefine)
	}

	if len(updateFields) == 0 {
		return w.ec.Throw("initUpdateSql", ErrWorkerKeyFieldNotDefine)
	}

	condStr := strings.Join(condFields, " AND ")
	fieldStr := strings.Join(updateFields, ",")

	w.updateSql = "UPDATE " + w.tableName + " SET " + fieldStr + " WHERE " + condStr
	w.logger.D("Update SQL: ", w.updateSql)
	return nil
}

func (w *DataWorker) selectImpl(mapper interface{}, limitCnt int) ([]DBTableRow, error) {
	var err error = nil
	defer w.ec.DeferThrow("selectImpl", &err)

	selectSql := fmt.Sprintf("%s LIMIT %d", w.selectSql, limitCnt)
	rows, err := w.dbDriver.NameQuery(w.rowReflectName, selectSql, mapper)
	if err != nil {
		return nil, w.ec.Throw("selectImpl", err)
	}

	return rows, nil
}
