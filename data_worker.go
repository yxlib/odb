// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
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
	ErrWorkerDBCountUnknownErr   = errors.New("unknown error while count")
)

const (
	CACHE_FIELD_UPDATE_TIME = "cache_update_time"
)

const (
	INSERT_TAG_DEFAULT     = "i"
	SELECT_TAG_DEFAULT     = "s"
	SELECT_KEY_TAG_DEFAULT = "sk"
	UPDATE_TAG_DEFAULT     = "u"
	UPDATE_KEY_TAG_DEFAULT = "uk"
)

type DataWorker struct {
	cacheDriver *CacheDriver
	// mapCacheField2DbField map[string]string
	cacheFields         []string
	cacheKeyName        string
	dbDriver            *DbDriver
	tableName           string
	rowReflectName      string
	cntRowReflectName   string
	keyField            string
	insertSql           string
	replaceSql          string
	selectSql           string
	countSql            string
	updateSql           string
	setUpdatedCacheKeys *yx.ObjectSet
	lckUpdatedCacheKeys *sync.Mutex
	bOpenAutoSave       bool
	chanExitSave        chan byte
	evtStop             *yx.Event
	logger              *yx.Logger
	ec                  *yx.ErrCatcher
}

func NewDataWorker(cacheDriver *CacheDriver, cacheKeyName string, dbDriver *DbDriver, tableName string) *DataWorker {
	w := &DataWorker{
		cacheDriver:  cacheDriver,
		cacheKeyName: cacheKeyName,
		// mapCacheField2DbField: mapCacheField2DbField,
		cacheFields:         make([]string, 0),
		dbDriver:            dbDriver,
		tableName:           tableName,
		rowReflectName:      "",
		cntRowReflectName:   "",
		keyField:            "",
		insertSql:           "",
		replaceSql:          "",
		selectSql:           "",
		countSql:            "",
		updateSql:           "",
		setUpdatedCacheKeys: yx.NewObjectSet(),
		lckUpdatedCacheKeys: &sync.Mutex{},
		bOpenAutoSave:       false,
		chanExitSave:        make(chan byte, 1),
		evtStop:             yx.NewEvent(),
		logger:              yx.NewLogger("DataWorker"),
		ec:                  yx.NewErrCatcher("DataWorker"),
	}

	w.chanExitSave <- 1
	w.initCacheFields()
	// if len(w.mapCacheField2DbField) == 0 {
	// 	w.initCacheField2DbField()
	// }

	// for cacheField := range mapCacheField2DbField {
	// 	w.cacheFields = append(w.cacheFields, cacheField)
	// }

	return w
}

func (w *DataWorker) Init(reflectName string, insertTag string, selectTag string, selectKeyTag string, updateTag string, updateKeyTag string) error {
	var err error = nil
	defer w.ec.DeferThrow("Init", &err)

	// fix tag
	if len(insertTag) == 0 {
		insertTag = INSERT_TAG_DEFAULT
	}

	if len(selectTag) == 0 {
		selectTag = SELECT_TAG_DEFAULT
	}

	if len(selectKeyTag) == 0 {
		selectKeyTag = SELECT_KEY_TAG_DEFAULT
	}

	if len(updateTag) == 0 {
		updateTag = UPDATE_TAG_DEFAULT
	}

	if len(updateKeyTag) == 0 {
		updateKeyTag = UPDATE_KEY_TAG_DEFAULT
	}

	// init sql
	w.rowReflectName = reflectName

	rowObj, err := w.dbDriver.CreateTableRow(reflectName)
	if err != nil {
		return err
	}

	defer w.dbDriver.ReuseTableRow(rowObj, reflectName)

	err = w.initInsertSql(rowObj, insertTag)
	if err != nil {
		w.logger.W("init insert SQL failed, err: ", err)
	}

	err = w.initReplaceSql(rowObj, insertTag, updateTag)
	if err != nil {
		w.logger.W("init replace SQL failed, err: ", err)
	}

	err = w.initSelectSql(rowObj, selectTag, selectKeyTag)
	if err != nil {
		w.logger.W("init select SQL failed, err: ", err)
	}

	err = w.initUpdateSql(rowObj, updateTag, updateKeyTag)
	if err != nil {
		w.logger.W("init update SQL failed, err: ", err)
	}

	return nil
}

func (w *DataWorker) HasCache() bool {
	return w.cacheDriver != nil
}

func (w *DataWorker) HasDb() bool {
	return w.dbDriver != nil
}

func (w *DataWorker) OpenAutoSave() {
	w.bOpenAutoSave = true
}

func (w *DataWorker) IsOpenAutoSave() bool {
	return w.bOpenAutoSave
}

func (w *DataWorker) Start(saveIntv time.Duration, clearExpireIntv time.Duration) {
	w.logger.I("Auto save for table: ", w.tableName)

	<-w.chanExitSave
	defer w.exitSave()

	stopChan := w.evtStop.GetChan()
	if stopChan == nil {
		return
	}

	saveTicker := time.NewTicker(saveIntv)
	clearExpireTick := time.NewTicker(clearExpireIntv)

	for {
		select {
		case <-clearExpireTick.C:
			w.ClearExpireCaches()

		case <-saveTicker.C:
			w.SaveCaches()

		case <-stopChan:
			w.SaveCaches()
			goto Exit0
		}
	}

Exit0:
	saveTicker.Stop()
	clearExpireTick.Stop()
}

func (w *DataWorker) Stop() {
	w.evtStop.Close()
	w.waitExitSave()
}

func (w *DataWorker) exitSave() {
	w.chanExitSave <- 1
}

func (w *DataWorker) waitExitSave() {
	<-w.chanExitSave
	w.chanExitSave <- 1
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
	if err != nil {
		return nil, w.ec.Throw("GetAllCacheKeys", err)
	}

	for i := 0; i < len(keys); i++ {
		keys[i] = w.getKey(keys[i])
	}

	return keys, nil
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

	updateTime := time.Now().UnixNano()
	mapField2Val[CACHE_FIELD_UPDATE_TIME] = strconv.FormatInt(updateTime, 10)
	err = w.SetCacheDataByMap(key, mapField2Val, true)
	return err
}

func (w *DataWorker) SetCacheDataByMap(key string, mapField2Val map[string]interface{}, bMarkUpdate bool) error {
	if !w.HasCache() {
		return w.ec.Throw("SetCacheDataByMap", ErrNoCache)
	}

	cacheKey := w.getCacheKey(key)
	err := w.cacheDriver.HMSet(cacheKey, mapField2Val)
	if err != nil {
		return w.ec.Throw("SetCacheDataByMap", err)
	}

	if bMarkUpdate {
		w.setCacheUpdatedImpl(cacheKey)
	}

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

func (w *DataWorker) DelCacheDatas(keys ...string) (int64, error) {
	delKeys := make([]string, 0)
	for _, key := range keys {
		cacheKey := w.getCacheKey(key)
		delKeys = append(delKeys, cacheKey)
	}

	return w.cacheDriver.Del(delKeys...)
}

func (w *DataWorker) DelCacheDataFields(key string, fields ...string) (int64, error) {
	cnt := int64(0)
	cacheKey := w.getCacheKey(key)
	for _, field := range fields {
		_, err := w.cacheDriver.HDel(cacheKey, field)
		if err != nil {
			continue
		}

		cnt++
	}

	return cnt, nil
}

func (w *DataWorker) InsertToDb(mapper interface{}) (int64, error) {
	return w.InsertToDbEx(mapper, "")
}

func (w *DataWorker) InsertToDbEx(mapper interface{}, extraCond string) (int64, error) {
	if !w.HasDb() {
		return 0, w.ec.Throw("InsertToDbEx", ErrNoDb)
	}

	insertSql := w.insertSql
	if len(extraCond) > 0 {
		insertSql = insertSql + extraCond
	}

	lastId, err := w.dbDriver.NameInsert(insertSql, mapper)
	if err != nil {
		return 0, w.ec.Throw("InsertToDb", err)
	}

	return lastId, nil
}

func (w *DataWorker) ReplaceToDb(mapper interface{}) (int64, error) {
	return w.ReplaceToDbEx(mapper, "")
}

func (w *DataWorker) ReplaceToDbEx(mapper interface{}, extraCond string) (int64, error) {
	if !w.HasDb() {
		return 0, w.ec.Throw("ReplaceToDbEx", ErrNoDb)
	}

	replaceSql := w.replaceSql
	if len(extraCond) > 0 {
		replaceSql = replaceSql + extraCond
	}

	effectRow, err := w.dbDriver.NameExec(replaceSql, mapper)
	if err != nil {
		return 0, w.ec.Throw("ReplaceToDb", err)
	}

	return effectRow, nil
}

func (w *DataWorker) SelectRowsFromDb(mapper interface{}, limitCnt int) ([]DBTableRow, error) {
	return w.SelectRowsFromDbEx(mapper, "", limitCnt)
}

func (w *DataWorker) SelectRowsFromDbEx(mapper interface{}, extraCond string, limitCnt int) ([]DBTableRow, error) {
	if !w.HasDb() {
		return nil, w.ec.Throw("SelectRowsFromDbEx", ErrNoDb)
	}

	rows, err := w.selectImpl(mapper, extraCond, limitCnt)
	return rows, w.ec.Throw("SelectRowsFromDb", err)
}

func (w *DataWorker) SelectFromDb(mapper interface{}) (DBTableRow, error) {
	return w.SelectFromDbEx(mapper, "")
}

func (w *DataWorker) SelectFromDbEx(mapper interface{}, extraCond string) (DBTableRow, error) {
	if !w.HasDb() {
		return nil, w.ec.Throw("SelectFromDbEx", ErrNoDb)
	}

	rows, err := w.selectImpl(mapper, extraCond, 1)
	if err != nil {
		return nil, w.ec.Throw("SelectFromDbEx", err)
	}

	if len(rows) == 0 {
		return nil, w.ec.Throw("SelectFromDbEx", ErrWorkerDBNotExist)
	}

	return rows[0], nil
}

func (w *DataWorker) Count(mapper interface{}) (int64, error) {
	return w.CountEx(mapper, "")
}

func (w *DataWorker) CountEx(mapper interface{}, extraCond string) (int64, error) {
	if !w.HasDb() {
		return 0, w.ec.Throw("CountEx", ErrNoDb)
	}

	countSql := w.countSql
	if len(extraCond) > 0 {
		countSql = countSql + extraCond
	}

	rows, err := w.dbDriver.NameQuery(w.cntRowReflectName, countSql, mapper)
	if err != nil {
		return 0, w.ec.Throw("CountEx", err)
	}

	if len(rows) == 0 {
		return 0, w.ec.Throw("CountEx", ErrWorkerDBNotExist)
	}

	rowObj, ok := rows[0].(*CountRow)
	if !ok {
		return 0, ErrWorkerDBCountUnknownErr
	}

	return rowObj.Count, nil
}

func (w *DataWorker) UpdateToDb(mapper interface{}) (int64, error) {
	return w.UpdateToDbEx(mapper, "")
}

func (w *DataWorker) UpdateToDbEx(mapper interface{}, extraCond string) (int64, error) {
	if !w.HasDb() {
		return 0, w.ec.Throw("UpdateToDbEx", ErrNoDb)
	}

	updateSql := w.updateSql
	if len(extraCond) > 0 {
		updateSql = updateSql + extraCond
	}

	effectRow, err := w.dbDriver.NameExec(updateSql, mapper)
	return effectRow, w.ec.Throw("UpdateToDb", err)
}

func (w *DataWorker) GetTableName() string {
	return w.tableName
}

func (w *DataWorker) GetInsertSql() string {
	return w.insertSql
}

func (w *DataWorker) SetInsertSql(insertSql string) {
	w.insertSql = insertSql
}

func (w *DataWorker) GetReplaceSql() string {
	return w.replaceSql
}

func (w *DataWorker) SetReplaceSql(replaceSql string) {
	w.replaceSql = replaceSql
}

func (w *DataWorker) GetUpdateSql() string {
	return w.updateSql
}

func (w *DataWorker) SetUpdateSql(updateSql string) {
	w.updateSql = updateSql
}

func (w *DataWorker) GetSelectSql() string {
	return w.selectSql
}

func (w *DataWorker) SetSelectSql(selectSql string) {
	w.selectSql = selectSql
}

func (w *DataWorker) GetDbDriver() (*DbDriver, bool) {
	return w.dbDriver, (w.dbDriver != nil)
}

// func (w *DataWorker) NameExec(query string, mapper interface{}) error {
// 	if !w.HasDb() {
// 		return w.ec.Throw("NameExec", ErrNoDb)
// 	}

// 	err := w.dbDriver.NameExec(query, mapper)
// 	return w.ec.Throw("NameExec", err)
// }

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
	updateTime := time.Now().UnixNano()
	mapField2Val[CACHE_FIELD_UPDATE_TIME] = strconv.FormatInt(updateTime, 10)
	err = w.SetCacheDataByMap(key, mapField2Val, false)
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

func (w *DataWorker) initCacheFields() {
	t := reflect.TypeOf(w).Elem()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := field.Tag.Get("mem")
		if name == "" {
			continue
		}

		w.cacheFields = append(w.cacheFields, name)
	}
}

// func (w *DataWorker) initCacheField2DbField() {
// 	if w.mapCacheField2DbField == nil {
// 		w.mapCacheField2DbField = make(map[string]string)
// 	}

// 	t := reflect.TypeOf(w).Elem()

// 	for i := 0; i < t.NumField(); i++ {
// 		field := t.Field(i)
// 		name := field.Tag.Get("db")
// 		if name == "" {
// 			continue
// 		}

// 		w.mapCacheField2DbField[name] = name
// 	}
// }

func (w *DataWorker) getCacheKey(key string) string {
	return w.tableName + "_" + key
}

func (w *DataWorker) getKey(cacheKey string) string {
	prefix := w.tableName + "_"
	idx := strings.Index(cacheKey, prefix)
	if idx == 0 {
		prefixLen := len(prefix)
		return cacheKey[prefixLen:]
	}

	return ""
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
	w.setUpdatedCacheKeys = yx.NewObjectSet()

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

	_, err = w.UpdateToDb(rowObj)
	return err
}

func (w *DataWorker) initInsertSql(tableObj interface{}, insertTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	insertFields, insertValues := w.getInsertFieldValues(v, insertTag)

	if len(insertFields) == 0 {
		return w.ec.Throw("initInsertSql", ErrWorkerInsertFieldNotFind)
	}

	fieldStr := strings.Join(insertFields, ",")
	valueStr := strings.Join(insertValues, ",")
	w.insertSql = "INSERT INTO " + w.tableName + " (" + fieldStr + ") VALUES (" + valueStr + ")"
	w.logger.D("Insert SQL: ", w.insertSql)
	return nil
}

func (w *DataWorker) getInsertFieldValues(v reflect.Type, insertTag string) ([]string, []string) {
	fields := make([]string, 0)
	values := make([]string, 0)

	if v.Kind() != reflect.Struct {
		return fields, values
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			superFields, superValues := w.getInsertFieldValues(field.Type, insertTag)
			fields = append(fields, superFields...)
			values = append(values, superValues...)
			continue
		}

		if field.Tag.Get(insertTag) != "" { // tag field
			fields = append(fields, name)
			values = append(values, ":"+name)
		}
	}

	return fields, values
}

func (w *DataWorker) initReplaceSql(tableObj interface{}, insertTag string, updateTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	replaceFields, replaceValues := w.getReplaceFieldValues(v, insertTag, updateTag)

	if len(replaceFields) == 0 {
		return w.ec.Throw("initReplaceSql", ErrWorkerReplaceFieldNotFind)
	}

	fieldStr := strings.Join(replaceFields, ",")
	valueStr := strings.Join(replaceValues, ",")
	w.replaceSql = "REPLACE INTO " + w.tableName + " (" + fieldStr + ") VALUES (" + valueStr + ")"
	w.logger.D("Replace SQL: ", w.replaceSql)
	return nil
}

func (w *DataWorker) getReplaceFieldValues(v reflect.Type, insertTag string, updateTag string) ([]string, []string) {
	fields := make([]string, 0)
	values := make([]string, 0)

	if v.Kind() != reflect.Struct {
		return fields, values
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			superFields, superValues := w.getReplaceFieldValues(field.Type, insertTag, updateTag)
			fields = append(fields, superFields...)
			values = append(values, superValues...)
			continue
		}

		if field.Tag.Get(insertTag) != "" || field.Tag.Get(updateTag) != "" { // tag field
			fields = append(fields, name)
			values = append(values, ":"+name)
		}
	}

	return fields, values
}

func (w *DataWorker) initSelectSql(tableObj interface{}, selectTag string, keyTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	selectFields, condFields := w.getSelectFields(v, selectTag, keyTag)

	// fields
	fieldStr := ""
	selectFieldsLen := len(selectFields)
	if selectFieldsLen == 0 {
		fieldStr = "*"
	} else if selectFieldsLen == 1 {
		fieldStr = selectFields[0]
	} else {
		fieldStr = strings.Join(selectFields, ",")
	}

	// conditions
	condStr := ""
	condFieldsLen := len(condFields)
	if condFieldsLen == 1 {
		condStr = condFields[0]
	} else if condFieldsLen > 1 {
		condStr = strings.Join(condFields, " AND ")
	}

	w.selectSql = "SELECT " + fieldStr + " FROM " + w.tableName
	w.countSql = "SELECT COUNT(*) AS count FROM " + w.tableName
	if condFieldsLen > 0 {
		w.selectSql += " WHERE " + condStr
		w.countSql += " WHERE " + condStr
	}

	refName, err := RowObjFactory.RegisterObject(&CountRow{}, nil, 10)
	if err != nil {
		return err
	}

	w.cntRowReflectName = refName

	w.logger.D("Select SQL: ", w.selectSql)
	w.logger.D("Count SQL: ", w.countSql)
	return nil
}

func (w *DataWorker) getSelectFields(v reflect.Type, selectTag string, keyTag string) ([]string, []string) {
	selectFields := make([]string, 0)
	condFields := make([]string, 0)

	if v.Kind() != reflect.Struct {
		return selectFields, condFields
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			superSelectFields, superCondFields := w.getSelectFields(field.Type, selectTag, keyTag)
			selectFields = append(selectFields, superSelectFields...)
			condFields = append(condFields, superCondFields...)
			continue
		}

		if field.Tag.Get(selectTag) != "" { // tag field
			selectFields = append(selectFields, name)
		}

		if field.Tag.Get(keyTag) != "" { // key field
			condFields = append(condFields, name+"=:"+name)
		}
	}

	return selectFields, condFields
}

func (w *DataWorker) initUpdateSql(tableObj interface{}, updateTag string, keyTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	updateFields, condFields := w.getUpdateFields(v, updateTag, keyTag)

	// sql := ""
	// keyField := ""

	if len(updateFields) == 0 {
		return w.ec.Throw("initUpdateSql", ErrWorkerKeyFieldNotDefine)
	}

	// fields
	fieldStr := ""
	updateFieldsLen := len(updateFields)
	if updateFieldsLen == 1 {
		fieldStr = updateFields[0]
	} else if updateFieldsLen > 1 {
		fieldStr = strings.Join(updateFields, ",")
	}

	// conditions
	condStr := ""
	condFieldsLen := len(condFields)
	if condFieldsLen == 1 {
		condStr = condFields[0]
	} else if condFieldsLen > 1 {
		condStr = strings.Join(condFields, " AND ")
	}

	w.updateSql = "UPDATE " + w.tableName + " SET " + fieldStr
	if condFieldsLen > 0 {
		w.updateSql += " WHERE " + condStr
	}

	w.logger.D("Update SQL: ", w.updateSql)
	return nil
}

func (w *DataWorker) getUpdateFields(v reflect.Type, updateTag string, keyTag string) ([]string, []string) {
	updateFields := make([]string, 0)
	condFields := make([]string, 0)

	if v.Kind() != reflect.Struct {
		return updateFields, condFields
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name == "" {
			superUpdateFields, superCondFields := w.getUpdateFields(field.Type, updateTag, keyTag)
			updateFields = append(updateFields, superUpdateFields...)
			condFields = append(condFields, superCondFields...)
			continue
		}

		if field.Tag.Get(updateTag) != "" { // tag field
			updateFields = append(updateFields, name+"=:"+name)
		}

		if field.Tag.Get(keyTag) != "" { // key field
			condFields = append(condFields, name+"=:"+name)
		}
	}

	return updateFields, condFields
}

func (w *DataWorker) selectImpl(mapper interface{}, extraCond string, limitCnt int) ([]DBTableRow, error) {
	// var err error = nil
	// defer w.ec.DeferThrow("selectImpl", &err)

	selectSql := w.selectSql
	if len(extraCond) > 0 {
		selectSql = selectSql + extraCond
	}

	if limitCnt > 0 {
		selectSql = fmt.Sprintf("%s LIMIT %d", selectSql, limitCnt)
	}

	rows, err := w.dbDriver.NameQuery(w.rowReflectName, selectSql, mapper)
	if err != nil {
		return nil, w.ec.Throw("selectImpl", err)
	}

	return rows, nil
}
