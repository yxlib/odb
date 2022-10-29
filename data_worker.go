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

type BaseDBTableRow struct {
}

func (r *BaseDBTableRow) FromCache(mapField2Val map[string]string) error {
	return nil
}

func (r *BaseDBTableRow) ToCache(fields []string) (map[string]interface{}, error) {
	return nil, nil
}

func FromCache(r DBTableRow, mapField2Val map[string]string) error {
	t := reflect.TypeOf(r).Elem()
	v := reflect.ValueOf(r).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := field.Tag.Get("mem")
		if name == "" {
			continue
		}

		strVal, ok := mapField2Val[name]
		if ok {
			value := v.FieldByName(field.Name)
			fromCacheValue(value, strVal)
		}
	}

	return nil
}

func fromCacheValue(value reflect.Value, strVal string) {
	data := value.Interface()

	switch data.(type) {
	case bool:
		boolVal, err := strconv.ParseBool(strVal)
		if err == nil {
			value.SetBool(boolVal)
		}

	case int, int8, int16, int32, int64:
		intVal, err := strconv.ParseInt(strVal, 10, 64)
		if err == nil {
			value.SetInt(intVal)
		}

	case uint, uint8, uint16, uint32, uint64:
		uintVal, err := strconv.ParseUint(strVal, 10, 64)
		if err == nil {
			value.SetUint(uintVal)
		}

	case float32, float64:
		floatVal, err := strconv.ParseFloat(strVal, 64)
		if err == nil {
			value.SetFloat(floatVal)
		}

	case []byte:
		value.SetBytes([]byte(strVal))

	case string:
		value.SetString(strVal)
	}
}

func ToCache(r DBTableRow, fields []string) (map[string]interface{}, error) {
	mapField2Val := make(map[string]interface{})

	mapField2Flag := make(map[string]bool)
	if len(fields) > 0 {
		for _, field := range fields {
			mapField2Flag[field] = true
		}
	}

	t := reflect.TypeOf(r).Elem()
	v := reflect.ValueOf(r).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := field.Tag.Get("mem")
		if name == "" {
			continue
		}

		ok := (len(mapField2Flag) == 0)
		if !ok {
			_, ok = mapField2Flag[name]
		}

		if ok {
			value := v.FieldByName(field.Name)
			mapField2Val[name] = toCacheValue(value)
		}
	}

	return mapField2Val, nil
}

func toCacheValue(value reflect.Value) string {
	data := value.Interface()

	switch v := data.(type) {
	case bool:
		boolVal := value.Bool()
		return strconv.FormatBool(boolVal)

	case int, int8, int16, int32, int64:
		intVal := value.Int()
		return strconv.FormatInt(intVal, 10)

	case uint, uint8, uint16, uint32, uint64:
		uintVal := value.Uint()
		return strconv.FormatUint(uintVal, 10)

	case float32, float64:
		floatVal := value.Float()
		return strconv.FormatFloat(floatVal, 'f', 5, 64)

	case []byte:
		return string(v)

	case string:
		return v
	}

	return ""
}

type DataWorker struct {
	cacheDriver *CacheDriver
	// mapCacheField2DbField map[string]string
	cacheFields         []string
	cacheKeyName        string
	dbDriver            *DbDriver
	tableName           string
	rowReflectName      string
	keyField            string
	insertSql           string
	replaceSql          string
	selectSql           string
	updateSql           string
	setUpdatedCacheKeys *yx.Set
	lckUpdatedCacheKeys *sync.Mutex
	bOpenAutoSave       bool
	bAutoSave           bool
	evtStop             *yx.Event
	evtExit             *yx.Event
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
		keyField:            "",
		insertSql:           "",
		replaceSql:          "",
		selectSql:           "",
		updateSql:           "",
		setUpdatedCacheKeys: yx.NewSet(yx.SET_TYPE_OBJ),
		lckUpdatedCacheKeys: &sync.Mutex{},
		bOpenAutoSave:       false,
		bAutoSave:           false,
		evtStop:             yx.NewEvent(),
		evtExit:             yx.NewEvent(),
		logger:              yx.NewLogger("DataWorker"),
		ec:                  yx.NewErrCatcher("DataWorker"),
	}

	w.initCacheFields()
	// if len(w.mapCacheField2DbField) == 0 {
	// 	w.initCacheField2DbField()
	// }

	// for cacheField := range mapCacheField2DbField {
	// 	w.cacheFields = append(w.cacheFields, cacheField)
	// }

	return w
}

func (w *DataWorker) Init(reflectName string, insertTag string, selectTag string, selectKeyTag string, updateTag string, updateKeyTag string, mapperDbTag string, mapperValTag string) error {
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
		w.logger.W("init insert SQL failed, err: ", err)
	}

	err = w.initReplaceSql(rowObj, insertTag, updateTag)
	if err != nil {
		w.logger.W("init replace SQL failed, err: ", err)
	}

	err = w.initSelectSql(rowObj, selectTag, selectKeyTag, mapperDbTag, mapperValTag)
	if err != nil {
		w.logger.W("init select SQL failed, err: ", err)
	}

	err = w.initUpdateSql(rowObj, updateTag, updateKeyTag, mapperDbTag, mapperValTag)
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

func (w *DataWorker) GetTableName() string {
	return w.tableName
}

func (w *DataWorker) NameExec(query string, mapper interface{}) error {
	if !w.HasDb() {
		return w.ec.Throw("NameExec", ErrNoDb)
	}

	err := w.dbDriver.NameExec(query, mapper)
	return w.ec.Throw("NameExec", err)
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
	mapField2Val[CACHE_FIELD_UPDATE_TIME] = time.Now().UnixNano()
	err = w.SetCacheDataByMap(key, mapField2Val)
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

func (w *DataWorker) initSelectSql(tableObj interface{}, selectTag string, keyTag string, mapperDbTag string, mapperValTag string) error {
	v := reflect.TypeOf(tableObj).Elem()

	selectFields := make([]string, 0)
	condFields := make([]string, 0)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")
		if name != "" {
			if field.Tag.Get(selectTag) != "" { // tag field
				selectFields = append(selectFields, name)
			}
		}

		if field.Tag.Get(keyTag) != "" { // key field
			dbName := field.Tag.Get(mapperDbTag)
			valName := field.Tag.Get(mapperValTag)
			if dbName != "" && valName != "" {
				condFields = append(condFields, dbName+"=:"+valName)
			} else if name != "" {
				condFields = append(condFields, name+"=:"+name)
			}
		}
	}

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
	if condFieldsLen > 0 {
		w.selectSql += " WHERE " + condStr
	}

	w.logger.D("Select SQL: ", w.selectSql)
	return nil
}

func (w *DataWorker) initUpdateSql(tableObj interface{}, updateTag string, keyTag string, mapperDbTag string, mapperValTag string) error {
	v := reflect.TypeOf(tableObj).Elem()
	// sql := ""
	// keyField := ""

	updateFields := make([]string, 0)
	condFields := make([]string, 0)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := field.Tag.Get("db")

		if name != "" {
			if field.Tag.Get(updateTag) != "" { // tag field
				updateFields = append(updateFields, name+"=:"+name)
			}
		}

		if field.Tag.Get(keyTag) != "" { // key field
			dbName := field.Tag.Get(mapperDbTag)
			valName := field.Tag.Get(mapperValTag)
			if dbName != "" && valName != "" {
				condFields = append(condFields, dbName+"=:"+valName)
			} else if name != "" {
				condFields = append(condFields, name+"=:"+name)
			}
		}
	}

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

func (w *DataWorker) selectImpl(mapper interface{}, limitCnt int) ([]DBTableRow, error) {
	var err error = nil
	defer w.ec.DeferThrow("selectImpl", &err)

	selectSql := w.selectSql
	if limitCnt > 0 {
		selectSql = fmt.Sprintf("%s LIMIT %d", w.selectSql, limitCnt)
	}

	rows, err := w.dbDriver.NameQuery(w.rowReflectName, selectSql, mapper)
	if err != nil {
		return nil, w.ec.Throw("selectImpl", err)
	}

	return rows, nil
}
