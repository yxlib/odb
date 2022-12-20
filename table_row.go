// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package odb

import (
	"reflect"
	"strconv"
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

type CountRow struct {
	BaseDBTableRow

	Count int64 `db:"count"`
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
