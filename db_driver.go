// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/yxlib/yx"
)

var (
	ErrMapperIsNil  = errors.New("mapper is nil")
	ErrNotRowObject = errors.New("not table row object")
)

const (
	INIT_REUSE_COUNT = 10
	MAX_REUSE_COUNT  = 100
)

type DbDriver struct {
	db      *sqlx.DB
	factory *yx.ObjectFactory
	logger  *yx.Logger
	ec      *yx.ErrCatcher
}

func NewDbDriver(rowObjFactory *yx.ObjectFactory) *DbDriver {
	return &DbDriver{
		db:      nil,
		factory: rowObjFactory,
		logger:  yx.NewLogger("DbDriver"),
		ec:      yx.NewErrCatcher("DbDriver"),
	}
}

func (d *DbDriver) Open(userName string, pwd string, host string, port uint16, database string, charset string) error {
	var err error = nil
	defer d.ec.DeferThrow("Open", &err)

	if d.db != nil {
		return nil
	}

	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", userName, pwd, host, port, database, charset)
	d.logger.D("open db: ", dataSourceName)
	db, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	d.db = db

	d.logger.I("open mysql success")
	return nil
}

func (d *DbDriver) Close() error {
	if d.db != nil {
		err := d.db.Close()
		return d.ec.Throw("Close", err)
	}

	return nil
}

func (d *DbDriver) CreateTableRow(rowReflectName string) (DBTableRow, error) {
	obj, err := d.factory.CreateObject(rowReflectName)
	if err != nil {
		return nil, d.ec.Throw("CreateTableRow", err)
	}

	row, ok := obj.(DBTableRow)
	if !ok {
		return nil, d.ec.Throw("CreateTableRow", ErrNotRowObject)
	}

	return row, nil
}

func (d *DbDriver) ReuseTableRow(rowObj DBTableRow, rowReflectName string) {
	d.factory.ReuseObject(rowObj, rowReflectName)
}

func (d *DbDriver) NameExec(query string, mapper interface{}) error {
	_, err := d.db.NamedExec(query, mapper)
	return d.ec.Throw("NameExec", err)
}

func (d *DbDriver) NameInsert(query string, mapper interface{}) (int64, error) {
	var err error = nil
	defer d.ec.DeferThrow("NameInsert", &err)

	r, err := d.db.NamedExec(query, mapper)
	if err != nil {
		return 0, err
	}

	lastId, err := r.LastInsertId()
	if err != nil {
		return 0, err
	}

	return lastId, nil
}

func (d *DbDriver) NameQuery(rowReflectName string, query string, mapper interface{}) ([]DBTableRow, error) {
	var err error = nil
	defer d.ec.DeferThrow("NameQuery", &err)

	if mapper == nil {
		err = ErrMapperIsNil
		return nil, err
	}

	rows, err := d.db.NamedQuery(query, mapper)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var row DBTableRow = nil
	rowDatas := make([]DBTableRow, 0)

	for rows.Next() {
		row, err = d.CreateTableRow(rowReflectName)
		if err != nil {
			return nil, err
		}

		err = rows.StructScan(row)
		if err != nil {
			return nil, err
		}

		rowDatas = append(rowDatas, row)
	}

	return rowDatas, nil
}

func (d *DbDriver) Select(dest interface{}, query string, args ...interface{}) error {
	err := d.db.Select(dest, query, args...)
	return d.ec.Throw("Select", err)
}

func (d *DbDriver) Exec(query string, args ...interface{}) (sql.Result, error) {
	r, err := d.db.Exec(query, args...)
	return r, d.ec.Throw("Exec", err)
}

func (d *DbDriver) Query(sql string) ([]map[string]string, error) {
	rows, err := d.db.Query(sql)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	cols, _ := rows.Columns()

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))

	for i := range values {
		scans[i] = &values[i]
	}

	res := make([]map[string]string, 0)
	for rows.Next() {
		_ = rows.Scan(scans...)
		row := make(map[string]string)
		for k, v := range values {
			key := cols[k]
			row[key] = string(v)
		}

		res = append(res, row)
	}

	return res, nil
}
