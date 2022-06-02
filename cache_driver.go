// Copyright 2022 Guan Jianchang. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odb

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
	"github.com/yxlib/yx"
)

var (
	ErrCacheSubscribeFailed = errors.New("Subscribe failed")
)

type RedisSubscribeCb func(channel string, pattern string, payload string)

type CacheDriver struct {
	db            *redis.Client
	evtNotifyExit *yx.Event
	evtExit       *yx.Event
	logger        *yx.Logger
	ec            *yx.ErrCatcher
}

func NewCacheDriver() *CacheDriver {
	return &CacheDriver{
		db:            nil,
		evtNotifyExit: yx.NewEvent(),
		evtExit:       yx.NewEvent(),
		logger:        yx.NewLogger("CacheDriver"),
		ec:            yx.NewErrCatcher("CacheDriver"),
	}
}

func (d *CacheDriver) Open(addr string, pwd string, db int) error {
	if d.db != nil {
		return nil
	}

	d.db = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       db,
	})

	_, err := d.db.Ping().Result()
	if err != nil {
		return d.ec.Throw("Open", err)
	}

	d.logger.I("open redis " + addr + " success")
	return nil
}

func (d *CacheDriver) Close() error {
	if d.db == nil {
		return nil
	}

	d.evtNotifyExit.Send()
	err := d.db.Close()
	return d.ec.Throw("Close", err)
}

func (d *CacheDriver) WaitExit() {
	d.evtExit.Wait()
}

func (d *CacheDriver) Publish(channel string, message interface{}) error {
	err := d.db.Publish(channel, message).Err()
	return d.ec.Throw("Publish", err)
}

func (d *CacheDriver) Subscribe(cb RedisSubscribeCb, channels ...string) error {
	pubsub := d.db.Subscribe(channels...)
	if pubsub == nil {
		return d.ec.Throw("Subscribe", ErrCacheSubscribeFailed)
	}

	ch := pubsub.Channel()

	for {
		select {
		case <-d.evtNotifyExit.C:
			goto Exit0

		case msg := <-ch:
			cb(msg.Channel, msg.Pattern, msg.Payload)
		}
	}

Exit0:
	d.evtExit.Send()
	return nil
}

func (d *CacheDriver) Exists(keys ...string) (int64, error) {
	count, err := d.db.Exists(keys...).Result()
	return count, d.ec.Throw("Exists", err)
}

func (d *CacheDriver) Set(key string, value interface{}, expiration time.Duration) error {
	err := d.db.Set(key, value, expiration).Err()
	return d.ec.Throw("Set", err)
}

func (d *CacheDriver) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	succ, err := d.db.SetNX(key, value, expiration).Result()
	return succ, d.ec.Throw("SetNX", err)
}

func (d *CacheDriver) MSet(pairs ...interface{}) (string, error) {
	str, err := d.db.MSet(pairs...).Result()
	return str, d.ec.Throw("MSet", err)
}

func (d *CacheDriver) Get(key string) (string, error) {
	str, err := d.db.Get(key).Result()
	return str, d.ec.Throw("Get", err)
}

func (d *CacheDriver) MGet(keys ...string) ([]interface{}, error) {
	data, err := d.db.MGet(keys...).Result()
	return data, d.ec.Throw("MGet", err)
}

func (d *CacheDriver) Expire(key string, expiration time.Duration) (bool, error) {
	ok, err := d.db.Expire(key, expiration).Result()
	return ok, d.ec.Throw("Expire", err)
}

func (d *CacheDriver) ExpireAt(key string, tm time.Time) (bool, error) {
	ok, err := d.db.ExpireAt(key, tm).Result()
	return ok, d.ec.Throw("ExpireAt", err)
}

func (d *CacheDriver) Persist(key string) (bool, error) {
	ok, err := d.db.Persist(key).Result()
	return ok, d.ec.Throw("Persist", err)
}

func (d *CacheDriver) TTL(key string) (time.Duration, error) {
	t, err := d.db.TTL(key).Result()
	return t, d.ec.Throw("TTL", err)
}

func (d *CacheDriver) Incr(key string) (int64, error) {
	n, err := d.db.Incr(key).Result()
	return n, d.ec.Throw("Incr", err)
}

// list
func (d *CacheDriver) RPush(key string, values ...interface{}) error {
	err := d.db.RPush(key, values...).Err()
	return d.ec.Throw("RPush", err)
}

func (d *CacheDriver) LSet(key string, index int64, value interface{}) error {
	err := d.db.LSet(key, index, value).Err()
	return d.ec.Throw("LSet", err)
}

func (d *CacheDriver) LRem(key string, count int64, value interface{}) (int64, error) {
	n, err := d.db.LRem(key, count, value).Result()
	return n, d.ec.Throw("LRem", err)
}

func (d *CacheDriver) LLen(key string) (int64, error) {
	n, err := d.db.LLen(key).Result()
	return n, d.ec.Throw("LLen", err)
}

func (d *CacheDriver) LRange(key string, start int64, stop int64) ([]string, error) {
	s, err := d.db.LRange(key, start, stop).Result()
	return s, d.ec.Throw("LRange", err)
}

func (d *CacheDriver) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	s, err := d.db.BLPop(timeout, keys...).Result()
	return s, d.ec.Throw("BLPop", err)
}

// hash
func (d *CacheDriver) HSet(key string, field string, value interface{}) (bool, error) {
	ok, err := d.db.HSet(key, field, value).Result()
	return ok, d.ec.Throw("HSet", err)
}

func (d *CacheDriver) HMSet(key string, fields map[string]interface{}) error {
	err := d.db.HMSet(key, fields).Err()
	return d.ec.Throw("HMSet", err)
}

func (d *CacheDriver) HMGet(key string, fields ...string) ([]interface{}, error) {
	o, err := d.db.HMGet(key, fields...).Result()
	return o, d.ec.Throw("HMGet", err)

}

func (d *CacheDriver) HGetAll(key string) (map[string]string, error) {
	data, err := d.db.HGetAll(key).Result()
	return data, d.ec.Throw("HGetAll", err)
}

func (d *CacheDriver) HExists(key string, field string) (bool, error) {
	bExist, err := d.db.HExists(key, field).Result()
	return bExist, d.ec.Throw("HExists", err)
}

func (d *CacheDriver) HSetNX(key string, field string, value interface{}) (bool, error) {
	succ, err := d.db.HSetNX(key, field, value).Result()
	return succ, d.ec.Throw("HSetNX", err)
}

func (d *CacheDriver) HDel(key string, field string) (int64, error) {
	n, err := d.db.HDel(key, field).Result()
	return n, d.ec.Throw("HDel", err)
}

func (d *CacheDriver) Del(keys ...string) (int64, error) {
	i, err := d.db.Del(keys...).Result()
	return i, d.ec.Throw("Del", err)
}

func (d *CacheDriver) Keys(pattern string) ([]string, error) {
	keys, err := d.db.Keys(pattern).Result()
	return keys, d.ec.Throw("Keys", err)
}
