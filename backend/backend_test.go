// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package backend

import (
	"encoding/json"
	"fmt"
	"github.com/gocelery/gocelery"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"reflect"
	"testing"

	uuid "github.com/satori/go.uuid"
)

var (
	redisPool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://")
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}

	redisBackendWithConn = NewRedisBackend(redisPool)
	amqpBackend          = NewAMQPCeleryBackend("amqp://")
)

// TestBackendRedisGetResult is Redis specific test to get result from backend
func TestBackendRedisGetResult(t *testing.T) {
	testCases := []struct {
		name    string
		backend *RedisCeleryBackend
	}{
		{
			name:    "get result from redis backend with connection",
			backend: redisBackendWithConn,
		},
	}
	for _, tc := range testCases {
		taskID := uuid.Must(uuid.NewV4()).String()
		// value must be float64 for testing due to json limitation
		value := reflect.ValueOf(rand.Float64())
		resultMessage := gocelery.GetReflectionResultMessage(&value)
		messageBytes, err := json.Marshal(resultMessage)
		if err != nil {
			t.Errorf("test '%s': error marshalling result message: %v", tc.name, err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		conn := tc.backend.Get()
		defer conn.Close()
		_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, messageBytes)
		if err != nil {
			t.Errorf("test '%s': error setting result message to celery: %v", tc.name, err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		res, err := tc.backend.GetResult(taskID)
		if err != nil {
			t.Errorf("test '%s': error getting result from backend: %v", tc.name, err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		if !reflect.DeepEqual(res, resultMessage) {
			t.Errorf("test '%s': result message received %v is different from original %v", tc.name, res, resultMessage)
		}
		gocelery.ReleaseResultMessage(resultMessage)
	}
}

// TestBackendRedisSetResult is Redis specific test to set result to backend
func TestBackendRedisSetResult(t *testing.T) {
	testCases := []struct {
		name    string
		backend *RedisCeleryBackend
	}{
		{
			name:    "set result to redis backend with connection",
			backend: redisBackendWithConn,
		},
	}
	for _, tc := range testCases {
		taskID := uuid.Must(uuid.NewV4()).String()
		value := reflect.ValueOf(rand.Float64())
		resultMessage := gocelery.GetReflectionResultMessage(&value)
		err := tc.backend.SetResult(taskID, resultMessage)
		if err != nil {
			t.Errorf("test '%s': error setting result to backend: %v", tc.name, err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		conn := tc.backend.Get()
		defer conn.Close()
		val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
		if err != nil {
			t.Errorf("test '%s': error getting data from redis: %v", tc.name, err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		if val == nil {
			t.Errorf("test '%s': result not available from redis", tc.name)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		var res gocelery.ResultMessage
		err = json.Unmarshal(val.([]byte), &res)
		if err != nil {
			t.Errorf("test '%s': error parsing json result", tc.name)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		if !reflect.DeepEqual(&res, resultMessage) {
			t.Errorf("test '%s': result message received %v is different from original %v", tc.name, &res, resultMessage)
		}
		gocelery.ReleaseResultMessage(resultMessage)
	}
}

// TestBackendSetGetResult tests set/get result feature for all backends
func TestBackendSetGetResult(t *testing.T) {
	testCases := []struct {
		name    string
		backend gocelery.CeleryBackend
	}{
		{
			name:    "set/get result to redis backend with connection",
			backend: redisBackendWithConn,
		},
		{
			name:    "set/get result to amqp backend",
			backend: amqpBackend,
		},
	}
	for _, tc := range testCases {
		taskID := uuid.Must(uuid.NewV4()).String()
		value := reflect.ValueOf(rand.Float64())
		resultMessage := gocelery.GetReflectionResultMessage(&value)
		err := tc.backend.SetResult(taskID, resultMessage)
		if err != nil {
			t.Errorf("error setting result to backend: %v", err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		res, err := tc.backend.GetResult(taskID)
		if err != nil {
			t.Errorf("error getting result from backend: %v", err)
			gocelery.ReleaseResultMessage(resultMessage)
			continue
		}
		if !reflect.DeepEqual(res, resultMessage) {
			t.Errorf("result message received %v is different from original %v", res, resultMessage)
		}
		gocelery.ReleaseResultMessage(resultMessage)
	}
}
