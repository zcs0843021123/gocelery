// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package broker

import (
	"encoding/json"
	"github.com/gocelery/gocelery"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"reflect"
	"testing"
	"time"
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
	redisBrokerWithConn  = NewRedisBroker(redisPool)
	amqpBroker           = NewAMQPCeleryBroker("amqp://")
)

func makeCeleryMessage() (*gocelery.CeleryMessage, error) {
	taskMessage := gocelery.GetTaskMessage("add")
	taskMessage.Args = []interface{}{rand.Intn(10), rand.Intn(10)}
	defer gocelery.ReleaseTaskMessage(taskMessage)
	encodedTaskMessage, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}
	return gocelery.GetCeleryMessage(encodedTaskMessage), nil
}

// TestBrokerRedisSend is Redis specific test that sets CeleryMessage to queue
func TestBrokerRedisSend(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBroker
	}{
		{
			name:   "send task to redis broker with connection",
			broker: redisBrokerWithConn,
		},
	}
	for _, tc := range testCases {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		conn := tc.broker.Get()
		defer conn.Close()
		messageJSON, err := conn.Do("BRPOP", tc.broker.QueueName, "1")
		if err != nil || messageJSON == nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		messageList := messageJSON.([]interface{})
		if string(messageList[0].([]byte)) != "celery" {
			t.Errorf("test '%s': non celery message received", tc.name)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		var message gocelery.CeleryMessage
		if err := json.Unmarshal(messageList[1].([]byte), &message); err != nil {
			t.Errorf("test '%s': failed to unmarshal received message: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(celeryMessage, &message) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, &message, celeryMessage)
		}
		gocelery.ReleaseCeleryMessage(celeryMessage)
	}
}

// TestBrokerRedisGet is Redis specific test that gets CeleryMessage from queue
func TestBrokerRedisGet(t *testing.T) {
	testCases := []struct {
		name   string
		broker *RedisCeleryBroker
	}{
		{
			name:   "get task from redis broker with connection",
			broker: redisBrokerWithConn,
		},
	}
	for _, tc := range testCases {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		jsonBytes, err := json.Marshal(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to marshal celery message: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		conn := tc.broker.Get()
		defer conn.Close()
		_, err = conn.Do("LPUSH", tc.broker.QueueName, jsonBytes)
		if err != nil {
			t.Errorf("test '%s': failed to push celery message to redis: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		message, err := tc.broker.GetCeleryMessage()
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		if !reflect.DeepEqual(message, celeryMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, celeryMessage)
		}
		gocelery.ReleaseCeleryMessage(celeryMessage)
	}
}

// TestBrokerSendGet tests set/get features for all brokers
func TestBrokerSendGet(t *testing.T) {
	testCases := []struct {
		name   string
		broker gocelery.CeleryBroker
	}{
		{
			name:   "send/get task for redis broker with connection",
			broker: redisBrokerWithConn,
		},
		{
			name:   "send/get task for amqp broker",
			broker: amqpBroker,
		},
	}
	for _, tc := range testCases {
		celeryMessage, err := makeCeleryMessage()
		if err != nil || celeryMessage == nil {
			t.Errorf("test '%s': failed to construct celery message: %v", tc.name, err)
			continue
		}
		err = tc.broker.SendCeleryMessage(celeryMessage)
		if err != nil {
			t.Errorf("test '%s': failed to send celery message to broker: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		// wait arbitrary time for message to propagate
		time.Sleep(1 * time.Second)
		message, err := tc.broker.GetTaskMessage()
		if err != nil {
			t.Errorf("test '%s': failed to get celery message from broker: %v", tc.name, err)
			gocelery.ReleaseCeleryMessage(celeryMessage)
			continue
		}
		originalMessage := celeryMessage.GetTaskMessage()
		if !reflect.DeepEqual(message, originalMessage) {
			t.Errorf("test '%s': received message %v different from original message %v", tc.name, message, originalMessage)
		}
		gocelery.ReleaseCeleryMessage(celeryMessage)
	}
}
