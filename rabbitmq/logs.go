package rabbitmq

import (
	"github.com/yyle88/zaplog"
	"go.uber.org/zap"
)

type Log interface {
	ErrorLog(msg string, fields ...zap.Field)
	DebugLog(msg string, fields ...zap.Field)
}

var log Log = newMqLog()

func SetLog(mqLog Log) {
	log = mqLog
}

type mqLog struct{}

func newMqLog() *mqLog {
	return &mqLog{}
}

func (z *mqLog) ErrorLog(msg string, fields ...zap.Field) {
	zaplog.LOGS.Skip(1).Error(msg, fields...)
}

func (z *mqLog) DebugLog(msg string, fields ...zap.Field) {
	zaplog.LOGS.Skip(1).Debug(msg, fields...)
}
