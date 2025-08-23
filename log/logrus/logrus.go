package logrus

import (
	"github.com/sirupsen/logrus"
	"github.com/unkn0wn-root/cascache"
)

type LogrusLogger struct{ E *logrus.Entry }

func (l LogrusLogger) Debug(msg string, f cascache.Fields) {
	l.E.WithFields(logrus.Fields(f)).Debug(msg)
}
func (l LogrusLogger) Info(msg string, f cascache.Fields) { l.E.WithFields(logrus.Fields(f)).Info(msg) }
func (l LogrusLogger) Warn(msg string, f cascache.Fields) { l.E.WithFields(logrus.Fields(f)).Warn(msg) }
func (l LogrusLogger) Error(msg string, f cascache.Fields) {
	l.E.WithFields(logrus.Fields(f)).Error(msg)
}
