package logger

import (
	"io"
	"os"

	"github.com/rs/zerolog"
)

type ZeroLogger struct {
	zlogger zerolog.Logger
}

func NewZeroLog(env string) *ZeroLogger {
	return NewWithWriter(env, os.Stdout)
}

func NewWithWriter(env string, w io.Writer) *ZeroLogger {
	logger := zerolog.New(w).With().Timestamp().Logger()

	switch env {
	case "production":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	return &ZeroLogger{zlogger: logger}
}

// logWithFields applies dynamic fields efficiently using typed methods
func (l *ZeroLogger) logWithFields(event *zerolog.Event, fields []Field) *zerolog.Event {
	for _, f := range fields {
		switch v := f.Value.(type) {
		case string:
			event.Str(f.Key, v)
		case int:
			event.Int(f.Key, v)
		case int64:
			event.Int64(f.Key, v)
		case float64:
			event.Float64(f.Key, v)
		case bool:
			event.Bool(f.Key, v)
		default:
			event.Interface(f.Key, v) // fallback for complex types
		}
	}
	return event
}

func (l *ZeroLogger) Debug(msg string, fields ...Field) {
	l.logWithFields(l.zlogger.Debug(), fields).Msg(msg)
}

func (l *ZeroLogger) Info(msg string, fields ...Field) {
	l.logWithFields(l.zlogger.Info(), fields).Msg(msg)
}

func (l *ZeroLogger) Warn(msg string, fields ...Field) {
	l.logWithFields(l.zlogger.Warn(), fields).Msg(msg)
}

func (l *ZeroLogger) Error(msg string, fields ...Field) {
	l.logWithFields(l.zlogger.Error(), fields).Msg(msg)
}
