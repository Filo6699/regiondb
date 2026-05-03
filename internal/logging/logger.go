package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

type Sink interface {
	slog.Handler
}

type Clock func() time.Time

type Logger struct {
	logger *slog.Logger
	pid    int
}

func New(writer io.Writer) *Logger {
	return NewWithSink(newTextSink(writer), time.Now)
}

func NewWithSink(sink Sink, clock Clock) *Logger {
	return &Logger{
		logger: slog.New(clockSink{
			sink:  sink,
			clock: clock,
		}),
		pid: os.Getpid(),
	}
}

func newTextSink(writer io.Writer) Sink {
	return slog.NewTextHandler(writer, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attribute slog.Attr) slog.Attr {
			switch attribute.Key {
			case slog.TimeKey:
				attribute.Key = "timestamp"
				attribute.Value = slog.TimeValue(attribute.Value.Time().UTC())
			case slog.LevelKey:
				attribute.Value = slog.StringValue(strings.ToLower(attribute.Value.String()))
			case slog.MessageKey:
				attribute.Key = "event"
			}
			return attribute
		},
	})
}

func (l *Logger) Info(component, event string, attributes ...slog.Attr) {
	l.log(slog.LevelInfo, component, event, attributes)
}

func (l *Logger) Warn(component, event string, attributes ...slog.Attr) {
	l.log(slog.LevelWarn, component, event, attributes)
}

func (l *Logger) Error(component, event string, attributes ...slog.Attr) {
	l.log(slog.LevelError, component, event, attributes)
}

func (l *Logger) log(level slog.Level, component, event string, attributes []slog.Attr) {
	safe := make([]slog.Attr, 0, len(attributes)+2)
	safe = append(safe,
		slog.String("component", component),
		slog.Int("pid", l.pid),
	)
	for _, attribute := range attributes {
		if !sensitiveKey(attribute.Key) {
			safe = append(safe, attribute)
		}
	}
	l.logger.LogAttrs(context.Background(), level, event, safe...)
}

func sensitiveKey(key string) bool {
	key = strings.ToLower(key)
	return strings.Contains(key, "token") ||
		strings.Contains(key, "password") ||
		strings.Contains(key, "secret") ||
		key == "path" ||
		strings.HasSuffix(key, "_path") ||
		key == "dir" ||
		strings.HasSuffix(key, "_dir")
}

type clockSink struct {
	sink  Sink
	clock Clock
}

func (s clockSink) Enabled(ctx context.Context, level slog.Level) bool {
	return s.sink.Enabled(ctx, level)
}

func (s clockSink) Handle(ctx context.Context, record slog.Record) error {
	record.Time = s.clock()
	return s.sink.Handle(ctx, record)
}

func (s clockSink) WithAttrs(attributes []slog.Attr) slog.Handler {
	return clockSink{
		sink:  s.sink.WithAttrs(attributes),
		clock: s.clock,
	}
}

func (s clockSink) WithGroup(name string) slog.Handler {
	return clockSink{
		sink:  s.sink.WithGroup(name),
		clock: s.clock,
	}
}
