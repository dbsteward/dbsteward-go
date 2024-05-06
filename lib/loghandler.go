package lib

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
)

func newLogHandler(dbs *DBSteward) slog.Handler {
	buf := bytes.Buffer{}
	f := slog.NewTextHandler(&buf, nil)
	return &logHandler{
		dbsteward: dbs,
		formatter: f,
		output:    &buf,
	}
}

// logHandler is an intermediate step to support both slog logging
// and the old method of dbsteward logging
type logHandler struct {
	dbsteward *DBSteward
	formatter slog.Handler
	output    *bytes.Buffer
}

// Enabled always returns true and let zerolog decide
func (h *logHandler) Enabled(_ context.Context, level slog.Level) bool {
	return true
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{
		dbsteward: h.dbsteward,
		output:    h.output,
		formatter: h.formatter.WithAttrs(attrs),
	}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{
		dbsteward: h.dbsteward,
		output:    h.output,
		formatter: h.formatter.WithGroup(name),
	}
}

// Handle is a bit of a hack. Just using TextFormatter to do the actual
// handling and and then extracting the result from the byte buffer to
// send it to zerolog as an intermediate step that maintains nearly
// the same behavior as previous while still supporting all of slog's
// features
func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	h.formatter.Handle(ctx, r)
	msg := strings.TrimSpace(h.output.String())
	if msg == "" {
		msg = "<<logHander received empty message>>"
	}
	switch r.Level {
	case slog.LevelDebug:
		h.dbsteward.logger.Debug().Msgf(msg)
	case slog.LevelInfo:
		h.dbsteward.logger.Info().Msgf(msg)
	case slog.LevelWarn:
		h.dbsteward.logger.Warn().Msgf(msg)
	default:
		// Should be Error, but in case other levels get define at
		// least nothing gets lost
		h.dbsteward.logger.Error().Msgf(msg)
	}
	h.output.Reset()
	return nil
}
