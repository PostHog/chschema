package logger

import (
	"context"

	"github.com/rs/zerolog"
)

type contextKey string

const (
	loggerKey contextKey = "logger"
)

// WithLogger adds a logger to the context
func WithLogger(ctx context.Context, logger zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromContext retrieves the logger from context, or returns the global logger if not found
func FromContext(ctx context.Context) zerolog.Logger {
	if logger, ok := ctx.Value(loggerKey).(zerolog.Logger); ok {
		return logger
	}
	return Logger
}

// WithDatabase creates a sub-logger with database field
func WithDatabase(database string) zerolog.Logger {
	return Logger.With().Str("database", database).Logger()
}

// WithTable creates a sub-logger with table field
func WithTable(table string) zerolog.Logger {
	return Logger.With().Str("table", table).Logger()
}

// WithOperation creates a sub-logger with operation field
func WithOperation(operation string) zerolog.Logger {
	return Logger.With().Str("operation", operation).Logger()
}
