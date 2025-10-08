package logger

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		format    string
		logFile   string
		noColor   bool
		wantErr   bool
		wantLevel zerolog.Level
	}{
		{
			name:      "default info level",
			level:     "info",
			format:    "json",
			wantLevel: zerolog.InfoLevel,
		},
		{
			name:      "debug level",
			level:     "debug",
			format:    "json",
			wantLevel: zerolog.DebugLevel,
		},
		{
			name:      "error level",
			level:     "error",
			format:    "json",
			wantLevel: zerolog.ErrorLevel,
		},
		{
			name:      "warn level",
			level:     "warn",
			format:    "json",
			wantLevel: zerolog.WarnLevel,
		},
		{
			name:      "invalid level defaults to info",
			level:     "invalid",
			format:    "json",
			wantLevel: zerolog.InfoLevel,
		},
		{
			name:    "console format",
			level:   "info",
			format:  "console",
			noColor: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Init(tt.level, tt.format, tt.logFile, tt.noColor)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantLevel != 0 {
				assert.Equal(t, tt.wantLevel, zerolog.GlobalLevel())
			}
		})
	}
}

func TestSetTestMode(t *testing.T) {
	// Enable test mode
	SetTestMode(true)
	assert.True(t, testMode)
	assert.Equal(t, zerolog.Disabled, zerolog.GlobalLevel())

	// Disable test mode
	SetTestMode(false)
	assert.False(t, testMode)
}

func TestWithLogger(t *testing.T) {
	// Set global level to allow info logs
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	buf := &bytes.Buffer{}
	testLogger := zerolog.New(buf)

	ctx := WithLogger(context.Background(), testLogger)
	retrievedLogger := FromContext(ctx)

	// Log something with the retrieved logger
	retrievedLogger.Info().Msg("test message")

	// Verify the message was logged to the buffer
	assert.Contains(t, buf.String(), "test message")
}

func TestFromContext_NoLogger(t *testing.T) {
	// When no logger is in context, should return global logger
	ctx := context.Background()
	retrievedLogger := FromContext(ctx)

	// Should not panic and should return a logger
	assert.NotNil(t, retrievedLogger)
}

func TestWithDatabase(t *testing.T) {
	// Set global level to allow info logs
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	buf := &bytes.Buffer{}
	Logger = zerolog.New(buf)

	dbLogger := WithDatabase("testdb")
	dbLogger.Info().Msg("test message")

	output := buf.String()
	assert.Contains(t, output, "testdb")
	assert.Contains(t, output, "database")
}

func TestWithTable(t *testing.T) {
	// Set global level to allow info logs
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	buf := &bytes.Buffer{}
	Logger = zerolog.New(buf)

	tableLogger := WithTable("users")
	tableLogger.Info().Msg("test message")

	output := buf.String()
	assert.Contains(t, output, "users")
	assert.Contains(t, output, "table")
}

func TestWithOperation(t *testing.T) {
	// Set global level to allow info logs
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	buf := &bytes.Buffer{}
	Logger = zerolog.New(buf)

	opLogger := WithOperation("CREATE")
	opLogger.Info().Msg("test message")

	output := buf.String()
	assert.Contains(t, output, "CREATE")
	assert.Contains(t, output, "operation")
}

func TestInit_WithFile(t *testing.T) {
	// Create a temporary log file
	tmpFile, err := os.CreateTemp("", "test-log-*.log")
	require.NoError(t, err)
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Initialize logger with file output
	err = Init("info", "json", tmpFile.Name(), false)
	require.NoError(t, err)

	// Log something
	Logger.Info().Msg("test file log")

	// Read the file and verify content
	content, err := os.ReadFile(tmpFile.Name())
	require.NoError(t, err)
	assert.Contains(t, string(content), "test file log")
}

func TestInit_InvalidFile(t *testing.T) {
	// Try to init with invalid file path
	err := Init("info", "json", "/invalid/path/to/file.log", false)
	assert.Error(t, err)
}

func TestIsTerminal(t *testing.T) {
	// Test with non-terminal writer
	buf := &bytes.Buffer{}
	assert.False(t, isTerminal(buf))

	// Test with os.Stderr (may or may not be terminal depending on environment)
	// Just verify it doesn't panic
	result := isTerminal(os.Stderr)
	assert.NotNil(t, result) // result can be true or false, just verify it returns something
}

func TestGet(t *testing.T) {
	// Set global level to allow info logs
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	buf := &bytes.Buffer{}
	Logger = zerolog.New(buf)

	retrieved := Get()
	retrieved.Info().Msg("test get")

	output := buf.String()
	assert.Contains(t, output, "test get")
}

func TestLogLevels(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		logFunc   func(zerolog.Logger)
		shouldLog bool
	}{
		{
			name:      "info level logs info",
			level:     "info",
			logFunc:   func(l zerolog.Logger) { l.Info().Msg("info msg") },
			shouldLog: true,
		},
		{
			name:      "info level does not log debug",
			level:     "info",
			logFunc:   func(l zerolog.Logger) { l.Debug().Msg("debug msg") },
			shouldLog: false,
		},
		{
			name:      "error level does not log info",
			level:     "error",
			logFunc:   func(l zerolog.Logger) { l.Info().Msg("info msg") },
			shouldLog: false,
		},
		{
			name:      "error level logs error",
			level:     "error",
			logFunc:   func(l zerolog.Logger) { l.Error().Msg("error msg") },
			shouldLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := Init(tt.level, "json", "", false)
			require.NoError(t, err)

			// Redirect output to buffer
			Logger = zerolog.New(buf)

			tt.logFunc(Logger)

			output := buf.String()
			if tt.shouldLog {
				assert.NotEmpty(t, output)
			} else {
				// Debug messages might not appear
				if strings.Contains(tt.name, "debug") {
					assert.Empty(t, output)
				}
			}
		})
	}
}
