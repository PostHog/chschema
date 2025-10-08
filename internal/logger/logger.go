package logger

import (
	"io"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/term"
)

// Logger is the global logger instance
var Logger zerolog.Logger

// testMode indicates if logger is in test mode (suppresses output)
var testMode = false

// Init initializes the global logger with the specified configuration
func Init(level string, format string, logFile string, noColor bool) error {
	// Set log level
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	// Determine output writer
	var writer io.Writer
	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		writer = file
	} else {
		writer = os.Stderr
	}

	// Suppress output in test mode
	if testMode {
		writer = io.Discard
	}

	// Configure output format
	if format == "console" || (format == "auto" && isTerminal(writer)) {
		output := zerolog.ConsoleWriter{
			Out:        writer,
			TimeFormat: "15:04:05",
			NoColor:    noColor,
		}
		Logger = zerolog.New(output).With().Timestamp().Logger()
	} else {
		Logger = zerolog.New(writer).With().Timestamp().Logger()
	}

	// Set global logger
	log.Logger = Logger

	return nil
}

// isTerminal checks if the writer is a terminal for auto-detecting console format
func isTerminal(w io.Writer) bool {
	if f, ok := w.(*os.File); ok {
		return term.IsTerminal(int(f.Fd()))
	}
	return false
}

// SetTestMode enables or disables test mode
// In test mode, all log output is suppressed
func SetTestMode(enabled bool) {
	testMode = enabled
	if enabled {
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}
}

// Get returns the global logger instance
func Get() zerolog.Logger {
	return Logger
}
