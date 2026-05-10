package alerts

// helpers_test.go - shared test infrastructure for the kc/alerts package.
// Consolidates helpers that were previously scattered across alert test files.
//
// Note: this file originally imported github.com/zerodha/kite-mcp-server/testutil
// for testutil.DiscardLogger(). After Path A.11 extraction to algo2go, the
// testutil dep was inlined as a stdlib slog.New(slog.NewTextHandler(io.Discard,
// nil)) call so this module stays self-contained without an unpublished
// transitive testutil dep.

import (
	"io"
	"log/slog"
)

// testLogger returns a discard logger shared across the alerts test suite.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestStore creates a Store with no notify callback (suitable for most
// tests). It lives here so every alerts_*_test.go file can call it without
// re-declaring the factory.
func newTestStore() *Store {
	return NewStore(nil)
}
