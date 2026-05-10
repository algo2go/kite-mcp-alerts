//go:build libsql

// Package alerts — Turso/libSQL adapter tests for Phase 2.6 Path 6.
//
// Build tag: //go:build libsql. Requires ALERT_LIBSQL_TEST_URL env var
// pointing at a real libsql:// endpoint with ?authToken=<token> query param.
// SQLite-only and Postgres-only CI paths skip this file entirely.
//
// Running locally (with the Track 1 Turso credentials at
// ~/.path-e-tryout/turso-creds.env):
//
//   set -a; . ~/.path-e-tryout/turso-creds.env; set +a
//   ALERT_LIBSQL_TEST_URL="${TURSO_DB_URL}?authToken=${TURSO_DB_TOKEN}" \
//     go test -tags libsql -count=1 ./...
//
// Test isolation: libSQL is hosted SQLite-compatible; tests use a
// distinct table name prefix to avoid collisions with concurrent runs
// against the shared phase-2-6-canary database.

package alerts

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// libsqlTestURL returns the libSQL connection URL with auth token
// appended, skipping the test if the env var is not set.
func libsqlTestURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("ALERT_LIBSQL_TEST_URL")
	if url == "" {
		t.Skip("ALERT_LIBSQL_TEST_URL not set; skipping libSQL integration test")
	}
	return url
}

// ----- DialectLibSQL constant -----------------------------------------------

func TestDialectLibSQL_ConstantValue(t *testing.T) {
	t.Parallel()
	// Phase 2.6 Path 6: DialectLibSQL is the third dialect alongside
	// SQLite and Postgres. Its string value matches the database/sql
	// driver name "libsql" registered by github.com/tursodatabase/
	// libsql-client-go/libsql.
	assert.Equal(t, Dialect("libsql"), DialectLibSQL,
		"DialectLibSQL string value must match the database/sql driver name")
}

// ----- OpenLibSQL constructor -----------------------------------------------

func TestOpenLibSQL_EmptyURL_Errors(t *testing.T) {
	t.Parallel()
	_, err := OpenLibSQL("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty url")
}

func TestOpenLibSQL_OpensCleanly(t *testing.T) {
	t.Parallel()
	url := libsqlTestURL(t)
	db, err := OpenLibSQL(url)
	require.NoError(t, err, "OpenLibSQL with valid URL")
	require.NotNil(t, db)
	defer db.Close()
	assert.Equal(t, DialectLibSQL, db.Dialect(),
		"Dialect() must report libSQL after OpenLibSQL")
	require.NoError(t, db.Ping(), "Ping after OpenLibSQL")
}

func TestOpenLibSQL_SchemaApplied(t *testing.T) {
	t.Parallel()
	url := libsqlTestURL(t)
	db, err := OpenLibSQL(url)
	require.NoError(t, err)
	defer db.Close()

	// All 10 canonical tables should exist after OpenLibSQL since libSQL
	// accepts SQLite-flavored DDL natively (no Postgres-flavored variant
	// needed). Schema bootstrap uses SchemaDDL(DialectLibSQL) which
	// returns the same SQLite DDL byte-for-byte.
	expected := []string{
		"alerts", "telegram_chat_ids", "kite_tokens", "kite_credentials",
		"oauth_clients", "mcp_sessions", "config", "trailing_stops",
		"daily_pnl", "app_registry",
	}
	for _, table := range expected {
		t.Run(table, func(t *testing.T) {
			exists, err := db.TableExists(table)
			require.NoError(t, err)
			assert.True(t, exists, "table %q should exist after OpenLibSQL", table)
		})
	}
}

// ----- libSQL accepts SQLite-style `?` placeholders --------------------------

// TestLibSQL_QuestionMarkPlaceholders verifies that the production SQL
// (using `?` placeholders per kite-mcp-server Phase 2.4 placeholder
// rewriter) works on libSQL WITHOUT rewriting. libSQL is SQLite-
// compatible at the protocol level, so `?` is the native form (unlike
// Postgres which needs `$1, $2, ...`).
func TestLibSQL_QuestionMarkPlaceholders(t *testing.T) {
	t.Parallel()
	url := libsqlTestURL(t)
	db, err := OpenLibSQL(url)
	require.NoError(t, err)
	defer db.Close()

	// Use a distinct test table to avoid collision with shared canary DB.
	tableName := fmt.Sprintf("libsql_phtest_%d", time.Now().UnixNano())
	defer func() {
		_, _ = db.runExec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	}()

	// Note: tableName is computed from time.Now().UnixNano() with the
	// "libsql_phtest_" prefix, so it always matches isSafeIdent
	// ([a-zA-Z0-9_]+). Format-string concatenation here is safe.
	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id TEXT PRIMARY KEY,
		val TEXT NOT NULL
	)`, tableName)
	_, err = db.runExec(createSQL)
	require.NoError(t, err, "CREATE TABLE on libSQL")

	// INSERT with `?` placeholders — libSQL native form.
	insertSQL := fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (?, ?)
		ON CONFLICT (id) DO UPDATE SET val = excluded.val`, tableName)
	_, err = db.runExec(insertSQL, "row-1", "hello-libsql")
	require.NoError(t, err, "INSERT ON CONFLICT on libSQL")

	// SELECT back via runQueryRow.
	var got string
	selectSQL := fmt.Sprintf(`SELECT val FROM %s WHERE id = ?`, tableName)
	err = db.runQueryRow(selectSQL, "row-1").Scan(&got)
	require.NoError(t, err)
	assert.Equal(t, "hello-libsql", got)

	// Upsert via the same key — verify ON CONFLICT path.
	_, err = db.runExec(insertSQL, "row-1", "updated-libsql")
	require.NoError(t, err)
	err = db.runQueryRow(selectSQL, "row-1").Scan(&got)
	require.NoError(t, err)
	assert.Equal(t, "updated-libsql", got)
}

// ----- Production Save/Load round-trip via libSQL --------------------------

// TestLibSQL_SaveTokenRoundTrip exercises the production SaveToken +
// LoadTokens API via libSQL backend. Confirms the placeholder rewriter
// (which is a no-op on libSQL since it accepts `?`) works through the
// full Save → Load path.
func TestLibSQL_SaveTokenRoundTrip(t *testing.T) {
	t.Parallel()
	url := libsqlTestURL(t)
	db, err := OpenLibSQL(url)
	require.NoError(t, err)
	defer db.Close()

	// Save a token with a unique email to avoid collision.
	email := fmt.Sprintf("libsql-test-%d@example.com", time.Now().UnixNano())
	defer func() {
		_, _ = db.runExec(`DELETE FROM kite_tokens WHERE email = ?`, email)
	}()

	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.SaveToken(email, "tok-libsql", "uid-libsql", "User LibSQL", now))

	// Load and verify.
	var found bool
	tokens, err := db.LoadTokens()
	require.NoError(t, err)
	for _, tok := range tokens {
		if tok.Email == email {
			found = true
			assert.Equal(t, "tok-libsql", tok.AccessToken)
			assert.Equal(t, "uid-libsql", tok.UserID)
			assert.Equal(t, "User LibSQL", tok.UserName)
			assert.WithinDuration(t, now, tok.StoredAt, time.Second)
			break
		}
	}
	assert.True(t, found, "saved token should be in LoadTokens result")
}

// Ensure the *sql.DB underlying handle is libsql-driven when DialectLibSQL is set.
// This is a structural assertion: the dialect helpers route correctly.
func TestOpenLibSQL_DialectRouting(t *testing.T) {
	t.Parallel()
	url := libsqlTestURL(t)
	db, err := OpenLibSQL(url)
	require.NoError(t, err)
	defer db.Close()

	// TableExists routes through DialectLibSQL → which uses
	// sqlite_master (same as DialectSQLite, since libSQL is SQLite-
	// compatible).
	exists, err := db.TableExists("alerts")
	require.NoError(t, err)
	assert.True(t, exists)

	// Direct package-level helper with explicit DialectLibSQL.
	exists, err = TableExists(DialectLibSQL, db.db, "alerts")
	require.NoError(t, err)
	assert.True(t, exists)

	// Negative case: nonexistent table.
	exists, err = TableExists(DialectLibSQL, db.db, "definitely_not_a_real_table_xyz")
	require.NoError(t, err)
	assert.False(t, exists)

	// Ping via *sql.DB.
	require.NoError(t, db.db.Ping())
}

// Compile-time guard: assert *DB satisfies SQLDB interface for libSQL backend.
// Passes regardless of build tag because this is a static type check on the
// existing struct; included here for symmetry with sqlite/postgres tests.
var _ SQLDB = (*DB)(nil)
