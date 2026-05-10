// Phase 2.1.6 — dialect helper tests.
//
// SQLite-only at v0.3.0; the Postgres branch of each helper is
// asserted to error or return SQLite-equivalent output via the
// public type signature only (no Postgres connection in CI yet).
// Phase 2.2 will add Postgres-against-real-db tests when the
// OpenPostgresDB constructor lands.

package alerts

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper: open a fresh SQLite DB with the standard schema applied.
// Mirrors the helpers used by the existing _test.go files in this
// package; kept local to this test file to avoid introducing a new
// shared helper that would need its own contract.
func openSQLiteForDialectTest(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	require.NoError(t, err, "sql.Open sqlite memory")
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping(), "sqlite memory ping")
	return db
}

// ----- PragmaInit ----------------------------------------------------------

func TestPragmaInit_SQLite_Succeeds(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	require.NoError(t, PragmaInit(DialectSQLite, db))

	// Verify journal_mode is WAL (or "memory" for :memory: DBs — the
	// SET succeeds either way; we just check it didn't error out).
	var jm string
	require.NoError(t, db.QueryRow("PRAGMA journal_mode").Scan(&jm))
	// modernc :memory: runs the PRAGMA without error but stays
	// "memory" mode — the goal of this test is "no error from
	// PragmaInit", not "actual WAL on disk-less DB".
	assert.NotEmpty(t, jm)

	var bt int
	require.NoError(t, db.QueryRow("PRAGMA busy_timeout").Scan(&bt))
	assert.Equal(t, 5000, bt, "busy_timeout pragma should be 5000 after PragmaInit")
}

func TestPragmaInit_Postgres_NoOp(t *testing.T) {
	t.Parallel()
	// Postgres branch is a no-op; passes through any non-nil *sql.DB
	// without issuing any query. We use a SQLite handle as a stand-in
	// because a real Postgres connection is Phase 2.2 work.
	db := openSQLiteForDialectTest(t)
	require.NoError(t, PragmaInit(DialectPostgres, db))
}

func TestPragmaInit_NilDB_Errors(t *testing.T) {
	t.Parallel()
	err := PragmaInit(DialectSQLite, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil db")
}

func TestPragmaInit_UnknownDialect_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	err := PragmaInit(Dialect("mysql"), db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown dialect")
}

// ----- TableExists ---------------------------------------------------------

func TestTableExists_SQLite_Present(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := db.Exec(`CREATE TABLE foo (id TEXT PRIMARY KEY)`)
	require.NoError(t, err)

	exists, err := TableExists(DialectSQLite, db, "foo")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestTableExists_SQLite_Absent(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)

	exists, err := TableExists(DialectSQLite, db, "nonexistent_table")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestTableExists_NilDB_Errors(t *testing.T) {
	t.Parallel()
	_, err := TableExists(DialectSQLite, nil, "foo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil db")
}

func TestTableExists_EmptyName_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := TableExists(DialectSQLite, db, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty name")
}

func TestTableExists_UnknownDialect_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := TableExists(Dialect("oracle"), db, "foo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown dialect")
}

// ----- ColumnExists --------------------------------------------------------

func TestColumnExists_SQLite_Present(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := db.Exec(`CREATE TABLE foo (id TEXT PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	exists, err := ColumnExists(DialectSQLite, db, "foo", "val")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestColumnExists_SQLite_Absent(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := db.Exec(`CREATE TABLE foo (id TEXT PRIMARY KEY)`)
	require.NoError(t, err)

	exists, err := ColumnExists(DialectSQLite, db, "foo", "missing_col")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestColumnExists_SQLite_TableMissing(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)

	// pragma_table_info on a missing table returns 0 rows, not an
	// error. ColumnExists relays that as (false, nil) — the legacy
	// behavior we're preserving.
	exists, err := ColumnExists(DialectSQLite, db, "no_such_table", "any")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestColumnExists_NilDB_Errors(t *testing.T) {
	t.Parallel()
	_, err := ColumnExists(DialectSQLite, nil, "foo", "bar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil db")
}

func TestColumnExists_EmptyTable_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := ColumnExists(DialectSQLite, db, "", "col")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty table")
}

func TestColumnExists_EmptyColumn_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := ColumnExists(DialectSQLite, db, "foo", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty table")
}

func TestColumnExists_UnsafeIdent_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	// Quote injection via table name — must be rejected before
	// reaching the literal-substitution path.
	_, err := ColumnExists(DialectSQLite, db, "foo'; DROP TABLE foo; --", "col")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsafe table identifier")
}

func TestColumnExists_UnknownDialect_Errors(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	_, err := ColumnExists(Dialect("mssql"), db, "foo", "col")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown dialect")
}

// ----- isSafeIdent ---------------------------------------------------------

func TestIsSafeIdent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"empty", "", false},
		{"alpha", "foo", true},
		{"alpha_underscore", "foo_bar", true},
		{"alpha_digits", "foo123", true},
		{"digits_only", "12345", true},
		{"single_underscore", "_", true},
		{"with_space", "foo bar", false},
		{"with_quote", "foo'bar", false},
		{"with_semicolon", "foo;bar", false},
		{"with_hyphen", "foo-bar", false},
		{"with_dot", "foo.bar", false},
		{"with_paren", "foo(bar)", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isSafeIdent(c.in), "isSafeIdent(%q)", c.in)
		})
	}
}

// ----- SchemaDDL -----------------------------------------------------------

func TestSchemaDDL_SQLite_ContainsKnownTables(t *testing.T) {
	t.Parallel()
	ddl := SchemaDDL(DialectSQLite)
	require.NotEmpty(t, ddl)

	expected := []string{
		"CREATE TABLE IF NOT EXISTS alerts",
		"CREATE TABLE IF NOT EXISTS telegram_chat_ids",
		"CREATE TABLE IF NOT EXISTS kite_tokens",
		"CREATE TABLE IF NOT EXISTS kite_credentials",
		"CREATE TABLE IF NOT EXISTS oauth_clients",
		"CREATE TABLE IF NOT EXISTS mcp_sessions",
		"CREATE TABLE IF NOT EXISTS config",
		"CREATE TABLE IF NOT EXISTS trailing_stops",
		"CREATE TABLE IF NOT EXISTS daily_pnl",
		"CREATE TABLE IF NOT EXISTS app_registry",
	}
	for _, want := range expected {
		assert.True(t, strings.Contains(ddl, want), "SchemaDDL missing %q", want)
	}
}

func TestSchemaDDL_Postgres_ReturnsSQLiteFlavor_AtV030(t *testing.T) {
	t.Parallel()
	// Phase 2.1.6 contract: Postgres branch returns SQLite-flavored
	// DDL for now. Phase 2.2 (OpenPostgresDB constructor) will fill
	// in the Postgres-flavored DDL. This test asserts the v0.3.0
	// contract so a future regression to "empty Postgres branch"
	// is caught immediately.
	pgDDL := SchemaDDL(DialectPostgres)
	sqliteDDL := SchemaDDL(DialectSQLite)
	assert.Equal(t, sqliteDDL, pgDDL,
		"Phase 2.1.6 contract: Postgres branch returns SQLite-flavored DDL until Phase 2.2 fills it in")
}

func TestSchemaDDL_UnknownDialect_ReturnsEmpty(t *testing.T) {
	t.Parallel()
	assert.Empty(t, SchemaDDL(Dialect("oracle")))
}

// ----- Schema-DDL applies cleanly ------------------------------------------

// TestSchemaDDL_AppliesCleanly verifies that the DDL string returned
// by SchemaDDL(SQLite) actually executes against a real SQLite
// connection without error. Catches drift between sqliteSchemaDDL
// constant and what OpenDB declares inline (until Phase 2.2 unifies
// them).
func TestSchemaDDL_AppliesCleanly(t *testing.T) {
	t.Parallel()
	db := openSQLiteForDialectTest(t)
	ddl := SchemaDDL(DialectSQLite)

	_, err := db.Exec(ddl)
	require.NoError(t, err, "SchemaDDL output failed to apply")

	// Spot-check: 'alerts' table exists after applying.
	exists, err := TableExists(DialectSQLite, db, "alerts")
	require.NoError(t, err)
	assert.True(t, exists, "alerts table missing after SchemaDDL exec")

	// And 'app_registry' (last in DDL) — verifies the multi-statement
	// exec ran the whole script.
	exists, err = TableExists(DialectSQLite, db, "app_registry")
	require.NoError(t, err)
	assert.True(t, exists, "app_registry table missing after SchemaDDL exec")
}
