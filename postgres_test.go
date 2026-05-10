//go:build postgres

// Package alerts — Postgres integration tests for Phase 2.2.
//
// Build tag: //go:build postgres. These tests require a running Postgres
// server reachable via the ALERT_POSTGRES_TEST_URL env var. SQLite-only
// CI (the default `go test ./...` path) skips this file entirely.
//
// Running locally (with the WSL2 Postgres install used during Phase 2.2):
//
//   ALERT_POSTGRES_TEST_URL='postgres://kitemcp:kitemcp@localhost:5432/kitemcp_test?sslmode=disable' \
//     go test -tags postgres ./...
//
// Test isolation: each TestPG_* function creates its own clean schema
// (DROP SCHEMA test_xxx CASCADE; CREATE SCHEMA test_xxx; SET search_path)
// so concurrent runs and re-runs don't collide. The user is granted
// SUPERUSER in the bootstrap so DROP/CREATE SCHEMA is permitted.

package alerts

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pgTestURL returns the Postgres connection URL from ALERT_POSTGRES_TEST_URL,
// skipping the test if unset.
func pgTestURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("ALERT_POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("ALERT_POSTGRES_TEST_URL not set; skipping Postgres integration test")
	}
	return url
}

// pgWithCleanSchema opens a Postgres connection scoped to a fresh
// per-test schema. Returns a cleanup func that DROPs the schema.
func pgWithCleanSchema(t *testing.T) (*DB, func()) {
	t.Helper()
	url := pgTestURL(t)

	// Test schema name derived from the test name; sanitize to
	// [a-z0-9_].
	schemaName := "test_" + strings.ToLower(strings.NewReplacer(
		"/", "_", "-", "_", " ", "_", ".", "_",
	).Replace(t.Name()))
	if !isSafeIdent(schemaName) {
		t.Fatalf("test name produced unsafe schema identifier: %q", schemaName)
	}

	// Open a raw *sql.DB to bootstrap the schema, then re-open through
	// OpenPostgresDB once the schema is set as the search_path. We need
	// the URL to set search_path so all subsequent queries (including
	// SchemaDDL CREATE TABLE) land in the test schema.
	rawDB, err := sql.Open("pgx", url)
	require.NoError(t, err, "raw sql.Open pgx")
	defer rawDB.Close()
	require.NoError(t, rawDB.Ping())

	_, err = rawDB.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
	require.NoError(t, err, "drop test schema")
	_, err = rawDB.Exec(fmt.Sprintf(`CREATE SCHEMA %s`, schemaName))
	require.NoError(t, err, "create test schema")

	// Re-open with search_path set to the new schema. pgx's connection
	// string supports this via &options=-c search_path=<schema>.
	scopedURL := url
	if strings.Contains(scopedURL, "?") {
		scopedURL += "&options=-c%20search_path=" + schemaName
	} else {
		scopedURL += "?options=-c%20search_path=" + schemaName
	}

	db, err := OpenPostgresDB(scopedURL)
	require.NoError(t, err, "OpenPostgresDB scopedURL")

	cleanup := func() {
		_ = db.Close()
		_, _ = rawDB.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
	}
	return db, cleanup
}

// ----- OpenPostgresDB --------------------------------------------------------

func TestPG_OpenPostgresDB_EmptyURL_Errors(t *testing.T) {
	t.Parallel()
	_, err := OpenPostgresDB("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty url")
}

func TestPG_OpenPostgresDB_InvalidURL_Errors(t *testing.T) {
	t.Parallel()
	// Skip the Postgres-required tests check — invalid URL doesn't need a server.
	_, err := OpenPostgresDB("not-a-valid-postgres-url-format")
	require.Error(t, err, "OpenPostgresDB should error on garbage URL")
}

func TestPG_OpenPostgresDB_OpensCleanly(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	require.NotNil(t, db)
	assert.Equal(t, DialectPostgres, db.Dialect(), "Dialect() must report Postgres")
	require.NoError(t, db.Ping())
}

func TestPG_OpenPostgresDB_SchemaApplied(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	// All 10 canonical tables should exist after OpenPostgresDB.
	expected := []string{
		"alerts", "telegram_chat_ids", "kite_tokens", "kite_credentials",
		"oauth_clients", "mcp_sessions", "config", "trailing_stops",
		"daily_pnl", "app_registry",
	}
	for _, table := range expected {
		t.Run(table, func(t *testing.T) {
			exists, err := db.TableExists(table)
			require.NoError(t, err)
			assert.True(t, exists, "table %q should exist after OpenPostgresDB", table)
		})
	}
}

// ----- Dialect helpers under DialectPostgres --------------------------------

func TestPG_TableExists_Present(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	exists, err := TableExists(DialectPostgres, db.db, "alerts")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestPG_TableExists_Absent(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	exists, err := TableExists(DialectPostgres, db.db, "nonexistent_table_xyz")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestPG_ColumnExists_Present(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	exists, err := ColumnExists(DialectPostgres, db.db, "alerts", "email")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestPG_ColumnExists_Absent(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	exists, err := ColumnExists(DialectPostgres, db.db, "alerts", "nonexistent_col")
	require.NoError(t, err)
	assert.False(t, exists)
}

// ----- *DB-method-style wrappers under DialectPostgres ----------------------

func TestPG_DB_TableExists_Wrapper(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	exists, err := db.TableExists("alerts")
	require.NoError(t, err)
	assert.True(t, exists, "wrapper should delegate to Postgres path via Dialect()")
}

func TestPG_DB_ColumnExists_Wrapper(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	exists, err := db.ColumnExists("alerts", "instrument_token")
	require.NoError(t, err)
	assert.True(t, exists, "instrument_token column should exist after OpenPostgresDB")
}

// ----- ON CONFLICT round-trips on Postgres ----------------------------------

// TestPG_OnConflictUpsert verifies that the Phase 2.1 ON CONFLICT
// rewrites work on Postgres. Tests the alerts table because it's the
// most-modified upsert (16-column SET clause). Inserts twice with the
// same id; second insert should UPDATE not INSERT.
func TestPG_OnConflictUpsert_Alerts(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	upsert := `INSERT INTO alerts
		(id, email, tradingsymbol, exchange, instrument_token, target_price,
		 direction, triggered, created_at, triggered_at, triggered_price,
		 reference_price, notification_sent_at,
		 alert_type, composite_logic, composite_name, conditions_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		ON CONFLICT (id) DO UPDATE SET
			email = excluded.email,
			tradingsymbol = excluded.tradingsymbol,
			exchange = excluded.exchange,
			instrument_token = excluded.instrument_token,
			target_price = excluded.target_price,
			direction = excluded.direction,
			triggered = excluded.triggered,
			created_at = excluded.created_at,
			triggered_at = excluded.triggered_at,
			triggered_price = excluded.triggered_price,
			reference_price = excluded.reference_price,
			notification_sent_at = excluded.notification_sent_at,
			alert_type = excluded.alert_type,
			composite_logic = excluded.composite_logic,
			composite_name = excluded.composite_name,
			conditions_json = excluded.conditions_json`

	// First insert: target_price 100.
	_, err := db.db.Exec(upsert,
		"alert-1", "u@example.com", "RELIANCE", "NSE", int64(738561),
		100.0, "above", 0, "2026-05-09T10:00:00Z", nil, nil, nil, nil,
		"single", nil, nil, nil)
	require.NoError(t, err, "initial INSERT")

	// Second insert with same id: target_price 200 — should UPDATE.
	_, err = db.db.Exec(upsert,
		"alert-1", "u@example.com", "RELIANCE", "NSE", int64(738561),
		200.0, "above", 0, "2026-05-09T11:00:00Z", nil, nil, nil, nil,
		"single", nil, nil, nil)
	require.NoError(t, err, "upsert INSERT")

	// Verify there's only one row, and target_price is 200 (the second value).
	var (
		count       int
		targetPrice float64
	)
	err = db.db.QueryRow(`SELECT COUNT(*), MAX(target_price) FROM alerts WHERE id = $1`, "alert-1").Scan(&count, &targetPrice)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should have exactly one row after upsert")
	assert.Equal(t, 200.0, targetPrice, "target_price should be the upserted value")
}

func TestPG_OnConflictDoNothing_Tokens(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	// Use kite_tokens for ON CONFLICT DO UPDATE pattern.
	upsert := `INSERT INTO kite_tokens (email, access_token, user_id, user_name, stored_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (email) DO UPDATE SET
			access_token = excluded.access_token,
			user_id = excluded.user_id,
			user_name = excluded.user_name,
			stored_at = excluded.stored_at`

	_, err := db.db.Exec(upsert, "u@example.com", "tok1", "uid1", "User One", "2026-05-09T10:00:00Z")
	require.NoError(t, err)
	_, err = db.db.Exec(upsert, "u@example.com", "tok2", "uid1", "User One", "2026-05-09T11:00:00Z")
	require.NoError(t, err)

	var token string
	err = db.db.QueryRow(`SELECT access_token FROM kite_tokens WHERE email = $1`, "u@example.com").Scan(&token)
	require.NoError(t, err)
	assert.Equal(t, "tok2", token, "second token should overwrite first")
}

// ----- Composite primary key ON CONFLICT ------------------------------------

// TestPG_OnConflict_CompositePK exercises the daily_pnl table which
// has a composite (date, email) PK. SQLite Phase 2.1 used
// `ON CONFLICT (date, email)` — verify Postgres accepts the same form.
func TestPG_OnConflict_CompositePK_DailyPnL(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	upsert := `INSERT INTO daily_pnl
		(date, email, holdings_pnl, holdings_pnl_currency,
		 positions_pnl, positions_pnl_currency,
		 net_pnl, net_pnl_currency,
		 holdings_count, trades_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (date, email) DO UPDATE SET
			holdings_pnl = excluded.holdings_pnl,
			net_pnl = excluded.net_pnl`

	_, err := db.db.Exec(upsert, "2026-05-09", "u@example.com",
		100.0, "INR", 50.0, "INR", 150.0, "INR", 1, 2)
	require.NoError(t, err)

	_, err = db.db.Exec(upsert, "2026-05-09", "u@example.com",
		200.0, "INR", 50.0, "INR", 250.0, "INR", 1, 2)
	require.NoError(t, err)

	var holdings, net float64
	err = db.db.QueryRow(`SELECT holdings_pnl, net_pnl FROM daily_pnl WHERE date = $1 AND email = $2`,
		"2026-05-09", "u@example.com").Scan(&holdings, &net)
	require.NoError(t, err)
	assert.Equal(t, 200.0, holdings, "holdings_pnl should be upserted value")
	assert.Equal(t, 250.0, net, "net_pnl should be upserted value")
}

// ----- Round-trip with REAL/DOUBLE PRECISION values -------------------------

// TestPG_DoublePrecision_RoundTrip verifies that the SQLite-flavored
// float64 values our code passes (target_price 100.5, etc.) round-trip
// cleanly through Postgres DOUBLE PRECISION columns without precision
// loss in the typical retail-price range.
func TestPG_DoublePrecision_RoundTrip(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	values := []float64{
		100.0,
		3225.45, // typical RELIANCE price
		0.05,    // typical penny stock
		99999.99,
		0.0,
	}
	for i, v := range values {
		_, err := db.db.Exec(`INSERT INTO alerts
			(id, email, tradingsymbol, exchange, instrument_token, target_price,
			 direction, triggered, created_at)
			VALUES ($1, 'u@example.com', 'X', 'NSE', 1, $2, 'above', 0, '2026-05-09T10:00:00Z')`,
			fmt.Sprintf("price-%d", i), v)
		require.NoError(t, err)
	}

	for i, v := range values {
		var got float64
		err := db.db.QueryRow(`SELECT target_price FROM alerts WHERE id = $1`,
			fmt.Sprintf("price-%d", i)).Scan(&got)
		require.NoError(t, err)
		assert.Equal(t, v, got, "DOUBLE PRECISION should round-trip retail prices exactly")
	}
}

// ----- BIGINT instrument_token ----------------------------------------------

// TestPG_BigIntInstrumentToken verifies BIGINT can hold values that
// overflow Postgres's 4-byte INTEGER but fit Go's int64.
func TestPG_BigIntInstrumentToken(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	// 4-byte INTEGER max is 2^31 - 1 = 2147483647. Use a value clearly
	// over that (but under int64 max) to verify BIGINT not INTEGER.
	bigTok := int64(9999999999) // 10 billion

	_, err := db.db.Exec(`INSERT INTO alerts
		(id, email, tradingsymbol, exchange, instrument_token, target_price,
		 direction, triggered, created_at)
		VALUES ('big-1', 'u@example.com', 'X', 'NSE', $1, 100, 'above', 0, '2026-05-09T10:00:00Z')`,
		bigTok)
	require.NoError(t, err, "BIGINT should accept 10B value")

	var got int64
	err = db.db.QueryRow(`SELECT instrument_token FROM alerts WHERE id = 'big-1'`).Scan(&got)
	require.NoError(t, err)
	assert.Equal(t, bigTok, got, "BIGINT should round-trip 10B value exactly")
}

// ----- Idempotent OpenPostgresDB --------------------------------------------

// TestPG_OpenPostgresDB_Idempotent verifies that calling
// OpenPostgresDB on a database where the schema already exists is a
// no-op (CREATE TABLE IF NOT EXISTS clauses in postgresSchemaDDL).
func TestPG_OpenPostgresDB_Idempotent(t *testing.T) {
	t.Parallel()
	db, cleanup := pgWithCleanSchema(t)
	defer cleanup()

	// Insert a row through the first connection.
	_, err := db.db.Exec(`INSERT INTO alerts
		(id, email, tradingsymbol, exchange, instrument_token, target_price,
		 direction, triggered, created_at)
		VALUES ('idem-1', 'u@example.com', 'X', 'NSE', 1, 100, 'above', 0, '2026-05-09T10:00:00Z')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Re-open: should NOT drop the row (CREATE TABLE IF NOT EXISTS skips).
	url := pgTestURL(t)
	schemaName := "test_" + strings.ToLower(strings.NewReplacer(
		"/", "_", "-", "_", " ", "_", ".", "_",
	).Replace(t.Name()))
	scopedURL := url
	if strings.Contains(scopedURL, "?") {
		scopedURL += "&options=-c%20search_path=" + schemaName
	} else {
		scopedURL += "?options=-c%20search_path=" + schemaName
	}
	db2, err := OpenPostgresDB(scopedURL)
	require.NoError(t, err, "second OpenPostgresDB on existing schema")
	defer db2.Close()

	var count int
	err = db2.db.QueryRow(`SELECT COUNT(*) FROM alerts WHERE id = 'idem-1'`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "row should survive re-open (CREATE TABLE IF NOT EXISTS)")
}
