//go:build postgres

// Package alerts — Phase 2.4 round-trip migration test.
//
// Build tag: //go:build postgres. Requires both a SQLite tempfile
// and a Postgres connection (ALERT_POSTGRES_TEST_URL env var). The
// SQLite-only CI path skips this file entirely.
//
// What it tests: the dump-and-load contract from .research/phase-2-
// postgres-adapter-design.md Phase 2.4. Populates a SQLite database
// with diverse data via every Save* method, reads everything back
// via the Load* methods, then re-inserts the same data into a fresh
// Postgres database (via the same Save* methods routed through
// OpenPostgresDB), and asserts that the Postgres reads return rows
// equivalent to the SQLite originals.
//
// What it does NOT test:
//   - SQL-level dump/load via pg_dump | psql (out of scope; we test
//     the Go API contract, not the wire format).
//   - Migration of LIVE production data (Phase 2.6 canary work).
//   - Performance comparison between dialects (orthogonal).
//
// Test isolation: each round-trip uses its own per-test Postgres
// schema (DROP/CREATE schema_xxx CASCADE; SET search_path) so
// concurrent runs and re-runs don't collide. SQLite uses a tempfile.

package alerts

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	domain "github.com/algo2go/kite-mcp-domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTripPair opens a SQLite tempfile + a fresh Postgres test schema
// and returns both *DB handles plus a cleanup function. Both handles
// share the canonical schema applied by their respective Open
// constructors (OpenDB → SchemaDDL(SQLite); OpenPostgresDB →
// SchemaDDL(Postgres)).
func roundTripPair(t *testing.T) (sqliteDB, postgresDB *DB, cleanup func()) {
	t.Helper()

	// SQLite tempfile.
	sqlitePath := filepath.Join(t.TempDir(), "roundtrip.db")
	sqliteDB, err := OpenDB(sqlitePath)
	require.NoError(t, err, "OpenDB sqlite tempfile")

	// Postgres scoped to a per-test schema (mirrors postgres_test.go's
	// pgWithCleanSchema; duplicated here so this file stays standalone
	// and runnable without ordering deps on postgres_test.go).
	pgURL := os.Getenv("ALERT_POSTGRES_TEST_URL")
	if pgURL == "" {
		_ = sqliteDB.Close()
		t.Skip("ALERT_POSTGRES_TEST_URL not set; skipping round-trip test")
	}
	schemaName := "rt_" + strings.ToLower(strings.NewReplacer(
		"/", "_", "-", "_", " ", "_", ".", "_",
	).Replace(t.Name()))
	if !isSafeIdent(schemaName) {
		_ = sqliteDB.Close()
		t.Fatalf("test name produced unsafe schema identifier: %q", schemaName)
	}

	// Bootstrap the schema using a raw *sql.DB (NOT OpenPostgresDB).
	// OpenPostgresDB applies the full SchemaDDL — racing parallel tests
	// would collide on shared types in the default schema. The raw
	// connection only does the admin work (DROP/CREATE SCHEMA), then
	// closes; the real *DB is opened after with search_path scoped.
	rawAdmin, err := sql.Open("pgx", pgURL)
	require.NoError(t, err, "raw sql.Open pgx for bootstrap")
	_, err = rawAdmin.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
	require.NoError(t, err, "drop test schema")
	_, err = rawAdmin.Exec(fmt.Sprintf(`CREATE SCHEMA %s`, schemaName))
	require.NoError(t, err, "create test schema")
	_ = rawAdmin.Close()

	scopedURL := pgURL
	if strings.Contains(scopedURL, "?") {
		scopedURL += "&options=-c%20search_path=" + schemaName
	} else {
		scopedURL += "?options=-c%20search_path=" + schemaName
	}
	postgresDB, err = OpenPostgresDB(scopedURL)
	require.NoError(t, err, "scoped OpenPostgresDB")

	cleanup = func() {
		_ = sqliteDB.Close()
		_ = postgresDB.Close()
		// Drop schema via a raw connection (no SchemaDDL needed).
		dropDB, dropErr := sql.Open("pgx", pgURL)
		if dropErr == nil {
			_, _ = dropDB.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
			_ = dropDB.Close()
		}
	}
	return sqliteDB, postgresDB, cleanup
}

// ----- Round-trip per persistence kind -------------------------------------

// TestRoundTrip_Alerts populates SQLite with single-leg + composite
// alerts, reads via LoadAlerts, re-inserts into Postgres, reads back,
// and asserts the Postgres data matches the SQLite originals.
func TestRoundTrip_Alerts(t *testing.T) {
	t.Parallel()
	sqliteDB, pgDB, cleanup := roundTripPair(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Second)
	originals := []*Alert{
		{
			ID:              "alert-single-1",
			Email:           "u1@example.com",
			Tradingsymbol:   "RELIANCE",
			Exchange:        "NSE",
			InstrumentToken: 738561,
			TargetPrice:     2500.0,
			Direction:       "above",
			Triggered:       false,
			CreatedAt:       now,
			AlertType:       domain.AlertTypeSingle,
		},
		{
			ID:              "alert-single-2",
			Email:           "u2@example.com",
			Tradingsymbol:   "INFY",
			Exchange:        "NSE",
			InstrumentToken: 408065,
			TargetPrice:     1500.50,
			Direction:       "below",
			Triggered:       true,
			TriggeredAt:     now,
			TriggeredPrice:  1495.20,
			CreatedAt:       now.Add(-1 * time.Hour),
			AlertType:       domain.AlertTypeSingle,
		},
		{
			ID:              "alert-zero-price",
			Email:           "u3@example.com",
			Tradingsymbol:   "X",
			Exchange:        "BSE",
			InstrumentToken: 4294967295, // uint32 max — exercises the upper boundary
			TargetPrice:     0.05,       // penny stock — exercises DOUBLE PRECISION precision
			Direction:       "above",
			CreatedAt:       now,
			AlertType:       domain.AlertTypeSingle,
		},
	}

	// Populate SQLite.
	for _, a := range originals {
		require.NoError(t, sqliteDB.SaveAlert(a), "SaveAlert sqlite %q", a.ID)
	}
	sqliteAlerts, err := sqliteDB.LoadAlerts()
	require.NoError(t, err, "LoadAlerts sqlite")
	require.Equal(t, 3, totalAlertCount(sqliteAlerts), "sqlite should hold 3 alerts")

	// Re-insert into Postgres via the same Save API.
	for _, byEmail := range sqliteAlerts {
		for _, a := range byEmail {
			require.NoError(t, pgDB.SaveAlert(a), "SaveAlert postgres %q", a.ID)
		}
	}
	pgAlerts, err := pgDB.LoadAlerts()
	require.NoError(t, err, "LoadAlerts postgres")
	require.Equal(t, 3, totalAlertCount(pgAlerts), "postgres should hold 3 alerts")

	// Per-email count check.
	for email, want := range sqliteAlerts {
		got := pgAlerts[email]
		assert.Len(t, got, len(want), "alert count mismatch for %s", email)
	}

	// Spot-check field equivalence (focus on dialect-flavored columns).
	uint32MaxFound := false
	pennyPriceFound := false
	for _, byEmail := range pgAlerts {
		for _, a := range byEmail {
			if a.ID == "alert-zero-price" {
				uint32MaxFound = true
				assert.Equal(t, uint32(4294967295), a.InstrumentToken,
					"uint32 max instrument_token should round-trip exactly "+
						"(Postgres BIGINT column accepts uint32 max trivially)")
				assert.Equal(t, 0.05, a.TargetPrice,
					"penny price should round-trip via DOUBLE PRECISION")
				pennyPriceFound = true
			}
			if a.Triggered {
				assert.Equal(t, originalByID(originals, a.ID).TriggeredPrice, a.TriggeredPrice,
					"triggered_price round-trip for %s", a.ID)
			}
		}
	}
	assert.True(t, uint32MaxFound, "alert-zero-price should be loaded from Postgres")
	assert.True(t, pennyPriceFound, "penny price should round-trip exactly")
}

// TestRoundTrip_Tokens covers the kite_tokens table — the canonical
// "per-email upsert" pattern that drives the most-used ON CONFLICT site.
func TestRoundTrip_Tokens(t *testing.T) {
	t.Parallel()
	sqliteDB, pgDB, cleanup := roundTripPair(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Second)
	type tok struct {
		email, accessToken, userID, userName string
		storedAt                             time.Time
	}
	originals := []tok{
		{"u1@example.com", "tok-uno", "ZA1", "User One", now},
		{"u2@example.com", "tok-dos", "ZA2", "User Two", now.Add(-30 * time.Minute)},
		{"u3@example.com", "tok-tres", "ZA3", "User Three", now.Add(-2 * time.Hour)},
	}

	for _, o := range originals {
		require.NoError(t, sqliteDB.SaveToken(o.email, o.accessToken, o.userID, o.userName, o.storedAt))
	}
	sqliteTokens, err := sqliteDB.LoadTokens()
	require.NoError(t, err)
	require.Len(t, sqliteTokens, 3)

	// Round-trip into Postgres.
	for _, o := range sqliteTokens {
		require.NoError(t, pgDB.SaveToken(o.Email, o.AccessToken, o.UserID, o.UserName, o.StoredAt))
	}
	pgTokens, err := pgDB.LoadTokens()
	require.NoError(t, err)
	require.Len(t, pgTokens, 3)

	// Sort both by email for stable comparison.
	sort.Slice(sqliteTokens, func(i, j int) bool { return sqliteTokens[i].Email < sqliteTokens[j].Email })
	sort.Slice(pgTokens, func(i, j int) bool { return pgTokens[i].Email < pgTokens[j].Email })

	for i := range sqliteTokens {
		assert.Equal(t, sqliteTokens[i].Email, pgTokens[i].Email)
		assert.Equal(t, sqliteTokens[i].AccessToken, pgTokens[i].AccessToken)
		assert.Equal(t, sqliteTokens[i].UserID, pgTokens[i].UserID)
		assert.Equal(t, sqliteTokens[i].UserName, pgTokens[i].UserName)
		// StoredAt round-trip — RFC3339 string layer means second precision.
		assert.WithinDuration(t, sqliteTokens[i].StoredAt, pgTokens[i].StoredAt, time.Second)
	}
}

// TestRoundTrip_Credentials covers the kite_credentials table —
// exercises the encryption integration (TEXT-stored base64 ciphertext
// portability across dialects).
func TestRoundTrip_Credentials(t *testing.T) {
	t.Parallel()
	sqliteDB, pgDB, cleanup := roundTripPair(t)
	defer cleanup()

	// Set the same encryption key on both DBs so encrypted creds
	// round-trip via base64 strings (no key-rotation in scope).
	key := make([]byte, 32) // zero-key; deterministic for test
	sqliteDB.SetEncryptionKey(key)
	pgDB.SetEncryptionKey(key)

	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, sqliteDB.SaveCredential("u1@example.com", "api-key-1", "api-secret-1", "app-1", now))
	require.NoError(t, sqliteDB.SaveCredential("u2@example.com", "api-key-2", "api-secret-2", "app-2", now))

	sqliteCreds, err := sqliteDB.LoadCredentials()
	require.NoError(t, err)
	require.Len(t, sqliteCreds, 2)

	for _, c := range sqliteCreds {
		require.NoError(t, pgDB.SaveCredential(c.Email, c.APIKey, c.APISecret, c.AppID, c.StoredAt))
	}
	pgCreds, err := pgDB.LoadCredentials()
	require.NoError(t, err)
	require.Len(t, pgCreds, 2)

	sort.Slice(sqliteCreds, func(i, j int) bool { return sqliteCreds[i].Email < sqliteCreds[j].Email })
	sort.Slice(pgCreds, func(i, j int) bool { return pgCreds[i].Email < pgCreds[j].Email })

	for i := range sqliteCreds {
		assert.Equal(t, sqliteCreds[i].Email, pgCreds[i].Email)
		assert.Equal(t, sqliteCreds[i].APIKey, pgCreds[i].APIKey)
		assert.Equal(t, sqliteCreds[i].APISecret, pgCreds[i].APISecret)
		assert.Equal(t, sqliteCreds[i].AppID, pgCreds[i].AppID)
	}
}

// TestRoundTrip_DailyPnL exercises the daily_pnl table with its
// composite (date, email) primary key — the same site that exposed the
// SQLite-vs-Postgres ON CONFLICT-on-INSERT-SELECT difference during
// Phase 2.1.6. Confirms the composite-PK upsert is dialect-portable
// for the VALUES form (which is what the production code uses).
func TestRoundTrip_DailyPnL(t *testing.T) {
	t.Parallel()
	sqliteDB, pgDB, cleanup := roundTripPair(t)
	defer cleanup()

	originals := []*DailyPnLEntry{
		{
			Date:                  "2026-05-09",
			Email:                 "u1@example.com",
			HoldingsPnL:           1234.56,
			HoldingsPnLCurrency:   "INR",
			PositionsPnL:          -78.90,
			PositionsPnLCurrency:  "INR",
			NetPnL:                1155.66,
			NetPnLCurrency:        "INR",
			HoldingsCount:         5,
			TradesCount:           12,
		},
		{
			Date:                  "2026-05-09",
			Email:                 "u2@example.com",
			HoldingsPnL:           0.0,
			HoldingsPnLCurrency:   "INR",
			PositionsPnL:          250.75,
			PositionsPnLCurrency:  "INR",
			NetPnL:                250.75,
			NetPnLCurrency:        "INR",
			HoldingsCount:         0,
			TradesCount:           3,
		},
		{
			Date:                  "2026-05-08",
			Email:                 "u1@example.com",
			HoldingsPnL:           500.0,
			HoldingsPnLCurrency:   "INR",
			PositionsPnL:          0.0,
			PositionsPnLCurrency:  "INR",
			NetPnL:                500.0,
			NetPnLCurrency:        "INR",
			HoldingsCount:         3,
			TradesCount:           0,
		},
	}

	for _, e := range originals {
		require.NoError(t, sqliteDB.SaveDailyPnL(e), "SaveDailyPnL sqlite %s/%s", e.Date, e.Email)
	}

	// Load via per-user/per-date range; concatenate.
	sqliteAll := loadAllDailyPnL(t, sqliteDB)
	require.Len(t, sqliteAll, 3, "sqlite should hold 3 daily_pnl entries")

	for _, e := range sqliteAll {
		require.NoError(t, pgDB.SaveDailyPnL(e), "SaveDailyPnL postgres %s/%s", e.Date, e.Email)
	}
	pgAll := loadAllDailyPnL(t, pgDB)
	require.Len(t, pgAll, 3, "postgres should hold 3 daily_pnl entries")

	sort.Slice(sqliteAll, func(i, j int) bool {
		return sqliteAll[i].Date+sqliteAll[i].Email < sqliteAll[j].Date+sqliteAll[j].Email
	})
	sort.Slice(pgAll, func(i, j int) bool {
		return pgAll[i].Date+pgAll[i].Email < pgAll[j].Date+pgAll[j].Email
	})

	for i := range sqliteAll {
		assert.Equal(t, sqliteAll[i].HoldingsPnL, pgAll[i].HoldingsPnL,
			"holdings_pnl round-trip for %s/%s", sqliteAll[i].Date, sqliteAll[i].Email)
		assert.Equal(t, sqliteAll[i].NetPnL, pgAll[i].NetPnL,
			"net_pnl round-trip for %s/%s", sqliteAll[i].Date, sqliteAll[i].Email)
		assert.Equal(t, sqliteAll[i].HoldingsCount, pgAll[i].HoldingsCount)
		assert.Equal(t, sqliteAll[i].TradesCount, pgAll[i].TradesCount)
	}
}

// TestRoundTrip_TelegramChatIDs exercises the telegram_chat_ids table.
// The chat_id column maps to int64 (BIGINT in Postgres, INTEGER in
// SQLite). Verifies the BIGINT-equivalent mapping at the production-API
// level.
func TestRoundTrip_TelegramChatIDs(t *testing.T) {
	t.Parallel()
	sqliteDB, pgDB, cleanup := roundTripPair(t)
	defer cleanup()

	originals := map[string]int64{
		"u1@example.com": 12345678,
		"u2@example.com": -9876543210, // negative; some Telegram chat IDs are negative
		"u3@example.com": 5000000000,  // > int32 — exercises BIGINT
	}

	for email, chatID := range originals {
		require.NoError(t, sqliteDB.SaveTelegramChatID(email, chatID))
	}
	sqliteMap, err := sqliteDB.LoadTelegramChatIDs()
	require.NoError(t, err)
	require.Len(t, sqliteMap, 3)

	for email, chatID := range sqliteMap {
		require.NoError(t, pgDB.SaveTelegramChatID(email, chatID))
	}
	pgMap, err := pgDB.LoadTelegramChatIDs()
	require.NoError(t, err)
	require.Len(t, pgMap, 3)

	for email, want := range originals {
		assert.Equal(t, want, pgMap[email], "telegram_chat_ids round-trip for %s", email)
	}
}

// ----- helpers --------------------------------------------------------------

func totalAlertCount(byEmail map[string][]*Alert) int {
	n := 0
	for _, slice := range byEmail {
		n += len(slice)
	}
	return n
}

func originalByID(originals []*Alert, id string) *Alert {
	for _, a := range originals {
		if a.ID == id {
			return a
		}
	}
	return nil
}

// loadAllDailyPnL queries every daily_pnl row directly. The public
// LoadDailyPnL takes per-email/date filters; for round-trip equivalence
// we want the full table which is easier via raw query.
func loadAllDailyPnL(t *testing.T, db *DB) []*DailyPnLEntry {
	t.Helper()
	rows, err := db.RawQuery(`SELECT date, email, holdings_pnl, holdings_pnl_currency,
		positions_pnl, positions_pnl_currency, net_pnl, net_pnl_currency,
		holdings_count, trades_count
		FROM daily_pnl`)
	require.NoError(t, err)
	defer rows.Close()

	var out []*DailyPnLEntry
	for rows.Next() {
		e := &DailyPnLEntry{}
		require.NoError(t, rows.Scan(&e.Date, &e.Email,
			&e.HoldingsPnL, &e.HoldingsPnLCurrency,
			&e.PositionsPnL, &e.PositionsPnLCurrency,
			&e.NetPnL, &e.NetPnLCurrency,
			&e.HoldingsCount, &e.TradesCount))
		out = append(out, e)
	}
	require.NoError(t, rows.Err())
	return out
}
