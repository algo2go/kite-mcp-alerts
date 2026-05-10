// Phase 2.1.6 — SQL dialect-dispatch helper.
//
// Centralizes the small set of SQL paths that genuinely DIFFER between
// SQLite and Postgres, so every other call site in this package (and
// downstream consumers like kite-mcp-billing) can stay dialect-agnostic.
//
// Per kite-mcp-server .research/phase-2-sql-portability-audit.md
// (commit da91a39 in kite-mcp-server) and Stage 1 (v0.2.0 of all 5
// algo2go persistence repos): the upsert sites were converted to the
// portable ON CONFLICT (...) DO UPDATE form, but the following sites
// remain dialect-specific and route through this helper:
//
//   1. PRAGMA dispatch — SQLite-only WAL/busy_timeout/foreign_keys
//   2. Table-existence check — sqlite_master vs information_schema.tables
//   3. Column-existence check — pragma_table_info vs information_schema.columns
//   4. Schema DDL — REAL vs DOUBLE PRECISION, INTEGER-as-bool vs BOOLEAN,
//      INTEGER PRIMARY KEY AUTOINCREMENT vs BIGSERIAL (audit only)
//
// At v0.3.0 (this commit) the SchemaDDL helper still returns the
// SQLite-flavored block for BOTH dialects with TODO markers. Phase 2.2
// (OpenPostgresDB constructor) will fill in the Postgres branch with
// real Postgres-flavored DDL. The other three helpers (PragmaInit,
// TableExists, ColumnExists) are FULLY dialect-aware in this commit
// and unblock the SQLite-keeping migration code from referencing
// pragma_table_info / sqlite_master directly.
//
// Why a small hand-coded dispatcher instead of a query builder?
// Empirical reality: the audit identified ~10 dialect-specific sites
// across all 5 repos. ORM/builder cost > benefit at our scale. See
// kite-mcp-server .research/phase-2-postgres-adapter-design.md "Why
// not squirrel/sq/GORM?" for the full rationale.

package alerts

import (
	"database/sql"
	"fmt"
)

// Dialect identifies the database backend dialect for SQL dispatch.
//
// At v0.3.0 only DialectSQLite is wired through. DialectPostgres is
// the Phase 2.2 (OpenPostgresDB constructor) deliverable — at this
// commit, helpers either error on the Postgres branch or return
// SQLite-flavored output (SchemaDDL only) so the type signature stays
// stable for the eventual Postgres wire-up without a future API break.
type Dialect string

const (
	// DialectSQLite — modernc.org/sqlite (cgo-free) is the only
	// production driver at v0.3.0. WAL journal + busy_timeout=5000ms
	// + foreign_keys=ON via DSN _pragma=foreign_keys(1).
	DialectSQLite Dialect = "sqlite"

	// DialectPostgres — github.com/jackc/pgx/v5 via database/sql
	// stdlib. Phase 2.2 deliverable. NOT WIRED at v0.3.0 — helpers
	// recognize the value but only SchemaDDL has a (placeholder)
	// branch. Other helpers route Postgres calls through the
	// information_schema queries that work on real Postgres servers
	// even though no Postgres adapter exists in this repo yet.
	DialectPostgres Dialect = "postgres"
)

// PragmaInit applies dialect-specific connection-init pragmas.
//
//   SQLite:   PRAGMA journal_mode=WAL + PRAGMA busy_timeout=5000.
//             foreign_keys=ON is handled at the DSN layer
//             (dsnWithFKPragma) per-connection because pool reopens
//             reset the per-connection PRAGMA state.
//   Postgres: no-op. WAL is the only journal mode (cluster-level);
//             statement_timeout / lock_timeout are configured in
//             postgresql.conf or via SET on the session; FK
//             enforcement is always ON and cannot be disabled per
//             connection.
//
// Returns nil iff the database accepted all required init pragmas
// (or, for Postgres, immediately).
//
// Callers: OpenDB (db.go) for SQLite. Postgres adapter at Phase 2.2
// passes its newly-opened *sql.DB through PragmaInit(DialectPostgres)
// for symmetric init flow even though the body is empty.
func PragmaInit(d Dialect, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("alerts.PragmaInit: nil db")
	}
	switch d {
	case DialectSQLite:
		if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
			return fmt.Errorf("alerts.PragmaInit sqlite WAL: %w", err)
		}
		if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
			return fmt.Errorf("alerts.PragmaInit sqlite busy_timeout: %w", err)
		}
		return nil
	case DialectPostgres:
		// No-op: Postgres has no per-connection equivalent for these
		// pragmas. WAL is cluster-level; busy_timeout has no analog
		// (Postgres locks block until lock_timeout expires).
		return nil
	default:
		return fmt.Errorf("alerts.PragmaInit: unknown dialect %q", d)
	}
}

// TableExists reports whether a table with the given name is present
// in the connected database. Uses the dialect's catalog query:
//
//   SQLite:   SELECT 1 FROM sqlite_master WHERE type='table' AND name=?
//   Postgres: SELECT 1 FROM information_schema.tables
//             WHERE table_schema=current_schema() AND table_name=$1
//
// Returns (false, nil) iff the table is absent. Returns (false, err)
// only on database errors; a missing table is NOT an error.
//
// Callers: db_migrations.go's migrateRegistryCheckConstraint (replaces
// the direct sqlite_master query at v0.2.0); Postgres-readiness for
// kite-mcp-billing's legacy self-migration (Phase 2.1.6 cross-repo
// edit at billing/store.go:142+161).
func TableExists(d Dialect, db *sql.DB, name string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("alerts.TableExists: nil db")
	}
	if name == "" {
		return false, fmt.Errorf("alerts.TableExists: empty name")
	}
	var query string
	switch d {
	case DialectSQLite:
		query = `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`
	case DialectPostgres:
		// information_schema.tables is part of the SQL standard and
		// fully supported by Postgres. current_schema() returns the
		// first schema in search_path (typically "public") so this
		// scopes the lookup to the active schema.
		query = `SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=$1`
	default:
		return false, fmt.Errorf("alerts.TableExists: unknown dialect %q", d)
	}
	var one int
	err := db.QueryRow(query, name).Scan(&one)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("alerts.TableExists query: %w", err)
	}
	return true, nil
}

// ColumnExists reports whether the named column is present on the
// given table. Used for incremental schema migrations that ALTER TABLE
// ADD COLUMN only when the column is absent.
//
//   SQLite:   SELECT COUNT(*) FROM pragma_table_info(<table>) WHERE name=?
//             (table name is positional, not parametric, in pragma syntax)
//   Postgres: SELECT 1 FROM information_schema.columns
//             WHERE table_schema=current_schema()
//               AND table_name=$1 AND column_name=$2
//
// Returns (false, nil) iff the column is absent. Returns (false, err)
// only on database errors. The SQLite branch silently returns
// (false, nil) when the parent table is absent (matching the legacy
// pragma_table_info behavior).
//
// SQLite naming caveat: pragma_table_info is a virtual table function
// that takes the target-table name as a string LITERAL, not a bind
// parameter. We sanity-check the name to permit only [a-zA-Z0-9_]
// to prevent SQL injection through the literal-substitution path.
//
// Callers: db_migrations.go's migrateAlerts (replaces the direct
// pragma_table_info query at v0.2.0); db.go's mcp_sessions.session_id_enc
// migration; kite-mcp-billing's admin_email-column check.
func ColumnExists(d Dialect, db *sql.DB, table, column string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("alerts.ColumnExists: nil db")
	}
	if table == "" || column == "" {
		return false, fmt.Errorf("alerts.ColumnExists: empty table=%q column=%q", table, column)
	}
	if !isSafeIdent(table) {
		return false, fmt.Errorf("alerts.ColumnExists: unsafe table identifier %q", table)
	}
	switch d {
	case DialectSQLite:
		// pragma_table_info takes the table name as a literal in the
		// virtual-table function call. The bound parameter is the
		// column name being matched (the WHERE clause).
		q := fmt.Sprintf(`SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name = ?`, table)
		var count int
		if err := db.QueryRow(q, column).Scan(&count); err != nil {
			return false, fmt.Errorf("alerts.ColumnExists sqlite: %w", err)
		}
		return count > 0, nil
	case DialectPostgres:
		var one int
		err := db.QueryRow(
			`SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2`,
			table, column,
		).Scan(&one)
		switch {
		case err == sql.ErrNoRows:
			return false, nil
		case err != nil:
			return false, fmt.Errorf("alerts.ColumnExists postgres: %w", err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("alerts.ColumnExists: unknown dialect %q", d)
	}
}

// ----- Method-style wrappers on *DB ---------------------------------------
//
// Downstream callers like kite-mcp-billing hold a *alerts.DB (the
// concrete handle), not a raw *sql.DB. They cannot call the package-
// level helpers above directly because we don't expose the inner
// *sql.DB (encapsulation). The methods below delegate to the
// package-level helpers using the inner sql handle, keeping the
// dialect dispatch in one place while letting consumers stay on the
// stable *alerts.DB type.

// TableExists is the *alerts.DB-level wrapper for the package-level
// TableExists helper. Uses DialectSQLite at v0.3.0 (only driver in
// production); Phase 2.2 will add a Dialect() method on *DB so this
// wrapper picks the dialect from the open connection.
func (d *DB) TableExists(name string) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("alerts.DB.TableExists: nil receiver")
	}
	return TableExists(DialectSQLite, d.db, name)
}

// ColumnExists is the *alerts.DB-level wrapper for the package-level
// ColumnExists helper. See TableExists for the dialect-detection
// rationale.
func (d *DB) ColumnExists(table, column string) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("alerts.DB.ColumnExists: nil receiver")
	}
	return ColumnExists(DialectSQLite, d.db, table, column)
}

// isSafeIdent restricts SQL identifiers (table names) to the subset
// [a-zA-Z0-9_] that we use throughout this codebase. Used by
// ColumnExists to guard the literal-substitution code path that the
// SQLite pragma_table_info() virtual function requires.
func isSafeIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' {
			continue
		}
		return false
	}
	return true
}

// SchemaDDL returns the dialect-specific CREATE TABLE / CREATE INDEX
// block for the kite-mcp-alerts shared schema (alerts, telegram_chat_ids,
// kite_tokens, kite_credentials, oauth_clients, mcp_sessions, config,
// trailing_stops, daily_pnl, app_registry).
//
// At v0.3.0 (this commit) BOTH dialects return the SQLite-flavored
// DDL block. The Postgres-flavored block (REAL → DOUBLE PRECISION,
// INTEGER booleans → BOOLEAN, etc.) lands in Phase 2.2 when the
// OpenPostgresDB constructor is implemented. The function signature is
// fixed now so Phase 2.2 only needs to fill in the case body — no API
// break.
//
// CALLER CONTRACT: OpenDB (current callsite) keeps its inline DDL string
// literal at v0.3.0 to preserve byte-identical schema output across
// driver-only-bumps. Phase 2.2 will migrate OpenDB to call SchemaDDL()
// directly, after which any schema change happens here in one place.
//
// Why no migration of OpenDB at v0.3.0? Risk minimization: bumping
// callsites + adding helpers in the same commit doubles the regression
// surface. Phase 2.1.6 keeps callsite refactors limited to migrations
// (where the helpers are NEW value); Phase 2.2 will bring OpenDB
// into the helper.
func SchemaDDL(d Dialect) string {
	switch d {
	case DialectSQLite, DialectPostgres:
		// Phase 2.1.6: SQLite-flavored DDL works on Postgres for
		// the simple schema cases (TEXT/INTEGER columns, TEXT PKs,
		// CHECK constraints, partial indexes). The cosmetic mismatches
		// (REAL → DOUBLE PRECISION, INTEGER-as-bool semantics) are
		// Postgres-tolerated for now; Phase 2.2 will produce a
		// truly-Postgres-flavored variant.
		return sqliteSchemaDDL
	default:
		return ""
	}
}

// sqliteSchemaDDL is the v0.2.0 inline schema literal copied here for
// the SchemaDDL helper. OpenDB at v0.3.0 still uses its own inline
// string; this constant is consumed by SchemaDDL only and serves as
// the future single source of truth (Phase 2.2 OpenDB migration).
const sqliteSchemaDDL = `
CREATE TABLE IF NOT EXISTS alerts (
    id                   TEXT PRIMARY KEY,
    email                TEXT NOT NULL,
    tradingsymbol        TEXT NOT NULL,
    exchange             TEXT NOT NULL,
    instrument_token     INTEGER NOT NULL,
    target_price         REAL NOT NULL DEFAULT 0,
    direction            TEXT NOT NULL DEFAULT 'above',
    triggered            INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL,
    triggered_at         TEXT,
    triggered_price      REAL,
    reference_price      REAL,
    notification_sent_at TEXT,
    alert_type           TEXT NOT NULL DEFAULT 'single',
    composite_logic      TEXT,
    composite_name       TEXT,
    conditions_json      TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_email ON alerts(email);

CREATE TABLE IF NOT EXISTS telegram_chat_ids (
    email   TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kite_tokens (
    email        TEXT PRIMARY KEY,
    access_token TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    user_name    TEXT NOT NULL,
    stored_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kite_credentials (
    email      TEXT PRIMARY KEY,
    api_key    TEXT NOT NULL,
    api_secret TEXT NOT NULL,
    stored_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS oauth_clients (
    client_id     TEXT PRIMARY KEY,
    client_secret TEXT NOT NULL,
    redirect_uris TEXT NOT NULL,
    client_name   TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    is_kite_key   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mcp_sessions (
    session_id      TEXT PRIMARY KEY,
    email           TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    terminated      INTEGER NOT NULL DEFAULT 0,
    session_id_enc  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trailing_stops (
    id               TEXT PRIMARY KEY,
    email            TEXT NOT NULL,
    exchange         TEXT NOT NULL,
    tradingsymbol    TEXT NOT NULL,
    instrument_token INTEGER NOT NULL,
    order_id         TEXT NOT NULL,
    variety          TEXT NOT NULL DEFAULT 'regular',
    trail_amount     REAL NOT NULL DEFAULT 0,
    trail_pct        REAL NOT NULL DEFAULT 0,
    direction        TEXT NOT NULL CHECK(direction IN ('long','short')),
    high_water_mark  REAL NOT NULL,
    current_stop     REAL NOT NULL,
    active           INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL,
    deactivated_at   TEXT,
    modify_count     INTEGER NOT NULL DEFAULT 0,
    last_modified_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_trailing_stops_email ON trailing_stops(email);
CREATE INDEX IF NOT EXISTS idx_trailing_stops_active ON trailing_stops(active);

CREATE TABLE IF NOT EXISTS daily_pnl (
    date                    TEXT NOT NULL,
    email                   TEXT NOT NULL,
    holdings_pnl            REAL NOT NULL DEFAULT 0,
    holdings_pnl_currency   TEXT NOT NULL DEFAULT 'INR',
    positions_pnl           REAL NOT NULL DEFAULT 0,
    positions_pnl_currency  TEXT NOT NULL DEFAULT 'INR',
    net_pnl                 REAL NOT NULL DEFAULT 0,
    net_pnl_currency        TEXT NOT NULL DEFAULT 'INR',
    holdings_count          INTEGER NOT NULL DEFAULT 0,
    trades_count            INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (date, email)
);
CREATE INDEX IF NOT EXISTS idx_daily_pnl_email ON daily_pnl(email);

CREATE TABLE IF NOT EXISTS app_registry (
    id            TEXT PRIMARY KEY,
    api_key       TEXT NOT NULL,
    api_secret    TEXT NOT NULL,
    assigned_to   TEXT NOT NULL DEFAULT '',
    label         TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled','invalid','replaced')),
    registered_by TEXT NOT NULL DEFAULT '',
    source        TEXT NOT NULL DEFAULT 'admin',
    last_used_at  TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_app_registry_assigned ON app_registry(assigned_to);
CREATE INDEX IF NOT EXISTS idx_app_registry_api_key ON app_registry(api_key);`
