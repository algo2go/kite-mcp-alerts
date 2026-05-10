package alerts

import (
	"database/sql"
	"fmt"
	"strings"
)

// migrateRegistryCheckConstraint recreates the app_registry table with an expanded
// CHECK constraint that includes 'invalid' and 'replaced' statuses. This is needed
// because SQLite does not support ALTER CHECK.
//
// The "is migration needed?" probe queries sqlite_master for the table's
// declared CREATE TABLE SQL (only SQLite preserves this — Postgres
// information_schema does not). Therefore the early-exit on Postgres
// is "migration unnecessary" — Postgres adapter (Phase 2.2) will
// declare the expanded CHECK in its initial DDL, no migration needed.
func migrateRegistryCheckConstraint(db *sql.DB) error {
	// Existence probe via dialect-portable helper. If the table is
	// absent (fresh install path), DDL will create it with the
	// expanded CHECK; nothing to migrate.
	exists, err := TableExists(DialectSQLite, db, "app_registry")
	if err != nil {
		return fmt.Errorf("check app_registry exists: %w", err)
	}
	if !exists {
		return nil
	}

	// SQLite-specific: read the table's declared CREATE TABLE SQL
	// to detect whether the legacy CHECK constraint is in place.
	// Phase 2.2 will route this through dialect.go (Postgres branch
	// returns empty, treated as already-migrated since fresh
	// Postgres installs include the expanded CHECK in DDL).
	var tableSql string
	err = db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='app_registry'`).Scan(&tableSql)
	if err != nil {
		return nil // table doesn't exist yet (will be created by DDL), nothing to migrate
	}
	// If the table already supports 'invalid', no migration needed.
	if strings.Contains(tableSql, "'invalid'") {
		return nil
	}

	// Recreate: new table → copy data → drop old → rename
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin registry migration: %w", err)
	}
	defer tx.Rollback() // #nosec G104 -- rollback after commit is a no-op

	if _, err := tx.Exec(`CREATE TABLE app_registry_new (
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
	)`); err != nil {
		return fmt.Errorf("create registry_new: %w", err)
	}

	// Copy existing data. Use COALESCE for new columns that may not exist yet in the source.
	if _, err := tx.Exec(`INSERT INTO app_registry_new (id, api_key, api_secret, assigned_to, label, status, registered_by, source, last_used_at, created_at, updated_at)
		SELECT id, api_key, api_secret, assigned_to, label, status, registered_by,
			COALESCE(source, 'admin'), COALESCE(last_used_at, ''), created_at, updated_at
		FROM app_registry`); err != nil {
		return fmt.Errorf("copy registry data: %w", err)
	}

	if _, err := tx.Exec(`DROP TABLE app_registry`); err != nil {
		return fmt.Errorf("drop old registry: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE app_registry_new RENAME TO app_registry`); err != nil {
		return fmt.Errorf("rename registry: %w", err)
	}
	// Recreate indexes
	tx.Exec(`CREATE INDEX IF NOT EXISTS idx_app_registry_assigned ON app_registry(assigned_to)`) // #nosec G104 -- idempotent index creation
	tx.Exec(`CREATE INDEX IF NOT EXISTS idx_app_registry_api_key ON app_registry(api_key)`)     // #nosec G104 -- idempotent index creation

	return tx.Commit()
}

// migrateAlerts applies incremental schema migrations to the alerts table.
//
// All column-existence probes route through ColumnExists (Phase 2.1.6
// dialect helper) so the migration code stays dialect-agnostic. The
// ALTER TABLE ADD COLUMN syntax itself is portable across SQLite +
// Postgres; no dispatch needed for the mutation.
func migrateAlerts(db *sql.DB) error {
	// Check if reference_price column exists via dialect helper.
	exists, err := ColumnExists(DialectSQLite, db, "alerts", "reference_price")
	if err != nil {
		return fmt.Errorf("check reference_price column: %w", err)
	}
	if !exists {
		if _, err := db.Exec(`ALTER TABLE alerts ADD COLUMN reference_price REAL`); err != nil {
			return fmt.Errorf("add reference_price column: %w", err)
		}
	}

	// Add notification_sent_at column if missing.
	// SQLite returns an error if the column already exists; ignore it.
	db.Exec(`ALTER TABLE alerts ADD COLUMN notification_sent_at TEXT`) // #nosec G104 -- idempotent migration

	// Composite alert columns (Option B from the session handoff). Legacy
	// single-leg alerts leave these NULL; loader normalizes alert_type to
	// 'single' when NULL/empty. All ALTER TABLE ADD COLUMN statements are
	// idempotent — SQLite returns an error if the column already exists,
	// which we ignore since the goal is "column present after call".
	db.Exec(`ALTER TABLE alerts ADD COLUMN alert_type TEXT DEFAULT 'single'`) // #nosec G104 -- idempotent migration
	db.Exec(`ALTER TABLE alerts ADD COLUMN composite_logic TEXT`)              // #nosec G104 -- idempotent migration
	db.Exec(`ALTER TABLE alerts ADD COLUMN composite_name TEXT`)               // #nosec G104 -- idempotent migration
	db.Exec(`ALTER TABLE alerts ADD COLUMN conditions_json TEXT`)              // #nosec G104 -- idempotent migration

	// Daily P&L currency columns (Slice 6d): sibling currency labels
	// for the holdings_pnl / positions_pnl / net_pnl REAL columns. The
	// 3 floats stay REAL so SQLite SUM()/AVG() continues to work; the
	// new TEXT columns let cross-currency aggregation be detected
	// in-process via the DailyPnLEntry.*Money() accessors. Existing
	// rows backfill to 'INR' via DEFAULT — gokiteconnect emits INR
	// prices by contract, so the backfill matches reality.
	db.Exec(`ALTER TABLE daily_pnl ADD COLUMN holdings_pnl_currency TEXT NOT NULL DEFAULT 'INR'`)  // #nosec G104 -- idempotent migration
	db.Exec(`ALTER TABLE daily_pnl ADD COLUMN positions_pnl_currency TEXT NOT NULL DEFAULT 'INR'`) // #nosec G104 -- idempotent migration
	db.Exec(`ALTER TABLE daily_pnl ADD COLUMN net_pnl_currency TEXT NOT NULL DEFAULT 'INR'`)        // #nosec G104 -- idempotent migration

	return nil
}
