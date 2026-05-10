package alerts

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/algo2go/kite-mcp-domain"
)

// SaveAlert inserts or replaces an alert in the database. Both single-leg
// and composite alerts route through here — the composite_* columns stay
// NULL for single-leg alerts, and the top-level Direction/TargetPrice are
// zero-valued (but present) for composite alerts so the NOT NULL columns
// are satisfied.
func (d *DB) SaveAlert(alert *Alert) error {
	triggered := 0
	if alert.Triggered {
		triggered = 1
	}
	var triggeredAt sql.NullString
	if !alert.TriggeredAt.IsZero() {
		triggeredAt = sql.NullString{String: alert.TriggeredAt.Format(time.RFC3339), Valid: true}
	}
	var trigPrice sql.NullFloat64
	if alert.TriggeredPrice != 0 {
		trigPrice = sql.NullFloat64{Float64: alert.TriggeredPrice, Valid: true}
	}
	var refPrice sql.NullFloat64
	if alert.ReferencePrice != 0 {
		refPrice = sql.NullFloat64{Float64: alert.ReferencePrice, Valid: true}
	}
	var notifSentAt sql.NullString
	if !alert.NotificationSentAt.IsZero() {
		notifSentAt = sql.NullString{String: alert.NotificationSentAt.Format(time.RFC3339), Valid: true}
	}

	// Composite columns: only populated for composite alerts. Normalize
	// missing AlertType to 'single' so legacy call-sites that construct
	// Alert without the new fields still save correctly.
	alertType := alert.AlertType
	if alertType == "" {
		alertType = domain.AlertTypeSingle
	}
	var compositeLogic, compositeName, conditionsJSON sql.NullString
	if alert.IsComposite() {
		if alert.CompositeLogic != "" {
			compositeLogic = sql.NullString{String: string(alert.CompositeLogic), Valid: true}
		}
		if alert.CompositeName != "" {
			compositeName = sql.NullString{String: alert.CompositeName, Valid: true}
		}
		if len(alert.Conditions) > 0 {
			raw, err := json.Marshal(alert.Conditions)
			if err != nil {
				return fmt.Errorf("marshal conditions_json: %w", err)
			}
			conditionsJSON = sql.NullString{String: string(raw), Valid: true}
		}
	}

	// SQL portability: ON CONFLICT (id) DO UPDATE SET ... is the
	// dialect-portable upsert form per Phase 2.1 audit.
	_, err := d.runExec(`INSERT INTO alerts
		(id, email, tradingsymbol, exchange, instrument_token, target_price,
		 direction, triggered, created_at, triggered_at, triggered_price,
		 reference_price, notification_sent_at,
		 alert_type, composite_logic, composite_name, conditions_json)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
			conditions_json = excluded.conditions_json`,
		alert.ID, alert.Email, alert.Tradingsymbol, alert.Exchange,
		alert.InstrumentToken, alert.TargetPrice, string(alert.Direction),
		triggered, alert.CreatedAt.Format(time.RFC3339),
		triggeredAt, trigPrice, refPrice, notifSentAt,
		string(alertType), compositeLogic, compositeName, conditionsJSON)
	if err != nil {
		return fmt.Errorf("save alert: %w", err)
	}
	return nil
}

// DeleteAlert removes an alert by ID for the given email.
func (d *DB) DeleteAlert(email, alertID string) error {
	_, err := d.runExec(`DELETE FROM alerts WHERE id = ? AND email = ?`, alertID, email)
	if err != nil {
		return fmt.Errorf("delete alert: %w", err)
	}
	return nil
}

// DeleteAlertsByEmail removes all alerts for the given email.
func (d *DB) DeleteAlertsByEmail(email string) error {
	_, err := d.runExec(`DELETE FROM alerts WHERE email = ?`, email)
	if err != nil {
		return fmt.Errorf("delete alerts by email: %w", err)
	}
	return nil
}

// DeleteTelegramChatID removes the Telegram chat ID mapping for the given email.
func (d *DB) DeleteTelegramChatID(email string) error {
	_, err := d.runExec(`DELETE FROM telegram_chat_ids WHERE email = ?`, email)
	if err != nil {
		return fmt.Errorf("delete telegram chat id: %w", err)
	}
	return nil
}

// UpdateAlertNotification records when a Telegram notification was sent for an alert.
func (d *DB) UpdateAlertNotification(alertID string, sentAt time.Time) error {
	_, err := d.runExec("UPDATE alerts SET notification_sent_at = ? WHERE id = ?",
		sentAt.Format(time.RFC3339), alertID)
	if err != nil {
		return fmt.Errorf("update notification_sent_at: %w", err)
	}
	return nil
}

// UpdateTriggered marks an alert as triggered with the given price and time.
func (d *DB) UpdateTriggered(alertID string, price float64, at time.Time) error {
	_, err := d.runExec(`UPDATE alerts SET triggered = 1, triggered_at = ?, triggered_price = ? WHERE id = ?`,
		at.Format(time.RFC3339), price, alertID)
	if err != nil {
		return fmt.Errorf("update triggered: %w", err)
	}
	return nil
}

// SaveTelegramChatID stores or updates a Telegram chat ID for the given email.
func (d *DB) SaveTelegramChatID(email string, chatID int64) error {
	_, err := d.runExec(`INSERT INTO telegram_chat_ids (email, chat_id) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET chat_id = excluded.chat_id`, email, chatID)
	if err != nil {
		return fmt.Errorf("save telegram chat id: %w", err)
	}
	return nil
}

// SaveToken stores or updates a Kite token for the given email.
// If an encryption key is set, the access token is encrypted at rest.
func (d *DB) SaveToken(email, accessToken, userID, userName string, storedAt time.Time) error {
	storeToken := accessToken
	if d.encryptionKey != nil {
		var err error
		storeToken, err = encrypt(d.encryptionKey, accessToken)
		if err != nil {
			return fmt.Errorf("encrypt access_token: %w", err)
		}
	}
	_, err := d.runExec(`INSERT INTO kite_tokens (email, access_token, user_id, user_name, stored_at) VALUES (?,?,?,?,?)
		ON CONFLICT (email) DO UPDATE SET
			access_token = excluded.access_token,
			user_id = excluded.user_id,
			user_name = excluded.user_name,
			stored_at = excluded.stored_at`,
		email, storeToken, userID, userName, storedAt.Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("save token: %w", err)
	}
	return nil
}

// DeleteToken removes a cached token for the given email.
func (d *DB) DeleteToken(email string) error {
	_, err := d.runExec(`DELETE FROM kite_tokens WHERE email = ?`, email)
	if err != nil {
		return fmt.Errorf("delete token: %w", err)
	}
	return nil
}

// SaveCredential stores or updates Kite credentials for the given email.
// If an encryption key is set, api_key and api_secret are encrypted at rest.
func (d *DB) SaveCredential(email, apiKey, apiSecret, appID string, storedAt time.Time) error {
	storeKey, storeSecret := apiKey, apiSecret
	if d.encryptionKey != nil {
		var err error
		storeKey, err = encrypt(d.encryptionKey, apiKey)
		if err != nil {
			return fmt.Errorf("encrypt api_key: %w", err)
		}
		storeSecret, err = encrypt(d.encryptionKey, apiSecret)
		if err != nil {
			return fmt.Errorf("encrypt api_secret: %w", err)
		}
	}
	_, err := d.runExec(`INSERT INTO kite_credentials (email, api_key, api_secret, stored_at, app_id) VALUES (?,?,?,?,?)
		ON CONFLICT (email) DO UPDATE SET
			api_key = excluded.api_key,
			api_secret = excluded.api_secret,
			stored_at = excluded.stored_at,
			app_id = excluded.app_id`,
		email, storeKey, storeSecret, storedAt.Format(time.RFC3339), appID)
	if err != nil {
		return fmt.Errorf("save credential: %w", err)
	}
	return nil
}

// DeleteCredential removes Kite credentials for the given email.
func (d *DB) DeleteCredential(email string) error {
	_, err := d.runExec(`DELETE FROM kite_credentials WHERE email = ?`, email)
	if err != nil {
		return fmt.Errorf("delete credential: %w", err)
	}
	return nil
}

// SaveClient stores or updates an OAuth client in the database.
// If an encryption key is set and client_secret is non-empty, it is encrypted at rest.
func (d *DB) SaveClient(clientID, clientSecret, redirectURIsJSON, clientName string, createdAt time.Time, isKiteKey bool) error {
	isKiteKeyInt := 0
	if isKiteKey {
		isKiteKeyInt = 1
	}
	storeSecret := clientSecret
	if d.encryptionKey != nil && clientSecret != "" {
		var err error
		storeSecret, err = encrypt(d.encryptionKey, clientSecret)
		if err != nil {
			return fmt.Errorf("encrypt client_secret: %w", err)
		}
	}
	_, err := d.runExec(`INSERT INTO oauth_clients (client_id, client_secret, redirect_uris, client_name, created_at, is_kite_key) VALUES (?,?,?,?,?,?)
		ON CONFLICT (client_id) DO UPDATE SET
			client_secret = excluded.client_secret,
			redirect_uris = excluded.redirect_uris,
			client_name = excluded.client_name,
			created_at = excluded.created_at,
			is_kite_key = excluded.is_kite_key`,
		clientID, storeSecret, redirectURIsJSON, clientName, createdAt.Format(time.RFC3339), isKiteKeyInt)
	if err != nil {
		return fmt.Errorf("save oauth client: %w", err)
	}
	return nil
}

// DeleteClient removes an OAuth client by ID.
func (d *DB) DeleteClient(clientID string) error {
	_, err := d.runExec(`DELETE FROM oauth_clients WHERE client_id = ?`, clientID)
	if err != nil {
		return fmt.Errorf("delete oauth client: %w", err)
	}
	return nil
}

// SaveSession stores or updates an MCP session in the database.
// The session_id PK is stored as HMAC-SHA256(key, sessionID) so the original
// session ID never appears in plaintext in the database. The original ID is
// stored encrypted in session_id_enc for recovery on restart.
func (d *DB) SaveSession(sessionID, email string, createdAt, expiresAt time.Time, terminated bool) error {
	terminatedI := 0
	if terminated {
		terminatedI = 1
	}
	hashedID := d.hashSessionID(sessionID)
	var sessionIDEnc string
	if d.encryptionKey != nil {
		var err error
		sessionIDEnc, err = encrypt(d.encryptionKey, sessionID)
		if err != nil {
			return fmt.Errorf("encrypt session_id: %w", err)
		}
	}
	_, err := d.runExec(`INSERT INTO mcp_sessions (session_id, email, created_at, expires_at, terminated, session_id_enc) VALUES (?,?,?,?,?,?)
		ON CONFLICT (session_id) DO UPDATE SET
			email = excluded.email,
			created_at = excluded.created_at,
			expires_at = excluded.expires_at,
			terminated = excluded.terminated,
			session_id_enc = excluded.session_id_enc`,
		hashedID, email, createdAt.Format(time.RFC3339), expiresAt.Format(time.RFC3339), terminatedI, sessionIDEnc)
	if err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	return nil
}

// DeleteSession removes an MCP session by ID.
// The session_id is hashed before lookup to match the HMAC-hashed PK in the database.
func (d *DB) DeleteSession(sessionID string) error {
	hashedID := d.hashSessionID(sessionID)
	_, err := d.runExec(`DELETE FROM mcp_sessions WHERE session_id = ?`, hashedID)
	if err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return nil
}

// SaveTrailingStop inserts or replaces a trailing stop in the database.
func (d *DB) SaveTrailingStop(ts *TrailingStop) error {
	active := 0
	if ts.Active {
		active = 1
	}
	var deactivatedAt, lastModifiedAt sql.NullString
	if !ts.DeactivatedAt.IsZero() {
		deactivatedAt = sql.NullString{String: ts.DeactivatedAt.Format(time.RFC3339), Valid: true}
	}
	if !ts.LastModifiedAt.IsZero() {
		lastModifiedAt = sql.NullString{String: ts.LastModifiedAt.Format(time.RFC3339), Valid: true}
	}
	_, err := d.runExec(`INSERT INTO trailing_stops
		(id, email, exchange, tradingsymbol, instrument_token, order_id, variety,
		 trail_amount, trail_pct, direction, high_water_mark, current_stop,
		 active, created_at, deactivated_at, modify_count, last_modified_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (id) DO UPDATE SET
			email = excluded.email,
			exchange = excluded.exchange,
			tradingsymbol = excluded.tradingsymbol,
			instrument_token = excluded.instrument_token,
			order_id = excluded.order_id,
			variety = excluded.variety,
			trail_amount = excluded.trail_amount,
			trail_pct = excluded.trail_pct,
			direction = excluded.direction,
			high_water_mark = excluded.high_water_mark,
			current_stop = excluded.current_stop,
			active = excluded.active,
			created_at = excluded.created_at,
			deactivated_at = excluded.deactivated_at,
			modify_count = excluded.modify_count,
			last_modified_at = excluded.last_modified_at`,
		ts.ID, ts.Email, ts.Exchange, ts.Tradingsymbol, ts.InstrumentToken,
		ts.OrderID, ts.Variety, ts.TrailAmount, ts.TrailPct, ts.Direction,
		ts.HighWaterMark, ts.CurrentStop, active,
		ts.CreatedAt.Format(time.RFC3339), deactivatedAt, ts.ModifyCount, lastModifiedAt)
	if err != nil {
		return fmt.Errorf("save trailing stop: %w", err)
	}
	return nil
}

// DeactivateTrailingStop marks a trailing stop as inactive.
func (d *DB) DeactivateTrailingStop(id string) error {
	_, err := d.runExec(`UPDATE trailing_stops SET active = 0, deactivated_at = ? WHERE id = ?`,
		time.Now().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("deactivate trailing stop: %w", err)
	}
	return nil
}

// UpdateTrailingStop updates the high water mark, current stop, and modify count.
func (d *DB) UpdateTrailingStop(id string, hwm, currentStop float64, modifyCount int) error {
	_, err := d.runExec(`UPDATE trailing_stops SET high_water_mark = ?, current_stop = ?, modify_count = ?, last_modified_at = ? WHERE id = ?`,
		hwm, currentStop, modifyCount, time.Now().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("update trailing stop: %w", err)
	}
	return nil
}

// SaveDailyPnL inserts or replaces a daily P&L entry.
//
// Currency-aware (Slice 6d): writes the holdings/positions/net pnl
// currency labels alongside the float magnitudes. Empty Currency
// fields on the struct normalize to 'INR' via pnlCurrencyOrINR — the
// DB CHECK is NOT NULL so we must always supply a value, and INR is
// the production default (gokiteconnect emits INR prices by contract).
func (d *DB) SaveDailyPnL(entry *DailyPnLEntry) error {
	_, err := d.runExec(`INSERT INTO daily_pnl
		(date, email,
		 holdings_pnl, holdings_pnl_currency,
		 positions_pnl, positions_pnl_currency,
		 net_pnl, net_pnl_currency,
		 holdings_count, trades_count)
		VALUES (?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (date, email) DO UPDATE SET
			holdings_pnl = excluded.holdings_pnl,
			holdings_pnl_currency = excluded.holdings_pnl_currency,
			positions_pnl = excluded.positions_pnl,
			positions_pnl_currency = excluded.positions_pnl_currency,
			net_pnl = excluded.net_pnl,
			net_pnl_currency = excluded.net_pnl_currency,
			holdings_count = excluded.holdings_count,
			trades_count = excluded.trades_count`,
		entry.Date, entry.Email,
		entry.HoldingsPnL, pnlCurrencyOrINR(entry.HoldingsPnLCurrency),
		entry.PositionsPnL, pnlCurrencyOrINR(entry.PositionsPnLCurrency),
		entry.NetPnL, pnlCurrencyOrINR(entry.NetPnLCurrency),
		entry.HoldingsCount, entry.TradesCount)
	if err != nil {
		return fmt.Errorf("save daily pnl: %w", err)
	}
	return nil
}

// SaveRegistryEntry stores or updates an app registration.
// If an encryption key is set, api_key and api_secret are encrypted at rest.
func (d *DB) SaveRegistryEntry(e *RegistryDBEntry) error {
	storeKey, storeSecret := e.APIKey, e.APISecret
	if d.encryptionKey != nil {
		var err error
		storeKey, err = encrypt(d.encryptionKey, e.APIKey)
		if err != nil {
			return fmt.Errorf("encrypt registry api_key: %w", err)
		}
		storeSecret, err = encrypt(d.encryptionKey, e.APISecret)
		if err != nil {
			return fmt.Errorf("encrypt registry api_secret: %w", err)
		}
	}
	lastUsedAtS := ""
	if e.LastUsedAt != nil {
		lastUsedAtS = e.LastUsedAt.Format(time.RFC3339)
	}
	_, err := d.runExec(`INSERT INTO app_registry (id, api_key, api_secret, assigned_to, label, status, registered_by, source, last_used_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (id) DO UPDATE SET
			api_key = excluded.api_key,
			api_secret = excluded.api_secret,
			assigned_to = excluded.assigned_to,
			label = excluded.label,
			status = excluded.status,
			registered_by = excluded.registered_by,
			source = excluded.source,
			last_used_at = excluded.last_used_at,
			created_at = excluded.created_at,
			updated_at = excluded.updated_at`,
		e.ID, storeKey, storeSecret, e.AssignedTo, e.Label, e.Status, e.RegisteredBy, e.Source, lastUsedAtS,
		e.CreatedAt.Format(time.RFC3339), e.UpdatedAt.Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("save registry entry: %w", err)
	}
	return nil
}

// DeleteRegistryEntry removes an app registration by ID.
func (d *DB) DeleteRegistryEntry(id string) error {
	_, err := d.runExec(`DELETE FROM app_registry WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete registry entry: %w", err)
	}
	return nil
}
