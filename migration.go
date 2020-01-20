package migration

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Migrate do the sql migration
func Migrate(ctx context.Context, db *sql.DB, appID string, statements []string) error {
	if appID == "" {
		panic("migrate: invalid params: appID can't be empty string")
	}

	if err := ensureMetatable(ctx, db); err != nil {
		return err
	}

	if err := setLogicalLock(ctx, db, true); err != nil {
		return err
	}
	defer setLogicalLock(ctx, db, false)

	var curAppID string
	if err := db.QueryRowContext(ctx,
		"select value from __meta where `key` = 'application_id';",
	).Scan(&curAppID); err != nil {
		return err
	}

	if curAppID == "" {
		if _, err := db.ExecContext(ctx,
			"update __meta set value = ? where `key` = 'application_id';",
			appID,
		); err != nil {
			return err
		}
		curAppID = appID
	}
	if curAppID != appID {
		return fmt.Errorf("Invalid application_id on database")
	}

	var userVersion int
	if err := db.QueryRowContext(ctx,
		"select value from __meta where `key` = 'user_version';",
	).Scan(&userVersion); err != nil {
		return err
	}
	for ; userVersion < len(statements); userVersion++ {
		statement := statements[userVersion]
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx,
		"update __meta set value = ? where `key` = 'user_version';",
		userVersion,
	); err != nil {
		return err
	}

	return nil
}

func ensureMetatable(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, ""+
		"create table if not exists "+
		"__meta(`key` text, value text, primary key (`key`(255)));",
	); err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, ""+
		"insert ignore into __meta(`key`, value) values "+
		"('application_id', ''), "+
		"('user_version', '0'), "+
		"('locked', 'false');",
	); err != nil {
		return err
	}

	return nil
}

// setLogicalLock set logical lock in __meta table
// because mysql doesn't have transactional DDL
// this is needed, so calling Migrate in parallel (multiple process) is safe
func setLogicalLock(ctx context.Context, db *sql.DB, desired bool) error {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	for {
		result, err := db.ExecContext(ctx, ""+
			"update __meta set value = ? where `key` = 'locked' and value = ?",
			boolToString(desired), boolToString(!desired),
		)
		if err != nil {
			return err
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if affected == 1 {
			return nil
		}

		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
