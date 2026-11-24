package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Versioned schema migrations.
//
// Each file in migrations/ is one migration named NNNN_description.sql, where
// NNNN is its version: 0001, 0002, and so on, with no gaps. Open applies every
// migration a database has not recorded yet, in version order, and records
// each one in schema_migrations. A merged migration is never edited: a later
// schema change is a new file.

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// migrationLockKey is the advisory lock that serialises migrations between
// instances starting at the same time. It must differ from posterLockKey.
const migrationLockKey = int64(0x706c696d2d6d6967) // "plim-mig" in hex ASCII

var migrationFileName = regexp.MustCompile(`^([0-9]{4})_([a-z0-9_]+)\.sql$`)

type migration struct {
	Version int
	Name    string
	SQL     string
}

// loadMigrations reads the migration files at the root of fsys and returns
// them in version order. Misnamed files, duplicate versions and gaps are
// errors, so a typo cannot silently skip a migration.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		m := migrationFileName.FindStringSubmatch(e.Name())
		if e.IsDir() || m == nil {
			return nil, fmt.Errorf("migration %q is not named NNNN_description.sql", e.Name())
		}
		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", e.Name(), err)
		}
		if version == 0 {
			return nil, fmt.Errorf("migration %q: versions start at 0001", e.Name())
		}
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{Version: version, Name: m[2], SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			if i > 0 && m.Version == out[i-1].Version {
				return nil, fmt.Errorf("two migrations have version %04d", m.Version)
			}
			return nil, fmt.Errorf("migration %04d is missing", i+1)
		}
	}
	return out, nil
}

// migrate applies the pending migrations in fsys inside one transaction.
//
// The transaction first takes migrationLockKey, so an instance starting while
// another migrates waits for it to commit and then finds nothing left to do.
// Either every pending migration is applied and recorded or, on any error,
// none is. Statements that refuse to run in a transaction (CREATE INDEX
// CONCURRENTLY, for example) cannot be used in a migration.
func migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	migrations, err := loadMigrations(fsys)
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
		    version     INTEGER PRIMARY KEY,
		    name        TEXT NOT NULL,
		    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[int]bool{}
	rows, err := tx.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name); err != nil {
			return fmt.Errorf("record migration %04d_%s: %w", m.Version, m.Name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

// migrationsFS is the migrations directory compiled into the binary.
func migrationsFS() fs.FS {
	sub, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		// fs.Sub only fails for an invalid path, and "migrations" is a constant.
		panic(err)
	}
	return sub
}
