package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-goe/goe"
	"github.com/go-goe/goe/model"
	_ "modernc.org/sqlite"
)

type Driver struct {
	dns string
	sql *sql.DB
	Config
}

func (d *Driver) GetDatabaseConfig() *goe.DatabaseConfig {
	return &d.Config.DatabaseConfig
}

type Config struct {
	goe.DatabaseConfig
	MigratePath string // output sql file, if defined the driver will not auto apply the migration
}

func Open(dns string, config Config) (driver *Driver) {
	return &Driver{
		dns:    dns,
		Config: config,
	}
}

func (dr *Driver) Init() error {
	var err error
	dr.sql, err = sql.Open("sqlite", dr.dns)
	if err != nil {
		// logged by goe
		return err
	}

	return dr.sql.Ping()
}

func (dr *Driver) KeywordHandler(s string) string {
	return keywordHandler(s)
}

func keywordHandler(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func (dr *Driver) Name() string {
	return "SQLite"
}

func (dr *Driver) Stats() sql.DBStats {
	return dr.sql.Stats()
}

func (dr *Driver) Close() error {
	return dr.sql.Close()
}

func (dr *Driver) NewConnection() goe.Connection {
	return Connection{sql: dr.sql, config: dr.Config, dns: dr.dns}
}

type Connection struct {
	dns    string
	config Config
	sql    *sql.DB
}

func (c Connection) QueryContext(ctx context.Context, query *model.Query) (goe.Rows, error) {
	buildSql(query, c.sql, c.dns)
	rows, err := c.sql.QueryContext(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (c Connection) QueryRowContext(ctx context.Context, query *model.Query) goe.Row {
	buildSql(query, c.sql, c.dns)
	row := c.sql.QueryRowContext(ctx, query.RawSql, query.Arguments...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query, c.sql, c.dns)
	_, err := c.sql.ExecContext(ctx, query.RawSql, query.Arguments...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (goe.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, opts)
	return Transaction{tx: tx, config: dr.Config, dns: dr.dns}, err
}

type Transaction struct {
	dns    string
	config Config
	tx     *sql.Tx
}

func (t Transaction) QueryContext(ctx context.Context, query *model.Query) (goe.Rows, error) {
	buildSql(query, t.tx, t.dns)
	rows, err := t.tx.QueryContext(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (t Transaction) QueryRowContext(ctx context.Context, query *model.Query) goe.Row {
	buildSql(query, t.tx, t.dns)
	return Row{row: t.tx.QueryRowContext(ctx, query.RawSql, query.Arguments...)}
}

func (t Transaction) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query, t.tx, t.dns)
	_, err := t.tx.ExecContext(ctx, query.RawSql, query.Arguments...)

	return err
}

func (t Transaction) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (t Transaction) Rollback() error {
	err := t.tx.Rollback()
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

type Rows struct {
	rows *sql.Rows
}

func (rs Rows) Close() error {
	return rs.rows.Close()
}

func (rs Rows) Next() bool {
	return rs.rows.Next()
}

func (rs Rows) Scan(dest ...any) error {
	return rs.rows.Scan(dest...)
}

type Row struct {
	row *sql.Row
}

func (r Row) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}
