package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-goe/goe"
	"github.com/go-goe/goe/model"
	"modernc.org/sqlite"
)

type Driver struct {
	dns string
	sql *sql.DB
	config
}

func (d *Driver) GetDatabaseConfig() *model.DatabaseConfig {
	return &d.config.DatabaseConfig
}

type ExecQuerierContext interface {
	driver.ExecerContext
	driver.QueryerContext
}

type ConnectionHook func(
	conn ExecQuerierContext,
	dsn string,
) error

type config struct {
	model.DatabaseConfig
	MigratePath    string
	ConnectionHook ConnectionHook
}

type Config struct {
	Logger           model.Logger
	IncludeArguments bool          // include all arguments used on query
	QueryThreshold   time.Duration // query threshold to warning on slow queries

	MigratePath    string         // output sql file, if defined the driver will not auto apply the migration.
	ConnectionHook ConnectionHook // ConnectionHook is called after each connection is opened.
}

func NewConfig(c Config) config {
	return config{
		DatabaseConfig: model.DatabaseConfig{
			Logger:           c.Logger,
			IncludeArguments: c.IncludeArguments,
			QueryThreshold:   c.QueryThreshold,
		},
		MigratePath:    c.MigratePath,
		ConnectionHook: c.ConnectionHook,
	}
}

var lock = struct {
	sync.Mutex
}{sync.Mutex{}}

// OpenInMemory opens a in memory database.
func OpenInMemory(c config) (driver *Driver) {
	return Open("file:goe?mode=memory&cache=shared", c)
}

// Open opens a sqlite connection. By default uses "PRAGMA foreign_keys = ON;" and "PRAGMA busy_timeout = 5000;".
func Open(dns string, c config) (driver *Driver) {
	return &Driver{
		dns:    dns,
		config: c,
	}
}

func (dr *Driver) Init() error {
	dr.DatabaseConfig.SetInitCallback(func() error {
		lock.Lock()
		defer lock.Unlock()
		var err error
		dr.setHooks()
		dr.sql, err = sql.Open("sqlite", dr.dns)
		if err != nil {
			// logged by goe
			return err
		}

		return dr.sql.Ping()
	})
	return nil
}

func (dr *Driver) setHooks() {
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		conn.ExecContext(context.Background(), "PRAGMA foreign_keys = ON;", nil)
		conn.ExecContext(context.Background(), "PRAGMA busy_timeout = 5000;", nil)
		return nil
	})
	if dr.ConnectionHook != nil {
		sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
			return dr.ConnectionHook(conn, dsn)
		})
	}
	schemas := dr.Schemas()
	dns, params, _ := strings.Cut(dr.dns, "?")
	if len(schemas) != 0 {
		rx := regexp.MustCompile(`([^/]+?)(?:\.[a-zA-Z0-9]+)?$`)
		currentDb := rx.FindString(dns)
		currentDb = strings.TrimPrefix(currentDb, "file:")
		var ex string
		ix := strings.Index(currentDb, ".")
		if ix != -1 {
			ex = currentDb[ix:]
		}
		schemaBuilder := strings.Builder{}

		for _, schema := range schemas {
			schemaBuilder.WriteString(
				fmt.Sprintf("ATTACH DATABASE '%v' AS %v;\n",
					strings.Replace(dns, currentDb, schema[1:len(schema)-1]+ex, 1)+func() string {
						if params != "" {
							return "?" + params
						}
						return ""
					}(),
					schema))
		}
		sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
			conn.ExecContext(context.Background(), schemaBuilder.String(), nil)
			return nil
		})
	}
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

var errMap = map[int][]error{
	1555: {goe.ErrBadRequest, goe.ErrUniqueValue},
	2067: {goe.ErrBadRequest, goe.ErrUniqueValue},
	787:  {goe.ErrBadRequest, goe.ErrForeignKey},
}

type wrapErrors struct {
	msg  string
	errs []error
}

func (e *wrapErrors) Error() string {
	return "goe: " + e.msg
}

func (e *wrapErrors) Unwrap() []error {
	return e.errs
}

func (dr *Driver) ErrorTranslator() func(err error) error {
	return func(err error) error {
		if sqliteError, ok := err.(*sqlite.Error); ok {
			return &wrapErrors{msg: err.Error(), errs: append(errMap[sqliteError.Code()], err)}
		}
		return err
	}
}

func (dr *Driver) NewConnection() model.Connection {
	return Connection{sql: dr.sql, config: dr.config, dns: dr.dns}
}

type Connection struct {
	dns    string
	config config
	sql    *sql.DB
}

func (c Connection) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	buildSql(query)
	rows, err := c.sql.QueryContext(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (c Connection) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	buildSql(query)
	row := c.sql.QueryRowContext(ctx, query.RawSql, query.Arguments...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query)
	_, err := c.sql.ExecContext(ctx, query.RawSql, query.Arguments...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (model.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, opts)
	return Transaction{tx: tx, config: dr.config, dns: dr.dns}, err
}

type Transaction struct {
	dns    string
	config config
	tx     *sql.Tx
	saves  int64
}

func (t Transaction) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	buildSql(query)
	rows, err := t.tx.QueryContext(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (t Transaction) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	buildSql(query)
	return Row{row: t.tx.QueryRowContext(ctx, query.RawSql, query.Arguments...)}
}

func (t Transaction) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query)
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

type SavePoint struct {
	name string
	tx   Transaction
}

func (t Transaction) SavePoint() (model.SavePoint, error) {
	t.saves++
	point := "sp_" + strconv.FormatInt(t.saves, 10)
	_, err := t.tx.Exec("SAVEPOINT " + point)
	if err != nil {
		// goe can't log
		return nil, t.config.ErrorHandler(context.TODO(), err)
	}
	return SavePoint{point, t}, nil
}

func (s SavePoint) Rollback() error {
	_, err := s.tx.tx.Exec("ROLLBACK TO SAVEPOINT " + s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (s SavePoint) Commit() error {
	_, err := s.tx.tx.Exec("RELEASE SAVEPOINT " + s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
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
