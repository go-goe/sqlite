package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-goe/goe/enum"
	"github.com/go-goe/goe/model"
)

type body struct {
	driver  *Driver
	table   *model.TableMigrate
	dataMap map[string]*dataType
	sql     *strings.Builder
	conn    *sql.DB
	tables  map[string]*model.TableMigrate
	dbTable
}

func (db *Driver) MigrateContext(ctx context.Context, migrator *model.Migrator) error {
	dataMap := map[string]*dataType{
		"string":    {"text", "''"},
		"int16":     {"integer", "0"},
		"int32":     {"integer", "0"},
		"int64":     {"integer", "0"},
		"float32":   {"real", "0"},
		"float64":   {"real", "0"},
		"[]uint8":   {"bytea", "X''"},
		"time.Time": {"datetime", "'0000-01-01'"},
		"bool":      {"boolean", "false"},
		"uuid.UUID": {"uuid", "'00000000-0000-0000-0000-000000000000'"},
	}

	sql := new(strings.Builder)
	var err error

	sqlColumns := new(strings.Builder)
	for _, t := range migrator.Tables {
		err = checkTableChanges(body{
			table:   t,
			dataMap: dataMap,
			driver:  db,
			sql:     sql,
			conn:    db.sql,
			tables:  migrator.Tables,
		})
		if err != nil {
			return err
		}

		err = checkIndex(t.Indexes, t, sqlColumns, db.sql)
		if err != nil {
			return err
		}
	}

	for _, t := range migrator.Tables {
		if !t.Migrated {
			createTable(t, dataMap, sql, migrator.Tables, false)
		}
	}

	sql.WriteString(sqlColumns.String())

	if sql.Len() != 0 {
		return db.rawExecContext(ctx, sql.String())
	}
	return nil
}

func (db *Driver) rawExecContext(ctx context.Context, rawSql string, args ...any) error {
	if db.config.MigratePath == "" {
		query := model.Query{Type: enum.RawQuery, RawSql: rawSql, Arguments: args}
		query.Header.Err = wrapperExec(ctx, db.NewConnection(), &query)
		if query.Header.Err != nil {
			return db.GetDatabaseConfig().ErrorQueryHandler(ctx, query)
		}
		db.GetDatabaseConfig().InfoHandler(ctx, query)
		return nil
	}
	root, err := os.OpenRoot(db.config.MigratePath)
	if err != nil {
		return err
	}
	defer root.Close()

	file, err := root.OpenFile(db.Name()+"_"+strconv.FormatInt(time.Now().Unix(), 10)+".sql", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(rawSql)
	return err
}

func wrapperExec(ctx context.Context, conn model.Connection, query *model.Query) error {
	queryStart := time.Now()
	defer func() { query.Header.QueryDuration = time.Since(queryStart) }()
	return conn.ExecContext(ctx, query)
}

func (db *Driver) DropTable(schema, table string) error {
	if len(schema) > 2 {
		table = schema + "." + table
		checkAttach(db.sql, db.dns, map[string]bool{schema[1 : len(schema)-1]: true})
	}
	return db.rawExecContext(context.TODO(), fmt.Sprintf("DROP TABLE IF EXISTS %v;", table))
}

func (db *Driver) RenameTable(schema, table, newTable string) error {
	if len(schema) > 2 {
		table = schema + "." + table
		newTable = schema + "." + newTable
	}
	return db.rawExecContext(context.TODO(), fmt.Sprintf("ALTER TABLE %v RENAME TO %v;", table, newTable))
}

func (db *Driver) RenameColumn(schema, table, oldColumn, newColumn string) error {
	if len(schema) > 2 {
		table = schema + "." + table
		checkAttach(db.sql, db.dns, map[string]bool{schema[1 : len(schema)-1]: true})
	}
	return db.rawExecContext(context.TODO(), renameColumn(table, oldColumn, newColumn))
}

func (db *Driver) DropColumn(schema, table, column string) error {
	if len(schema) > 2 {
		table = schema + "." + table
		checkAttach(db.sql, db.dns, map[string]bool{schema[1 : len(schema)-1]: true})
	}
	return db.rawExecContext(context.TODO(), dropColumn(table, column))
}

func renameColumn(table, oldColumnName, newColumnName string) string {
	return fmt.Sprintf("ALTER TABLE %v RENAME COLUMN %v TO %v;\n", table, oldColumnName, newColumnName)
}

func dropColumn(table, columnName string) string {
	return fmt.Sprintf("ALTER TABLE %v DROP COLUMN %v;\n", table, columnName)
}

func createTableSql(create, pks string, attributes []string, sql *strings.Builder) {
	sql.WriteString(create)
	for _, a := range attributes {
		sql.WriteString(a)
	}
	sql.WriteString(pks)
	sql.WriteString(");\n")
}

type dbColumn struct {
	columnName   string
	dataType     string
	defaultValue *string
	nullable     bool
	migrated     bool
}

type dbTable struct {
	columns map[string]*dbColumn
}

func checkTableChanges(b body) error {
	var sqlTableInfos string
	if b.table.Schema != nil {
		sqlTableInfos = fmt.Sprintf(`SELECT
		name AS column_name,
		lower(type) AS data_type,
		dflt_value AS column_default,
		NOT "notnull" AS is_nullable
		FROM %v.pragma_table_info($1);
		`, *b.table.Schema)
	} else {
		sqlTableInfos = `SELECT
		name AS column_name,
		lower(type) AS data_type,
		dflt_value AS column_default,
		NOT "notnull" AS is_nullable
		FROM pragma_table_info($1);
		`
	}

	rows, err := b.conn.QueryContext(context.Background(), sqlTableInfos, b.table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	dts := make(map[string]*dbColumn)
	dt := dbColumn{}
	for rows.Next() {
		err = rows.Scan(&dt.columnName, &dt.dataType, &dt.defaultValue, &dt.nullable)
		if err != nil {
			return err
		}

		dts[dt.columnName] = &dbColumn{
			columnName:   dt.columnName,
			dataType:     dt.dataType,
			defaultValue: dt.defaultValue,
			nullable:     dt.nullable,
		}
	}
	if len(dts) == 0 {
		return nil
	}
	b.dbTable = dbTable{columns: dts}
	b.table.Migrated = true
	checkFields(b)
	return nil
}

func primaryKeyIsForeignKey(table *model.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.ManyToOnes, func(m model.ManyToOneMigrate) bool {
		return m.Name == attName
	}) || slices.ContainsFunc(table.OneToOnes, func(o model.OneToOneMigrate) bool {
		return o.Name == attName
	})
}

func foreignKeyIsPrimarykey(table *model.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.PrimaryKeys, func(pk model.PrimaryKeyMigrate) bool {
		return pk.Name == attName
	})
}

func createTable(tbl *model.TableMigrate, dataMap map[string]*dataType, sql *strings.Builder, tables map[string]*model.TableMigrate, skipDependency bool) {
	t := table{}
	t.name = fmt.Sprintf("CREATE TABLE %v (", tbl.EscapingTableName())
	for _, att := range tbl.PrimaryKeys {
		if primaryKeyIsForeignKey(tbl, att.Name) {
			continue
		}
		att.DataType = checkDataType(att.DataType, dataMap).typeName
		if att.AutoIncrement {
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL,", att.EscapingName, att.DataType))
		} else {
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL %v,", att.EscapingName, att.DataType, setDefault(att.Default)))
		}
	}

	for _, att := range tbl.Attributes {
		att.DataType = checkDataType(att.DataType, dataMap).typeName
		t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v %v,", att.EscapingName, att.DataType, func() string {
			if att.Nullable {
				return "NULL"
			} else {
				return "NOT NULL"
			}
		}(), setDefault(att.Default)))
	}

	for _, att := range tbl.OneToOnes {
		tb := tables[att.TargetTable]
		if tb.Migrated {
			t.createAttrs = append(t.createAttrs, foreingOneToOne(att, dataMap))
		} else {
			if tb != tbl && !skipDependency {
				createTable(tb, dataMap, sql, tables, false)
			}
			t.createAttrs = append(t.createAttrs, foreingOneToOne(att, dataMap))
		}
	}

	for _, att := range tbl.ManyToOnes {
		tb := tables[att.TargetTable]
		if tb.Migrated {
			t.createAttrs = append(t.createAttrs, foreingManyToOne(att, dataMap))
		} else {
			if tb != tbl && !skipDependency {
				createTable(tb, dataMap, sql, tables, false)
			}
			t.createAttrs = append(t.createAttrs, foreingManyToOne(att, dataMap))
		}
	}

	tbl.Migrated = true
	t.createPk = fmt.Sprintf("primary key (%v", tbl.PrimaryKeys[0].EscapingName)
	for _, pk := range tbl.PrimaryKeys[1:] {
		t.createPk += fmt.Sprintf(",%v", pk.EscapingName)
	}
	t.createPk += ")"
	createTableSql(t.name, t.createPk, t.createAttrs, sql)
}

func setDefault(d string) string {
	if d == "" {
		return ""
	}

	return fmt.Sprintf("DEFAULT %v", d)
}

func foreingManyToOne(att model.ManyToOneMigrate, dataMap map[string]*dataType) string {
	att.DataType = checkDataType(att.DataType, dataMap).typeName
	return fmt.Sprintf("%v %v %v REFERENCES %v(%v),", att.EscapingName, att.DataType, func() string {
		if att.Nullable {
			return "NULL"
		}
		return "NOT NULL"
	}(), att.EscapingTargetTable, att.EscapingTargetColumn)
}

func foreingOneToOne(att model.OneToOneMigrate, dataMap map[string]*dataType) string {
	att.DataType = checkDataType(att.DataType, dataMap).typeName
	return fmt.Sprintf("%v %v UNIQUE %v REFERENCES %v(%v),",
		att.EscapingName,
		att.DataType,
		func() string {
			if att.Nullable {
				return "NULL"
			}
			return "NOT NULL"
		}(), att.EscapingTargetTable, att.EscapingTargetColumn)
}

type table struct {
	name        string
	createPk    string
	createAttrs []string
}

type databaseIndex struct {
	indexName string
	unique    bool
	attname   string
	table     string
	migrated  bool
}

func checkIndex(indexes []model.IndexMigrate, table *model.TableMigrate, sql *strings.Builder, conn *sql.DB) error {

	var schema string
	if table.Schema != nil {
		schema = *table.Schema + "."
	}
	sqlQuery := fmt.Sprintf(`
	WITH index_list AS (
		SELECT
			name AS index_name,
			[unique] AS is_unique,
			origin,
			partial
		FROM %vpragma_index_list($1)
		WHERE origin != 'pk'  -- exclude primary key
	),
	index_columns AS (
		SELECT
			il.index_name,
			ii.name AS column_name,
			ii.seqno
		FROM index_list il
		JOIN %vpragma_index_info(il.index_name) ii
	)
	SELECT DISTINCT
		il.index_name,
		il.is_unique,
		$1 AS table_name,
		ic.column_name
	FROM index_list il
	JOIN index_columns ic ON il.index_name = ic.index_name;
	`, schema, schema)

	rows, err := conn.QueryContext(context.Background(), sqlQuery, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	dis := make(map[string]*databaseIndex)
	di := databaseIndex{}
	for rows.Next() {
		err = rows.Scan(&di.indexName, &di.unique, &di.table, &di.attname)
		if err != nil {
			return err
		}
		dis[di.indexName] = &databaseIndex{
			indexName: di.indexName,
			unique:    di.unique,
			attname:   di.attname,
			table:     di.table,
		}
	}

	for i := range indexes {
		if dbIndex, exist := dis[indexes[i].Name]; exist {
			if indexes[i].Unique != dbIndex.unique {
				if table.Schema != nil {
					sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", *table.Schema+"."+indexes[i].EscapingName) + "\n")
				} else {
					sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", indexes[i].EscapingName) + "\n")
				}
				sql.WriteString(createIndex(indexes[i], table))
			}
			dbIndex.migrated = true
			continue
		}
		sql.WriteString(createIndex(indexes[i], table))
	}

	for _, dbIndex := range dis {
		if !dbIndex.migrated {
			if !slices.ContainsFunc(table.OneToOnes, func(o model.OneToOneMigrate) bool {
				return o.Name == dbIndex.attname
			}) {
				sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", keywordHandler(dbIndex.indexName)) + "\n")
			}
		}
	}
	return nil
}

func createIndex(index model.IndexMigrate, table *model.TableMigrate) string {
	return fmt.Sprintf("CREATE %v %v ON %v (%v);\n",
		func() string {
			if index.Unique {
				return "UNIQUE INDEX"
			}
			return "INDEX"
		}(),
		func() string {
			if table.Schema != nil {
				return *table.Schema + "." + index.EscapingName
			}
			return index.EscapingName
		}(),
		table.EscapingName,
		func() string {
			s := fmt.Sprintf("%v", index.Attributes[0].EscapingName)
			for _, a := range index.Attributes[1:] {
				s += fmt.Sprintf(",%v", a.EscapingName)
			}
			return s
		}(),
	)
}

func checkFields(b body) {
	var alter bool
	for _, att := range b.table.PrimaryKeys {
		if column := b.dbTable.columns[att.Name]; column != nil {
			column.migrated = true
			if primaryKeyIsForeignKey(b.table, att.Name) {
				continue
			}

			dataType := checkDataType(att.DataType, b.dataMap).typeName
			if column.dataType != dataType {
				alter = true
				break
			}
			if !att.AutoIncrement && column.defaultValue != nil {
				if att.Default == "" {
					// drop default
					alter = true
					break
				}
				if *column.defaultValue != att.Default {
					// update default
					alter = true
					break
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				// create default
				alter = true
				break
			}
		}
	}

	for _, att := range b.table.OneToOnes {
		if column, exist := b.dbTable.columns[att.Name]; exist {
			column.migrated = true
			// change from many to one to one to one
			if unique := checkFkUnique(b.conn, b.table.Name, att.Name); !unique {
				if foreignKeyIsPrimarykey(b.table, att.Name) {
					continue
				}
				alter = true
				break
			}
			if column.nullable != att.Nullable {
				alter = true
				break
			}
			continue
		}
		alter = true
		break
	}

	for _, att := range b.table.ManyToOnes {
		if column, exist := b.dbTable.columns[att.Name]; exist {
			column.migrated = true
			// change from one to one to many to one
			if unique := checkFkUnique(b.conn, b.table.Name, att.Name); unique {
				alter = true
				break
			}
			if column.nullable != att.Nullable {
				alter = true
				break
			}
			continue
		}
		alter = true
		break
	}

	var newColumns []string
	for _, att := range b.table.Attributes {
		if column, exist := b.dbTable.columns[att.Name]; exist {
			column.migrated = true
			dataType := checkDataType(att.DataType, b.dataMap).typeName
			if column.dataType != dataType {
				alter = true
			}
			if column.nullable != att.Nullable {
				alter = true
			}
			if column.defaultValue != nil {
				if att.Default == "" {
					// drop default
					alter = true
				}
				if *column.defaultValue != setDefault(att.Default)[8:] {
					// update default
					alter = true
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				// create default
				alter = true
			}
			continue
		}
		newColumns = append(newColumns, addColumn(b.table, att.EscapingName, checkDataType(att.DataType, b.dataMap), att.Nullable))
		alter = true
	}

	for _, c := range b.dbTable.columns {
		if !c.migrated {
			alter = true
			break
		}
	}

	for _, c := range newColumns {
		b.sql.WriteString(c)
	}

	if alter {
		alterSqlite(b)
	}

}

func alterSqlite(b body) {
	newTable := *b.table
	newTable.Name = "new_" + newTable.Name
	newTable.EscapingName = keywordHandler(newTable.Name)
	sqlBuilder := &strings.Builder{}

	insertColumns, selectColumns := tableAttributes(b.table)
	sqlBuilder.WriteString("BEGIN TRANSACTION; PRAGMA foreign_keys=OFF; \n")
	createTable(&newTable, b.dataMap, sqlBuilder, b.tables, true)
	sqlBuilder.WriteString(
		fmt.Sprintf("INSERT INTO %v (%v) SELECT %v FROM %v;\n",
			newTable.EscapingTableName(),
			insertColumns,
			selectColumns,
			b.table.EscapingTableName()))
	sqlBuilder.WriteString("DROP TABLE" + b.table.EscapingTableName() + ";\n")
	sqlBuilder.WriteString(fmt.Sprintf("ALTER TABLE %v RENAME TO %v;\n", newTable.EscapingTableName(), b.table.EscapingName))
	sqlBuilder.WriteString("PRAGMA foreign_keys=ON; COMMIT;")

	b.sql.WriteString(sqlBuilder.String())
}

func tableAttributes(t *model.TableMigrate) (string, string) {
	sql := strings.Builder{}
	sql.WriteString(t.PrimaryKeys[0].EscapingName)
	for _, p := range t.PrimaryKeys[1:] {
		sql.WriteString("," + p.EscapingName)
	}
	for _, a := range t.Attributes {
		sql.WriteString("," + a.EscapingName)
	}
	for _, a := range t.OneToOnes {
		sql.WriteString("," + a.EscapingName)
	}
	for _, a := range t.ManyToOnes {
		sql.WriteString("," + a.EscapingName)
	}
	newColumns := sql.String()

	return newColumns, newColumns
}

func checkFkUnique(conn *sql.DB, table, attribute string) bool {
	sql := `
	WITH index_list AS (
		SELECT
			name AS index_name,
			[unique] AS is_unique,
			origin,
			partial
		FROM pragma_index_list($1)
		WHERE origin != 'pk'  -- exclude primary key
	),
	index_columns AS (
		SELECT
			il.index_name,
			ii.name AS column_name,
			ii.seqno
		FROM index_list il
		JOIN pragma_index_info(il.index_name) ii
		WHERE ii.name = $2
	)
	SELECT DISTINCT
		il.is_unique
	FROM index_list il
	JOIN index_columns ic ON il.index_name = ic.index_name;`

	var b bool
	row := conn.QueryRowContext(context.Background(), sql, table, attribute)
	row.Scan(&b)
	return b
}

func addColumn(table *model.TableMigrate, column string, dataType dataType, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NULL;\n", table.EscapingTableName(), column, dataType.typeName)
	}
	return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NOT NULL DEFAULT %v;\n", table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
}

type dataType struct {
	typeName  string
	zeroValue string
}

func checkDataType(structDataType string, dataMap map[string]*dataType) dataType {
	dt := dataType{typeName: structDataType}
	switch structDataType {
	case "int8", "uint8", "uint16":
		dt = dataType{"int16", "0"}
	case "int", "uint", "uint32":
		dt = dataType{"int32", "0"}
	case "uint64":
		dt = dataType{"int64", "0"}
	}

	if dataMap[dt.typeName] != nil {
		return *dataMap[dt.typeName]
	}

	for _, s := range []string{"number", "numeric", "decimal"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0"}
		}
	}

	for _, s := range []string{"date", "time"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0000-01-01"}
		}
	}

	for _, s := range []string{"char", "varchar", "text"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "''"}
		}
	}

	return dt
}
