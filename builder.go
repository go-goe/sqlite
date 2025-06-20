package sqlite

import (
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/go-goe/goe/enum"
	"github.com/go-goe/goe/model"
)

var joins = map[enum.JoinType]string{
	enum.Join:      "JOIN",
	enum.LeftJoin:  "LEFT JOIN",
	enum.RightJoin: "RIGHT JOIN",
}

var operators = map[enum.OperatorType]string{
	enum.And:           "AND",
	enum.Or:            "OR",
	enum.Equals:        "=",
	enum.NotEquals:     "<>",
	enum.Greater:       ">",
	enum.GreaterEquals: ">=",
	enum.Less:          "<",
	enum.LessEquals:    "<=",
	enum.Like:          "LIKE",
	enum.NotLike:       "NOT LIKE",
	enum.In:            "IN",
	enum.NotIn:         "NOT IN",
	enum.Is:            "IS",
	enum.IsNot:         "IS NOT",
}

var functions = map[enum.FunctionType]string{
	enum.UpperFunction: "UPPER",
	enum.LowerFunction: "LOWER",
}

var aggregates = map[enum.AggregateType]string{
	enum.CountAggregate: "COUNT",
}

type connection interface {
	Query(string, ...any) (*sql.Rows, error)
	Exec(string, ...any) (sql.Result, error)
}

func buildSql(query *model.Query, pool connection, dns string) {
	schemes := make(map[string]bool, 0)
	switch query.Type {
	case enum.SelectQuery:
		query.RawSql = buildSelect(query, &schemes)
	case enum.InsertQuery:
		query.RawSql = buildInsert(query, &schemes)
	case enum.UpdateQuery:
		query.RawSql = buildUpdate(query, &schemes)
	case enum.DeleteQuery:
		query.RawSql = buildDelete(query, &schemes)
	}

	myQuery := struct {
		Id     int
		Scheme string
		File   string
	}{}
	mySchemes := make([]struct {
		Id     int
		Scheme string
		File   string
	}, 0)
	rows, err := pool.Query("PRAGMA database_list;")
	if err != nil {
		return
	}
	rx := regexp.MustCompile(`[^/]+\.db`)
	currentDb := rx.FindString(dns)

	for rows.Next() {
		rows.Scan(&myQuery.Id, &myQuery.Scheme, &myQuery.File)
		mySchemes = append(mySchemes, myQuery)
	}

	schemeBuilders := strings.Builder{}
	for s := range schemes {
		if !slices.ContainsFunc(mySchemes, func(mq struct {
			Id     int
			Scheme string
			File   string
		}) bool {
			return s[1:len(s)-1] == mq.Scheme
		}) {
			schemeBuilders.WriteString(fmt.Sprintf("ATTACH DATABASE '%v' AS %v;\n", strings.Replace(dns, currentDb, s[1:len(s)-1]+".db", 1), s))
		}
	}

	if schemeBuilders.Len() != 0 {
		pool.Exec(schemeBuilders.String())
	}
}

func buildSelect(query *model.Query, schemes *map[string]bool) string {
	builder := strings.Builder{}

	builder.WriteString("SELECT")

	builder.WriteString(writeAttributes(query.Attributes[0]))
	for _, a := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(writeAttributes(a))
	}

	builder.WriteString("FROM")
	builder.WriteString(query.Tables[0].String())
	if query.Tables[0].Scheme != nil && !(*schemes)[*query.Tables[0].Scheme] {
		(*schemes)[*query.Tables[0].Scheme] = true
	}
	for _, t := range query.Tables[1:] {
		builder.WriteByte(',')
		builder.WriteString(t.String())
		if t.Scheme != nil && !(*schemes)[*t.Scheme] {
			(*schemes)[*t.Scheme] = true
		}
	}

	for _, j := range query.Joins {
		builder.WriteByte('\n')
		builder.WriteString(
			joins[j.JoinOperation] + " " +
				j.Table.String() + " on (" +
				(j.FirstArgument.Table + "." + j.FirstArgument.Name) + " = " +
				(j.SecondArgument.Table + "." + j.SecondArgument.Name) + ")",
		)
		if j.Table.Scheme != nil && !(*schemes)[*j.Table.Scheme] {
			(*schemes)[*j.Table.Scheme] = true
		}
	}

	writeWhere(query, &builder, schemes)

	if query.OrderBy != nil {
		builder.WriteByte('\n')
		if query.OrderBy.Desc {
			builder.WriteString("ORDER BY" + query.OrderBy.Attribute.Table + "." + query.OrderBy.Attribute.Name + "DESC")
		} else {
			builder.WriteString("ORDER BY" + query.OrderBy.Attribute.Table + "." + query.OrderBy.Attribute.Name + "ASC")
		}
	}

	if query.Limit != 0 {
		builder.WriteByte('\n')
		builder.WriteString("LIMIT " + strconv.Itoa(query.Limit))
	}
	if query.Offset != 0 {
		builder.WriteByte('\n')
		builder.WriteString("OFFSET " + strconv.Itoa(query.Offset))
	}

	return builder.String()
}

func buildInsert(query *model.Query, schemes *map[string]bool) string {
	builder := strings.Builder{}

	builder.WriteString("INSERT INTO")
	builder.WriteString(query.Tables[0].String())
	builder.WriteByte('(')
	if query.Tables[0].Scheme != nil && !(*schemes)[*query.Tables[0].Scheme] {
		(*schemes)[*query.Tables[0].Scheme] = true
	}

	builder.WriteString(query.Attributes[0].Name)
	for _, att := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(att.Name)
	}
	builder.WriteString(")VALUES($")

	i := 1
	builder.WriteString(strconv.Itoa(i))

	for range query.SizeArguments - 1 {
		i++
		builder.WriteString(",$" + strconv.Itoa(i))
	}
	builder.WriteByte(')')

	for range query.BatchSizeQuery - 1 {
		i++
		builder.WriteString(",($" + strconv.Itoa(i))

		for range query.SizeArguments - 1 {
			i++
			builder.WriteString(",$" + strconv.Itoa(i))
		}
		builder.WriteByte(')')
	}

	if query.ReturningId != nil {
		builder.WriteString("RETURNING")
		builder.WriteString(query.ReturningId.Name)
	}

	return builder.String()
}

func buildUpdate(query *model.Query, schemes *map[string]bool) string {
	builder := strings.Builder{}

	builder.WriteString("UPDATE")
	builder.WriteString(query.Tables[0].String())
	builder.WriteString("SET")
	if query.Tables[0].Scheme != nil && !(*schemes)[*query.Tables[0].Scheme] {
		(*schemes)[*query.Tables[0].Scheme] = true
	}

	i := 1
	builder.WriteString(query.Attributes[0].Name + "=$" + strconv.Itoa(i))
	for _, att := range query.Attributes[1:] {
		i++
		builder.WriteString("," + att.Name + "=$" + strconv.Itoa(i))
	}

	writeWhere(query, &builder, schemes)

	return builder.String()
}

func buildDelete(query *model.Query, schemes *map[string]bool) string {
	builder := strings.Builder{}

	builder.WriteString("DELETE FROM")
	builder.WriteString(query.Tables[0].String())
	if query.Tables[0].Scheme != nil && !(*schemes)[*query.Tables[0].Scheme] {
		(*schemes)[*query.Tables[0].Scheme] = true
	}
	writeWhere(query, &builder, schemes)

	return builder.String()
}

func writeAttributes(a model.Attribute) string {
	if a.FunctionType != 0 {
		return " " + functions[a.FunctionType] + "(" + a.Table + "." + a.Name + ")"
	}

	if a.AggregateType != 0 {
		return " " + aggregates[a.AggregateType] + "(" + a.Table + "." + a.Name + ")"
	}

	return a.Table + "." + a.Name
}

func writeWhere(query *model.Query, builder *strings.Builder, scheme *map[string]bool) {
	if query.WhereOperations != nil {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")

		for _, w := range query.WhereOperations {
			switch w.Type {
			case enum.OperationWhere:
				builder.WriteString(writeAttributes(w.Attribute) + " " + operators[w.Operator] + " $" + strconv.Itoa(query.WhereIndex))
				query.WhereIndex++
			case enum.OperationIsWhere:
				builder.WriteString(writeAttributes(w.Attribute) + " " + operators[w.Operator] + " NULL")
			case enum.OperationAttributeWhere:
				builder.WriteString(writeAttributes(w.Attribute) + " " + operators[w.Operator] + " " + writeAttributes(w.AttributeValue))
			case enum.OperationInWhere:
				if w.QueryIn != nil {
					if w.QueryIn.Arguments != nil {
						w.QueryIn.WhereIndex = query.WhereIndex
						query.Arguments = append(query.Arguments[:w.QueryIn.WhereIndex-1], append(w.QueryIn.Arguments, query.Arguments[w.QueryIn.WhereIndex-1:]...)...)
						builder.WriteString(writeAttributes(w.Attribute) + " " + operators[w.Operator] + " (" + buildSelect(w.QueryIn, scheme) + ")")
						query.WhereIndex = w.QueryIn.WhereIndex
						continue
					}
					builder.WriteString(writeAttributes(w.Attribute) + " " + operators[w.Operator] + " (" + buildSelect(w.QueryIn, scheme) + ")")
					continue
				}
				writeWhereInArgument(&w, builder, query)
			case enum.LogicalWhere:
				builder.WriteString(" " + operators[w.Operator] + " ")
			}
		}
	}
}

func writeWhereInArgument(where *model.Where, builder *strings.Builder, query *model.Query) {
	if where.SizeIn == 0 {
		return
	}
	builder.WriteString(writeAttributes(where.Attribute) + " " + operators[where.Operator] + " " + "($" + strconv.Itoa(query.WhereIndex))
	query.WhereIndex++
	for range where.SizeIn - 1 {
		builder.WriteString(",$" + strconv.Itoa(query.WhereIndex))
		query.WhereIndex++
	}
	builder.WriteByte(')')
}
