package sqlite

import (
	"fmt"
	"log"
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

func buildSql(query *model.Query, logQuery bool) string {
	var sql string

	switch query.Type {
	case enum.SelectQuery:
		sql = buildSelect(query)
	case enum.InsertQuery:
		sql = buildInsert(query)
	case enum.UpdateQuery:
		sql = buildUpdate(query)
	case enum.DeleteQuery:
		sql = buildDelete(query)
	case enum.RawQuery:
		sql = query.RawSql
	}

	if logQuery {
		log.Println("\n" + sql)
	}

	return sql
}

func buildSelect(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("SELECT")

	builder.WriteString(writeAttributes(query.Attributes[0]))
	for _, a := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(writeAttributes(a))
	}

	builder.WriteString("FROM")
	builder.WriteString(query.Tables[0])
	for _, t := range query.Tables[1:] {
		builder.WriteByte(',')
		builder.WriteString(t)
	}

	for _, j := range query.Joins {
		builder.WriteByte('\n')
		builder.WriteString(fmt.Sprintf("%v %v on (%v = %v)",
			joins[j.JoinOperation],
			j.Table,
			(j.FirstArgument.Table + "." + j.FirstArgument.Name),
			(j.SecondArgument.Table + "." + j.SecondArgument.Name),
		))
	}

	writeWhere(query, &builder)

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
		builder.WriteString(fmt.Sprintf("LIMIT %v", query.Limit))
	}
	if query.Offset != 0 {
		builder.WriteByte('\n')
		builder.WriteString(fmt.Sprintf("OFFSET %v", query.Offset))
	}

	return builder.String()
}

func buildInsert(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("INSERT INTO")
	builder.WriteString(query.Tables[0])
	builder.WriteByte('(')

	builder.WriteString(query.Attributes[0].Name)
	for _, att := range query.Attributes[1:] {
		builder.WriteByte(',')
		builder.WriteString(att.Name)
	}
	builder.WriteString(")VALUES(")

	i := 1
	builder.WriteString(fmt.Sprintf("$%v", i))

	for range query.SizeArguments - 1 {
		i++
		builder.WriteByte(',')
		builder.WriteString(fmt.Sprintf("$%v", i))
	}
	builder.WriteByte(')')

	for range query.BatchSizeQuery - 1 {
		i++
		builder.WriteString(",(")
		builder.WriteString(fmt.Sprintf("$%v", i))

		for range query.SizeArguments - 1 {
			i++
			builder.WriteByte(',')
			builder.WriteString(fmt.Sprintf("$%v", i))
		}
		builder.WriteByte(')')
	}

	if query.ReturningId != nil {
		builder.WriteString("RETURNING")
		builder.WriteString(query.ReturningId.Name)
	}

	return builder.String()
}

func buildUpdate(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("UPDATE")
	builder.WriteString(query.Tables[0])
	builder.WriteString("SET")

	i := 1
	builder.WriteString(query.Attributes[0].Name)
	builder.WriteByte('=')
	builder.WriteString(fmt.Sprintf("$%v", i))
	for _, att := range query.Attributes[1:] {
		i++
		builder.WriteByte(',')
		builder.WriteString(att.Name)
		builder.WriteByte('=')
		builder.WriteString(fmt.Sprintf("$%v", i))
	}

	writeWhere(query, &builder)

	return builder.String()
}

func buildDelete(query *model.Query) string {
	builder := strings.Builder{}

	builder.WriteString("DELETE FROM")
	builder.WriteString(query.Tables[0])
	writeWhere(query, &builder)

	return builder.String()
}

func writeAttributes(a model.Attribute) string {
	if a.FunctionType != 0 {
		return fmt.Sprintf(" %v(%v)", functions[a.FunctionType], a.Table+"."+a.Name)
	}

	if a.AggregateType != 0 {
		return fmt.Sprintf(" %v(%v)", aggregates[a.AggregateType], a.Table+"."+a.Name)
	}

	return a.Table + "." + a.Name
}

func writeWhere(query *model.Query, builder *strings.Builder) {
	if query.WhereOperations != nil {
		builder.WriteByte('\n')
		builder.WriteString("WHERE")

		for _, w := range query.WhereOperations {
			switch w.Type {
			case enum.OperationWhere:
				builder.WriteString(fmt.Sprintf("%v %v $%v", writeAttributes(w.Attribute), operators[w.Operator], query.WhereIndex))
				query.WhereIndex++
			case enum.OperationIsWhere:
				builder.WriteString(fmt.Sprintf("%v %v NULL", writeAttributes(w.Attribute), operators[w.Operator]))
			case enum.OperationAttributeWhere:
				builder.WriteString(fmt.Sprintf("%v %v %v", writeAttributes(w.Attribute), operators[w.Operator], writeAttributes(w.AttributeValue)))
			case enum.OperationInWhere:
				if w.QueryIn != nil {
					if w.QueryIn.Arguments != nil {
						w.QueryIn.WhereIndex = query.WhereIndex
						query.Arguments = append(query.Arguments[:w.QueryIn.WhereIndex-1], append(w.QueryIn.Arguments, query.Arguments[w.QueryIn.WhereIndex-1:]...)...)
						builder.WriteString(fmt.Sprintf("%v %v (%v)", writeAttributes(w.Attribute), operators[w.Operator], buildSelect(w.QueryIn)))
						query.WhereIndex = w.QueryIn.WhereIndex
						continue
					}
					builder.WriteString(fmt.Sprintf("%v %v (%v)", writeAttributes(w.Attribute), operators[w.Operator], buildSelect(w.QueryIn)))
					continue
				}
				writeWhereInArgument(&w, builder, query)
			case enum.LogicalWhere:
				builder.WriteString(fmt.Sprintf(" %v ", operators[w.Operator]))
			}
		}
	}
}

func writeWhereInArgument(where *model.Where, builder *strings.Builder, query *model.Query) {
	if where.SizeIn == 0 {
		return
	}
	builder.WriteString(fmt.Sprintf("%v %v ($%v", writeAttributes(where.Attribute), operators[where.Operator], query.WhereIndex))
	query.WhereIndex++
	for range where.SizeIn - 1 {
		builder.WriteString(fmt.Sprintf(",$%v", query.WhereIndex))
		query.WhereIndex++
	}
	builder.WriteByte(')')
}
