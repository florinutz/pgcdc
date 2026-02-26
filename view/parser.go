package view

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // required for TiDB parser value expressions
)

var (
	tumblingRe = regexp.MustCompile(`(?i)\s+TUMBLING\s+WINDOW\s+(\S+)\s*$`)
	slidingRe  = regexp.MustCompile(`(?i)\s+SLIDING\s+WINDOW\s+(\S+)\s+SLIDE\s+(\S+)\s*$`)
	sessionRe  = regexp.MustCompile(`(?i)\s+SESSION\s+WINDOW\s+(\S+)\s*$`)
	latenessRe = regexp.MustCompile(`(?i)\s+ALLOWED\s+LATENESS\s+(\S+)`)
)

// Parse parses a streaming SQL view query into a ViewDef.
// The query must SELECT from pgcdc_events and end with a window clause:
//   - TUMBLING WINDOW <duration>
//   - SLIDING WINDOW <duration> SLIDE <duration>
//   - SESSION WINDOW <duration>
//
// Optionally, ALLOWED LATENESS <duration> can appear before the window clause.
func Parse(name, query string, emit EmitMode, maxGroups int) (*ViewDef, error) {
	if maxGroups <= 0 {
		maxGroups = 100000
	}

	// 1. Extract ALLOWED LATENESS clause (before window clause).
	var allowedLateness time.Duration
	remaining := query
	if m := latenessRe.FindStringSubmatch(remaining); m != nil {
		dur, err := time.ParseDuration(m[1])
		if err != nil {
			return nil, fmt.Errorf("invalid allowed lateness duration %q: %w", m[1], err)
		}
		if dur < 0 {
			return nil, fmt.Errorf("allowed lateness must be non-negative, got %v", dur)
		}
		allowedLateness = dur
		remaining = latenessRe.ReplaceAllString(remaining, "")
	}

	// 2. Extract window clause (not standard SQL).
	winType, windowSize, slideSize, sessionGap, stripped, err := extractWindowClause(remaining)
	if err != nil {
		return nil, err
	}

	// 3. Parse remaining SQL with TiDB parser.
	p := parser.New()
	stmts, _, err := p.Parse(stripped, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse sql: %w", err)
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected exactly one statement, got %d", len(stmts))
	}

	selStmt, ok := stmts[0].(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("expected SELECT statement")
	}

	vd := &ViewDef{
		Name:            name,
		Query:           query,
		Emit:            emit,
		MaxGroups:       maxGroups,
		WindowSize:      windowSize,
		WindowType:      winType,
		SlideSize:       slideSize,
		SessionGap:      sessionGap,
		AllowedLateness: allowedLateness,
	}

	// 3. Validate FROM.
	if selStmt.From == nil || selStmt.From.TableRefs == nil {
		return nil, fmt.Errorf("missing FROM clause")
	}
	tableName := extractTableName(selStmt.From.TableRefs)
	if tableName != "pgcdc_events" {
		return nil, fmt.Errorf("FROM must be pgcdc_events, got %q", tableName)
	}
	vd.FromTable = tableName

	// 4. Extract GROUP BY.
	if selStmt.GroupBy != nil {
		for _, item := range selStmt.GroupBy.Items {
			field := exprToFieldName(item.Expr)
			if field == "" {
				return nil, fmt.Errorf("unsupported GROUP BY expression")
			}
			vd.GroupBy = append(vd.GroupBy, field)
		}
	}

	// 5. Extract SELECT items.
	for _, field := range selStmt.Fields.Fields {
		si, err := parseSelectField(field, vd.GroupBy)
		if err != nil {
			return nil, err
		}
		vd.SelectItems = append(vd.SelectItems, si)
	}

	// 6. Compile WHERE.
	if selStmt.Where != nil {
		pred, err := compileExpr(selStmt.Where)
		if err != nil {
			return nil, fmt.Errorf("compile WHERE: %w", err)
		}
		vd.Where = pred
	}

	// 7. Compile HAVING.
	// Build a mapping from auto-generated aggregate alias to SELECT alias,
	// so HAVING can resolve COUNT(*) to the user's "cnt" alias.
	if selStmt.Having != nil {
		aggAliasMap := make(map[string]string)
		for _, si := range vd.SelectItems {
			if si.Aggregate != nil {
				autoAlias := autoAggAlias(*si.Aggregate, si.Field)
				aggAliasMap[autoAlias] = si.Alias
			}
		}
		pred, err := compileHavingExpr(selStmt.Having.Expr, aggAliasMap)
		if err != nil {
			return nil, fmt.Errorf("compile HAVING: %w", err)
		}
		vd.Having = pred
	}

	return vd, nil
}

// extractWindowClause strips the window clause suffix and returns the window parameters.
func extractWindowClause(query string) (WindowType, time.Duration, time.Duration, time.Duration, string, error) {
	// Try sliding first (more specific pattern matches first to avoid ambiguity).
	if m := slidingRe.FindStringSubmatch(query); m != nil {
		windowSize, err := time.ParseDuration(m[1])
		if err != nil {
			return 0, 0, 0, 0, "", fmt.Errorf("invalid sliding window size %q: %w", m[1], err)
		}
		slideSize, err := time.ParseDuration(m[2])
		if err != nil {
			return 0, 0, 0, 0, "", fmt.Errorf("invalid slide size %q: %w", m[2], err)
		}
		if windowSize <= 0 {
			return 0, 0, 0, 0, "", fmt.Errorf("window size must be positive, got %v", windowSize)
		}
		if slideSize <= 0 {
			return 0, 0, 0, 0, "", fmt.Errorf("slide size must be positive, got %v", slideSize)
		}
		if slideSize > windowSize {
			return 0, 0, 0, 0, "", fmt.Errorf("slide size (%v) must not exceed window size (%v)", slideSize, windowSize)
		}
		stripped := slidingRe.ReplaceAllString(query, "")
		return WindowSliding, windowSize, slideSize, 0, stripped, nil
	}

	// Try session.
	if m := sessionRe.FindStringSubmatch(query); m != nil {
		gap, err := time.ParseDuration(m[1])
		if err != nil {
			return 0, 0, 0, 0, "", fmt.Errorf("invalid session gap %q: %w", m[1], err)
		}
		if gap <= 0 {
			return 0, 0, 0, 0, "", fmt.Errorf("session gap must be positive, got %v", gap)
		}
		stripped := sessionRe.ReplaceAllString(query, "")
		return WindowSession, 0, 0, gap, stripped, nil
	}

	// Try tumbling.
	if m := tumblingRe.FindStringSubmatch(query); m != nil {
		dur, err := time.ParseDuration(m[1])
		if err != nil {
			return 0, 0, 0, 0, "", fmt.Errorf("invalid window duration %q: %w", m[1], err)
		}
		if dur <= 0 {
			return 0, 0, 0, 0, "", fmt.Errorf("window duration must be positive, got %v", dur)
		}
		stripped := tumblingRe.ReplaceAllString(query, "")
		return WindowTumbling, dur, 0, 0, stripped, nil
	}

	return 0, 0, 0, 0, "", fmt.Errorf("missing window clause (TUMBLING WINDOW, SLIDING WINDOW, or SESSION WINDOW)")
}

// extractTableName walks a TableRefsClause to find the table name.
func extractTableName(refs *ast.Join) string {
	if refs.Left != nil {
		if ts, ok := refs.Left.(*ast.TableSource); ok {
			if tn, ok := ts.Source.(*ast.TableName); ok {
				return tn.Name.L
			}
		}
	}
	return ""
}

// exprToFieldName converts an AST expression to a dotted field name.
func exprToFieldName(expr ast.ExprNode) string {
	switch e := expr.(type) {
	case *ast.ColumnNameExpr:
		return columnToField(e.Name)
	default:
		return ""
	}
}

// columnToField converts a ColumnName to a dotted field.
// payload.amount → TiDB parses as table=payload, column=amount → "payload.amount"
// payload.row.region → TiDB parses as schema=payload, table=row, column=region → "payload.row.region"
// bare "channel" → table="", column=channel → "channel"
func columnToField(col *ast.ColumnName) string {
	if col.Schema.L != "" {
		return col.Schema.L + "." + col.Table.L + "." + col.Name.L
	}
	if col.Table.L != "" {
		return col.Table.L + "." + col.Name.L
	}
	return col.Name.L
}

// parseSelectField parses one SELECT field into a SelectItem.
func parseSelectField(field *ast.SelectField, groupBy []string) (SelectItem, error) {
	alias := ""
	if field.AsName.L != "" {
		alias = field.AsName.L
	}

	// Check for aggregate functions.
	if funcCall, ok := field.Expr.(*ast.AggregateFuncExpr); ok {
		aggFunc, err := parseAggFunc(funcCall.F)
		if err != nil {
			return SelectItem{}, err
		}

		// Handle COUNT(DISTINCT x).
		if aggFunc == AggCount && funcCall.Distinct {
			aggFunc = AggCountDistinct
		}

		innerField := ""
		if len(funcCall.Args) > 0 {
			innerField = exprToFieldName(funcCall.Args[0])
		}

		if alias == "" {
			if aggFunc == AggCountDistinct {
				alias = "count_distinct"
			} else {
				alias = strings.ToLower(funcCall.F)
			}
			if innerField != "" {
				alias += "_" + strings.ReplaceAll(innerField, ".", "_")
			}
		}

		return SelectItem{
			Aggregate: &aggFunc,
			Field:     innerField,
			Alias:     alias,
		}, nil
	}

	// Plain column reference (group key or metadata).
	fieldName := exprToFieldName(field.Expr)
	if fieldName == "" {
		return SelectItem{}, fmt.Errorf("unsupported SELECT expression")
	}

	if alias == "" {
		// Use the last part as alias.
		parts := strings.Split(fieldName, ".")
		alias = parts[len(parts)-1]
	}

	isGroupKey := false
	for _, g := range groupBy {
		if g == fieldName {
			isGroupKey = true
			break
		}
	}

	return SelectItem{
		Field:      fieldName,
		Alias:      alias,
		IsGroupKey: isGroupKey,
	}, nil
}

// parseAggFunc maps a function name to an AggFunc.
func parseAggFunc(name string) (AggFunc, error) {
	switch strings.ToUpper(name) {
	case "COUNT":
		return AggCount, nil
	case "SUM":
		return AggSum, nil
	case "AVG":
		return AggAvg, nil
	case "MIN":
		return AggMin, nil
	case "MAX":
		return AggMax, nil
	case "STDDEV", "STDDEV_POP":
		return AggStddev, nil
	default:
		return 0, fmt.Errorf("unsupported aggregate function: %s", name)
	}
}

// compileExpr compiles a TiDB AST expression into a Predicate.
func compileExpr(expr ast.ExprNode) (Predicate, error) {
	switch e := expr.(type) {
	case *ast.BinaryOperationExpr:
		return compileBinaryOp(e)
	case *ast.PatternLikeOrIlikeExpr:
		return compilePatternLike(e)
	case *ast.IsNullExpr:
		return compileIsNull(e)
	case *ast.ParenthesesExpr:
		return compileExpr(e.Expr)
	case *ast.PatternInExpr:
		return compilePatternIn(e)
	case *ast.AggregateFuncExpr:
		// HAVING clause references like COUNT(*) > 5
		// Return a predicate that looks up the aggregate alias in the row map.
		alias := aggregateAlias(e)
		return func(_ EventMeta, row map[string]any) bool {
			_, ok := row[alias]
			return ok
		}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func aggregateAlias(e *ast.AggregateFuncExpr) string {
	innerField := ""
	if len(e.Args) > 0 {
		innerField = exprToFieldName(e.Args[0])
	}
	return autoAggAlias(aggFuncFromName(e.F), innerField)
}

// autoAggAlias generates the canonical auto-alias for an aggregate.
func autoAggAlias(fn AggFunc, field string) string {
	var name string
	switch fn {
	case AggCount:
		name = "count"
	case AggSum:
		name = "sum"
	case AggAvg:
		name = "avg"
	case AggMin:
		name = "min"
	case AggMax:
		name = "max"
	case AggCountDistinct:
		name = "count_distinct"
	case AggStddev:
		name = "stddev"
	}
	if field != "" {
		name += "_" + strings.ReplaceAll(field, ".", "_")
	}
	return name
}

func aggFuncFromName(name string) AggFunc {
	switch strings.ToUpper(name) {
	case "COUNT":
		return AggCount
	case "SUM":
		return AggSum
	case "AVG":
		return AggAvg
	case "MIN":
		return AggMin
	case "MAX":
		return AggMax
	case "STDDEV", "STDDEV_POP":
		return AggStddev
	default:
		return AggCount
	}
}

// compileHavingExpr compiles a HAVING expression, resolving aggregate aliases
// to their SELECT aliases via aggAliasMap.
func compileHavingExpr(expr ast.ExprNode, aggAliasMap map[string]string) (Predicate, error) {
	switch e := expr.(type) {
	case *ast.BinaryOperationExpr:
		return compileHavingBinaryOp(e, aggAliasMap)
	case *ast.ParenthesesExpr:
		return compileHavingExpr(e.Expr, aggAliasMap)
	case *ast.AggregateFuncExpr:
		alias := resolveHavingAlias(e, aggAliasMap)
		return func(_ EventMeta, row map[string]any) bool {
			_, ok := row[alias]
			return ok
		}, nil
	default:
		return compileExpr(expr) // fallback for non-aggregate expressions
	}
}

func compileHavingBinaryOp(e *ast.BinaryOperationExpr, aggAliasMap map[string]string) (Predicate, error) {
	switch e.Op {
	case opcode.LogicAnd:
		left, err := compileHavingExpr(e.L, aggAliasMap)
		if err != nil {
			return nil, err
		}
		right, err := compileHavingExpr(e.R, aggAliasMap)
		if err != nil {
			return nil, err
		}
		return func(m EventMeta, p map[string]any) bool {
			return left(m, p) && right(m, p)
		}, nil
	case opcode.LogicOr:
		left, err := compileHavingExpr(e.L, aggAliasMap)
		if err != nil {
			return nil, err
		}
		right, err := compileHavingExpr(e.R, aggAliasMap)
		if err != nil {
			return nil, err
		}
		return func(m EventMeta, p map[string]any) bool {
			return left(m, p) || right(m, p)
		}, nil
	case opcode.EQ, opcode.NE, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		return compileHavingComparisonWithMap(e, aggAliasMap)
	default:
		return nil, fmt.Errorf("unsupported HAVING binary operator: %s", e.Op)
	}
}

func compileHavingComparisonWithMap(e *ast.BinaryOperationExpr, aggAliasMap map[string]string) (Predicate, error) {
	// Left is aggregate, right is literal.
	if agg, ok := e.L.(*ast.AggregateFuncExpr); ok {
		alias := resolveHavingAlias(agg, aggAliasMap)
		rightVal, err := exprToValue(e.R)
		if err != nil {
			return nil, fmt.Errorf("HAVING comparison: %w", err)
		}
		return makeComparisonPred(alias, e.Op, rightVal), nil
	}
	// Left is literal, right is aggregate.
	if agg, ok := e.R.(*ast.AggregateFuncExpr); ok {
		alias := resolveHavingAlias(agg, aggAliasMap)
		leftVal, err := exprToValue(e.L)
		if err != nil {
			return nil, fmt.Errorf("HAVING comparison: %w", err)
		}
		flipped := flipOp(e.Op)
		return makeComparisonPred(alias, flipped, leftVal), nil
	}
	// Fallback to regular comparison.
	return compileComparison(e)
}

func resolveHavingAlias(e *ast.AggregateFuncExpr, aggAliasMap map[string]string) string {
	auto := aggregateAlias(e)
	if mapped, ok := aggAliasMap[auto]; ok {
		return mapped
	}
	return auto
}

func compileBinaryOp(e *ast.BinaryOperationExpr) (Predicate, error) {
	switch e.Op {
	case opcode.LogicAnd:
		left, err := compileExpr(e.L)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.R)
		if err != nil {
			return nil, err
		}
		return func(m EventMeta, p map[string]any) bool {
			return left(m, p) && right(m, p)
		}, nil

	case opcode.LogicOr:
		left, err := compileExpr(e.L)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.R)
		if err != nil {
			return nil, err
		}
		return func(m EventMeta, p map[string]any) bool {
			return left(m, p) || right(m, p)
		}, nil

	case opcode.EQ, opcode.NE, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		return compileComparison(e)

	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", e.Op)
	}
}

func compileComparison(e *ast.BinaryOperationExpr) (Predicate, error) {
	leftField := exprToFieldName(e.L)
	rightField := exprToFieldName(e.R)

	// Left is field, right is literal.
	if leftField != "" {
		rightVal, err := exprToValue(e.R)
		if err != nil {
			// Right might be an aggregate in HAVING context.
			if _, ok := e.R.(*ast.AggregateFuncExpr); ok {
				return compileHavingComparison(e)
			}
			return nil, fmt.Errorf("comparison right side: %w", err)
		}
		return makeComparisonPred(leftField, e.Op, rightVal), nil
	}

	// Right is field, left is literal.
	if rightField != "" {
		leftVal, err := exprToValue(e.L)
		if err != nil {
			return nil, fmt.Errorf("comparison left side: %w", err)
		}
		// Flip the operator.
		flipped := flipOp(e.Op)
		return makeComparisonPred(rightField, flipped, leftVal), nil
	}

	// Both might be aggregate expressions (HAVING).
	return compileHavingComparison(e)
}

// compileHavingComparison handles HAVING expressions like COUNT(*) > 5.
func compileHavingComparison(e *ast.BinaryOperationExpr) (Predicate, error) {
	// Try: left is aggregate, right is literal.
	if agg, ok := e.L.(*ast.AggregateFuncExpr); ok {
		alias := aggregateAlias(agg)
		rightVal, err := exprToValue(e.R)
		if err != nil {
			return nil, fmt.Errorf("HAVING comparison: %w", err)
		}
		return makeComparisonPred(alias, e.Op, rightVal), nil
	}
	// Try: left is literal, right is aggregate.
	if agg, ok := e.R.(*ast.AggregateFuncExpr); ok {
		alias := aggregateAlias(agg)
		leftVal, err := exprToValue(e.L)
		if err != nil {
			return nil, fmt.Errorf("HAVING comparison: %w", err)
		}
		flipped := flipOp(e.Op)
		return makeComparisonPred(alias, flipped, leftVal), nil
	}
	return nil, fmt.Errorf("unsupported comparison operands")
}

func flipOp(op opcode.Op) opcode.Op {
	switch op {
	case opcode.GT:
		return opcode.LT
	case opcode.GE:
		return opcode.LE
	case opcode.LT:
		return opcode.GT
	case opcode.LE:
		return opcode.GE
	default:
		return op // EQ and NE are symmetric
	}
}

func compilePatternLike(e *ast.PatternLikeOrIlikeExpr) (Predicate, error) {
	field := exprToFieldName(e.Expr)
	if field == "" {
		return nil, fmt.Errorf("LIKE: left side must be a field reference")
	}
	patternVal, err := exprToValue(e.Pattern)
	if err != nil {
		return nil, fmt.Errorf("LIKE pattern: %w", err)
	}
	pattern, ok := patternVal.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE pattern must be a string")
	}

	// Convert SQL LIKE to Go regex.
	re, err := likeToRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("LIKE regex: %w", err)
	}

	not := e.Not
	return func(m EventMeta, p map[string]any) bool {
		val := resolveField(field, m, p)
		s, ok := val.(string)
		if !ok {
			return false
		}
		matched := re.MatchString(s)
		if not {
			return !matched
		}
		return matched
	}, nil
}

func compileIsNull(e *ast.IsNullExpr) (Predicate, error) {
	field := exprToFieldName(e.Expr)
	if field == "" {
		return nil, fmt.Errorf("IS NULL: expression must be a field reference")
	}
	not := e.Not
	return func(m EventMeta, p map[string]any) bool {
		val := resolveField(field, m, p)
		isNull := val == nil
		if not {
			return !isNull
		}
		return isNull
	}, nil
}

func compilePatternIn(e *ast.PatternInExpr) (Predicate, error) {
	field := exprToFieldName(e.Expr)
	if field == "" {
		return nil, fmt.Errorf("IN: left side must be a field reference")
	}
	vals := make([]any, 0, len(e.List))
	for _, item := range e.List {
		v, err := exprToValue(item)
		if err != nil {
			return nil, fmt.Errorf("IN list item: %w", err)
		}
		vals = append(vals, v)
	}
	not := e.Not
	return func(m EventMeta, p map[string]any) bool {
		fv := resolveField(field, m, p)
		found := false
		for _, v := range vals {
			if compareValues(fv, v) == 0 {
				found = true
				break
			}
		}
		if not {
			return !found
		}
		return found
	}, nil
}

// likeToRegex converts a SQL LIKE pattern to a compiled regexp.
func likeToRegex(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteString(regexp.QuoteMeta(string(pattern[i])))
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(pattern[i])))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
