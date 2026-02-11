package engine

import (
	"fmt"
	"math"
	"strings"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

func isScalarFuncName(name string) bool {
	switch name {
	case "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM",
		"LENGTH", "SUBSTR", "SUBSTRING", "CONCAT", "REPLACE",
		"ABS", "ROUND", "CEIL", "FLOOR",
		"COALESCE", "TYPEOF", "IFNULL", "NULLIF",
		"INSTR", "REVERSE", "REPEAT", "HEX":
		return true
	}
	return false
}

func evalScalarFunc(fc *parser.FuncCallExpr, doc *storage.Document) (interface{}, error) {
	args := make([]interface{}, len(fc.Args))
	for i, a := range fc.Args {
		v, err := evalValue(a, doc)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}

	switch fc.Name {
	case "UPPER":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return strings.ToUpper(toString(args[0])), nil

	case "LOWER":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return strings.ToLower(toString(args[0])), nil

	case "TRIM":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return strings.TrimSpace(toString(args[0])), nil

	case "LTRIM":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return strings.TrimLeft(toString(args[0]), " \t\n\r"), nil

	case "RTRIM":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return strings.TrimRight(toString(args[0]), " \t\n\r"), nil

	case "LENGTH":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return int64(len([]rune(toString(args[0])))), nil

	case "SUBSTR", "SUBSTRING":
		return evalSubstr(args)

	case "CONCAT":
		var sb strings.Builder
		for _, a := range args {
			if a != nil {
				sb.WriteString(toString(a))
			}
		}
		return sb.String(), nil

	case "REPLACE":
		if err := checkArgs(fc.Name, args, 3); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2])), nil

	case "INSTR":
		if err := checkArgs(fc.Name, args, 2); err != nil {
			return nil, err
		}
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		idx := strings.Index(toString(args[0]), toString(args[1]))
		if idx < 0 {
			return int64(0), nil
		}
		return int64(idx + 1), nil

	case "REVERSE":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		r := []rune(toString(args[0]))
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return string(r), nil

	case "REPEAT":
		if err := checkArgs(fc.Name, args, 2); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		n, ok := toFloat64(args[1])
		if !ok || n < 0 {
			return "", nil
		}
		return strings.Repeat(toString(args[0]), int(n)), nil

	case "HEX":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		var sb strings.Builder
		for _, b := range []byte(toString(args[0])) {
			fmt.Fprintf(&sb, "%02X", b)
		}
		return sb.String(), nil

	case "ABS":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		f, ok := toFloat64(args[0])
		if !ok {
			return nil, fmt.Errorf("ABS: argument must be numeric")
		}
		r := math.Abs(f)
		if isIntVal(args[0]) {
			return int64(r), nil
		}
		return r, nil

	case "ROUND":
		return evalRound(args)

	case "CEIL":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		f, ok := toFloat64(args[0])
		if !ok {
			return nil, fmt.Errorf("CEIL: argument must be numeric")
		}
		return int64(math.Ceil(f)), nil

	case "FLOOR":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		f, ok := toFloat64(args[0])
		if !ok {
			return nil, fmt.Errorf("FLOOR: argument must be numeric")
		}
		return int64(math.Floor(f)), nil

	case "COALESCE":
		for _, a := range args {
			if a != nil {
				return a, nil
			}
		}
		return nil, nil

	case "IFNULL":
		if err := checkArgs(fc.Name, args, 2); err != nil {
			return nil, err
		}
		if args[0] != nil {
			return args[0], nil
		}
		return args[1], nil

	case "NULLIF":
		if err := checkArgs(fc.Name, args, 2); err != nil {
			return nil, err
		}
		if fmt.Sprintf("%v", args[0]) == fmt.Sprintf("%v", args[1]) {
			return nil, nil
		}
		return args[0], nil

	case "TYPEOF":
		if err := checkArgs(fc.Name, args, 1); err != nil {
			return nil, err
		}
		return typeofVal(args[0]), nil

	default:
		return nil, fmt.Errorf("unknown scalar function: %s", fc.Name)
	}
}

func checkArgs(name string, args []interface{}, expected int) error {
	if len(args) != expected {
		return fmt.Errorf("%s: expected %d argument(s), got %d", name, expected, len(args))
	}
	return nil
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func typeofVal(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case int64, int:
		return "integer"
	case float64:
		return "real"
	case string:
		return "text"
	case bool:
		return "boolean"
	default:
		return "unknown"
	}
}

func evalSubstr(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTR: expected 2 or 3 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s := []rune(toString(args[0]))
	sf, ok := toFloat64(args[1])
	if !ok {
		return nil, fmt.Errorf("SUBSTR: start must be numeric")
	}
	start := int(sf) - 1
	if start < 0 {
		start = 0
	}
	if start >= len(s) {
		return "", nil
	}
	if len(args) == 3 {
		lf, ok := toFloat64(args[2])
		if !ok {
			return nil, fmt.Errorf("SUBSTR: length must be numeric")
		}
		end := start + int(lf)
		if end > len(s) {
			end = len(s)
		}
		if end < start {
			return "", nil
		}
		return string(s[start:end]), nil
	}
	return string(s[start:]), nil
}

func evalRound(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND: expected 1 or 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	f, ok := toFloat64(args[0])
	if !ok {
		return nil, fmt.Errorf("ROUND: argument must be numeric")
	}
	decimals := 0
	if len(args) == 2 {
		d, ok := toFloat64(args[1])
		if !ok {
			return nil, fmt.Errorf("ROUND: decimals must be numeric")
		}
		decimals = int(d)
	}
	pow := math.Pow(10, float64(decimals))
	r := math.Round(f*pow) / pow
	if decimals == 0 {
		return int64(r), nil
	}
	return r, nil
}
