// Package jsonequal compares JSON values without losing numeric precision.
package jsonequal

import (
	"bytes"
	"encoding/json"
	"io"
	"math/big"
	"strings"
)

type kind uint8

const (
	kindNull kind = iota
	kindBool
	kindString
	kindNumber
	kindArray
	kindObject
)

type value struct {
	kind    kind
	boolean bool
	text    string
	number  decimal
	array   []value
	object  map[string]value
}

type decimal struct {
	negative bool
	digits   string
	scale    big.Int
}

// Equal reports whether left and right are valid, unambiguous JSON documents
// with the same semantic value. Duplicate object keys and invalid JSON compare
// unequal, including when the input bytes are identical.
func Equal(left, right json.RawMessage) bool {
	left = bytes.TrimSpace(left)
	right = bytes.TrimSpace(right)
	if len(left) == 0 || len(right) == 0 {
		return len(left) == 0 && len(right) == 0
	}
	leftValue, ok := parse(left)
	if !ok {
		return false
	}
	rightValue, ok := parse(right)
	return ok && equalValue(leftValue, rightValue)
}

func parse(raw []byte) (value, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	parsed, ok := parseValue(decoder)
	if !ok {
		return value{}, false
	}
	_, err := decoder.Token()
	return parsed, err == io.EOF
}

func parseValue(decoder *json.Decoder) (value, bool) {
	token, err := decoder.Token()
	if err != nil {
		return value{}, false
	}
	switch token := token.(type) {
	case nil:
		return value{kind: kindNull}, true
	case bool:
		return value{kind: kindBool, boolean: token}, true
	case string:
		return value{kind: kindString, text: token}, true
	case json.Number:
		number, ok := parseDecimal(string(token))
		return value{kind: kindNumber, number: number}, ok
	case json.Delim:
		switch token {
		case '[':
			items := make([]value, 0)
			for decoder.More() {
				item, ok := parseValue(decoder)
				if !ok {
					return value{}, false
				}
				items = append(items, item)
			}
			end, err := decoder.Token()
			return value{kind: kindArray, array: items}, err == nil && end == json.Delim(']')
		case '{':
			items := make(map[string]value)
			for decoder.More() {
				keyToken, err := decoder.Token()
				key, ok := keyToken.(string)
				if err != nil || !ok {
					return value{}, false
				}
				if _, duplicate := items[key]; duplicate {
					return value{}, false
				}
				item, ok := parseValue(decoder)
				if !ok {
					return value{}, false
				}
				items[key] = item
			}
			end, err := decoder.Token()
			return value{kind: kindObject, object: items}, err == nil && end == json.Delim('}')
		}
	}
	return value{}, false
}

func parseDecimal(raw string) (decimal, bool) {
	negative := strings.HasPrefix(raw, "-")
	if negative {
		raw = raw[1:]
	}
	exponent := "0"
	if index := strings.IndexAny(raw, "eE"); index >= 0 {
		exponent = raw[index+1:]
		raw = raw[:index]
	}
	fractionDigits := 0
	if index := strings.IndexByte(raw, '.'); index >= 0 {
		fractionDigits = len(raw) - index - 1
		raw = raw[:index] + raw[index+1:]
	}
	digits := strings.TrimLeft(raw, "0")
	if digits == "" {
		return decimal{digits: "0"}, true
	}

	trailing := len(digits) - len(strings.TrimRight(digits, "0"))
	digits = digits[:len(digits)-trailing]
	var scale big.Int
	if _, ok := scale.SetString(exponent, 10); !ok {
		return decimal{}, false
	}
	scale.Sub(&scale, big.NewInt(int64(fractionDigits)))
	scale.Add(&scale, big.NewInt(int64(trailing)))
	return decimal{negative: negative, digits: digits, scale: scale}, true
}

func equalValue(left, right value) bool {
	if left.kind != right.kind {
		return false
	}
	switch left.kind {
	case kindNull:
		return true
	case kindBool:
		return left.boolean == right.boolean
	case kindString:
		return left.text == right.text
	case kindNumber:
		return left.number.negative == right.number.negative && left.number.digits == right.number.digits && left.number.scale.Cmp(&right.number.scale) == 0
	case kindArray:
		if len(left.array) != len(right.array) {
			return false
		}
		for index := range left.array {
			if !equalValue(left.array[index], right.array[index]) {
				return false
			}
		}
		return true
	case kindObject:
		if len(left.object) != len(right.object) {
			return false
		}
		for key, leftValue := range left.object {
			rightValue, ok := right.object[key]
			if !ok || !equalValue(leftValue, rightValue) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
