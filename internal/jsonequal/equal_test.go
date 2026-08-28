package jsonequal

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEqual(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		left, right string
		want        bool
	}{
		{name: "empty", want: true},
		{name: "one empty", right: "null", want: false},
		{name: "objects ignore order", left: `{"a":1,"b":[true,null]}`, right: ` { "b" : [true, null], "a": 1.0 } `, want: true},
		{name: "arrays preserve order", left: `[1,2]`, right: `[2,1]`, want: false},
		{name: "large collision", left: `9007199254740992`, right: `9007199254740993`, want: false},
		{name: "large equal", left: `900719925474099300000`, right: `9.007199254740993e20`, want: true},
		{name: "equivalent forms", left: `1`, right: `1.0e0`, want: true},
		{name: "negative zero", left: `-0`, right: `0.0`, want: true},
		{name: "different exponent", left: `1e20`, right: `1e21`, want: false},
		{name: "invalid identical", left: `{`, right: `{`, want: false},
		{name: "trailing token", left: `true false`, right: `true false`, want: false},
		{name: "duplicate identical", left: `{"a":1,"a":1}`, right: `{"a":1,"a":1}`, want: false},
		{name: "nested duplicate", left: `{"a":{"b":1,"b":2}}`, right: `{"a":{"b":2}}`, want: false},
		{name: "huge positive exponent", left: `1e100000000000000000000000000000`, right: `10e99999999999999999999999999999`, want: true},
		{name: "huge negative exponent", left: `1e-100000000000000000000000000000`, right: `10e-100000000000000000000000000001`, want: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := Equal(json.RawMessage(test.left), json.RawMessage(test.right)); got != test.want {
				t.Fatalf("Equal(%q, %q) = %v, want %v", test.left, test.right, got, test.want)
			}
		})
	}
}

func TestEqualLongCoefficient(t *testing.T) {
	t.Parallel()
	coefficient := "1" + strings.Repeat("0", 100_000)
	if !Equal(json.RawMessage(coefficient), json.RawMessage(coefficient+"e0")) {
		t.Fatal("long equal coefficient compared unequal")
	}
}
