package money

import (
	"errors"
	"math"
	"testing"
)

func TestParseValid(t *testing.T) {
	cases := map[string]int64{"0": 0, "0.00": 0, "25": 2500, "25.5": 2550, "25.50": 2550, "1000.01": 100001, "007.10": 710}
	for in, want := range cases {
		m, err := Parse(in, "BRL")
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if m.Minor() != want || m.Currency() != BRL || !m.IsValid() {
			t.Fatalf("%s: got %d", in, m.Minor())
		}
	}
	if m, _ := Parse("25.5", "BRL"); m.Amount() != "25.50" {
		t.Fatalf("normalization: %s", m.Amount())
	}
}

func TestParseInvalid(t *testing.T) {
	for _, in := range []string{"", " ", "NaN", "Infinity", "-Infinity", "1e3", "1E3", "25.000", "25.", ".5", "+25", "25,00", "0x10", "1 0", "١٢"} {
		if _, err := Parse(in, "BRL"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("%q: expected ErrInvalidAmount, got %v", in, err)
		}
	}
	if _, err := Parse("-1.00", "BRL"); !errors.Is(err, ErrNegativeAmount) {
		t.Errorf("negative: %v", err)
	}
	for _, cur := range []string{"", "brl", "BR", "BRLL", "B1L"} {
		if _, err := Parse("1.00", cur); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("currency %q: %v", cur, err)
		}
	}
}

func TestParseOverflow(t *testing.T) {
	if m, err := Parse("92233720368547758.07", "BRL"); err != nil || m.Minor() != math.MaxInt64 {
		t.Fatalf("max: %v %v", m, err)
	}
	for _, in := range []string{"92233720368547758.08", "92233720368547759.00", "100000000000000000.00", "999999999999999999999"} {
		if _, err := Parse(in, "BRL"); !errors.Is(err, ErrOverflow) {
			t.Errorf("%s: expected overflow, got %v", in, err)
		}
	}
}

func TestArithmetic(t *testing.T) {
	a := MustFromMinor(10000, BRL)
	b := MustFromMinor(8000, BRL)
	sum, _ := a.Add(b)
	if sum.Amount() != "180.00" {
		t.Fatal(sum.Amount())
	}
	diff, _ := b.Sub(a)
	if diff.Amount() != "-20.00" || !diff.IsNegative() {
		t.Fatal(diff.Amount())
	}
	neg, _ := diff.Negate()
	if neg.Amount() != "20.00" {
		t.Fatal(neg.Amount())
	}
	if c, _ := a.Compare(b); c != 1 {
		t.Fatal(c)
	}
	if c, _ := b.Compare(a); c != -1 {
		t.Fatal(c)
	}
	if c, _ := a.Compare(a); c != 0 || !a.Equal(a) {
		t.Fatal("equality")
	}
	z, _ := Zero(BRL)
	if !z.IsZero() || z.Amount() != "0.00" {
		t.Fatal("zero")
	}
}

func TestArithmeticOverflow(t *testing.T) {
	max := MustFromMinor(math.MaxInt64, BRL)
	min := MustFromMinor(math.MinInt64, BRL)
	one := MustFromMinor(1, BRL)
	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Error("add overflow")
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Error("sub overflow")
	}
	if _, err := min.Negate(); !errors.Is(err, ErrOverflow) {
		t.Error("negate overflow")
	}
	if _, err := max.Sub(min); !errors.Is(err, ErrOverflow) {
		t.Error("sub negative overflow")
	}
	if min.Amount() != "-92233720368547758.08" {
		t.Error(min.Amount())
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := MustFromMinor(100, BRL)
	usd := MustFromMinor(100, "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Error("add")
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Error("sub")
	}
	if _, err := brl.Compare(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Error("compare")
	}
	if brl.Equal(usd) || brl.SameCurrency(usd) {
		t.Error("equal")
	}
}

func TestUninitialized(t *testing.T) {
	var z Money
	ok := MustFromMinor(1, BRL)
	if z.IsValid() {
		t.Fatal("zero value must be invalid")
	}
	if _, err := z.Add(ok); !errors.Is(err, ErrUninitialized) {
		t.Error("add")
	}
	if _, err := ok.Sub(z); !errors.Is(err, ErrUninitialized) {
		t.Error("sub")
	}
	if _, err := z.Negate(); !errors.Is(err, ErrUninitialized) {
		t.Error("negate")
	}
	if z.Equal(z) {
		t.Error("equal")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	m, err := FromJSON(JSON{Amount: "25.00", Currency: "BRL"})
	if err != nil {
		t.Fatal(err)
	}
	if j := m.ToJSON(); j.Amount != "25.00" || j.Currency != "BRL" {
		t.Fatal(j)
	}
	s, err := ParseSigned("-3.25", "BRL")
	if err != nil || s.Minor() != -325 {
		t.Fatal(s, err)
	}
	if _, err := FromJSON(JSON{Amount: "-3.25", Currency: "BRL"}); !errors.Is(err, ErrNegativeAmount) {
		t.Fatal("external negative must be rejected")
	}
}
