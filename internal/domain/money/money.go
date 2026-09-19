// Package money implements an immutable monetary value object with exact
// integer arithmetic. Amounts are stored in minor units (cents) as int64 with a
// fixed scale of two decimal places; floating point is never used.
//
// Representation limits: the largest representable amount is
// 92233720368547758.07 (math.MaxInt64 minor units). Parsing, addition,
// subtraction and negation detect overflow and return ErrOverflow.
package money

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Scale is the fixed number of decimal places used by every amount.
const Scale = 2

const unitsPerMajor int64 = 100

var (
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrNegativeAmount   = errors.New("money: negative amount not allowed")
	ErrOverflow         = errors.New("money: arithmetic overflow")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrUninitialized    = errors.New("money: uninitialized value")
)

// amountPattern accepts a plain decimal with at most two fractional digits.
// Sign is handled separately. Scientific notation, blanks, "NaN", "Infinity"
// and any other form are rejected before this point.
var amountPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]{1,2})?$`)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// Currency is an ISO 4217 alphabetic code.
type Currency string

const BRL Currency = "BRL"

// ParseCurrency validates a currency code.
func ParseCurrency(code string) (Currency, error) {
	if !currencyPattern.MatchString(code) {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return Currency(code), nil
}

func (c Currency) String() string { return string(c) }

// Money is an immutable amount in minor units bound to a currency.
// The zero value is uninitialized and rejected by every operation.
type Money struct {
	minor    int64
	currency Currency
	valid    bool
}

// FromMinor builds a Money from minor units. Negative values are allowed here
// because internal calculations (differences) may be negative.
func FromMinor(minor int64, currency Currency) (Money, error) {
	if _, err := ParseCurrency(string(currency)); err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: currency, valid: true}, nil
}

// MustFromMinor is FromMinor for constants in tests and internal code.
func MustFromMinor(minor int64, currency Currency) Money {
	m, err := FromMinor(minor, currency)
	if err != nil {
		panic(err)
	}
	return m
}

// Zero returns the zero amount for a currency.
func Zero(currency Currency) (Money, error) { return FromMinor(0, currency) }

// Parse parses a non-negative decimal string such as "25.00" into Money.
// Accepted forms: digits with an optional fraction of one or two digits
// ("25", "25.5", "25.00"). Normalization for hashing is always the two-digit
// form produced by Amount(). Everything else is rejected without rounding.
func Parse(amount string, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur, valid: true}, nil
}

// ParseSigned parses a decimal string that may carry a leading '-'. It is
// intended for internal round trips (e.g. reconciliation differences), never
// for external financial inputs.
func ParseSigned(amount string, currency string) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		m, err := Parse(amount[1:], currency)
		if err != nil {
			return Money{}, err
		}
		return m.Negate()
	}
	return Parse(amount, currency)
}

func parseMinor(amount string) (int64, error) {
	if amount == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	if strings.HasPrefix(amount, "-") && amountPattern.MatchString(amount[1:]) {
		return 0, fmt.Errorf("%w: %q", ErrNegativeAmount, amount)
	}
	if !amountPattern.MatchString(amount) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	whole, frac, _ := strings.Cut(amount, ".")
	for len(frac) < Scale {
		frac += "0"
	}
	// Bound the integer part before conversion so ParseInt never sees a value
	// we cannot multiply safely.
	trimmed := strings.TrimLeft(whole, "0")
	if len(trimmed) > 17 {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}
	major, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	cents, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	if major > (math.MaxInt64-cents)/unitsPerMajor {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}
	return major*unitsPerMajor + cents, nil
}

// IsValid reports whether the value was constructed through a constructor.
func (m Money) IsValid() bool { return m.valid }

// Minor returns the amount in minor units.
func (m Money) Minor() int64 { return m.minor }

// Currency returns the currency code.
func (m Money) Currency() Currency { return m.currency }

// Amount renders the decimal string with exactly two fractional digits,
// e.g. "25.00" or "-0.50".
func (m Money) Amount() string {
	v := m.minor
	neg := v < 0
	var major, cents uint64
	if neg {
		// Avoid overflow on MinInt64 by working in unsigned space.
		u := uint64(-(v + 1)) + 1
		major, cents = u/uint64(unitsPerMajor), u%uint64(unitsPerMajor)
	} else {
		major, cents = uint64(v)/uint64(unitsPerMajor), uint64(v)%uint64(unitsPerMajor)
	}
	s := fmt.Sprintf("%d.%02d", major, cents)
	if neg {
		return "-" + s
	}
	return s
}

func (m Money) String() string { return m.Amount() + " " + string(m.currency) }

func (m Money) IsZero() bool     { return m.minor == 0 }
func (m Money) IsNegative() bool { return m.minor < 0 }
func (m Money) IsPositive() bool { return m.minor > 0 }

func (m Money) check(o Money) error {
	if !m.valid || !o.valid {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency, valid: true}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency, valid: true}, nil
}

// Negate returns -m.
func (m Money) Negate() (Money, error) {
	if !m.valid {
		return Money{}, ErrUninitialized
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency, valid: true}, nil
}

// Compare returns -1, 0 or 1. Currencies must match.
func (m Money) Compare(o Money) (int, error) {
	if err := m.check(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

// Equal reports value and currency equality; uninitialized values are never equal.
func (m Money) Equal(o Money) bool {
	return m.valid && o.valid && m.currency == o.currency && m.minor == o.minor
}

// SameCurrency reports whether both values share a currency.
func (m Money) SameCurrency(o Money) bool { return m.valid && o.valid && m.currency == o.currency }

// JSON is the external representation {"amount":"25.00","currency":"BRL"}.
type JSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// ToJSON serializes the value for transport.
func (m Money) ToJSON() JSON { return JSON{Amount: m.Amount(), Currency: string(m.currency)} }

// FromJSON parses an external (non-negative) representation.
func FromJSON(j JSON) (Money, error) { return Parse(j.Amount, j.Currency) }
