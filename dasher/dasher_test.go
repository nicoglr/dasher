package dasher_test

import (
	"errors"
	"testing"

	"4gclinical.com/dasher"
)

func TestPoisonRoundTrip(t *testing.T) {
	inner := errors.New("bad shape")
	p := dasher.Poison(inner)
	if !dasher.IsPoison(p) {
		t.Fatal("expected IsPoison true")
	}
	if !errors.Is(p, inner) {
		t.Error("Poison should wrap the inner error")
	}
	if dasher.IsPoison(errors.New("plain")) {
		t.Error("plain error must not be poison")
	}
	if dasher.Poison(nil) != nil {
		t.Error("Poison(nil) should be nil")
	}
}

func TestFailLoudReturnsErr(t *testing.T) {
	err := errors.New("boom")
	if got := (dasher.FailLoud{}).OnFatal("s", err); got != err {
		t.Errorf("FailLoud.OnFatal should return the error, got %v", got)
	}
}
