package model

import (
	"testing"
	"time"
)

func newIntervalRecurrence(seconds int) *Recurrence {
	return &Recurrence{
		Cadence: CadenceInterval,
		Seconds: &seconds,
	}
}
func newDailyRecurrence() *Recurrence {
	return &Recurrence{
		Cadence: CadenceDaily,
	}
}
func newWeeklyRecurrence(day Weekday) *Recurrence {
	return &Recurrence{
		Cadence:   CadenceWeekly,
		DayOfWeek: &day,
	}
}
func newMonthlyRecurrence(dayOfMonth int) *Recurrence {
	return &Recurrence{
		Cadence:    CadenceMonthly,
		DayOfMonth: &dayOfMonth,
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromEqualsFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt
	expected := firstRunAt
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromOnLaterSlot(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(2 * 3600 * time.Second)
	expected := from
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromBetweenSlots(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(90 * time.Minute)
	expected := firstRunAt.Add(7200 * time.Second)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromBeforeFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(-3600 * time.Second)
	expected := firstRunAt
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromEqualsFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt
	expected := firstRunAt.Add(3600 * time.Second)
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromOnLaterSlot(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(2 * 3600 * time.Second)
	expected := from.Add(3600 * time.Second)
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromBetweenSlots(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(90 * time.Minute)
	expected := firstRunAt.Add(7200 * time.Second)
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromBeforeFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(-3600 * time.Second)
	expected := firstRunAt
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeLatestRunOnOrBefore_Interval_FromBetweenSlots(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(90 * time.Minute)
	expected := firstRunAt.Add(3600 * time.Second)
	got, err := ComputeLatestRunOnOrBefore(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeLatestRunOnOrBefore_Interval_FromBeforeFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(-3600 * time.Second)
	got, err := ComputeLatestRunOnOrBefore(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", *got)
	}
}

func TestComputeNextRunOnOrAfter_Weekly_FromBetweenSlots(t *testing.T) {
	rec := newWeeklyRecurrence(WeekdayMonday)
	firstRunAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(10 * 24 * time.Hour)
	expected := firstRunAt.Add(14 * 24 * time.Hour)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeLatestRunOnOrBefore_Weekly_FromBetweenSlots(t *testing.T) {
	rec := newWeeklyRecurrence(WeekdayMonday)
	firstRunAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(10 * 24 * time.Hour)
	expected := firstRunAt.Add(7 * 24 * time.Hour)
	got, err := ComputeLatestRunOnOrBefore(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Monthly_HappyPath(t *testing.T) {
	rec := newMonthlyRecurrence(15)
	firstRunAt := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	from := time.Date(2025, 2, 10, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 2, 15, 12, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Monthly_ClampFebruary(t *testing.T) {
	rec := newMonthlyRecurrence(31)
	firstRunAt := time.Date(2025, 1, 31, 12, 0, 0, 0, time.UTC)
	from := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 2, 28, 12, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Monthly_WalkingAnniversary(t *testing.T) {
	rec := newMonthlyRecurrence(31)
	firstRunAt := time.Date(2025, 1, 31, 12, 0, 0, 0, time.UTC)
	from := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 3, 31, 12, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Daily_FromBetweenSlots(t *testing.T) {
	rec := newDailyRecurrence()
	firstRunAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(36 * time.Hour)
	expected := firstRunAt.Add(48 * time.Hour)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestResolveTimezone_RejectsLocal(t *testing.T) {
	if _, err := resolveTimezone("Local"); err == nil {
		t.Fatal(`expected an error for "Local" (FSM determinism hazard), got nil`)
	}
}

func TestResolveTimezone_RejectsInvalidZone(t *testing.T) {
	if _, err := resolveTimezone("Not/A/Real/Zone"); err == nil {
		t.Fatal("expected an error for an unresolvable zone, got nil")
	}
}

func TestResolveTimezone_EmptyAndUTCBothResolveToUTC(t *testing.T) {
	locEmpty, err := resolveTimezone("")
	if err != nil {
		t.Fatalf("empty string: unexpected error: %v", err)
	}
	if locEmpty != time.UTC {
		t.Errorf("empty string: got %v, want time.UTC", locEmpty)
	}
	locUTC, err := resolveTimezone("UTC")
	if err != nil {
		t.Fatalf("UTC: unexpected error: %v", err)
	}
	if locUTC != time.UTC {
		t.Errorf("UTC: got %v, want time.UTC", locUTC)
	}
}

func TestComputeDailyOnOrAfter_NonUTCTimezone_IsDSTAware(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	rec := newDailyRecurrence()
	// 9am America/New_York, expressed as its UTC instant on a winter (EST,
	// UTC-5) date: 9am EST == 14:00 UTC.
	firstRunAt := time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC)

	winterFrom := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "America/New_York", firstRunAt, winterFrom)
	if err != nil {
		t.Fatalf("winter: unexpected error: %v", err)
	}
	if got.In(loc).Hour() != 9 {
		t.Errorf("winter: local hour: got %d, want 9 (got=%v)", got.In(loc).Hour(), got)
	}
	if got.UTC().Hour() != 14 {
		t.Errorf("winter: UTC hour: got %d, want 14 (EST is UTC-5)", got.UTC().Hour())
	}

	// Same wall-clock schedule, but asked about a summer (EDT, UTC-4) date.
	// The UTC hour must shift by one relative to winter even though the
	// local wall-clock time is identical — proof this is real zone-aware
	// DST math, not a fixed offset baked in from firstRunAt's own instant.
	summerFrom := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	got, err = ComputeNextRunOnOrAfter(rec, "America/New_York", firstRunAt, summerFrom)
	if err != nil {
		t.Fatalf("summer: unexpected error: %v", err)
	}
	if got.In(loc).Hour() != 9 {
		t.Errorf("summer: local hour: got %d, want 9 (got=%v)", got.In(loc).Hour(), got)
	}
	if got.UTC().Hour() != 13 {
		t.Errorf("summer: UTC hour: got %d, want 13 (EDT is UTC-4)", got.UTC().Hour())
	}
}

func TestComputeDailyOnOrAfter_LocalizesFromAcrossDateBoundary(t *testing.T) {
	// from is deliberately chosen so its UTC calendar day (the 15th) differs
	// from its America/New_York calendar day (the 14th, since 03:00 UTC is
	// 22:00 EST the previous day). If from's Year/Month/Day were read in
	// UTC instead of the schedule's zone, the candidate would be built for
	// the wrong day.
	rec := newDailyRecurrence()
	firstRunAt := time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC) // 9am EST daily
	from := time.Date(2026, 1, 15, 3, 0, 0, 0, time.UTC)       // == 2026-01-14 22:00 EST

	got, err := ComputeNextRunOnOrAfter(rec, "America/New_York", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The 14th's 9am-EST slot (14:00 UTC on the 14th) already passed before
	// `from`, so the next one is the 15th's, at 14:00 UTC.
	want := time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		loc, _ := time.LoadLocation("America/New_York")
		t.Errorf("got %v (local %v), want %v", got, got.In(loc), want)
	}
}

func TestComputeNextRunOnOrAfter_NonUTCInput_ResultNormalizedToUTC(t *testing.T) {
	// A non-UTC-located result would silently break reflect.DeepEqual-based
	// storage round-trip comparisons (JSON round-tripping a named zone like
	// America/New_York reconstructs a different *time.Location object, a
	// numeric FixedZone, even though the instant is identical).
	rec := newDailyRecurrence()
	firstRunAt := time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC)
	from := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "America/New_York", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Location() != time.UTC {
		t.Errorf("Location: got %v, want time.UTC", got.Location())
	}
}

func TestComputeMonthlyOnOrAfter_NonUTCTimezone(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	rec := newMonthlyRecurrence(15)
	// 9am EST on the 15th of the month.
	firstRunAt := time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC)
	// from is 2026-02-14 23:00 UTC == 2026-02-14 18:00 EST — still the 14th
	// locally, so the next occurrence is the 15th, not already-passed.
	from := time.Date(2026, 2, 14, 23, 0, 0, 0, time.UTC)

	got, err := ComputeNextRunOnOrAfter(rec, "America/New_York", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.In(loc).Day() != 15 || got.In(loc).Hour() != 9 {
		t.Errorf("got %v (local %v), want the 15th at 9am local", got, got.In(loc))
	}
}
