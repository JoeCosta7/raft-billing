package model

import (
	"fmt"
	"math"
	"time"

	"raft-biling/internal/apperr"
)

func resolveTimezone(timezone string) (*time.Location, error) {
	if timezone == "Local" {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "timezone",
			Message: `timezone: "Local" is not allowed — it depends on the machine running Apply, which breaks Raft FSM determinism across replicas`}
	}
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "timezone", Message: fmt.Sprintf("timezone: %v", err)}
	}
	return loc, nil
}

// (comment describing what it does)
func computeIntervalOnOrAfter(firstRunAt time.Time, seconds int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) || from.Equal(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	timeDifference := from.Sub(firstRunAt)
	N := math.Ceil(timeDifference.Seconds() / float64(seconds))
	offset := time.Duration(int(N)*seconds) * time.Second
	result := firstRunAt.Add(offset).UTC()
	return &result, nil
}

func computeDailyOnOrAfter(firstRunAt time.Time, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	candidate := time.Date(fromLocal.Year(), fromLocal.Month(), fromLocal.Day(), hour, minute, second, 0, loc)
	if candidate.Before(from) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	result := candidate.UTC()
	return &result, nil
}

func computeWeeklyOnOrAfter(firstRunAt time.Time, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	targetWeekday := firstRunAtLocal.Weekday()
	daysUntilTarget := (int(targetWeekday) - int(fromLocal.Weekday()) + 7) % 7
	candidate := time.Date(fromLocal.Year(), fromLocal.Month(), fromLocal.Day()+daysUntilTarget, hour, minute, second, 0, loc)
	if candidate.Before(from) {
		candidate = candidate.AddDate(0, 0, 7)
	}
	result := candidate.UTC()
	return &result, nil
}

func computeMonthlyOnOrAfter(firstRunAt time.Time, dayOfMonth int, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	year, month := fromLocal.Year(), fromLocal.Month()
	for i := 0; i < 120; i++ {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		actualDay := min(dayOfMonth, lastDay)
		candidate := time.Date(year, month, actualDay, hour, minute, second, 0, loc)
		if !candidate.Before(from) {
			result := candidate.UTC()
			return &result, nil
		}
		next := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
		year, month = next.Year(), next.Month()
	}
	return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "day_of_month", Message: "computeMonthlyOnOrAfter: no slot found within 120 months"}
}

// Creating schedule initially or Resuming catch_up_all with no prior exec
func ComputeNextRunOnOrAfter(recurrence *Recurrence, timezone string, firstRunAt, from time.Time) (*time.Time, error) {
	if recurrence == nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "recurrence", Message: "recurrence: no recurrence, cannot compute"}
	}
	loc, err := resolveTimezone(timezone)
	if err != nil {
		return nil, err
	}
	switch recurrence.Cadence {
	case CadenceInterval:
		return computeIntervalOnOrAfter(firstRunAt, *recurrence.Seconds, from)
	case CadenceDaily:
		return computeDailyOnOrAfter(firstRunAt, from, loc)
	case CadenceWeekly:
		return computeWeeklyOnOrAfter(firstRunAt, from, loc)
	case CadenceMonthly:
		return computeMonthlyOnOrAfter(firstRunAt, *recurrence.DayOfMonth, from, loc)
	default:
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "cadence", Message: "recurrence.cadence: unknown cadence"}
	}
}

func computeIntervalAfter(firstRunAt time.Time, seconds int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	timeDifference := from.Sub(firstRunAt)
	N := math.Floor(timeDifference.Seconds()/float64(seconds)) + 1
	offset := time.Duration(int(N)*seconds) * time.Second
	result := firstRunAt.Add(offset).UTC()
	return &result, nil
}

func computeDailyAfter(firstRunAt time.Time, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	candidate := time.Date(fromLocal.Year(), fromLocal.Month(), fromLocal.Day(), hour, minute, second, 0, loc)
	if !candidate.After(from) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	result := candidate.UTC()
	return &result, nil
}

func computeWeeklyAfter(firstRunAt time.Time, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	candidate := time.Date(fromLocal.Year(), fromLocal.Month(), fromLocal.Day(), hour, minute, second, 0, loc)
	if !candidate.After(from) {
		candidate = candidate.AddDate(0, 0, 7)
	}
	result := candidate.UTC()
	return &result, nil
}

func computeMonthlyAfter(firstRunAt time.Time, dayOfMonth int, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		result := firstRunAt.UTC()
		return &result, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	year, month := fromLocal.Year(), fromLocal.Month()
	for i := 0; i < 120; i++ {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		actualDay := min(dayOfMonth, lastDay)
		candidate := time.Date(year, month, actualDay, hour, minute, second, 0, loc)
		if candidate.After(from) {
			result := candidate.UTC()
			return &result, nil
		}
		next := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
		year, month = next.Year(), next.Month()
	}
	return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "day_of_month", Message: "computeMonthlyAfter: no slot found within 120 months"}
}

func ComputeNextRunAfter(recurrence *Recurrence, timezone string, firstRunAt, from time.Time) (*time.Time, error) {
	if recurrence == nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "recurrence", Message: "recurrence: no recurrence, cannot compute"}
	}
	loc, err := resolveTimezone(timezone)
	if err != nil {
		return nil, err
	}
	switch recurrence.Cadence {
	case CadenceInterval:
		return computeIntervalAfter(firstRunAt, *recurrence.Seconds, from)
	case CadenceDaily:
		return computeDailyAfter(firstRunAt, from, loc)
	case CadenceWeekly:
		return computeWeeklyAfter(firstRunAt, from, loc)
	case CadenceMonthly:
		return computeMonthlyAfter(firstRunAt, *recurrence.DayOfMonth, from, loc)
	default:
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "cadence", Message: "recurrence.cadence: unknown cadence"}
	}
}

func computeIntervalLatestOnOrBefore(firstRunAt time.Time, seconds int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return nil, nil
	}
	timeDifference := from.Sub(firstRunAt)
	N := math.Floor(timeDifference.Seconds() / float64(seconds))
	offset := time.Duration(int(N)*seconds) * time.Second
	result := firstRunAt.Add(offset).UTC()
	return &result, nil
}

func computeDailyLatestOnOrBefore(firstRunAt time.Time, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return nil, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	candidate := time.Date(fromLocal.Year(), fromLocal.Month(), fromLocal.Day(), hour, minute, second, 0, loc)
	if candidate.After(from) {
		candidate = candidate.AddDate(0, 0, -1)
	}
	result := candidate.UTC()
	return &result, nil
}

func computeWeeklyLatestOnOrBefore(firstRunAt time.Time, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return nil, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	targetWeekday := firstRunAtLocal.Weekday()
	daysBackToTarget := (int(fromLocal.Weekday()) - int(targetWeekday) + 7) % 7
	candidate := time.Date(fromLocal.Year(), fromLocal.Month(), fromLocal.Day()-daysBackToTarget, hour, minute, second, 0, loc)

	if candidate.After(from) {
		candidate = candidate.AddDate(0, 0, -7)
	}
	result := candidate.UTC()
	return &result, nil
}

func computeMonthlyLatestOnOrBefore(firstRunAt time.Time, dayOfMonth int, from time.Time, loc *time.Location) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return nil, nil
	}
	firstRunAtLocal := firstRunAt.In(loc)
	hour, minute, second := firstRunAtLocal.Hour(), firstRunAtLocal.Minute(), firstRunAtLocal.Second()
	fromLocal := from.In(loc)
	year, month := fromLocal.Year(), fromLocal.Month()
	for i := 0; i < 120; i++ {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		actualDay := min(dayOfMonth, lastDay)
		candidate := time.Date(year, month, actualDay, hour, minute, second, 0, loc)
		if !candidate.After(from) {
			result := candidate.UTC()
			return &result, nil
		}
		prev := time.Date(year, month-1, 1, 0, 0, 0, 0, time.UTC)
		year, month = prev.Year(), prev.Month()
	}
	return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "day_of_month", Message: "computeMonthlyLatestOnOrBefore: no slot found within 120 months"}
}

// Catch up latest only
func ComputeLatestRunOnOrBefore(recurrence *Recurrence, timezone string, firstRunAt, from time.Time) (*time.Time, error) {
	if recurrence == nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "recurrence", Message: "recurrence: no recurrence, cannot compute"}
	}
	loc, err := resolveTimezone(timezone)
	if err != nil {
		return nil, err
	}
	switch recurrence.Cadence {
	case CadenceInterval:
		return computeIntervalLatestOnOrBefore(firstRunAt, *recurrence.Seconds, from)
	case CadenceDaily:
		return computeDailyLatestOnOrBefore(firstRunAt, from, loc)
	case CadenceWeekly:
		return computeWeeklyLatestOnOrBefore(firstRunAt, from, loc)
	case CadenceMonthly:
		return computeMonthlyLatestOnOrBefore(firstRunAt, *recurrence.DayOfMonth, from, loc)
	default:
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "cadence", Message: "recurrence.cadence: unknown cadence"}
	}
}
