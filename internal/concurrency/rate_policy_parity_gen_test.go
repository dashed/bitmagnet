package concurrency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

var updateDHTRatePolicyParity = flag.Bool(
	"update-dht-rate-policy-parity",
	false,
	"rewrite the Rust DHT rate and keyed-limiter parity fixtures",
)

const (
	rateLimiterSubsystem     = "dht_rate_limiter"
	keyedLimiterSubsystem    = "dht_keyed_limiter"
	rateLimiterGoldenSHA256  = "130b229c6fd4dd971d3c908a456495239edb933f5f6c357643eeb9252a2773a2"
	keyedLimiterGoldenSHA256 = "53787bb82f1b4c51519a4e412848ead5d9e03a316bc8403a928004f2446bfac8"
)

var rateLimiterFixtureIDs = [...]string{
	"allow_refill_exact",
	"reservation_cancel_before_action",
	"reservation_cancel_after_action_noop",
	"invalid_reservation_over_burst",
}

var keyedLimiterFixtureIDs = [...]string{
	"initial_burst_is_independent_per_key",
	"exact_string_keys_remain_distinct",
	"get_refreshes_lru_recency_and_capacity_evicts_oldest",
	"zero_ttl_disables_expiry",
	"positive_ttl_wall_clock_limit",
}

type rateLimiterFixture struct {
	ID        string              `json:"id"`
	Subsystem string              `json:"subsystem"`
	Input     rateLimiterInput    `json:"input"`
	Expected  rateLimiterExpected `json:"expected"`
}

type rateLimiterInput struct {
	LimitPerSecond float64           `json:"limitPerSecond"`
	Burst          int               `json:"burst"`
	TickNanos      int64             `json:"tickNanos"`
	AnchorUnixNano int64             `json:"anchorUnixNano"`
	Steps          []rateLimiterStep `json:"steps"`
}

type rateLimiterStep struct {
	Operation     string `json:"operation"`
	AtTick        int64  `json:"atTick"`
	Count         int    `json:"count"`
	ReservationID string `json:"reservationId"`
}

type rateLimiterExpected struct {
	Events []rateLimiterEvent `json:"events"`
}

type rateLimiterEvent struct {
	Operation             string `json:"operation"`
	AtTick                int64  `json:"atTick"`
	Count                 int    `json:"count"`
	ReservationID         string `json:"reservationId"`
	Allowed               bool   `json:"allowed"`
	ReservationOK         bool   `json:"reservationOk"`
	ReservationDelayNanos int64  `json:"reservationDelayNanos"`
	TokensBeforeMilli     int64  `json:"tokensBeforeMilli"`
	TokensAfterMilli      int64  `json:"tokensAfterMilli"`
}

type keyedLimiterFixture struct {
	ID        string               `json:"id"`
	Subsystem string               `json:"subsystem"`
	Input     keyedLimiterInput    `json:"input"`
	Expected  keyedLimiterExpected `json:"expected"`
}

type keyedLimiterInput struct {
	LimitPerSecond float64            `json:"limitPerSecond"`
	Burst          int                `json:"burst"`
	Capacity       int                `json:"capacity"`
	TTLNanos       int64              `json:"ttlNanos"`
	Steps          []keyedLimiterStep `json:"steps"`
}

type keyedLimiterStep struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
}

type keyedLimiterExpected struct {
	Events                     []keyedLimiterEvent `json:"events"`
	TTLClockInjectionAvailable bool                `json:"ttlClockInjectionAvailable"`
	PositiveTTLBoundaryFixture bool                `json:"positiveTtlBoundaryFixture"`
	ImplementationLimit        string              `json:"implementationLimit"`
}

type keyedLimiterEvent struct {
	Operation                 string   `json:"operation"`
	Key                       string   `json:"key"`
	Allowed                   bool     `json:"allowed"`
	SameInstanceAsPreviousKey bool     `json:"sameInstanceAsPreviousKey"`
	KeysOldestToNewest        []string `json:"keysOldestToNewest"`
}

func TestGenerateDHTRatePolicyParity(t *testing.T) {
	rateFixtures := generateRateLimiterFixtures(t)
	keyedFixtures := generateKeyedLimiterFixtures(t)

	assertRateLimiterGoldens(t, rateFixtures)
	assertKeyedLimiterGoldens(t, keyedFixtures)

	reconcileRatePolicyFixture(
		t,
		filepath.Join("..", "..", "testdata", "parity", "dht", "rate_limiter.jsonl"),
		*updateDHTRatePolicyParity,
		rateLimiterGoldenSHA256,
		rateFixtures,
	)
	reconcileRatePolicyFixture(
		t,
		filepath.Join("..", "..", "testdata", "parity", "dht", "keyed_limiter.jsonl"),
		*updateDHTRatePolicyParity,
		keyedLimiterGoldenSHA256,
		keyedFixtures,
	)
}

func generateRateLimiterFixtures(t *testing.T) []rateLimiterFixture {
	t.Helper()
	const tick = 100 * time.Millisecond
	anchor := time.Unix(1_700_000_000, 123_000_000)
	scenarios := []struct {
		id    string
		steps []rateLimiterStep
	}{
		{
			id: "allow_refill_exact",
			steps: []rateLimiterStep{
				{Operation: "tokens", AtTick: 0},
				{Operation: "allow", AtTick: 0, Count: 2},
				{Operation: "allow", AtTick: 0, Count: 1},
				{Operation: "tokens", AtTick: 1},
				{Operation: "allow", AtTick: 1, Count: 1},
				{Operation: "tokens", AtTick: 5},
				{Operation: "allow", AtTick: 5, Count: 2},
			},
		},
		{
			id: "reservation_cancel_before_action",
			steps: []rateLimiterStep{
				{Operation: "reserve", AtTick: 0, Count: 2, ReservationID: "immediate"},
				{Operation: "reserve", AtTick: 0, Count: 2, ReservationID: "future"},
				{Operation: "cancel", AtTick: 1, ReservationID: "future"},
				{Operation: "reserve", AtTick: 1, Count: 2, ReservationID: "after_cancel"},
			},
		},
		{
			id: "reservation_cancel_after_action_noop",
			steps: []rateLimiterStep{
				{Operation: "reserve", AtTick: 0, Count: 2, ReservationID: "immediate"},
				{Operation: "reserve", AtTick: 0, Count: 2, ReservationID: "future"},
				{Operation: "cancel", AtTick: 3, ReservationID: "future"},
				{Operation: "reserve", AtTick: 3, Count: 2, ReservationID: "after_late_cancel"},
			},
		},
		{
			id: "invalid_reservation_over_burst",
			steps: []rateLimiterStep{
				{Operation: "reserve", AtTick: 0, Count: 3, ReservationID: "invalid"},
				{Operation: "cancel", AtTick: 0, ReservationID: "invalid"},
				{Operation: "tokens", AtTick: 0},
			},
		},
	}

	fixtures := make([]rateLimiterFixture, 0, len(scenarios))
	for index, scenario := range scenarios {
		if scenario.id != rateLimiterFixtureIDs[index] {
			t.Fatalf("rate scenario %d ID = %q, want %q", index, scenario.id, rateLimiterFixtureIDs[index])
		}
		input := rateLimiterInput{
			LimitPerSecond: 10,
			Burst:          2,
			TickNanos:      int64(tick),
			AnchorUnixNano: anchor.UnixNano(),
			Steps:          scenario.steps,
		}
		fixtures = append(fixtures, rateLimiterFixture{
			ID: scenario.id, Subsystem: rateLimiterSubsystem, Input: input,
			Expected: rateLimiterExpected{Events: runRateLimiterSteps(t, input)},
		})
	}
	return fixtures
}

func runRateLimiterSteps(t *testing.T, input rateLimiterInput) []rateLimiterEvent {
	t.Helper()
	limiter := rate.NewLimiter(rate.Limit(input.LimitPerSecond), input.Burst)
	anchor := time.Unix(0, input.AnchorUnixNano)
	tick := time.Duration(input.TickNanos)
	reservations := make(map[string]*rate.Reservation)
	events := make([]rateLimiterEvent, 0, len(input.Steps))
	for _, step := range input.Steps {
		at := anchor.Add(time.Duration(step.AtTick) * tick)
		event := rateLimiterEvent{
			Operation: step.Operation, AtTick: step.AtTick, Count: step.Count,
			ReservationID:     step.ReservationID,
			TokensBeforeMilli: tokensMilli(limiter.TokensAt(at)),
		}
		switch step.Operation {
		case "tokens":
		case "allow":
			event.Allowed = limiter.AllowN(at, step.Count)
		case "reserve":
			reservation := limiter.ReserveN(at, step.Count)
			reservations[step.ReservationID] = reservation
			event.ReservationOK = reservation.OK()
			event.ReservationDelayNanos = int64(reservation.DelayFrom(at))
		case "cancel":
			reservation, ok := reservations[step.ReservationID]
			if !ok {
				t.Fatalf("cancel references unknown reservation %q", step.ReservationID)
			}
			reservation.CancelAt(at)
		default:
			t.Fatalf("unknown rate operation %q", step.Operation)
		}
		event.TokensAfterMilli = tokensMilli(limiter.TokensAt(at))
		events = append(events, event)
	}
	return events
}

func generateKeyedLimiterFixtures(t *testing.T) []keyedLimiterFixture {
	t.Helper()
	positiveTTL := 20 * time.Second
	scenarios := []keyedLimiterFixture{
		newKeyedFixture("initial_burst_is_independent_per_key", 0, 2, 4, 0, []keyedLimiterStep{
			{Operation: "allow", Key: "alpha"}, {Operation: "allow", Key: "alpha"},
			{Operation: "allow", Key: "alpha"}, {Operation: "allow", Key: "beta"},
			{Operation: "allow", Key: "beta"}, {Operation: "allow", Key: "beta"},
		}),
		newKeyedFixture("exact_string_keys_remain_distinct", 0, 1, 8, 0, []keyedLimiterStep{
			{Operation: "allow", Key: "192.0.2.1"},
			{Operation: "allow", Key: "::ffff:192.0.2.1"},
			{Operation: "allow", Key: "fe80::1%7"},
			{Operation: "allow", Key: "fe80::1%8"},
			{Operation: "allow", Key: "192.0.2.1"},
			{Operation: "allow", Key: "::ffff:192.0.2.1"},
		}),
		newKeyedFixture("get_refreshes_lru_recency_and_capacity_evicts_oldest", 0, 1, 2, 0, []keyedLimiterStep{
			{Operation: "get", Key: "alpha"}, {Operation: "get", Key: "beta"},
			{Operation: "get", Key: "alpha"}, {Operation: "get", Key: "gamma"},
			{Operation: "get", Key: "beta"},
		}),
		newKeyedFixture("zero_ttl_disables_expiry", 0, 1, 2, 0, []keyedLimiterStep{
			{Operation: "get", Key: "stable"}, {Operation: "get", Key: "stable"},
		}),
		newKeyedFixture("positive_ttl_wall_clock_limit", 0, 1, 2, positiveTTL, []keyedLimiterStep{
			{Operation: "get", Key: "wall-clock"}, {Operation: "get", Key: "wall-clock"},
		}),
	}

	fixtures := make([]keyedLimiterFixture, 0, len(scenarios))
	for index, fixture := range scenarios {
		if fixture.ID != keyedLimiterFixtureIDs[index] {
			t.Fatalf("keyed scenario %d ID = %q, want %q", index, fixture.ID, keyedLimiterFixtureIDs[index])
		}
		fixture.Expected.Events = runKeyedLimiterSteps(t, fixture.Input)
		if fixture.ID == "positive_ttl_wall_clock_limit" {
			fixture.Expected.ImplementationLimit = "positive TTL expiry and reset boundaries use time.Now plus a non-injectable background reaper; only pre-expiry identity is deterministic without sleeps or production changes"
		}
		fixtures = append(fixtures, fixture)
	}
	return fixtures
}

func newKeyedFixture(
	id string,
	limit rate.Limit,
	burst int,
	capacity int,
	ttl time.Duration,
	steps []keyedLimiterStep,
) keyedLimiterFixture {
	return keyedLimiterFixture{
		ID: id, Subsystem: keyedLimiterSubsystem,
		Input: keyedLimiterInput{
			LimitPerSecond: float64(limit), Burst: burst, Capacity: capacity,
			TTLNanos: int64(ttl), Steps: steps,
		},
		Expected: keyedLimiterExpected{
			Events: []keyedLimiterEvent{}, TTLClockInjectionAvailable: false,
			PositiveTTLBoundaryFixture: false, ImplementationLimit: "",
		},
	}
}

func runKeyedLimiterSteps(t *testing.T, input keyedLimiterInput) []keyedLimiterEvent {
	t.Helper()
	created := NewKeyedLimiter(
		rate.Limit(input.LimitPerSecond), input.Burst, input.Capacity, time.Duration(input.TTLNanos),
	)
	limiter, ok := created.(*keyedLimiter)
	if !ok {
		t.Fatalf("NewKeyedLimiter returned %T, want production *keyedLimiter", created)
	}
	previous := make(map[string]*rate.Limiter)
	events := make([]keyedLimiterEvent, 0, len(input.Steps))
	for _, step := range input.Steps {
		event := keyedLimiterEvent{Operation: step.Operation, Key: step.Key, KeysOldestToNewest: []string{}}
		switch step.Operation {
		case "allow":
			event.Allowed = limiter.Allow(step.Key)
		case "get":
			value := limiter.getLimiter(step.Key)
			if prior, exists := previous[step.Key]; exists {
				event.SameInstanceAsPreviousKey = prior == value
			}
			previous[step.Key] = value
		default:
			t.Fatalf("unknown keyed limiter operation %q", step.Operation)
		}
		event.KeysOldestToNewest = append(event.KeysOldestToNewest, limiter.lru.Keys()...)
		events = append(events, event)
	}
	return events
}

func tokensMilli(tokens float64) int64 {
	return int64(math.Round(tokens * 1_000))
}

func assertRateLimiterGoldens(t *testing.T, fixtures []rateLimiterFixture) {
	t.Helper()
	if len(fixtures) != len(rateLimiterFixtureIDs) {
		t.Fatalf("rate fixture count = %d, want %d", len(fixtures), len(rateLimiterFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != rateLimiterFixtureIDs[index] || fixture.Subsystem != rateLimiterSubsystem {
			t.Fatalf("unstable rate fixture %d: %#v", index, fixture)
		}
	}
}

func assertKeyedLimiterGoldens(t *testing.T, fixtures []keyedLimiterFixture) {
	t.Helper()
	if len(fixtures) != len(keyedLimiterFixtureIDs) {
		t.Fatalf("keyed fixture count = %d, want %d", len(fixtures), len(keyedLimiterFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != keyedLimiterFixtureIDs[index] || fixture.Subsystem != keyedLimiterSubsystem {
			t.Fatalf("unstable keyed fixture %d: %#v", index, fixture)
		}
		if fixture.ID == "positive_ttl_wall_clock_limit" {
			if fixture.Expected.TTLClockInjectionAvailable || fixture.Expected.PositiveTTLBoundaryFixture || fixture.Expected.ImplementationLimit == "" {
				t.Fatalf("positive TTL limitation was overclaimed: %#v", fixture.Expected)
			}
		}
	}
}

func reconcileRatePolicyFixture[T any](
	t *testing.T,
	path string,
	update bool,
	goldenSHA256 string,
	fixtures []T,
) {
	t.Helper()
	var generated bytes.Buffer
	encoder := json.NewEncoder(&generated)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	generatedBytes := generated.Bytes()
	if update {
		if err := os.WriteFile(path, generatedBytes, 0o644); err != nil {
			t.Fatalf("write rate fixture: %v", err)
		}
	}
	digest := sha256.Sum256(generatedBytes)
	digestHex := hex.EncodeToString(digest[:])
	if goldenSHA256 != "TODO" && digestHex != goldenSHA256 {
		t.Fatalf("generated rate fixture digest = %s, want independent golden %s", digestHex, goldenSHA256)
	}
	checked, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rate fixture; rerun with -update-dht-rate-policy-parity: %v", err)
	}
	if !bytes.Equal(checked, generatedBytes) {
		t.Fatal("DHT rate fixture is stale; rerun with -update-dht-rate-policy-parity")
	}
}
