package backpressure

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/health"
)

func TestComputeZone(t *testing.T) {
	c := New(500, 2000, 0, 0, nil, nil)

	tests := []struct {
		name    string
		lag     int64
		current Zone
		want    Zone
	}{
		{"below warn → green", 100, ZoneGreen, ZoneGreen},
		{"at warn → yellow", 500, ZoneGreen, ZoneYellow},
		{"between warn and critical → yellow", 1000, ZoneGreen, ZoneYellow},
		{"at critical → red", 2000, ZoneGreen, ZoneRed},
		{"above critical → red", 3000, ZoneYellow, ZoneRed},
		// Hysteresis: red stays red until lag drops below warn.
		{"red + lag between warn and critical → stays red", 1000, ZoneRed, ZoneRed},
		{"red + lag at warn → stays red", 500, ZoneRed, ZoneRed},
		{"red + lag below warn → green", 499, ZoneRed, ZoneGreen},
		{"red + zero lag → green", 0, ZoneRed, ZoneGreen},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.computeZone(tt.lag, tt.current)
			if got != tt.want {
				t.Errorf("computeZone(%d, %v) = %v, want %v", tt.lag, tt.current, got, tt.want)
			}
		})
	}
}

func TestComputeThrottle(t *testing.T) {
	c := New(1000, 2000, 500*time.Millisecond, 0, nil, nil)

	tests := []struct {
		name string
		lag  int64
		want time.Duration
	}{
		{"at warn boundary → 0", 1000, 0},
		{"midpoint → half max", 1500, 250 * time.Millisecond},
		{"at critical → max", 2000, 500 * time.Millisecond},
		{"beyond critical → capped at max", 3000, 500 * time.Millisecond},
		{"below warn → 0", 500, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.computeThrottle(tt.lag)
			if got != tt.want {
				t.Errorf("computeThrottle(%d) = %v, want %v", tt.lag, got, tt.want)
			}
		})
	}
}

func TestComputeThrottle_EqualThresholds(t *testing.T) {
	c := New(1000, 1000, 500*time.Millisecond, 0, nil, nil)
	got := c.computeThrottle(1000)
	if got != 500*time.Millisecond {
		t.Errorf("equal thresholds: got %v, want %v", got, 500*time.Millisecond)
	}
}

func TestShedSet(t *testing.T) {
	c := New(500, 2000, 0, 0, nil, nil)
	c.SetAdapterPriority("kafka", PriorityCritical)
	c.SetAdapterPriority("stdout", PriorityNormal)
	c.SetAdapterPriority("webhook", PriorityBestEffort)

	tests := []struct {
		name        string
		zone        Zone
		shedKafka   bool
		shedStdout  bool
		shedWebhook bool
	}{
		{"green → nothing shed", ZoneGreen, false, false, false},
		{"yellow → best-effort shed", ZoneYellow, false, false, true},
		{"red → normal+best-effort shed", ZoneRed, false, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shed := c.computeShedSet(tt.zone)
			if shed["kafka"] != tt.shedKafka {
				t.Errorf("kafka shed=%v, want %v", shed["kafka"], tt.shedKafka)
			}
			if shed["stdout"] != tt.shedStdout {
				t.Errorf("stdout shed=%v, want %v", shed["stdout"], tt.shedStdout)
			}
			if shed["webhook"] != tt.shedWebhook {
				t.Errorf("webhook shed=%v, want %v", shed["webhook"], tt.shedWebhook)
			}
		})
	}
}

func TestEvaluate_ZoneTransitions(t *testing.T) {
	var lag atomic.Int64
	lag.Store(0)

	h := health.NewChecker()
	h.Register("backpressure")

	c := New(500, 2000, 500*time.Millisecond, 0, h, nil)
	c.SetLagFunc(lag.Load)
	c.SetAdapterPriority("be", PriorityBestEffort)
	c.SetAdapterPriority("norm", PriorityNormal)

	// Green zone.
	lag.Store(100)
	c.evaluate()
	if c.Zone() != ZoneGreen {
		t.Fatalf("expected green, got %v", c.Zone())
	}
	if c.ThrottleDuration() != 0 {
		t.Fatalf("expected 0 throttle in green, got %v", c.ThrottleDuration())
	}
	if c.IsPaused() {
		t.Fatal("should not be paused in green")
	}
	if c.IsShed("be") || c.IsShed("norm") {
		t.Fatal("nothing should be shed in green")
	}

	// Yellow zone.
	lag.Store(1250) // midpoint between 500 and 2000
	c.evaluate()
	if c.Zone() != ZoneYellow {
		t.Fatalf("expected yellow, got %v", c.Zone())
	}
	if c.ThrottleDuration() != 250*time.Millisecond {
		t.Fatalf("expected 250ms throttle, got %v", c.ThrottleDuration())
	}
	if c.IsPaused() {
		t.Fatal("should not be paused in yellow")
	}
	if !c.IsShed("be") {
		t.Fatal("best-effort should be shed in yellow")
	}
	if c.IsShed("norm") {
		t.Fatal("normal should not be shed in yellow")
	}

	// Red zone.
	lag.Store(3000)
	c.evaluate()
	if c.Zone() != ZoneRed {
		t.Fatalf("expected red, got %v", c.Zone())
	}
	if !c.IsPaused() {
		t.Fatal("should be paused in red")
	}
	if !c.IsShed("be") || !c.IsShed("norm") {
		t.Fatal("both best-effort and normal should be shed in red")
	}

	// Hysteresis: lag drops to yellow band → stays red.
	lag.Store(1000)
	c.evaluate()
	if c.Zone() != ZoneRed {
		t.Fatalf("hysteresis: expected red, got %v", c.Zone())
	}
	if !c.IsPaused() {
		t.Fatal("hysteresis: should still be paused")
	}

	// Lag drops below warn → exits red to green.
	lag.Store(200)
	c.evaluate()
	if c.Zone() != ZoneGreen {
		t.Fatalf("expected green after hysteresis exit, got %v", c.Zone())
	}
	if c.IsPaused() {
		t.Fatal("should not be paused after exiting red")
	}
}

func TestWaitResume(t *testing.T) {
	c := New(500, 2000, 0, 0, nil, nil)

	var lag atomic.Int64
	lag.Store(3000)
	c.SetLagFunc(lag.Load)

	// Enter red zone.
	c.evaluate()
	if !c.IsPaused() {
		t.Fatal("should be paused")
	}

	// WaitResume should unblock when we exit red.
	done := make(chan struct{})
	go func() {
		_ = c.WaitResume(context.Background())
		close(done)
	}()

	// Exit red.
	lag.Store(0)
	c.evaluate()

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		t.Fatal("WaitResume did not unblock after exiting red zone")
	}
}

func TestWaitResume_ContextCancelled(t *testing.T) {
	c := New(500, 2000, 0, 0, nil, nil)

	var lag atomic.Int64
	lag.Store(3000)
	c.SetLagFunc(lag.Load)
	c.evaluate()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.WaitResume(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitResume did not unblock after context cancel")
	}
}

func TestRun_NilLagFn(t *testing.T) {
	c := New(500, 2000, 0, 0, nil, nil)
	// Run with nil lagFn should return immediately.
	err := c.Run(context.Background())
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestIsShed_UnregisteredAdapter(t *testing.T) {
	c := New(500, 2000, 0, 0, nil, nil)
	if c.IsShed("unknown") {
		t.Fatal("unregistered adapter should not be shed")
	}
}

func TestZoneString(t *testing.T) {
	tests := []struct {
		z    Zone
		want string
	}{
		{ZoneGreen, "green"},
		{ZoneYellow, "yellow"},
		{ZoneRed, "red"},
		{Zone(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.z.String(); got != tt.want {
			t.Errorf("Zone(%d).String() = %q, want %q", tt.z, got, tt.want)
		}
	}
}
