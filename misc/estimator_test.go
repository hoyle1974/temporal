package misc

import "testing"

func TestEstimator(t *testing.T) {
	estimator := NewCompressionEstimator(1000)

	estimator.OnWriteData(1000)
	if !estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = true but got false")
	}
	estimator.OnFlush(500, false)
	if estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = false but got true")
	}
	estimator.OnWriteData(900)
	if estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = false but got true")
	}
	estimator.OnWriteData(100)
	if !estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = true but got false")
	}
	estimator.OnFlush(1000, true)
	if estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = false but got true")
	}
	estimator.OnWriteData(1000)
	if estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = false but got true")
	}
	estimator.OnWriteData(1000)
	if !estimator.ShouldTryFlush() {
		t.Fatalf("expected ShouldTryFlush = true but got false")
	}
}
