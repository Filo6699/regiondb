package fs_split

import "testing"

func TestClampWALStreamLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		requested int
		budget    int
		want      int
	}{
		{name: "below budget", requested: 2, budget: 64, want: 2},
		{name: "at budget", requested: 64, budget: 64, want: 64},
		{name: "above budget", requested: 1024, budget: 64, want: 64},
		{name: "exhausted budget", requested: 2, budget: 0, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := clampWALStreamLimit(test.requested, test.budget); got != test.want {
				t.Fatalf(
					"clampWALStreamLimit(%d, %d) = %d, want %d",
					test.requested,
					test.budget,
					got,
					test.want,
				)
			}
		})
	}
}

func TestEffectiveWALHandleLimitRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	if _, err := EffectiveWALHandleLimit(0, 0); err == nil {
		t.Fatal("EffectiveWALHandleLimit() accepted a zero handle limit")
	}
	if _, err := EffectiveWALHandleLimit(1, -1); err == nil {
		t.Fatal("EffectiveWALHandleLimit() accepted a negative reserve")
	}
	if _, err := AvailableWALDescriptors(-1); err == nil {
		t.Fatal("AvailableWALDescriptors() accepted a negative reserve")
	}
}
