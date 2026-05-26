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
