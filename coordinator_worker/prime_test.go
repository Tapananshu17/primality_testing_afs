package main

import "testing"

func TestIsPrime_BoundaryAndSmall(t *testing.T) {
	t.Parallel()

	cases := []struct {
		n    uint64
		want bool
	}{
		// Below 2 — never prime.
		{0, false},
		{1, false},

		// The first batch of primes.
		{2, true}, {3, true}, {5, true}, {7, true},
		{11, true}, {13, true}, {17, true}, {19, true},
		{23, true}, {29, true}, {31, true}, {37, true},
		{41, true}, {43, true}, {47, true},

		// Composite neighbours.
		{4, false}, {6, false}, {8, false}, {9, false},
		{10, false}, {12, false}, {14, false}, {15, false},
		{25, false}, {49, false},

		// 3-digit primes and composites.
		{97, true}, {98, false}, {99, false}, {100, false}, {101, true},
	}

	for _, c := range cases {
		if got := IsPrime(c.n); got != c.want {
			t.Errorf("IsPrime(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

func TestIsPrime_WitnessValues(t *testing.T) {
	t.Parallel()

	// All 12 witnesses used inside IsPrime must themselves be correctly
	// identified as prime.
	witnesses := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}
	for _, w := range witnesses {
		if !IsPrime(w) {
			t.Errorf("IsPrime(%d) = false, want true (witness value)", w)
		}
	}
}

func TestIsPrime_KnownLargePrimes(t *testing.T) {
	t.Parallel()

	// Independently verified large primes.
	primes := []uint64{
		1_000_000_007,           // commonly used in competitive programming
		9_999_999_999_971,       // ~10^13
		999_999_999_999_999_877, // near uint64 max, verified prime
	}
	for _, p := range primes {
		if !IsPrime(p) {
			t.Errorf("IsPrime(%d) = false, want true", p)
		}
	}
}

func TestIsPrime_KnownLargeComposites(t *testing.T) {
	t.Parallel()

	// Large composites that must NOT be reported as prime.
	composites := []uint64{
		1_000_000_000_000_000_000, // 10^18 = 2^18 × 5^18
		999_999_999_999_999_999,   // = 3^3 × 37 × ... (composite)
		1_000_000_008,             // 1_000_000_007 + 1, even
		4,                         // classic edge case
		// Carmichael numbers — fooled by naive Fermat but not Miller-Rabin.
		561, 1105, 1729, 2465, 2821,
	}
	for _, c := range composites {
		if IsPrime(c) {
			t.Errorf("IsPrime(%d) = true, want false (composite)", c)
		}
	}
}

func TestIsPrime_Carmichael(t *testing.T) {
	t.Parallel()

	// Carmichael numbers pass the naive Fermat test for every base
	// coprime to them, but must be caught as composite by Miller-Rabin.
	carmichael := []uint64{561, 1105, 1729, 2465, 2821, 6601, 8911, 10585}
	for _, n := range carmichael {
		if IsPrime(n) {
			t.Errorf("IsPrime(%d) = true, want false (Carmichael number)", n)
		}
	}
}

func TestIsPrime_TwinPrimes(t *testing.T) {
	t.Parallel()

	// Twin prime pairs (p, p+2) — both must be prime.
	twins := [][2]uint64{
		{11, 13}, {17, 19}, {29, 31}, {41, 43},
		{59, 61}, {71, 73}, {1_000_000_007, 1_000_000_009},
	}
	for _, pair := range twins {
		if !IsPrime(pair[0]) {
			t.Errorf("IsPrime(%d) = false, want true (twin prime)", pair[0])
		}
		if !IsPrime(pair[1]) {
			t.Errorf("IsPrime(%d) = false, want true (twin prime)", pair[1])
		}
	}
}

// BenchmarkIsPrime_Large measures throughput on a large near-max prime.
func BenchmarkIsPrime_Large(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IsPrime(999_999_999_999_999_877)
	}
}

// BenchmarkIsPrime_Small measures throughput on a small prime (fast path).
func BenchmarkIsPrime_Small(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IsPrime(97)
	}
}
