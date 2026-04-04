// coordinator_worker/primality/isprime.go
//
// Deterministic Miller-Rabin primality test.
// No external imports — compiles standalone so unit tests work without AFS.
//
// With the 12 witnesses {2,3,5,7,11,13,17,19,23,29,31,37} the test is proven
// correct for every integer < 3.3×10²⁴, covering the full uint64 range.

package primality

import "math/bits"

// mulmod computes (a * b) % m without overflow.
// FIX #10: Uses math/bits.Mul64 for a true 128-bit multiply instead of the
// slow Russian-peasant loop, which was O(64) multiplications per call.
// This is 10-20x faster for the hot path of Miller-Rabin.
func mulmod(a, b, m uint64) uint64 {
	hi, lo := bits.Mul64(a%m, b%m)
	_, rem := bits.Div64(hi, lo, m)
	return rem
}

// powmod computes (base ^ exp) % mod using square-and-multiply.
func powmod(base, exp, mod uint64) uint64 {
	result := uint64(1)
	base %= mod
	for exp > 0 {
		if exp&1 == 1 {
			result = mulmod(result, base, mod)
		}
		exp >>= 1
		base = mulmod(base, base, mod)
	}
	return result
}

// millerRabinWitness runs one round of Miller-Rabin for witness a.
//
//	n-1 = 2^r * d  (d must be odd, pre-computed by caller).
//	Returns false → n is definitely composite.
//	Returns true  → n might be prime (not disproved by this witness).
func millerRabinWitness(n, a, d uint64, r int) bool {
	x := powmod(a, d, n)
	if x == 1 || x == n-1 {
		return true
	}
	for i := 0; i < r-1; i++ {
		x = mulmod(x, x, n)
		if x == n-1 {
			return true
		}
	}
	return false
}

// IsPrime reports whether n is prime.
// Deterministic for all uint64 values with the 12-witness set below.
func IsPrime(n uint64) bool {
	if n < 2 {
		return false
	}

	witnesses := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}
	for _, p := range witnesses {
		if n == p {
			return true
		}
		if n%p == 0 {
			return false
		}
	}
	if n < 41 {
		return true
	}

	// Express n-1 as 2^r * d with d odd.
	d, r := n-1, 0
	for d%2 == 0 {
		d /= 2
		r++
	}

	for _, a := range witnesses {
		if a >= n {
			continue
		}
		if !millerRabinWitness(n, a, d, r) {
			return false
		}
	}
	return true
}