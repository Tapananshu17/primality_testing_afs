package primality

import "math/bits"

func mulmod(a, b, m uint64) uint64 {
	hi, lo := bits.Mul64(a%m, b%m)
	_, rem := bits.Div64(hi, lo, m)
	return rem
}


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