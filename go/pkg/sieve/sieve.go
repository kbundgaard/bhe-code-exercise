package sieve

import (
	"math"
)

type Sieve interface {
	NthPrime(n int64) int64
}

func NewSieve() Sieve {
	// Priming the sieve with the first few primes. This is required
	// because the calculated upper bound only applies for n>6
	return &SegmentedSieve{
		primes: []int64{2, 3, 5, 7, 11, 13, 17},
		max:    17,
	}
}

type SegmentedSieve struct {
	primes []int64
	max    int64
}

func (s *SegmentedSieve) NthPrime(n int64) int64 {
	// Calculating every prime up through the nth prime is not the most
	// efficient way of finding just the nth prime. However, if we need
	// to find many primes in general, the sieve approach will give us a
	// cache of every prime up through the nth one, so finding the nth
	// prime for a smaller n becomes a simple lookup. This does however
	// come at the cost of having to store a _lot_ of primes in memory.
	if n < 0 {
		// Should really return an error here, but sticking to the
		// predefined method signature so the tests won't have to
		// change.
		n = -n
	}
	if n >= int64(len(s.primes)) {
		// Calculate upper bound for sieve based on corrolary 3.13 from
		// Rosser and Schoenfeld.
		// https://projecteuclid.org/journals/illinois-journal-of-mathematics/volume-6/issue-1/Approximate-formulas-for-some-functions-of-prime-numbers/10.1215/ijm/1255631807.full
		nf := float64(n)
		bound := int64(math.Ceil(nf * (math.Log(nf) + math.Log(math.Log(nf)))))
		s.sieve(bound)
	}
	return s.primes[n]
}

func (s *SegmentedSieve) sieve(n int64) {
	// Several of the optimizations applied here come from the following paper
	// https://research.cs.wisc.edu/techreports/1990/TR909.pdf
	segmentSize := int64(math.Ceil(math.Sqrt(float64(n))))
	if s.max < segmentSize {
		// We need all primes up through sqrt(n) for filtering the remainder of the range
		newPrimes := initialSieve(s.max+1, segmentSize, s.primes)
		s.primes = append(s.primes, newPrimes...)
		s.max = segmentSize
	}

	// Split the remainder of the range we're checking into smaller
	// segments that we can process independently
	channels := []chan []int64{}
	start := s.max + 1
	end := min(n, start+segmentSize)
	for end < n {
		channel := make(chan []int64)
		channels = append(channels, channel)
		go func(start, end int64) {
			channel <- sieveSegment(start, end, s.primes)
		}(start, end)
		start = end + 1
		end = min(n, start+segmentSize)
	}
	channel := make(chan []int64)
	channels = append(channels, channel)
	go func(start, end int64) {
		channel <- sieveSegment(start, end, s.primes)
	}(start, end)

	// As long as we read the channels in order, the order
	// of the found primes is preserved.
	for _, channel := range channels {
		newPrimes := <-channel
		s.primes = append(s.primes, newPrimes...)
	}
	s.max = end
}

func sieveSegment(start, end int64, primes []int64) []int64 {
	candidates := make([]int64, end-start+1)
	for i := range candidates {
		candidates[i] = start + int64(i)
	}

	for _, prime := range primes {
		if prime*prime > end {
			// We've already checked all the smaller primes, so this and
			// all subsequent primes cannot possibly divide any of the
			// candidates that are left. This is a slight twist on the
			// approach in the paper since we often start off knowing
			// more primes than necessary because of prior executions.
			break
		}
		// For each prime, we rebuild the page with only the remaining
		// possible candidates so we don't have to "remove" a candidate
		// more than once. We could shave off some time here by using
		// a linked list instead and modifying it in place.
		vetted := []int64{}
		for _, candidate := range candidates {
			if candidate%prime > 0 {
				vetted = append(vetted, candidate)
			}
		}
		candidates = vetted
	}
	return candidates
}

func initialSieve(start, end int64, primes []int64) []int64 {
	candidates := sieveSegment(start, end, primes)

	// For the first pass of the sieve, we're not guaranteed to already have every prime
	// less than sqrt(end), so we have to grab additional primes from amongst the
	// candidates we've found.
	vetted := []int64{}
	if len(candidates) > 0 {
		prime := candidates[0]
		for prime*prime < end {
			intermediary := []int64{}
			for _, candidate := range candidates {
				if candidate%prime > 0 {
					intermediary = append(intermediary, candidate)
				}
			}
			candidates = intermediary
			vetted = append(vetted, prime)
			if len(candidates) == 0 {
				break
			}
			prime = candidates[0]
		}
		vetted = append(vetted, candidates...)
	}
	return vetted
}

func min(a, b int64) int64 {
	// Available as a generic built-in starting from 1.21
	if a < b {
		return a
	}
	return b
}
