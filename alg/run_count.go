package alg

import "unicode/utf8"

func RunCount(inputs ...string) int {
	var count int
	for _, input := range inputs {
		count += utf8.RuneCount([]byte(input))
	}
	return count
}
