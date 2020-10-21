package alg

import "math"

func Sqrt(x float64) float64 {
	z := float64(1)
	tmp := float64(0)
	for math.Abs(tmp-z) > 0.0000000001 {
		tmp = z
		z = (z + x/z) / 2
	}
	return z
}
