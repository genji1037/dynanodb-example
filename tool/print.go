package tool

import "fmt"

func PrintFloatIfNotNil(prefix string, f *float64) {
	if f != nil {
		fmt.Println(prefix, *f)
	}
}
