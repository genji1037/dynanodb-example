package progress

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Progress struct {
	WriteCount     int64
	WriteTimeTotal int64

	ReadCount     int64
	ReadTimeTotal int64

	TimeTotal time.Duration
}

var P Progress

func ObserveWrite(duration time.Duration) {
	atomic.AddInt64(&P.WriteCount, 1)
	atomic.AddInt64(&P.WriteTimeTotal, int64(duration))
}

func ObserveRead(duration time.Duration) {
	atomic.AddInt64(&P.ReadCount, 1)
	atomic.AddInt64(&P.ReadTimeTotal, int64(duration))
}

func Report() {
	fmt.Println("=====report=====")

	if P.WriteCount > 0 {
		fmt.Println("write tps", float64(P.WriteCount)/(float64(P.TimeTotal)/float64(time.Second)))
		fmt.Println("average write latency", time.Duration(P.WriteTimeTotal/P.WriteCount))
	}

	if P.ReadCount > 0 {
		fmt.Println("read tps", float64(P.ReadCount)/(float64(P.TimeTotal)/float64(time.Second)))
		fmt.Println("average read latency", time.Duration(P.ReadTimeTotal/P.ReadCount))
	}

	fmt.Println("total cost", P.TimeTotal)

}
