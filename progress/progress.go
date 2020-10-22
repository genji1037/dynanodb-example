package progress

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Progress struct {
	WriteCount     int64
	WriteTimeTotal int64
	TimeTotal      time.Duration
}

var P Progress

func ObserveWrite(duration time.Duration) {
	atomic.AddInt64(&P.WriteCount, 1)
	atomic.AddInt64(&P.WriteTimeTotal, int64(duration))
}

func Report() {
	fmt.Println("=====report=====")
	fmt.Println("tps", P.WriteCount/int64(P.TimeTotal/time.Second))
	fmt.Println("average write latency", time.Duration(P.WriteTimeTotal/P.WriteCount))
	fmt.Println("total cost", P.TimeTotal)

}
