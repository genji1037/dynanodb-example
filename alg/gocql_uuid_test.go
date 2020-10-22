package alg

import (
	"fmt"
	"github.com/gocql/gocql"
	"testing"
	"time"
)

func TestToSortableTimeUUID(t *testing.T) {
	t1 := gocql.TimeUUID()
	time.Sleep(time.Millisecond)
	t2 := gocql.TimeUUID()
	fmt.Println(t1.String(), t2.String())
	fmt.Println(ToSortableTimeUUID(t1), ToSortableTimeUUID(t2))
}
