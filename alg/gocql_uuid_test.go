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

func TestUUIDLongTimeAgo(t *testing.T) {
	weekAgo := time.Now().Add(-time.Hour * 24 * 7)
	u := gocql.UUIDFromTime(weekAgo)
	fmt.Println(u.String())
	fmt.Println(ToSortableTimeUUID(u))
	fmt.Println(FromSortableTimeUUID(ToSortableTimeUUID(u)))
}
