package alg

import (
	"github.com/gocql/gocql"
)

func ToSortableTimeUUID(u gocql.UUID) string {
	u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7] = u[6], u[7], u[4], u[5], u[0], u[1], u[2], u[3]
	return u.String()
}

func FromSortableTimeUUID(str string) string {
	return FromSortableTimeUUIDBytes([]byte(str))
}

func FromSortableTimeUUIDBytes(bs []byte) string {
	verHiMid := make([]byte, 8)
	copy(verHiMid, bs[:8])
	copy(bs[:4], bs[9:13])        // move lo
	copy(bs[4:8], bs[14:18])      // move lo
	copy(bs[14:18], verHiMid[:4]) // move verHi
	copy(bs[9:13], verHiMid[4:8]) // move mid
	return string(bs)
}
