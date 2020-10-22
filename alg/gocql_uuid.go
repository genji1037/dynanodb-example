package alg

import (
	"github.com/gocql/gocql"
)

func ToSortableTimeUUID(u gocql.UUID) string {
	u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7] = u[6], u[7], u[4], u[5], u[0], u[1], u[2], u[3]
	return u.String()
}
