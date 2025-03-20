package test

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// Make an interface so I can use dbconn with both testing.T and testing.B
type TestingT interface {
	Cleanup(f func())
	Errorf(format string, args ...any)
	FailNow()
}

func dbconn(t TestingT) *pgxpool.Pool {
	dbpool, err := pgxpool.New(context.Background(), "postgres://pgledger:pgledger@localhost:5432/pgledger")
	assert.NoError(t, err)

	t.Cleanup(dbpool.Close)

	return dbpool
}
