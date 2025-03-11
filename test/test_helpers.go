package test

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

// Make an interface so I can use dbconn with both testing.T and testing.B
type TestingT interface {
	Cleanup(f func())
	Errorf(format string, args ...any)
	FailNow()
}

func dbconn(t TestingT) *pgxpool.Pool {
	err := godotenv.Load("../.env")
	assert.NoError(t, err)

	dbURL := os.Getenv("DATABASE_URL")
	assert.NotEmpty(t, dbURL, "DATABASE_URL environment variable not set")

	dbpool, err := pgxpool.New(context.Background(), dbURL)
	assert.NoError(t, err)

	t.Cleanup(dbpool.Close)

	return dbpool
}
