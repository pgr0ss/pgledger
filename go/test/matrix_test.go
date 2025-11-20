package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestMatrixPostgresVersion(t *testing.T) {
	expectedVersion := os.Getenv("POSTGRES_VERSION")
	if expectedVersion == "" {
		expectedVersion = "18"
	}
	assert.Regexp(t, `^\d+$`, expectedVersion)

	conn := dbconn(t)

	rows, err := conn.Query(t.Context(), "select version()")
	assert.NoError(t, err)

	version, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	assert.Contains(t, version, fmt.Sprintf("PostgreSQL %s.", expectedVersion))
}
