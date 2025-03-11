package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkTransfers(b *testing.B) {
	conn := dbconn(b)
	ctx := context.Background()

	account1, err := createAccount(ctx, conn, "benchmark account 1")
	require.NoError(b, err)

	account2, err := createAccount(ctx, conn, "benchmark account 2")
	require.NoError(b, err)

	for b.Loop() {
		_, err := createTransfer(ctx, conn, account1.ID, account2.ID, "1.00")
		if err != nil {
			b.Fatal(err)
		}
	}
}
