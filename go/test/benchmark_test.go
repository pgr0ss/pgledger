package test

import (
	"context"
	"testing"
)

func BenchmarkTransfers(b *testing.B) {
	conn := dbconn(b)
	ctx := context.Background()

	account1 := createAccount(ctx, b, conn, "benchmark account 1", "USD")
	account2 := createAccount(ctx, b, conn, "benchmark account 2", "USD")

	for b.Loop() {
		createTransfer(ctx, b, conn, account1.ID, account2.ID, "1.00")
	}
}
