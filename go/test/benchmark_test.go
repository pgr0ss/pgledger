package test

import (
	"testing"
)

func BenchmarkTransfers(b *testing.B) {
	conn := dbconn(b)

	account1 := createAccount(b, conn, "benchmark account 1", "USD")
	account2 := createAccount(b, conn, "benchmark account 2", "USD")

	for b.Loop() {
		createTransfer(b, conn, account1.ID, account2.ID, "1.00")
	}
}
