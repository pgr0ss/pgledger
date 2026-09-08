package test

import (
	"testing"

	"github.com/pgr0ss/pgledger/ledgertest"
)

func BenchmarkTransfers(b *testing.B) {
	conn := ledgertest.Dbconn(b)

	account1 := ledgertest.CreateAccount(b, conn, "benchmark account 1", "USD")
	account2 := ledgertest.CreateAccount(b, conn, "benchmark account 2", "USD")

	for b.Loop() {
		ledgertest.CreateTransfer(b, conn, account1.ID, account2.ID, "1.00")
	}
}
