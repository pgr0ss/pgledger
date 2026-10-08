package test

import (
	"testing"

	"github.com/pgr0ss/pgledger/testhelpers"
)

func BenchmarkTransfers(b *testing.B) {
	conn := testhelpers.DBConn(b)

	account1 := testhelpers.CreateAccount(b, conn, "benchmark account 1", "USD")
	account2 := testhelpers.CreateAccount(b, conn, "benchmark account 2", "USD")

	for b.Loop() {
		testhelpers.CreateTransfer(b, conn, account1.ID, account2.ID, "1.00")
	}
}
