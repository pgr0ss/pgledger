package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	NumAccounts           = 10
	NumWorkers            = 20
	NumTransfersPerWorker = 5_000
)

func main() {
	ctx := context.Background()
	dbconn := Must1(pgxpool.New(ctx, "postgres://pgledger:pgledger@localhost:5432/pgledger"))

	fmt.Printf("Creating %d accounts\n", NumAccounts)
	accountIDS := []string{}
	for range NumAccounts {
		accountIDS = append(accountIDS, createAccount(ctx, dbconn))
	}

	fmt.Printf("Starting %d workers to each run %d transfers\n", NumWorkers, NumTransfersPerWorker)

	var wg sync.WaitGroup
	var totalTransfers atomic.Int64

	wg.Add(NumWorkers)
	startTime := time.Now()

	for range NumWorkers {
		go func() {
			defer wg.Done()
			for range NumTransfersPerWorker {
				perm := rand.Perm(len(accountIDS))
				from := accountIDS[perm[0]]
				to := accountIDS[perm[1]]

				createTransfer2(ctx, dbconn, from, to)
				totalTransfers.Add(1)
			}
		}()
	}

	fmt.Printf("Waiting for workers to finish\n")
	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("Total transfers: %d in %f seconds, %f transfers/second\n",
		totalTransfers.Load(),
		elapsed.Seconds(),
		float64(totalTransfers.Load())/elapsed.Seconds())
}

func Must1[T any](obj T, err error) T {
	if err != nil {
		fmt.Println("in here?")
		panic(err)
	}
	return obj
}

func createAccount(ctx context.Context, conn *pgxpool.Pool) string {
	rows := Must1(conn.Query(ctx, "select id from pgledger_create_account('acct')"))
	return Must1(pgx.CollectExactlyOneRow(rows, pgx.RowTo[string]))
}

func createTransfer2(ctx context.Context, conn *pgxpool.Pool, fromAccountID, toAccountID string) {
	rows := Must1(conn.Query(ctx, "select id from pgledger_create_transfer($1, $2, $3)", fromAccountID, toAccountID, rand.Uint32()))
	_ = Must1(pgx.CollectExactlyOneRow(rows, pgx.RowTo[string]))
}
