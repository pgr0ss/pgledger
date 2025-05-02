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
	TotalTransfers        = NumWorkers * NumTransfersPerWorker
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

	startingSize := dbSize(ctx, dbconn)

	var wg sync.WaitGroup
	var completedTransfers atomic.Int64

	wg.Add(NumWorkers)
	startTime := time.Now()

	for range NumWorkers {
		go func() {
			defer wg.Done()
			for range NumTransfersPerWorker {
				perm := rand.Perm(len(accountIDS))
				from := accountIDS[perm[0]]
				to := accountIDS[perm[1]]

				createTransfer(ctx, dbconn, from, to)
				completed := completedTransfers.Add(1)

				if completed%10000 == 0 {
					fmt.Printf("- Finished %d of %d transfers\n", completed, TotalTransfers)
				}
			}
		}()
	}

	fmt.Printf("Waiting for workers to finish\n")
	wg.Wait()

	elapsed := time.Since(startTime)

	fmt.Println("Running VACUUM FULL to clean up database")
	Must1(dbconn.Exec(ctx, "VACUUM FULL"))

	endingSize := dbSize(ctx, dbconn)

	fmt.Printf(`Completed transfers: %d in %f seconds, taking up %d bytes
- transfers/second: %f
- bytes/transfer: %d
`,
		completedTransfers.Load(),
		elapsed.Seconds(),
		endingSize-startingSize,
		float64(completedTransfers.Load())/elapsed.Seconds(),
		(endingSize-startingSize)/completedTransfers.Load())
}

func Must1[T any](obj T, err error) T {
	if err != nil {
		fmt.Println("in here?")
		panic(err)
	}
	return obj
}

func createAccount(ctx context.Context, conn *pgxpool.Pool) string {
	rows := Must1(conn.Query(ctx, "select id from pgledger_create_account('acct', 'USD')"))
	return Must1(pgx.CollectExactlyOneRow(rows, pgx.RowTo[string]))
}

func createTransfer(ctx context.Context, conn *pgxpool.Pool, fromAccountID, toAccountID string) {
	rows := Must1(conn.Query(ctx, "select id from pgledger_create_transfer($1, $2, $3)", fromAccountID, toAccountID, rand.Uint32()))
	_ = Must1(pgx.CollectExactlyOneRow(rows, pgx.RowTo[string]))
}

func dbSize(ctx context.Context, conn *pgxpool.Pool) int64 {
	rows := Must1(conn.Query(ctx, "select pg_database_size('pgledger')"))
	return Must1(pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64]))
}
