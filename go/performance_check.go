package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	numAccountsFlag = flag.Int("accounts", 10, "Number of accounts to create")
	numWorkersFlag  = flag.Int("workers", 20, "Number of concurrent workers")
	durationFlag    = flag.String("duration", "10s", "Duration to run the test (e.g., 30s, 1m, 5m)")
	vacuumFlag      = flag.Bool("vacuum", true, "Vacuum the database before and after to get better size estimates")
)

func parseArgs() (accounts, workers int, duration time.Duration, vacuum bool) {
	flag.Parse()

	accounts = *numAccountsFlag
	workers = *numWorkersFlag
	vacuum = *vacuumFlag

	if accounts < 2 {
		fmt.Fprintf(os.Stderr, "Need at least 2 accounts to perform transfers.\n")
		os.Exit(1)
	}

	duration, err := time.ParseDuration(*durationFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing runtime duration: %v\n", err)
		os.Exit(1)
	}

	return
}

func main() {
	numAccounts, numWorkers, runDuration, vacuum := parseArgs()

	ctx := context.Background()
	dbconn := Must1(pgxpool.New(ctx, "postgres://pgledger:pgledger@localhost:5432/pgledger"))
	defer dbconn.Close()

	fmt.Printf("Creating %d accounts\n", numAccounts)
	accountIDS := []string{}
	for range numAccounts {
		accountIDS = append(accountIDS, createAccount(ctx, dbconn))
	}

	if vacuum {
		fmt.Println("Running VACUUM FULL to clean up database")
		Must1(dbconn.Exec(ctx, "VACUUM FULL"))
	} else {
		fmt.Println("Skipping VACUUM FULL ")
	}

	startingSizeBytes, startingSizePretty := dbSize(ctx, dbconn)

	fmt.Printf("Starting %d workers to run transfers for %s\n", numWorkers, runDuration)

	var wg sync.WaitGroup
	var completedTransfers atomic.Int64

	runCtx, cancel := context.WithTimeout(ctx, runDuration)
	defer cancel()

	startTime := time.Now()

	for range numWorkers {
		wg.Go(func() {
			for {
				select {
				case <-runCtx.Done():
					return
				default:
					perm := rand.Perm(len(accountIDS))
					from := accountIDS[perm[0]]
					to := accountIDS[perm[1]]

					createTransfer(runCtx, dbconn, from, to)
					completed := completedTransfers.Add(1)

					if completed%10000 == 0 {
						fmt.Printf("- Completed %d transfers so far (elapsed: %d seconds)\n", completed, int(time.Since(startTime).Seconds()))
					}
				}
			}
		})
	}

	fmt.Printf("Waiting for workers to finish (up to %s)...\n", runDuration)
	wg.Wait()

	elapsed := time.Since(startTime)

	if vacuum {
		fmt.Println("Running VACUUM FULL to clean up database")
		Must1(dbconn.Exec(ctx, "VACUUM FULL"))
	} else {
		fmt.Println("Skipping VACUUM FULL ")
	}

	endingSizeBytes, endingSizePretty := dbSize(ctx, dbconn)
	totalCompleted := completedTransfers.Load()

	// Avoid division by zero if no transfers were completed
	bytesPerTransfer := int64(0)
	if totalCompleted > 0 {
		bytesPerTransfer = (endingSizeBytes - startingSizeBytes) / totalCompleted
	}

	fmt.Printf(`
Completed transfers: %d
Elapsed time in seconds: %0.1f
Database size before: %s
Database size after:  %s
Database size growth in bytes: %d
Transfers/second: %0.1f
Milliseconds/transfer: %0.1f
Bytes/transfer: %d
`,
		completedTransfers.Load(),
		elapsed.Seconds(),
		startingSizePretty,
		endingSizePretty,
		endingSizeBytes-startingSizeBytes,
		float64(totalCompleted)/elapsed.Seconds(),
		float64(elapsed.Milliseconds())/float64(totalCompleted)*float64(numWorkers),
		bytesPerTransfer)
}

func Must1[T any](obj T, err error) T {
	if err != nil {
		panic(err)
	}
	return obj
}

func createAccount(ctx context.Context, conn *pgxpool.Pool) string {
	rows := Must1(conn.Query(ctx, "select id from pgledger_create_account('acct', 'USD')"))
	return Must1(pgx.CollectExactlyOneRow(rows, pgx.RowTo[string]))
}

func createTransfer(ctx context.Context, conn *pgxpool.Pool, fromAccountID, toAccountID string) {
	rows, err := conn.Query(ctx, "select id from pgledger_create_transfer($1, $2, $3)", fromAccountID, toAccountID, rand.Uint32())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		panic(err)
	}

	_, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		panic(err)
	}
}

func dbSize(ctx context.Context, conn *pgxpool.Pool) (int64, string) {
	query := "select pg_database_size('pgledger') as size_bytes, pg_size_pretty(pg_database_size('pgledger')) as size_pretty"

	var sizeBytes int64
	var sizePretty string

	err := conn.QueryRow(ctx, query).Scan(&sizeBytes, &sizePretty)
	if err != nil {
		panic(err)
	}

	return sizeBytes, sizePretty
}
