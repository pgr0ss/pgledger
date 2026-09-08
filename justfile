set dotenv-load

dbname := "pgledger"

psql:
    docker compose exec postgres env PGPASSWORD={{ dbname }} psql -U {{ dbname }} {{ dbname }}

dbversion:
    docker compose exec postgres env PGPASSWORD={{ dbname }} psql -U {{ dbname }} {{ dbname }} -c 'select version();'

dbclean:
    docker compose exec postgres dropdb --force -U {{ dbname }} {{ dbname }} || echo "db doesn't exist"
    docker compose exec postgres createdb -U {{ dbname }} {{ dbname }}

dbload:
    docker compose exec --no-TTY postgres psql \
      -U {{ dbname }} \
      --single-transaction \
      -f /code/vendor/scoville-pgsql-ulid/ulid-to-uuid.sql \
      -f /code/vendor/scoville-pgsql-ulid/uuid-to-ulid.sql \
      -f /code/pgledger.sql \
      {{ dbname }}

dbreset: dbclean dbload

clean:
    cd go && go clean -testcache

tidy:
    cd go && go mod tidy

test:
    cd go && go test -v ./test/

# -count=1 disables Go's test cache
property-tests:
    cd go && go test -v -count=1 ./propertytest/

property-tests-continuous duration='60s':
    cd go && PGLEDGER_SOAK_DURATION={{ duration }} go run ./propertytest/continuous

benchmark:
    cd go/test && go test -bench=. -benchtime=10s

performance_check duration='10s':
    cd go && go run performance_check.go --duration {{ duration }}

lint: deadcode lint-sql golangci-lint

deadcode:
    #!/usr/bin/env bash
    set -euo pipefail
    out=$(cd go && go tool deadcode -test ./...)
    echo "$out"
    if [[ $? != 0 ]]; then
      exit $?
    elif [[ $out ]]; then
      echo "Failing due to deadcode output"
      exit 1
    else
        echo "No dead code"
    fi

golangci-lint:
    cd go && golangci-lint run --verbose

lint-sql:
    sqlfluff lint --verbose

format-sql:
    sqlfluff format

check: dbreset clean tidy format-sql test lint

run-examples: dbreset
    #!/usr/bin/env bash
    set -euo pipefail

    for file in $(ls examples/*.sql); do
      out="${file}.out"

      echo '-- This file contains the sql queries plus their output, but we set the filetype to sql for better syntax highlighting' > $out
      echo "-- vim: set filetype=sql:" >> $out
      echo >> $out

      cat "$file" | \
        docker compose exec --no-TTY postgres psql -U pgledger --echo-all --no-psqlrc \
        >> $out 2>&1
    done
