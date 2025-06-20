dbname := "pgledger"

MODERNIZE_CMD := "golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest"

psql:
  docker compose exec postgres env PGPASSWORD={{dbname}} psql -U {{dbname}} {{dbname}}

dbclean:
  docker compose exec postgres dropdb --force -U {{dbname}} {{dbname}} || echo "db doesn't exist"
  docker compose exec postgres createdb -U {{dbname}} {{dbname}}

dbload:
  docker compose exec --no-TTY postgres psql \
    -U {{dbname}} \
    --single-transaction \
    -f /code/vendor/scoville-pgsql-ulid/ulid-to-uuid.sql \
    -f /code/vendor/scoville-pgsql-ulid/uuid-to-ulid.sql \
    -f /code/pgledger.sql \
    {{dbname}}

dbreset: dbclean dbload

clean:
  cd go && go clean -testcache

tidy:
  cd go && go mod tidy

test:
  cd go && go test -v ./...

benchmark:
  cd go/test && go test -bench=. -benchtime=10s

performance_check duration='10s':
  cd go && go run performance_check.go --duration {{duration}}

lint: deadcode modernize lint-sql

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

modernize:
  cd go && go run {{MODERNIZE_CMD}} -test ./...

lint-sql:
  uvx sqlfluff lint --verbose

format-sql:
  uvx sqlfluff format

check: dbreset dbload clean tidy test lint
