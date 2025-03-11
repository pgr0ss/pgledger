set dotenv-path := ".env"

psql:
  docker compose exec postgres psql $DATABASE_URL

dbclean:
  docker compose exec postgres dropdb --force --maintenance-db $MAINTENANCE_URL $DBNAME || echo "db doesn't exist"
  docker compose exec postgres createdb --maintenance-db $MAINTENANCE_URL $DBNAME

dbload:
  docker compose exec --no-TTY postgres psql $DATABASE_URL --single-transaction -f /code/pgledger.sql

dbreset: dbclean dbload

clean:
  cd test && go clean -testcache

test:
  cd test && go test -v

benchmark:
  cd test && go test -bench=. -benchtime=10s

lint: deadcode modernize

deadcode:
  #!/usr/bin/env bash
  set -euo pipefail
  out=$(cd test && go tool deadcode -test ./...)
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
  cd test && go tool modernize -test ./...

check: dbreset dbload clean test lint
