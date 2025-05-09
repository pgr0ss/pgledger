# pgsql-ulid
Postgres helper functions for converting between [ULID](https://github.com/ulid/spec) and UUID.

Your application layer may deal exclusively with transformation of ULIDs, so what do you do when you need to query your DB for something? That's where these handy functions come in!

## Example
https://dbfiddle.uk/?rdbms=postgres_9.6&fiddle=e55e8cb2541a73034bf573f7318b40c5
## Usage

`ulid_to_uuid(ulid text) RETURNS uuid`

Takes a ULID string and returns a UUID.
```SQL
SELECT ulid_to_uuid('01FGB414J8PPBVHBMHGSXGS21C');

-- 017c1640-9248-b597-b8ae-91867b0c882c
```

`uuid_to_ulid(id uuid) RETURNS text`

Takes a UUID and returns a ULID string.
```SQL
SELECT uuid_to_ulid('017c1640-9248-b597-b8ae-91867b0c882c');

-- 01FGB414J8PPBVHBMHGSXGS21C
```

`parse_ulid(ulid text) RETURNS bytea`

Takes a ULID string and returns a Byte Array. In case you need the raw bytes for something else.

## Inspirations
- https://github.com/geckoboard/pgulid - Provides generation of ULIDs in Postgres.
- https://github.com/oklog/ulid - Logic for transforming ULID text into bytes.