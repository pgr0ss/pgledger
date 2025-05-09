CREATE OR REPLACE FUNCTION uuid_to_ulid(id uuid) RETURNS text AS $$
DECLARE
  encoding   bytea = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  output     text  = '';
  uuid_bytes bytea = uuid_send(id);
BEGIN

  -- Encode the timestamp
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 0) & 224) >> 5));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 0) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 1) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 1) & 7) << 2) | ((GET_BYTE(uuid_bytes, 2) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 2) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 2) & 1) << 4) | ((GET_BYTE(uuid_bytes, 3) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 3) & 15) << 1) | ((GET_BYTE(uuid_bytes, 4) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 4) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 4) & 3) << 3) | ((GET_BYTE(uuid_bytes, 5) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 5) & 31)));

  -- Encode the entropy
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 6) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 6) & 7) << 2) | ((GET_BYTE(uuid_bytes, 7) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 7) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 7) & 1) << 4) | ((GET_BYTE(uuid_bytes, 8) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 8) & 15) << 1) | ((GET_BYTE(uuid_bytes, 9) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 9) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 9) & 3) << 3) | ((GET_BYTE(uuid_bytes, 10) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 10) & 31)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 11) & 248) >> 3));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 11) & 7) << 2) | ((GET_BYTE(uuid_bytes, 12) & 192) >> 6)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 12) & 62) >> 1));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 12) & 1) << 4) | ((GET_BYTE(uuid_bytes, 13) & 240) >> 4)));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 13) & 15) << 1) | ((GET_BYTE(uuid_bytes, 14) & 128) >> 7)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 14) & 124) >> 2));
  output = output || CHR(GET_BYTE(encoding, ((GET_BYTE(uuid_bytes, 14) & 3) << 3) | ((GET_BYTE(uuid_bytes, 15) & 224) >> 5)));
  output = output || CHR(GET_BYTE(encoding, (GET_BYTE(uuid_bytes, 15) & 31)));

  RETURN output;
END
$$
LANGUAGE plpgsql
IMMUTABLE;