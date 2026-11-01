# Lake table fixtures

Tables written by this release's `write_delta` and `write_iceberg`: two
commits each (rows 1, 2, then 3). They freeze the format the reader must keep
opening when the writer changes; do not regenerate them for a format change.

- `delta-v1`: Delta protocol `minReaderVersion` 1.
- `iceberg-v2`: Iceberg `format-version` 2. `location` was rewritten from the
  generating machine's absolute path to a relative value; the reader resolves
  files from the table root and does not use it.
