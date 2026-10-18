# Listener wire fuzz corpus

This directory holds packet streams that failed the listener fuzz test.

- File name: `<kind>-seed<hex>-i<index>.bin`.
- `kind` is `named`, `stream` or `corpus`.
- Each file is raw bytes the test writes to a fresh MQTT connection.
- The test replays every `.bin` file of at most 64 KiB at start.
- A replayed stream that hangs the connection fails the test until the
  defect is fixed.
- Set `FUZZ_SEED` to repeat a run that printed a seed.
- Run the long pass with `FUZZ_SECONDS=600`. See `tools/wire-fuzz/run.sh`.
