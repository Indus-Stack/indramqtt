# Wire codec fuzz corpus

This directory holds inputs that failed the wire codec fuzz test.

- File name: `<kind>-seed<hex>-i<index>.bin`.
- `kind` is `panic`, `roundtrip`, `corpus` or `connack`.
- The test replays every `.bin` file of at most 64 KiB at start.
- A replayed input that panics fails the test until the defect is fixed.
- Set `FUZZ_SEED` to repeat a run that printed a seed.
- Run the long pass with `FUZZ_SECONDS=600`. See `tools/wire-fuzz/run.sh`.
