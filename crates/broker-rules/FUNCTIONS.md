# Standard rule SQL built-in functions (RQ-03)

Inventory of the standard rule dialect's documented function library and how
each function is served on top of the embedded streaming-SQL engine. Read
with `crates/broker-rules/src/stdlib.rs`, which implements every `new` and
every non-colliding `adapter` row through the engine's public user-function
registry. Names below are the dialect's API surface and must match exactly.

How to read the `served` column:

- `existing` — a streaming-engine built-in with identical name, arity and
  semantics. No code in `stdlib.rs`.
- `adapter` — a streaming-engine built-in covers it with a different name,
  argument order, arity or edge-case result. `stdlib.rs` registers a wrapper
  under the standard name when the name does not collide with a built-in;
  colliding rows (same name, different semantics) document the divergence
  and what the engine must expose so the wrapper can take effect.
- `new` — no streaming-engine equivalent. `stdlib.rs` implements it.
- `deferred` — out of the MVP per the task (schema-registry family).

General semantics (apply to every row unless the row says otherwise):

- Wrong argument count or wrong types return `Null` (fail closed, never a
  thrown error on the publish path).
- Empty input handling is listed per function; `Null` in means `Null` out.
- String results are UTF-8 JSON strings. Compressed or otherwise arbitrary
  byte results are base64-encoded so they stay valid JSON strings; the
  matching decompress function accepts that base64 form. This is an
  `adapter` encoding choice documented per row.
- `TODO(parity):` marks open questions where the rulebook did not decide a
  case. The implementation chooses the conservative behaviour and keeps
  moving.

## 1. Context / message accessors

| function | served | notes |
|---|---|---|
| `msgid()` | new | Message id from record meta (`id`, `msgid`, `__meta__.id`); absent means `Null`. |
| `qos()` | new | QoS (0/1/2) from record meta; absent means `Null`. |
| `flags()` | new | Flags object from record meta; absent means empty object. |
| `flag(name)` | new | Single flag by name; absent means `Null`. |
| `topic()` | new | Publish topic from record meta; absent means `Null`. |
| `topic(n)` | new | 1-based topic level; out-of-range means `Null`. TODO(parity): exact behaviour for empty levels and trailing slashes. |
| `clientid()` | new | Client id from record meta; absent means `Null`. |
| `clientip()` | new | Same as `peerhost()` (alias). |
| `peerhost()` | new | Peer address from record meta; absent means `Null`. |
| `username()` | new | Username from record meta; absent means `Null`. |
| `payload()` | new | Raw payload: record `payload` field when present, else the whole record as JSON. |
| `payload(path)` | new | Dotted path into the JSON-decoded payload; non-JSON payload or missing path means `Null`. |
| `contains_topic(filters, topic)` | new | Exact topic equality against a filter list. |
| `contains_topic(filters, topic, qos)` | new | As above plus QoS equality. |
| `contains_topic_match(filters, topic)` | new | Wildcard (`+`/`#`) match against a filter list. |
| `contains_topic_match(filters, topic, qos)` | new | As above plus QoS equality. |
| `null()` | new | Returns `Null`. |
| `coalesce(...)` | existing | First non-null argument. Engine built-in is identical. |
| `coalesce_ne(...)` | new | First non-null, non-empty-string argument. TODO(parity): whether `0`/`false` count as empty (they do not here). |

Arithmetic operators (`+ - * / div mod =`) are language syntax lowered by the
front end (RQ-02), not function calls. `stdlib.rs` still provides `div/2`
(integer division, `Null` on divide-by-zero) and `fmod/2` (float remainder)
as `new` helpers for the lowering to target.

## 2. Math

| function | served | notes |
|---|---|---|
| `abs(n)` | existing | Identical. |
| `acos(n)` | existing | Identical (`Null` outside `[-1,1]` per engine). |
| `acosh(n)` | new | Inverse hyperbolic cosine; `Null` for `n < 1`. |
| `asin(n)` | existing | Identical. |
| `asinh(n)` | new | Inverse hyperbolic sine. |
| `atan(n)` | existing | Identical. |
| `atanh(n)` | new | Inverse hyperbolic tangent; `Null` outside `(-1,1)`. |
| `ceil(n)` | existing | Identical. |
| `cos(n)` | existing | Identical (radians). |
| `cosh(n)` | existing | Identical. |
| `exp(n)` | existing | Identical. |
| `floor(n)` | existing | Identical. |
| `fmod(x, y)` | new | Float remainder (`x - trunc(x/y)*y`); `Null` on zero divisor or non-numbers. Integer `mod/2` stays on the existing engine `mod`. |
| `log(n)` | existing | Natural log, 1-arg form identical. |
| `log10(n)` | existing | Identical. |
| `log2(n)` | existing | Identical. |
| `power(x, y)` | existing | Identical (`pow` is the engine alias). |
| `round(n)` | existing | 1-arg form identical. |
| `sin(n)` | existing | Identical. |
| `sinh(n)` | existing | Identical. |
| `sqrt(n)` | existing | Identical (`Null` for negatives). |
| `tan(n)` | existing | Identical. |
| `tanh(n)` | existing | Identical. |

## 3. Bitwise

| function | served | notes |
|---|---|---|
| `bitnot(i)` | existing | Identical. |
| `bitand(x, y)` | existing | Identical. |
| `bitor(x, y)` | existing | Identical. |
| `bitxor(x, y)` | existing | Identical. |
| `bitsl(x, n)` | new | Arithmetic left shift; `Null` on wrong types. Negative shift means `Null` (fail closed). TODO(parity): whether negative shifts shift right. |
| `bitsr(x, n)` | new | Arithmetic right shift; same failure rule as `bitsl`. |

## 4. Bit and byte

| function | served | notes |
|---|---|---|
| `bitsize(b)` | new | Bit length of a string (bytes × 8); non-strings mean `Null`. |
| `bytesize(b)` | new | Byte length of a string; non-strings mean `Null`. (Engine `numbytes` is the same measure under another name.) |
| `subbits(b, len)` | new | First `len` bits of a string as binary string; see `subbits/3..6`. |
| `subbits(b, start, len)` | new | 1-based bit slice; out-of-range start means `Null`, over-long length clamps. TODO(parity): `type/signedness/endianness` variants accept `integer/float/bits × signed/unsigned × big/little`; float decoding beyond 64 bits means `Null`. |
| `subbits(b, start, len, type)` | new | As above with explicit type. |
| `subbits(b, start, len, type, signedness)` | new | As above with explicit signedness. |
| `subbits(b, start, len, type, signedness, endianness)` | new | Full form. Integer results are numbers; `float`/`bits` results are base64 strings (JSON-safe). |

## 5. Type conversion

| function | served | notes |
|---|---|---|
| `str(v)` | new | Any value to string (numbers/bools rendered, arrays/objects as JSON, `Null` means `Null`). TODO(parity): exact rendering of nested values (JSON here). |
| `str_utf8(v)` | new | Same as `str` (JSON strings are UTF-8 by construction). |
| `str_utf16_le(v)` | new | UTF-16LE bytes of the string, base64-encoded (raw UTF-16LE is not valid UTF-8 and cannot sit in a JSON string). TODO(parity): wire representation. |
| `bool(v)` | new | `true`/`false` stay; `1`/`0`, `"true"`/`"false"` convert; else `Null`. TODO(parity): full truthiness table. |
| `int(v)` | new | Truncating conversion (floats truncate toward zero, numeric strings parse, bools map to 1/0); else `Null`. |
| `float(v)` | new | Numeric conversion; numeric strings parse; bools map to 1.0/0.0; else `Null`. |
| `float(v, decimals)` | new | As above rounded to `decimals` places. |
| `float2str(f, precision)` | new | Fixed-precision rendering; non-numbers mean `Null`. |
| `map(v)` | new | JSON string to object, list of pairs to object, object identity; else `Null`. |
| `bin2hexstr(b)` | new | Upper-case hex without prefix. |
| `bin2hexstr(b, prefix)` | new | Upper-case hex with a binary prefix prepended. |
| `hexstr2bin(s)` | new | Hex string to UTF-8 string bytes; malformed means `Null`. |
| `hexstr2bin(s, prefix)` | new | As above after stripping the required prefix; missing prefix means `Null`. |
| `sqlserver_bin2hexstr(b)` | new | `bin2hexstr` with `0x` prefix (alias). |

## 6. Type checks

| function | served | notes |
|---|---|---|
| `is_null(v)` | new | `true` only for `Null`/`undefined`. Engine `isnull` is the same predicate under another name. |
| `is_null_var(v)` | new | `true` for `Null` and JSON `null`. |
| `is_not_null(v)` | new | Negation of `is_null`. |
| `is_not_null_var(v)` | new | Negation of `is_null_var`. |
| `is_str(v)` | new | Strings only. Engine `isnumeric`/`isnan` cover numbers; string predicate is new. |
| `is_bool(v)` | new | Booleans only. |
| `is_int(v)` | new | Integers only (floats are not ints). |
| `is_float(v)` | new | Floats only. |
| `is_num(v)` | new | Any number. |
| `is_map(v)` | new | Objects only. |
| `is_array(v)` | new | Arrays only. |
| `is_empty(v)` | new | `""`, `[]`, `{}` mean empty; `Null` means `true`. TODO(parity): whether `0`/`false` count (they do not here). |

## 7. Strings

| function | served | notes |
|---|---|---|
| `lower(s)` | existing | Identical. |
| `ltrim(s)` | existing | 1-arg form identical (whitespace). |
| `reverse(s)` | existing | Identical. |
| `rtrim(s)` | existing | 1-arg form identical; 2-arg char-strip form is `new` below. |
| `rtrim(s, chars)` | new | Strip trailing chars in `chars`. |
| `rm_prefix(s, prefix)` | new | Remove prefix on match, else input unchanged. |
| `strlen(s)` | adapter | Engine `length` is the same measure under another name. Registered under the standard name (no collision with the array `length/1` row only if the engine resolves by static type; see colliding-name note below). |
| `substr(s, start)` | adapter | Collides with engine `substr` (1-based). Standard indexing here follows the documented 0-based slice; the wrapper implements the standard form. Requires the engine to consult registered functions before built-ins (see §11). |
| `substr(s, start, len)` | adapter | Same as above with explicit length. |
| `trim(s)` | existing | 1-arg form identical. |
| `upper(s)` | existing | Identical. |
| `split(s, sep)` | adapter | Collides with engine `split/2`. Standard form drops empty segments; the wrapper implements that. Same dispatch note as `substr`. |
| `split(s, sep, opt)` | new | `notrim`/`leading`/`trailing` (+ `*_notrim`) segment control. |
| `concat(a, b)` | adapter | Collides with engine variadic `concat`. 2-arg form concatenates two values as strings. Same dispatch note as `substr`. |
| `concat(list)` | adapter | 1-arg list form. Same dispatch note as `substr`. |
| `tokens(s, seps)` | new | Split on any separator char, dropping empties. |
| `tokens(s, seps, nocrlf)` | new | As above additionally splitting on CR/LF. |
| `sprintf_s(fmt, args)` | new | `~s`/`~p`-style formatting over an arg list. TODO(parity): full format-spec coverage (this implements `~s`, `~p`, `~w`, `~n` and `~` escapes). |
| `pad(s, len)` | new | Right-pad (space) to `len`; longer inputs unchanged. Bounded by §11 cap. |
| `pad(s, len, side)` | new | `leading`/`trailing`/`both` sides. |
| `pad(s, len, side, char)` | new | As above with explicit pad char. |
| `replace(s, pat, rep)` | existing | 3-arg form identical. |
| `replace(s, pat, rep, where)` | new | `leading`/`trailing`/`all` occurrence control. |
| `regex_match(s, re)` | adapter | Engine `regexp_matches` is the same predicate under another name. |
| `regex_replace(s, re, rep)` | adapter | Engine `regexp_replace` is the same operation under another name. |
| `regex_extract(s, re)` | adapter | Engine `regexp_substring` returns the first match; the standard form returns the captured groups list. Wrapper implements the group list. |
| `ascii(c)` | new | First byte/char code; empty means `Null`. Inverse of engine `chr`. |
| `find(s, pat)` | new | Leading search; returns the matched tail or `""` when absent. |
| `find(s, pat, dir)` | new | `leading`/`trailing` direction. |
| `join_to_string(list)` | new | Comma-space join of stringified elements. |
| `join_to_string(sep, list)` | new | Explicit-separator join. (Engine `array_join` is the same idea with reversed argument order.) |
| `map_to_redis_hset_args(m)` | new | Redis HSET field list (`[marker, k1, v1, ...]`); non-string keys dropped, floats rendered with 6 compact decimals. |
| `join_to_sql_values_string(list)` | new | SQL value tuple `(a, b)` quoting strings with single quotes. TODO(parity): exact quoting/escaping rules. |
| `jq(prog, json)` | new | JSON projection. Implements the `.`-rooted dot-path subset (`.`, `.a`, `.a.b`) returning a list; anything else returns an empty list fail-closed. TODO(parity): full jq language coverage. |
| `jq(prog, json, timeout_ms)` | new | As above with an accepted-and-ignored timeout (subset evaluates without blocking). |
| `unescape(s)` | new | `\\n \\t \\r \\b \\f \\v \\' \\" \\? \\\\` and `\\xHH` escapes. Unknown escapes mean `Null` (fail closed). |

## 8. Maps

| function | served | notes |
|---|---|---|
| `map_new()` | new | Empty object. |
| `map_get(k, m)` | new | Dotted-path get; missing means `Null`. |
| `map_get(k, m, dflt)` | new | As above with default. |
| `map_put(k, v, m)` | new | Dotted-path put (creates intermediate objects). |
| `map_keys(m)` | adapter | Engine `keys` is the same operation under another name. |
| `map_values(m)` | adapter | Engine `values` is the same operation under another name. |
| `map_to_entries(m)` | new | `[{key, value}]` pair list. |
| `map_size(m)` | new | Key count; non-objects mean `Null`. |
| `mget(k, m)` / `mget(k, m, dflt)` | new | Flat (non-dotted) get with atom/binary key equivalence approximated by exact match; kept for backward compatibility. |
| `mput(k, v, m)` | new | Flat put; kept for backward compatibility. |

## 9. Arrays

| function | served | notes |
|---|---|---|
| `nth(n, list)` | new | 1-based; out-of-range means `""` per the documented behaviour; non-lists mean `Null`. |
| `length(list)` | adapter | Same name as the engine string `length/1`. Array form returns element count. Requires type-directed dispatch (see §11). |
| `sublist(len, list)` | new | First `len` elements. |
| `sublist(start, len, list)` | new | 1-based slice; clamps like the engine `array_slice`. |
| `first(list)` | new | Head element; empty or non-list means `Null` (fail closed; the reference raises). TODO(parity): raise-vs-`Null`. |
| `last(list)` | new | Last element; same failure rule as `first`. |
| `contains(elm, list)` | adapter | Engine `array_contains` is the same predicate with reversed argument order. |

## 10. Hash, compression, encoding, dates, UUID/system, state

| function | served | notes |
|---|---|---|
| `md5(s)` | existing | Identical (lower-case hex). |
| `sha(s)` | adapter | SHA-1 under the historical name; engine `sha1` is the same digest. |
| `sha256(s)` | existing | Identical. |
| `hash(type, data)` | new | `md5/sha/sha1/sha224/sha256/sha384/sha512` dispatch (lower-case hex); unknown algorithm means `Null`. TODO(parity): `sha3_*`/`blake2*` families (no digest crate wired; fail closed). |
| `hash_to_range(v, min, max)` | new | SHA-256 hex mapped into `[min, max]`; bad range means `Null`. |
| `map_to_range(v, min, max)` | new | Integer (or binary bytes as unsigned) mapped into `[min, max]`; bad range means `Null`. |
| `zip(b)` | new | zlib-compressed bytes, base64-encoded (JSON-safe). See §11 encoding note. |
| `unzip(b)` | new | Inverse of `zip` (accepts the base64 form; also tries raw bytes). |
| `gzip(b)` | new | gzip-compressed bytes, base64-encoded. Requires the `flate2` crate (already locked). |
| `gunzip(b)` | new | Inverse of `gzip`. |
| `zip_compress(b)` | new | zlib-compressed bytes, base64-encoded (same wire form as `zip`; kept as a separate name for compatibility). TODO(parity): exact `zip` vs `zip_compress` framing nuance. |
| `zip_uncompress(b)` | new | Inverse of `zip_compress`. |
| `lz4_compress(b)` | new | LZ4-frame-compressed bytes, base64-encoded. Requires the `lz4_flex` crate (already locked). |
| `lz4_uncompress(b)` | new | Inverse of `lz4_compress`. |
| `base64_encode(b)` | existing | 1-arg form identical. |
| `base64_encode(b, o1)` / `base64_encode(b, o1, o2)` | adapter | Collide with the 1-arg built-in. `no_padding`/`urlsafe` options per the documented forms. Same dispatch note as `substr`. |
| `base64_decode(s)` | existing | 1-arg form identical (valid UTF-8 only, else `Null`). |
| `base64_decode(s, o1)` / `base64_decode(s, o1, o2)` | adapter | Same options as encode. Same dispatch note as `substr`. |
| `json_encode(v)` | adapter | Engine `to_json` is the same operation under another name. |
| `json_decode(s)` | adapter | Engine `parse_json` is the same operation under another name. |
| `term_encode(t)` | new | JSON bytes of the term (the Erlang external-term framing has no JSON equivalent). TODO(parity): cross-runtime term compatibility. |
| `term_decode(b)` | new | Inverse of `term_encode` (JSON parse; `safe`-style: atoms do not leak into the host). |
| `now_rfc3339()` | new | Current UTC time, RFC-3339 seconds precision with `Z`. |
| `now_rfc3339(unit)` | new | As above with `second`/`millisecond`/`microsecond`/`nanosecond` precision. |
| `unix_ts_to_rfc3339(epoch)` | new | Seconds to RFC-3339. |
| `unix_ts_to_rfc3339(epoch, unit)` | new | With explicit `second`/`millisecond`/`microsecond`/`nanosecond` unit. |
| `rfc3339_to_unix_ts(s)` | new | RFC-3339 to seconds; malformed means `Null`. |
| `rfc3339_to_unix_ts(s, unit)` | new | With explicit output unit. |
| `now_timestamp()` | new | Current epoch seconds. |
| `now_timestamp(unit)` | new | With explicit unit. |
| `format_date(unit, offset, fmt)` | new | Current time formatted (`yyyy/MM/dd HH:mm:ss` tokens). TODO(parity): full token table. |
| `format_date(unit, offset, fmt, epoch)` | new | As above for an explicit epoch value. |
| `date_to_unix_ts(unit, fmt, s)` | new | Parse formatted date to epoch; malformed means `Null`. |
| `date_to_unix_ts(unit, offset, fmt, s)` | new | As above with explicit offset. |
| `timezone_to_second(tz)` | new | Alias of `timezone_to_offset_seconds`. |
| `timezone_to_offset_seconds(tz)` | new | `Z`/`UTC`/`+HH:MM` offsets to seconds; unknown zones mean `Null`. TODO(parity): named-zone database (unsupported here; fail closed). |
| `mongo_date()` / `mongo_date(ts)` / `mongo_date(ts, unit)` | new | RFC-3339 string of the timestamp (the tuple-to-BSON conversion lives in the connector, not the rule). TODO(parity): BSON date typing. |
| `getenv(name)` | new | Process environment lookup; absent means `""`. Reads `std::env` directly (no cache). TODO(parity): vendor prefixing and caching policy. |
| `random()` | adapter | Engine `rand` is the same source under another name (`[0,1)` float). |
| `uuid_v4()` | adapter | Engine `uuid` is the same value under another name (hyphenated lower-case). |
| `uuid_v4_no_hyphen()` | new | Same UUID without hyphens. |
| `proc_dict_get(k)` | new | Rule-worker-local slot; missing means `Null`. Bounded by §11 cap (fail closed past it). |
| `proc_dict_put(k, v)` | new | Set slot; returns the stored value. |
| `proc_dict_del(k)` | new | Delete slot; returns the removed value or `Null`. |
| `kv_store_get(k)` / `kv_store_get(k, dflt)` | new | Node-global slot; same bound as `proc_dict`. |
| `kv_store_put(k, v)` | new | Set global slot; returns the stored value. |
| `kv_store_del(k)` | new | Delete global slot; returns the removed value or `Null`. |
| `schema_encode(...)` | deferred | Schema-registry family, out of the MVP per the task. |
| `schema_decode(...)` | deferred | Schema-registry family, out of the MVP per the task. |

## 11. Colliding names, bounds and allocation

Colliding `adapter` rows (`substr`, `split`, `concat`, `length`,
`base64_encode/2,3`, `base64_decode/2,3`, `rtrim/2`) share a name with a
built-in whose arity or edge semantics differ. The engine resolves known
names before consulting registered functions, so a same-name wrapper
registered from outside cannot take effect on its own. This is served
from outside the engine crate: `crates/broker-rules/src/lib.rs`
`rewrite_colliding_functions` rewrites those names to internal `std_*`
aliases before parsing, and `stdlib.rs` registers both spellings, so SQL
evaluation uses the standard semantics with no engine change. Direct
registry calls work under either spelling.

Bounds (finite by construction; every default states its reason inline in
`stdlib.rs` next to the constant):

- `MAX_STDLIB_STRING_BYTES = 1_048_576` (1 MiB): caps any single string UDF
  result so attacker-controlled `pad`/`split` inputs cannot OOM the publish
  path; larger requests return `Null` fail-closed.
- `MAX_STDLIB_ARRAY_ELEMS = 10_000`: caps any single array UDF result for
  the same reason.
- `MAX_STDLIB_KV_ENTRIES = 10_000`: caps the `proc_dict`/`kv_store` maps;
  puts past the cap return `Null` and are counted in logs, never evicted
  silently.

Allocation: string/array/object results allocate exactly once for the value
they return, matching the engine equivalents (which also materialise
`Value::String`/`Array`/`Object`). Numeric/boolean/`Null` paths allocate
nothing. Regex helpers compile the pattern per call (one allocation) because
there is no shared pattern cache on the publish path; a bounded cache is the
`PERF(parity)` follow-up and must not add a lock to the hot path without
before/after numbers.
