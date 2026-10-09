# API conformance baseline — 2026-09-19 (T-14)

First live run of `tests/api_conformance` against a real kernel. This is the
number to improve against. No broker code was changed for this baseline.

## How it was produced

- Spec: the reference specification.0 OpenAPI, fetched live from the reference container
  `test container` (`POST /api/v5/login` with the disposable test password
  documented in `team/parity/REFERENCE.md`, then `GET /api-docs/swagger.json`;
  296 paths). The spec file lives outside the repo and is passed by path only:
  run with `--spec <path-to-openapi.json>`.
- Kernel: release `indramqtt` built from this tree at `cdf7530`, plus the
  `beam/ebin` edge from the same tree, both built in `test container`.
- Stack: fresh remote host container `test container` (removed after the run) —
  kernel `--api-bind 0.0.0.0:28083 --brokerlink-bind 127.0.0.1:28883
  --allow-anonymous --data-dir /opt/indramqtt/data` (fresh data dir),
  edge `mqtt_port 11883 kernel_port 28883`.
  Reached from the run host as API `100.104.51.36:13484`, MQTT
  `100.104.51.36:13483`.
- Stack prep before the run: `POST /api/v5/users/admin/change_pwd`
  `{old_pwd: public, new_pwd: Adm1n-test-pass!}` (admin self-change; the
  checker refuses to run while `must_change_password` is true). The
  `POST /login` case in `cases/users.json` uses that stack password, which
  matches the value already used by the repo's own e2e tests.
- Command (from the repo root):
  `python -m tests.api_conformance --spec <openapi.json> --base
  http://100.104.51.36:13484/api/v5 --user admin --password Adm1n-test-pass!
  --allow-mutations --cases tests/api_conformance/cases --exclude-tags
  "Plugins,AI Completion,A2A Registry" --mqtt 100.104.51.36:13483
  --report tests/api_conformance/baseline-2026-09-19.json`
- Raw report: `tests/api_conformance/baseline-2026-09-19.json` (420 rows).

## Headline

Spec operations after tag exclusion: 420. Exercised: 141
(31 pass, 11 fail, 99 not-implemented). Skipped for want of a case: 279
(these are endpoints the kernel does not implement). No auth errors.

## Summary by tag

| tag | pass | fail | not-impl | skipped |
|---|---|---|---|---|
| API Keys | 0 | 0 | 2 | 4 |
| Actions | 0 | 2 | 2 | 9 |
| Alarms | 0 | 0 | 1 | 2 |
| Audit | 0 | 0 | 1 | 0 |
| Authentication | 4 | 0 | 3 | 12 |
| Authorization | 0 | 0 | 7 | 23 |
| Auto Subscribe | 0 | 0 | 1 | 1 |
| Banned | 0 | 0 | 1 | 3 |
| Clients | 6 | 2 | 1 | 5 |
| Cluster | 0 | 0 | 4 | 10 |
| CoAP Gateways | 0 | 0 | 0 | 1 |
| Configs | 0 | 0 | 10 | 11 |
| Connectors | 5 | 1 | 0 | 3 |
| Dashboard | 5 | 3 | 0 | 3 |
| Dashboard Single Sign-On | 0 | 0 | 4 | 9 |
| Data Backup | 0 | 0 | 1 | 5 |
| Durable storage | 0 | 0 | 2 | 7 |
| Error Codes | 0 | 0 | 1 | 1 |
| ExHook | 0 | 0 | 1 | 6 |
| Features | 0 | 0 | 1 | 0 |
| File Transfer | 0 | 0 | 3 | 2 |
| Gateway Authentication | 0 | 0 | 0 | 11 |
| Gateway Clients | 0 | 0 | 0 | 6 |
| Gateway Listeners | 0 | 0 | 0 | 14 |
| Gateways | 0 | 0 | 1 | 3 |
| License | 0 | 0 | 3 | 2 |
| Listeners | 0 | 0 | 2 | 8 |
| Load Rebalance | 0 | 0 | 3 | 4 |
| LwM2M Gateways | 0 | 0 | 0 | 4 |
| MQTT | 0 | 0 | 5 | 14 |
| Message Queue | 0 | 0 | 2 | 5 |
| Message Stream | 0 | 0 | 2 | 5 |
| Message Transformation | 0 | 0 | 1 | 9 |
| Metrics | 2 | 0 | 2 | 3 |
| Monitor | 0 | 0 | 9 | 2 |
| Multi-tenancy | 0 | 0 | 4 | 12 |
| Node Eviction | 0 | 0 | 1 | 0 |
| Nodes | 0 | 2 | 0 | 2 |
| Opentelemetry | 0 | 0 | 0 | 3 |
| Publish | 1 | 0 | 0 | 0 |
| Retainer | 0 | 0 | 2 | 4 |
| Rules | 7 | 0 | 2 | 3 |
| Schema Registry | 0 | 0 | 2 | 10 |
| Schema Validation | 0 | 0 | 1 | 8 |
| Slow Subscriptions | 0 | 0 | 2 | 2 |
| Sources | 0 | 0 | 3 | 10 |
| Status | 0 | 0 | 1 | 0 |
| Subscriptions | 0 | 1 | 0 | 0 |
| TLS Management | 0 | 0 | 1 | 7 |
| Telemetry | 0 | 0 | 2 | 1 |
| Topics | 1 | 0 | 0 | 0 |
| Trace | 0 | 0 | 2 | 8 |

## The 11 failures (all response-shape/status-code, no crash or hang)

- `GET /action_types` (200): our list has `kafka`, spec enum has
  `kafka_producer` (plus many bridge types). Vocabulary mismatch.
- `POST /actions_probe`, `POST /connectors_probe` (400): our error code is
  `CONNECTION_FAILED`, spec documents `TEST_FAILED`.
- `GET /clients/{clientid}/mqueue_messages` (200): our `meta` lacks the
  spec-required `start` property.
- `POST /clients/{clientid}/subscribe` (201): spec documents only 200/400-ish;
  201 is "not documented". Same class: `POST /users` returns 201 where the
  spec documents 200, and `POST /users/{username}/change_pwd` returns 200
  where the spec documents 204.
- `GET /nodes`, `GET /nodes/{node}` (200): `load1`/`load5`/`load15` are
  strings, spec wants numbers.
- `GET /schemas/{name}` (200): ours returns an object, spec wants a string.
- `GET /subscriptions` (200): ours returns `{data, meta}`, spec wants an
  array.

## What passed (31)

Login, dashboard users CRUD + self/other password change, authn
built-in-database users CRUD + list, client list/get/kick (single + bulk),
client subscriptions get/subscribe/unsubscribe, publish, topics list, rules
CRUD + metrics + metrics reset, connectors CRUD + list, stats, monitor_current,
user_scopes, nodes list is exercised but shape-fails (see above).

## Honest limits of this baseline

- 279 operations are still skipped for want of a case; they are endpoints the
  kernel does not implement, so a case would only record `not-implemented`.
  Cases were added for every spec operation the kernel does implement (29 new
  cases in 7 files; the 2 pre-existing case files cover the other 2), except
  `POST /logout`, which is deliberately uncased: our logout revokes the
  checker's bearer token, so exercising it would turn every later operation
  into an auth error.
- Rule cases chain on one shared fixture: `POST /rules` leaves `conf-rule-1`
  behind and the `{id}` cases resolve `$from:GET /rules#/data/0/id` (envelope
  path — `#/0/id` does not work because our list nests under `data`). Each
  `{id}` case setup creates its own rule first, so reruns keep working, but
  rules accumulate (~6 per run) on a disposable stack.
- Connector cases delete-then-create `conf-conn-1` in setup so reruns do not
  stack duplicates (see defect note below).
- The checker validates only status codes and JSON response schemas; it does
  not judge behaviour (e.g. whether a kick really disconnected a client).

## Defect note (not fixed — baseline task is read-only on broker code)

While iterating, connector setups posted the same name three times. The
in-memory store accepted the duplicates, but from then on every connector
mutation (POST a new connector, PUT, DELETE) returned 500
`cannot persist connectors: connectors.connectors[1].id "conf-conn-1" is
duplicated (field 'id')` — i.e. duplicate ids poison connector persistence
and fail closed on all later mutations. Repro on a fresh stack: create the
same connector name twice, then any connector mutation 500s. The committed
cases avoid tripping this (delete-before-create in setup); the second live
run above (31 pass) is unaffected by it.

## Known defects of the specification

These rows fail the checker because the specification is wrong. The broker
follows the real behaviour that the management console proves. The checker
is not changed.

- `GET /api_key`, response 200. The specification declares one JSON object
  (the schema of one key record). The management console reads a bare
  array: `const data: APIKey[] = await http.get('/api_key')` in the API module of the console (`loadAPIKeyList`),
  and it maps over that array. The broker returns a bare array of key
  records, so the checker reports a type error for this row.

- `GET /audit`, response 200. The specification declares an array whose
  items are objects with the keys `data` and `meta`. The management
  console reads one object with `data` and `meta`:
  `queryAuditLogs` in the API module of the console returns
  `ListDataWithPagination<AuditLogItem>`, and the audit page reads
  `res.data`. The broker returns one object with `data` and `meta`, so the
  checker reports a type error for this row.
- `GET /rule_events`, response 200. The specification declares one object
  (the schema of one event). The management console reads an array of
  events: `const events: RuleEvent[] = await http.get('/rule_events')` in
  the rule input selector, and it filters and maps that array. The broker
  returns a bare array of event records, so the checker reports a type
  error for this row.
- `GET /message_transformations`, response 200. The specification declares
  an array of arrays of transformations. The management console reads a
  flat array: `getMessageTransforms` returns `MessageTransform[]`. The
  broker returns a flat array of transformation records, so the checker
  reports a type error for this row.
- `GET /message_transformations/transformation/{name}`, response 200. The
  specification declares an array of transformations. The management
  console reads one object: `getMessageTransformDetail` returns one
  `MessageTransform`. The broker returns one transformation record, so the
  checker reports a type error for this row.
- `GET /schemas/{name}`, response 200. The specification declares a string.
  The management console reads a JSON object: the schema form loader in the
  console (`useSchemaForm`) fetches `/schemas/connectors` and
  `/schemas/actions`, strips the label keys and reads paths inside the
  object. The broker returns the schema object, so the checker reports a
  type error for this row.
- `GET /queues`, response 200. The specification declares a bare array,
  but its own example shows one object with `data` and `meta`. The
  management console reads one object with `data` and `meta`:
  `getMessageQueues` in the API module of the console returns
  `ListDataWithCursor<MessageQueue>`, and the queue page reads
  `res?.data`. The broker returns one object with `data` and the standard
  page `meta`, so the checker reports a type error for this row.
- `GET /streams`, response 200. The specification declares a bare array,
  but its own example shows one object with `data` and `meta`. The
  management console reads one object with `data` and `meta`:
  `getMessageStreamList` in the API module of the console returns
  `ListDataWithCursor<MessageStreamItem>`, and the stream page reads
  `res?.data`. The broker returns one object with `data` and the standard
  page `meta`, so the checker reports a type error for this row.
- `GET /authentication/{id}/status`, response 200. The specification schema
  types `node_error`, `node_metrics`, `node_resource_metrics` and
  `node_status` as single objects. The specification example answers one
  array per field with one object per node, and the management console
  reads arrays (`node_status: Array<{node, status}>`,
  `node_metrics: Array<...>`). The broker returns one row per node in each
  array, so the checker reports a type error for this row.
- `POST /sso/mfa/setup_info`, response 401. The specification documents
  401 `UNAUTHORIZED` for an invalid or expired token. The management console
  sends `backend`, `setup_token` and `username` and reads `secret` plus
  `mechanism` (`postSSOmfaSetupInfo`). Single sign-on is unstable, so no
  setup token verifies and the broker answers the documented 401. A body
  without the three fields answers 400.
- `GET /authorization/sources`, response 200. The specification declares
  one object with `sources`. The broker returns one object with `sources`
  in stored order.
- `GET /authentication/{id}/status`, `GET /authorization/sources/{type}/status`
  and `GET /authorization/sources/{type}`, response 200. The broker omits
  `status` and `node_status` plus rate and resource fields it does not
  measure. Counters are live: each publish and subscribe verdict on the
  built-in table attributes to the built-in allow and deny counters, and a
  disabled or removed built-in source denies fail closed. The stored
  evaluation order gates the next verdict.
- `POST /trace`, `PUT /trace/{name}/stop` and `GET /trace`, response 200,
  field `log_size`. The specification declares an array of objects. The
  management console reads an object keyed by node name. Its trace type
  declares `log_size` as a map from node name to byte count. Its trace
  table sums the values of that object. The broker returns one object
  keyed by node name, so the checker reports a type error for these rows.
- `GET /data/files/{filename}`, response 200. The specification declares
  a JSON string with the single value `binary`. The management console
  reads a file body. Its backup download call requests the path with a
  blob response type. The broker serves the raw file bytes as
  `application/octet-stream`, so the checker reports a type error for
  this row.
- `POST /listeners/{id}`, response 200. The specification has no such
  operation (creates go to `POST /listeners`). The management console
  sends creates there: `addListener` in the API module of the console
  posts the full listener form to `/listeners/{id}`. The broker treats
  it as a create with the path id, so the checker has no row for it.
- `GET /listeners`, response 200. The specification declares no
  top-level `current_connections` on list rows. The management console
  reads it first: its listener table uses
  `row.current_connections ?? row.status?.current_connections ?? 0`.
  The broker returns the live count in both places, so the checker sees
  one extra field and the table shows the count.
- `POST /listeners` and `PUT /listeners/{id}`, request `bind`. The
  specification types `bind` as a string with the numeric default
  `1883`. The management console sends a bare port as a number. The
  broker accepts a string or a number and stores the string form, so the
  checker sees no error and the console create succeeds.
- `GET /action_types` and `GET /source_types`, response 200. The
  specification declares the full name enum. The management console
  reads a bare string array (`getActionTypes`) and offers creation
  forms only for installed types. The broker returns the documented
  names limited to the runtimes in this build, so the checker sees a
  shorter array and each listed name still validates against the enum.
- `GET /actions/{id}`, `GET /sources/{id}` and `GET /connectors/{id}`,
  path `id`. The specification shows the `{type}:{name}` key. The
  management console builds that key client side (`getBridgeKey`) and
  sends it encoded in the path. The broker accepts the key and the
  bare name, stores the bare name, and returns the bare `id` with the
  bare `name`, so the checker validates the key-form read and the
  console derives the same key back.
- `GET /actions/{id}` and `GET /sources/{id}`, response 200, fields
  `status` and `node_status`. The specification declares health rows
  per node. The management console renders those rows in the detail
  pages. The broker runs no health probe for actions or sources, so it
  derives the rows from the stored enable flag (`connected` while
  enabled, `disconnected` while stopped) on the local node, instead of
  a constant or a missing field.
- `POST /schema_registry` and `GET /schema_registry/{name}`, fields
  `source` and `schema`. The specification declares `source` text. The
  management console form posts the document and reads it back for
  editing. The broker accepts `source` or `schema` on write and
  returns both (`source` as text, `schema` parsed), so the checker
  validates the documented field and historic bodies keep working.
- `PUT /configs`, request body. The specification declares `text/plain`
  (a configuration text). The management console sends JSON:
  `putConfigs` in the API module of the console puts the object with
  `Content-Type: application/json`. The broker accepts a JSON object
  keyed by root, so the checker sends JSON and the console write
  succeeds.
- `GET /mqtt/delayed/messages`, response 200, field `meta`. The
  specification declares `count`, `limit` and `page`. The management
  console reads `data` and `meta.count` for the delayed table. The
  broker returns the shared page `meta` with one extra field
  (`hasnext`), as the retained list does, and clamps `limit` to
  `1..=1000` while the specification allows `10000`, so the checker
  sees the extra field and large limits page earlier.
- `GET /mqtt/delayed/messages/{node}/{msgid}`, response 200. The
  specification declares the shape without `payload`. The management
  console reads `detail?.payload` for the payload dialog and shows a
  placeholder when it is missing. The broker follows the specification
  and omits `payload`, so the checker validates the row and the dialog
  shows the placeholder.
- `GET /mqtt/delayed/messages`, field `msgid`. The specification types
  it as an integer. The management console types it as a string but
  passes it through to the path unchanged. The broker returns an
  integer, so the checker validates the row and the console read works.
- `PUT /mqtt/delayed`, request body. The specification marks no field
  required. The management console always sends `enable` and
  `max_delayed_messages` together. The broker keeps the current value
  of a missing field, so the checker sends full bodies and partial
  writes never clear a setting.
- `POST /mqtt/topic_metrics2`, response 400, field `code`. The
  specification lists `BAD_NAME` and `BAD_TOPIC_FILTER`. The broker
  keeps no per-filter counters, so each create fails with `BAD_REQUEST`
  and a message that the function is not available. The checker flags
  the code while the status stays documented.
- `/mqtt/topic_metrics` and `/mqtt/topic_metrics/{topic}`. The
  specification carries no operation on these paths. The management
  console still ships helpers for them, but the topic-metrics view
  calls only the `topic_metrics2` operations. The broker keeps no
  route there and answers 404.
- `PUT /mqtt/topic_rewrite`, field `dest_topic`. The specification
  fills `${clientid}` and `${username}` per client. The router has no
  client context on the match path yet, so the broker stores the
  template and renders `$1` captures only, leaving those placeholders
  literal. Plain capture templates rewrite exactly.
- `GET /clients/{clientid}/authorization/cache`, response 200. The
  specification declares one object with `access`, `result`, `topic` and
  `updated_time`. The broker returns a bare list of such objects, one per
  cached decision. One object cannot hold a cache with many decisions. The
  management console has no caller for this route, so no console read
  decides it. The broker keeps the list so no decision is lost, and the
  checker reports a type error for this row.
- `GET /clients/{clientid}/authorization/cache`, response 200, field
  `result`. The specification declares enum `["allow", "denny"]`. The
  word `denny` is a spelling error. The broker returns `deny` for a
  denied decision. The checker reports a type error for a denied row.
- `PUT /clients/{clientid}/keepalive`. The published specification file
  has no such path. The broker serves it as a hidden route. It takes
  `{interval: 0..65535}` and answers the full client detail, the same
  shape as `GET /clients/{clientid}`. An unknown id answers 404
  `CLIENTID_NOT_FOUND`. The checker exercises this row through
  `cases/keepalive.json`. A HTTP test in `crates/broker-api` proves it.
- `GET /clients/{clientid}/inflight_messages`, response 200. The
  specification lists `from_clientid` and `from_username` on each entry.
  The broker omits both. The queue tracker keeps no publisher name, so no
  true value exists. Recording one needs new publish-path allocation, and
  no bench numbers allow it. The management console reads the list
  (`loadInflightMsgs` in the API module of the console, the message list
  dialog) and shows a placeholder for a missing name. The checker reports
  missing fields for this row.
- `GET /clients/{clientid}/mqueue_messages`, response 200. The
  specification lists `from_clientid` and `from_username` on each entry.
  The broker omits both. The queue keeps no publisher name, so no true
  value exists. Recording one needs new publish-path allocation, and no
  bench numbers allow it. The broker omits `mqueue_priority`. The queue
  keeps no priority, so no true value exists. The
  management console reads the list (`loadMsgQueue` in the API module of
  the console, the message list dialog) and shows a placeholder for a
  missing name. The checker reports missing fields for this row.
- `GET /clients/{clientid}/mqueue_messages`, response 200, field `msgid`.
  The specification types it as an optional string. The queue entry holds
  no message identifier, so the broker omits it. The inflight list keeps
  the downlink packet id as `msgid`. No identifier was added to the queue:
  stamping one where the kernel queues the message would add work on the
  publish path. The    checker reports a missing field for this row.
- `GET /stats` and `GET /nodes/{node}/stats`, fields `delayed.max`,
  `subscribers.count`, `subscribers.max`, `subscriptions.shared.count` and
  `subscriptions.shared.max`. The specification lists them as optional. The
  broker omits them: it keeps no high water mark for delayed entries, no
  distinct-subscriber counter and no shared-entry counter,
  and the session mirror stores shared entries under their inner filter.
  The management console tolerates the absence: its node detail reads
  `stats['subscriptions.shared.count'] ?? 0`.
- `GET /metrics` and `GET /nodes/{node}/metrics`, omitted keys. The
  specification lists every key as optional. The broker serves each key
  it truly counts (packet families, byte counts, QoS splits, delivery
  drops, `rules.matched`, `actions.executed`, `messages.delayed`,
  transformation outcomes) and omits the rest (QoS 2 splits,
  unsubscribe/disconnect splits, retained writes, session and
  authentication lifecycles, validation splits). A constant would fake
  a measurement. The management console reads the served keys only.
- `GET /prometheus/*` area routes (`auth`, `data_integration`,
  `message_transformation`, `namespaced_stats`, `schema_validation`,
  `topic_metrics`). The specification declares one text exposition per
  area. One node holds every counter, so each route serves the same
  text: the true counters of `GET /metrics` and `GET /stats`, one line
  per key. The management console scrapes only `/prometheus/stats`
  (its generated scrape config points there) and never parses an area
  split, so the shared body keeps every scrape target working.
- `GET /prometheus/*`, query `mode`, `ns` and `only_global`. The
  specification offers node and aggregate modes plus namespace
  filters. One node holds every counter, so each mode reads the same
  values; the broker accepts the keys and ignores them. The management
  console sends no such query.
- `PUT /prometheus`, legacy body. The specification accepts the legacy
  shape or the recommend shape and answers the recommend shape. The
  broker maps each legacy field onto the recommend field
  (`push_gateway_server` to `push_gateway.url`, `vm_*_collector` to
  `collectors.*`, top-level `enable`, `interval`, `job_name` and
  `headers` to the push target) and answers the recommend shape. A mix
  of both shapes fails closed with `BAD_REQUEST`.
- `GET /prometheus`, field `enable_basic_auth`. The specification
  defaults it to true and documents public scrape endpoints while it
  is false. The broker stores and returns the flag honestly, but the
  scrape routes stay behind credentials either way: opening them would
  fail open, and the rulebook fails closed. The management console
   reads and writes the flag through the settings form.
