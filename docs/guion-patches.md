# Guion fork patch inventory

The Guion fork is rebuilt from upstream PowerSync Native v0.0.7. A patch stays in a Guion
release only when its current failure mode is covered by a deterministic regression test or it is
an explicit Guion build policy.

## Retained delta

| Patch | Failure mode | Regression evidence | Upstream v0.0.7 | Disposition |
| --- | --- | --- | --- | --- |
| Connect-time CRUD scan | CRUD queued before `connect()` can miss the early notification and remain stranded | `connect_uploads_crud_that_was_already_queued` | Missing | Retain; upstream candidate |
| Download writer scope | Connector credential/network awaits can retain the only writer and deadlock other writes | `fetching_credentials_does_not_hold_the_download_writer_lease` | Missing | Retain; upstream candidate |
| Connection status ordering | Transport and non-2xx errors can emit `ConnectionEstablished` before the error | `sync::download::http::tests` | Missing | Retain; upstream candidate |
| CRLF framing | JSON lines ending in CRLF expose a trailing `\r` to the parser | `util::line_split::test` | Missing | Retain; upstream candidate |
| rustls-only reqwest | Guion musl builds must not depend on native TLS/OpenSSL defaults | musl CI and dependency graph check | Policy differs | Retain as Guion build policy |

Upstream v0.0.7 already retries a failed `upload_data` call in the same upload cycle. Its
`upload_retry` test remains the source of truth; the fork does not add another retry worker.

## Dropped legacy patches

| Legacy patch | Why it is absent from v0.0.7-guion.1 |
| --- | --- |
| rusqlite 0.32 alignment and API adaptations | The SQLx SQLite consumer is being removed; use upstream optional rusqlite 0.39 and `powersync_sqlite_nostd` 0.5.2. |
| Reader `busy_timeout` | No deterministic failure remains with one PowerSync-owned pool. The upstream writer keeps its 30-second timeout. |
| Extra `BEGIN IMMEDIATE` sites | The PowerSync writer mutex serializes SDK writes; no current `BUSY_SNAPSHOT` regression justifies broader locking. |
| Reader lease release sender | The upstream lease can only be constructed when `PoolReaders` exists and returns through the same shared pool state. |
| `From<io::Error>` for `PowerSyncError` | No SDK or Guion consumer path uses this public conversion. |
| Broad Clippy allows | The private-interface warning is fixed by narrowing internal subscription command visibility. |

Do not restore a dropped patch based on suspicion or an intermittent stress failure. First add a
minimal deterministic test that identifies the failing invariant, then retain only the smallest
fix for that test.
