# Ampere Sync Engine — Design

_Status: draft for discussion · 2026-08-18_
_Repo: `~/projects/ampere` (Effect v4 RC, Bun, PG 17 `wal_level=logical`)_

A TypeScript/Effect re-imagining of ElectricSQL's sync service: one process tails
the Postgres WAL and fans committed changes out over a topic bus to distributed
per-partition actors, each of which materializes client-requested shapes and
serves them over SSE. Not a general-purpose Electric clone — opinionated for our
workloads.

## 1. Principles and non-goals

- **The WAL is the only durable log.** We do not build a second one. The
  replication slot is the retention pointer; everything downstream is
  reconstructible.
- **Snapshots are the recovery path.** Any actor can rebuild from a
  `REPEATABLE READ` snapshot plus the live stream. Therefore no component needs
  guaranteed delivery — only guaranteed _detection_ of loss.
- **Never silently miss a change.** Lossy transport is fine; undetected loss is
  not. One protocol rule (the LSN chain, §5) enforces this everywhere.
- **Row-local routing only.** A change's destination topics are a pure function
  of the changed row. This is the constraint that keeps the hot path stateless
  (and it is why general cross-table joins are out of scope — see §9).
- **Only committed data crosses a process boundary.** Actors never see
  uncommitted transactions or aborts.
- **On-demand sync.** Shapes can be enormous. An actor materializes and serves
  only the subsets clients actually request, never "everything the shape could
  contain." (Design pending Electric research — §8.)
- **No hardcoded partition semantics.** `(table, org_id)` is _our deployment's_
  config, not the library's opinion. The library takes declared key columns per
  table.

Non-goals: cross-table joins in shape definitions; write path; being
infra-agnostic at v1 (we target one Postgres, one bus, N actor hosts).

## 2. Topology

```
Postgres ══ logical replication (one slot, pgoutput v1) ══▶

  ┌─ INGEST (exactly one; cluster singleton or supervised process) ─┐
  │ connection FSM → decoder → converter → [v2 spooler†] → router   │
  │ publishes committed tx batches to topics + watermark heartbeats │
  │ acks slot with bounded lag                                      │
  └─────────────────────────────────────────────────────────────────┘
                 │  topic bus (RMQ topic exchange / ZMQ pub-sub)
                 │  lossy allowed · per-topic ordered · data-carrying
                 ▼
  ┌─ ACTOR per partition (Rivet / Effect Cluster entity / DO) ──────┐
  │ subscribes to its topic(s) on activation                        │
  │ cursor + gap check → materializer → SQLite state + shape logs   │
  │ snapshot-on-demand via gateway · serves SSE to clients          │
  └─────────────────────────────────────────────────────────────────┘
                 ▼
  SSE clients (offsets into the actor's shape logs)
```

† v2 spooler exists only if/when we adopt pgoutput protocol v2 (§4).

Supporting services: a **snapshot gateway** (bounded-concurrency access to a
Postgres pool for actor snapshots; prevents stampedes) and the bus itself.

## 3. Ingest pipeline

Single process, single-threaded data path (pgoutput parsing is buffer slicing;
Electric parses its entire firehose on one process). Fibers, not workers.

1. **Connection FSM.** Mirrors Electric's ConnectionSetup:
   `identify_system → advisory lock → create/verify publication → create slot
(pgoutput, NOEXPORT_SNAPSHOT) → read confirmed_flush_lsn → START_REPLICATION
(proto_version '1')`. Reconnect with `Schedule` backoff; failures must be
   handled _inside_ the singleton (cluster singletons die-and-wait on escape).
   Slot resume makes reconnection semantically free.
2. **Decoder.** Pure `Uint8Array → PgOutputMessage`. First-byte dispatch, total
   at proto v1. Tagged Schema structs + `toTaggedUnion`; `Unsupported`
   fallback. Hermetic tests ported byte-for-byte from Electric's
   `decoder_test.exs` fixtures.
3. **Converter.** Pure fold `(State, Message) → [State, Emit[]]`: relation
   cache, Begin…Commit assembly, unchanged-TOAST backfill from old tuple
   (requires `REPLICA IDENTITY FULL` — hard requirement, same as Electric),
   batching. Emits only complete committed transactions (v1).
4. **Spooler (future, v2 only).** If we adopt proto v2 streaming: stages
   interleaved uncommitted chunks per xid (memory, disk-spill past a
   threshold), seals on Stream Commit, discards on Stream Abort. Emission stays
   in commit order, so nothing downstream changes. Insertable stage; absent in
   v1.
5. **Router.** For each change: compute topic(s) from the table's declared key
   columns (§6), handle key-change as delete-to-old-topic + insert-to-new-topic,
   group per transaction, publish.

**Slot acking:** ingest acks `confirmed_flush_lsn` with a bounded lag (e.g. a
few minutes behind its high-water mark). Sole purpose: an ingest crash/restart
replays recent WAL and republishes; actors drop duplicates via the cursor rule.
Bounded WAL growth, no coordination with actors.

## 4. Protocol version

Pinned to **pgoutput v1**: only committed transactions arrive, contiguous, so
the decoder is stateless and the spooler doesn't exist. Cost: commit-to-first-
byte latency on very large transactions (server spills to its own disk and
sends after commit). Adopt v2 + `streaming on` only when that latency is a
measured problem; the spooler stage and the decoder's one-bit `inStream`
context are the contained changes.

## 5. Correctness protocol: the LSN chain

The bus may drop messages (ZMQ HWM, reconnects, RMQ transient queues). That is
allowed because loss is always detected:

- Every published batch carries `{ fromLsn, toLsn, changes }`.
- Actor state includes `cursor` = last applied `toLsn`, persisted atomically
  with the state it describes (same SQLite transaction — load-bearing).
- On receive:
    - `fromLsn == cursor` → apply, advance cursor.
    - `toLsn <= cursor` → duplicate (redelivery/replay) → drop. Makes ingest
      restart replay harmless.
    - `fromLsn > cursor` → **gap detected** → resync (§7).
- **Watermark heartbeats:** ingest periodically publishes "stream at LSN X" on
  a broadcast topic. Lets quiet partitions distinguish "no writes" from "I'm
  disconnected"; missed heartbeats → resubscribe + gap check. (Same trick as
  Electric's keepalive LSN broadcast.)

Decision (2026-08-18): **no ring buffer / no replay-request path.** Snapshots
are cheap for our shapes; any gap → re-snapshot. This deletes the ingest-side
replay buffer, the replay RPC, and the actor registry. Revisit only if
gap-triggered snapshot load shows up in practice; the LSN chain doesn't change
either way.

## 6. Topics and partition keys

Routing is **topic-based, derived from declared column names + values**.

- Per-table config: ordered key columns, e.g. `tasks → [org_id]`. Topic =
  `table.<col1val>[.<col2val>...]`, e.g. `tasks.org_123`. Values sanitized/
  hashed for topic-name safety.
- Constraint (the only one the library imposes): key columns must come from the
  row itself — routing is a pure function of the change. Old and new rows are
  both present (REPLICA IDENTITY FULL), so a row whose key column changes emits
  delete-on-old-topic + insert-on-new-topic.
- **OPEN DECISION — delivery model: topic subscription vs per-actor push
  list.** Two candidate models, undecided:
    - _Topic subscription (broker fan-out):_ ingest publishes unconditionally;
      actors subscribe on activation, unsubscribe on passivation. No registry in
      our code — the broker does fan-out; publishing to subscriber-less topics is
      ~free. Limitation: an actor must be awake and subscribed to receive; a
      sleeping actor misses everything (→ gap → §8.2 recovery on wake).
    - _Per-actor push list:_ ingest (or broker feature, e.g. per-actor durable
      queues / cluster persisted messages / runtime wake-on-message) delivers to
      known actors and can **wake sleeping ones**, keeping their logs continuous
      across idle periods. Costs: push-list/queue lifecycle management, and
      either unbounded accumulation for long sleeps (TTL ⇒ gaps anyway) or
      waking every actor on every write (defeats hibernation).
    - Note the interaction with §8.2 that weakens the case for push: an actor in
      `changes_only` mode with **no connected clients has no need for data** —
      its log only serves connected clients' resumption, and actors with clients
      are awake anyway (they hold the SSE connections). Wake-on-push therefore
      buys exactly one thing: log continuity across client-less periods, i.e.
      avoiding a `must-refetch` + subset replay when clients return to a cold
      partition. Whether that's worth the queue machinery depends on reconnect
      patterns and subset replay cost — decide with data, or when the actor
      runtime is chosen (some runtimes make wake-on-message trivial).
- Bus requirements: per-topic ordering from a single publisher (both RMQ
  per-queue and ZMQ PUB/SUB per-connection satisfy this), prefix/pattern
  subscription (RMQ topic exchange wildcards; ZMQ SUB prefix matching),
  loss-tolerance acceptable. Behind a `ChangeFeedApi` Effect service so
  RMQ vs ZMQ vs in-proc is a Layer swap. Do **not** use per-actor durable
  queues as a durability story — that's rebuilding the WAL in a broker.
  Transient/auto-delete queues only.

Coarse membership comes free from topics (an actor only receives its
partition). Fine membership (per-shape predicates within a partition) is
evaluated in the actor (§7).

## 7. Actor internals

Keyed by partition (topic). Hosts 1..N **shape/subset definitions** (client-
requested; see §8). Lifecycle:

- **Activate** (first client request or wake): open SQLite (disk/S3-backed —
  a cache, never a correctness dependency), read cursor, subscribe to topic.
- **Cold start or gap → resync:** through the snapshot gateway:
  `REPEATABLE READ READ ONLY` txn, `SELECT pg_current_snapshot(),
pg_current_wal_lsn()`, stream the query result (scoped to the requested
  subset, not the whole potential shape), then apply live batches filtered by
  **xid visibility** (Electric's stitch: changes visible in the snapshot are
  already in it → skip; first xid ≥ xmax → filtering off permanently).
  Display-setting GUCs pinned on the snapshot session (bytea_output, DateStyle,
  TimeZone, extra_float_digits, IntervalStyle) so text values are canonical.
- **Materializer** (per committed batch, per shape): evaluate the shape's
  row-local predicate against old and new rows → insert / update / delete /
  move-in (old fails, new passes → insert) / move-out (reverse → delete).
  Apply to materialized rows (by PK), append ops to the shape's log with
  offsets, publish to the in-actor PubSub. Cursor advances in the same SQLite
  transaction.
- **Serve:** SSE clients resume from offsets into the shape log; transaction
  boundaries preserved so clients apply atomically. Actor re-snapshot ⇒
  clients get `must-refetch` (steal Electric's control-message vocabulary:
  insert/update/delete ops, `up-to-date`, `must-refetch`).
- **Passivate:** unsubscribe, close SQLite. State survives if storage does;
  if not, next activation re-snapshots. Runtime (Rivet / Effect Cluster /
  DO) owns hibernation + placement; correctness never depends on it.

## 8. On-demand sync and subsets — design (from Electric source research, 2026-08-18)

Requirement: shapes can be very large; an actor must materialize **only the
subsets clients actually ask for**, never everything its partition could
contain.

### 8.1 What Electric does (findings)

Electric has two distinct mechanisms that both say "snapshot"; only the first
is the on-demand story:

1. **`log_mode: changes_only` + client-driven subset queries** — the on-demand
   mode. The shape's initial snapshot transaction still runs (`REPEATABLE READ`
    - `pg_current_snapshot()` + `pg_current_wal_lsn()`) but streams **zero
      rows**; the shape log begins as a single `snapshot-end` control message
      carrying `xmin/xmax/xip_list`. That pinned `(pg_snapshot, lsn)` pair is the
      consistency anchor. Live WAL changes then flow into the log normally (with
      the standard xid-visibility filter until the first xid ≥ xmax).
2. **Subquery shapes with move-in/move-out splicing** — server-side machinery
   (Materializer refcount sets, DNF tags, pattern-based move-outs, buffered
   move-in queries spliced at an LSN-consistent boundary). Feature-flagged even
   in Electric; heavy. **Deferred for Ampere** (see 8.4).

Subset mechanics (mechanism 1), the parts that matter:

- **A subset is an ad-hoc query with zero server state.** SQL is
  `base_shape_where AND (subset_where)` with the base shape's column
  projection, optional `order_by`/`limit`/`offset` (`order_by` required when
  paginating). The server does not remember which subsets any client fetched —
  no per-subset storage, no per-client filtering of the live stream. This
  statelessness is what makes it scale; resist "optimizing" it away.
- **Response is out-of-band, never written to the log:**
  `{"metadata": {xmin, xmax, xip_list, database_lsn, snapshot_mark}, "data":
[rows-as-insert-ops]}`, uncacheable, no up-to-date header. Each row also
  carries the `snapshot_mark` header.
- **The client dedupes**, via two small structures:
    - `SnapshotTracker`: per fetched subset, `{xmin, xmax, xip_list, keys}`.
      Reject a live change iff `keys.has(key) && visibleInSnapshot(xid)` — both
      conjuncts (keys alone drops concurrent txns; visibility alone drops changes
      to rows the subset's WHERE excluded). Retire a snapshot once any xid ≥ its
      xmax passes (and when the stream LSN passes `database_lsn` — Electric wrote
      this retirement path but never wired it; we should).
    - `insertedKeys`: apply inserts unconditionally as **upserts**; ignore
      updates/deletes for keys never inserted. This one set answers both the
      delete problem (deletes for unfetched rows are no-ops) and the overlap
      problem (overlapping subsets converge via upsert).
- **Ordering:** the client pauses the live stream while fetching + injecting a
  subset (atomic injection, no interleaving), and on cold start advances its
  offset to the subset response's offset header so nothing is skipped.
- **Refetch:** clients remember requested subsets (canonicalized params) and
  replay them all after `must-refetch`.
- **Validation is a security boundary:** allow-listed `queryable_columns` for
  subset where/order_by (can include columns that are never synced); no
  subqueries inside subsets; positional params 1..N; where must type-check
  boolean; no function calls/CASE/subselects in order_by; POST body preferred
  (large `= ANY($1)` lists break GET URLs).
- **The load-bearing tradeoff:** subsets bound _snapshot_ cost, not _live
  stream_ cost — the log carries every change matching the shape definition,
  fetched or not. Clients tolerate unknown-key traffic via `insertedKeys`.

### 8.2 Adoption in Ampere

Adopt mechanism 1 wholesale; it simplifies the actor dramatically:

- **The actor keeps no materialized table — only the change log.** A partition
  shape runs in `changes_only` mode permanently: on activation the actor pins
  its `(pg_snapshot, lsn)` anchor via the snapshot gateway (zero rows read),
  writes the `snapshot-end` control entry, and appends live batches from the
  bus (xid-filtered at the seam). Subset requests from clients are proxied
  ad-hoc through the snapshot gateway (`partition_where AND subset_where`) and
  returned out-of-band with the metadata envelope. Postgres is only ever
  queried for data clients actually asked for.
- **Gap recovery becomes trivial.** With no materialized state, a gap (§5)
  needs no data re-fetch: pin a fresh anchor, truncate/rotate the log, emit
  `must-refetch`; clients replay their remembered subsets. Cheaper than the
  re-snapshot path §7 assumed.
- **Materializer scope shrinks** to: membership evaluation of the partition
  predicate (routing already did the coarse cut), log append with offsets,
  client fan-out. The SQLite file holds the log + cursor, not row state.
- **Client library must implement** SnapshotTracker + insertedKeys + pause-
  and-inject + subset replay. Electric's TypeScript client already implements
  all of these — the strongest argument yet for speaking Electric's wire
  protocol (§12 Q2) and reusing/forking their client.

### 8.3 The residual cost to watch

A high-write partition streams all its changes to every connected client
regardless of subsets fetched. If that bites, the fix is narrower partition
keys (finer topics) — not per-client filtering in the actor. Revisit only with
evidence.

### 8.4 Subqueries in shape definitions — deferred

`WHERE fk IN (SELECT ...)` shapes need the full move-in/move-out engine:
per-dependency Materializer (refcounted link-value set, events only on 0↔1
transitions), DNF tag structure with per-row `tags`/`active_conditions`
headers, pattern-based move-out control messages (client synthesizes deletes
via a tag index), and buffered move-in queries (`new_views_match AND NOT
old_views_match`) spliced at a snapshot-consistent boundary. Electric gates
this behind feature flags; its TypeScript client doesn't even implement the
client half (only the Elixir client's `tag_tracker.ex` does). If we ever need
it, port the concepts from Electric; until then, subset-level filtering (8.2)
plus deliberate partition-key design covers our cases.

## 9. Failure matrix

| Failure                                    | Effect                         | Recovery                                                                                                                                   |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Ingest crashes                             | stream pauses                  | restarts (singleton failover), resumes from slot; lagged ack ⇒ recent WAL republishes; actors drop dupes via cursor                        |
| Postgres down                              | stream pauses                  | connection FSM retries with backoff; slot preserves position; actors keep serving cached state                                             |
| Bus drops messages                         | some actors miss batches       | LSN chain detects on next batch/heartbeat → re-snapshot                                                                                    |
| Bus down                                   | all actors stall               | heartbeat loss → actors mark stale; resubscribe + gap-check when bus returns                                                               |
| Actor crash / hibernation                  | that partition's clients pause | reactivate: cursor from SQLite if intact (+gap check), else re-snapshot; clients `must-refetch` if logs were lost                          |
| Snapshot stampede (mass gaps after outage) | DB load spike                  | snapshot gateway bounds concurrency; queue + jitter                                                                                        |
| Slot falls far behind (long ingest outage) | WAL accumulation on server     | bounded-lag ack limits steady-state; alerting on slot lag; worst case: drop slot, global re-snapshot (the system is rebuildable by design) |
| Transaction > memory (future v2)           | spooler pressure               | disk spill per xid; abort ⇒ delete spool                                                                                                   |

## 10. Effect implementation notes

- Follow `.claude/skills/effect-program-design` (deep modules, `Context.Service`
    - Layers at real seams, tagged errors, `Effect.fn` spans, bounded everything).
- Naming: anti-slop lint **bans `*Shape` symbol names** — pick the domain term
  early (`SyncView`, `Subset`, `Partition`…) and use it consistently.
- Key service seams (each swappable by Layer, each testable in isolation):
  `ReplicationStreamApi` (FSM + decode + convert), `ChangeFeedApi` (bus),
  `SnapshotGatewayApi`, `PartitionTopicConfig` (declared key columns),
  `ShapeStoreApi` (actor SQLite), actor host adapter (Rivet / Effect Cluster /
  in-proc for tests).
- Effect Cluster (researched 2026-08-18): good fit for singleton + entity
  placement/failover if we go that route; it is placement + messaging, **not**
  state — no persistence hooks, state wiped on passivation; no pub/sub
  primitive (bus stays separate regardless). All `effect/unstable/*` at
  4.0.0-rc — pin exact versions. Rivet/DO remain equally viable behind the host
  adapter seam; topic bus + LSN chain are runtime-agnostic.
- Testing ethos stolen from Electric: decoder = frozen byte fixtures
  (hermetic); converter/materializer = pure folds over struct sequences
  (hermetic); integration = real throwaway Postgres per test
  (`with_unique_db` pattern; docker PG17 already in the repo).

## 11. Decisions log

| Date  | Decision                                                  | Why                                                                                    |
| ----- | --------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| 08-18 | pgoutput proto v1, spooler deferred                       | stateless decoder; Electric-proven; v2 is a contained later add                        |
| 08-18 | REPLICA IDENTITY FULL required                            | old rows needed for routing key changes, move-out, TOAST backfill                      |
| 08-18 | WAL is the only durable log                               | don't reinvent it; snapshots + slot cover recovery                                     |
| 08-18 | Lossy topic bus + LSN chain (detect, don't guarantee)     | cheap transport, provable no-silent-loss                                               |
| 08-18 | Routing keys = declared key columns per table (row-local) | library stays unopinionated; hot path stateless                                        |
| 08-18 | OPEN: topic subscription vs per-actor push list           | hibernation/wake tradeoff; see §6; decide with actor-runtime choice                    |
| 08-18 | No ring buffer / replay path                              | snapshots are cheap; -1 component; gap ⇒ re-snapshot                                   |
| 08-18 | Only committed data below ingest                          | actors never implement rollback                                                        |
| 08-18 | On-demand subsets only, never eager full-shape fetch      | shapes can be huge; adopt Electric's `changes_only` + subset model (§8.2)              |
| 08-18 | Actors hold logs, not materialized tables                 | `changes_only` makes row state unnecessary; gap recovery = new anchor + `must-refetch` |
| 08-18 | Subquery shapes deferred                                  | move-in/move-out engine is heavy and feature-flagged even in Electric (§8.4)           |

## 12. Open questions

1. Delivery model: topic subscription vs per-actor push list (§6) — coupled to
   the actor-runtime choice (wake-on-message support) and to how much
   `must-refetch`-on-wake costs in practice. Then, within the bus option: RMQ
   topic exchange vs ZMQ pub/sub (host-local ZMQ strongest same-box candidate;
   RMQ if actors are network-far).
2. Client wire protocol: Electric-compatible (inherit their client + proxy/CDN
   caching semantics) or custom SSE?
3. Volume envelope: peak writes/sec, partition count, clients per partition —
   sizes heartbeat interval, snapshot gateway concurrency, slot-lag window.
4. §8: adopt Electric's subset/tag machinery wholesale, or simplify for our
   narrower shape model?
5. Actor runtime: Rivet vs Effect Cluster vs DO for v1.

## 13. Build order

1. `PgOutput` decoder + Cursor + ported Electric byte fixtures (pure, fully
   specified today).
2. Converter fold + tests (struct sequences in, fragments out).
3. `ReplicationStreamApi`: connection FSM against the repo's docker PG17;
   integration tests with throwaway DBs.
4. `ChangeFeedApi` with in-proc Layer; LSN-chain + heartbeat schema.
5. Materializer + `ShapeStoreApi` (SQLite): membership eval, logs, offsets,
   snapshot stitch — runnable single-process end-to-end at this point.
6. Subset/on-demand layer per §8 findings.
7. Actor-host adapter (runtime decision) + snapshot gateway + SSE tier.
