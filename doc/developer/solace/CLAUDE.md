# Claude instructions — Solace Platform integration

> **Fork note**: Remove this file before merging upstream to
> `MaterializeInc/materialize`.

This file provides context for working on the Solace connector in the
`SolaceDev/materialize-solace` fork. Read alongside the root `CLAUDE.md`
(Materialize project conventions) and `doc/developer/solace/README.md`
(architecture and SQL syntax).

## What this fork adds

A native Solace Platform connector spanning five crates:

| Crate | New / modified files |
|-------|---------------------|
| `mz-sql-parser` | `parser.rs`, `ast/defs/ddl.rs` — `SOLACE` keyword, AST nodes |
| `mz-sql` | `plan/statement/ddl.rs`, `plan/statement/ddl/connection.rs`, `pure.rs`, `solace_util.rs` |
| `mz-storage-types` | `sources/solace.rs`, `sinks.rs`, `connections.rs`, `connections/inline.rs` |
| `mz-storage` | `source/solace.rs`, `sink/solace.rs`, `source.rs`, `sink.rs`, `render/sinks.rs` |
| `mz-adapter` | `catalog/state.rs`, `catalog/migrate.rs`, `catalog/builtin_table_updates.rs`, `coord/ddl.rs` |

## Running tests

```bash
# Required on WSL2/Linux before starting the Solace broker container
sudo sysctl -w vm.max_map_count=512000

./bin/mzcompose --find solace --dev \
  --image-registry ghcr.io/jessemenning/materialize-solace \
  run <workflow>
```

Workflows: `round-trip`, `sink-round-trip`, `exactly-once`

## Rebuilding images after source changes

mzbuild fingerprints are computed from source content. The fork patches
`misc/python/materialize/mzbuild.py` to include Solace crate sources so that
parser changes invalidate the testdrive image hash. After any source-only
commit (no `Cargo.lock` change), trigger CI to build fresh images:

```bash
gh workflow run CI --repo SolaceDev/materialize-solace --ref main
```

Then pull the new images:
```bash
docker rmi <old-testdrive-spec>   # force pull of updated image
./bin/mzcompose --find solace --dev \
  --image-registry ghcr.io/jessemenning/materialize-solace \
  run sink-round-trip
```

Get current image specs with:
```bash
./bin/mzimage spec --dev --image-registry ghcr.io/jessemenning/materialize-solace testdrive
./bin/mzimage spec --dev --image-registry ghcr.io/jessemenning/materialize-solace materialized
```

## mzbuild fingerprint mechanics (important)

`mzbuild` fingerprints determine whether a Docker image can be pulled from the
registry or must be built. The fingerprint for `testdrive` and `materialized`
includes ALL files in every transitive Rust crate directory (via
`Crate.inputs()` → `{crate.path}/**`). This means adding or modifying ANY file
in `src/sql-parser/` (including tests) changes the fingerprint.

**Stale artifact trap on self-hosted runners**: When a CI run finds no exact
cache match for `target-xcompile/`, the directory is NOT cleared — the runner's
persistent filesystem retains old compiled artifacts. If those artifacts predate
a parser change, cargo may link the testdrive binary against stale
`mz-sql-parser` despite showing "Compiling mz-sql-parser" (which can mean only
the build script reran). The CI workflow now deletes `target-xcompile/` on cache
miss to guarantee a clean build.

## Key design decisions

**Exactly-once source**: ack fires only after persist commits. The
`ReplicationGroupMessageId` (RGMID) is the Materialize timestamp for Solace
messages. The persisted watermark is checked on restart to drop redeliveries.
See `src/storage/src/source/solace.rs` — the three-way select loop.

**Sink topic templates**: `{column_name}` placeholders in the TOPIC string are
resolved at plan time to `(name, column_index)` pairs stored in
`SolaceSinkConnection`. At publish time the sink renders them per row. This
avoids repeated string parsing in the hot path. See `src/storage/src/sink/solace.rs`.

**Dedup window**: an in-memory `HashMap<String, Instant>` keyed by rendered
topic suppresses duplicate publishes within the configured window. This is
best-effort (resets on restart) and intended for high-frequency MV updates
that would otherwise flood a topic.

**Parser client-side validation**: testdrive bundles `mz-sql-parser` and
validates SQL syntax before sending to the server. Both the testdrive and
materialized images must be built from the same parser source. This is why the
mzbuild fingerprint patch is load-bearing.

## Adding a new Solace DDL option

1. `src/sql-parser/src/ast/defs/ddl.rs` — add variant to the option name enum
2. `src/sql-parser/src/parser.rs` — add keyword to the option parser match arm
3. `src/sql/src/solace_util.rs` — add extraction helper if shared across source/sink
4. `src/sql/src/plan/statement/ddl.rs` — consume the option in the planner
5. `src/storage-types/src/sinks.rs` or `sources/solace.rs` — add field to the connection struct
6. `src/storage/src/sink/solace.rs` or `source/solace.rs` — use the field at runtime
7. `test/solace/` — add or extend a testdrive file

## Upstream merge checklist

Files and directories to remove before opening a PR to `MaterializeInc/materialize`:

- `doc/developer/solace/` (this directory)
- Verify `misc/python/materialize/mzbuild.py` hunk is intentionally included or reverted
