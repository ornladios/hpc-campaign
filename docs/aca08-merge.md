# ACA 0.8 fork merging

New archives use ACA 0.8. Application table columns and keys are unchanged;
identity and history live in separate tables. ADIOS2's `aca08_support` branch
accepts ACA 0.7 and 0.8, maps sparse host/directory/key IDs, and excludes deleted
ADIOS datasets and replicas. It opens the archive read-only and ignores the
additive merge tables. Older readers accepting only 0.7 reject 0.8 explicitly.
Changing the archive's version label is not a supported compatibility workaround.

The reader was built and tested in `/Users/pnb/shared/ADIOS2/build.debug`.
Select its Python package with:

```sh
export PYTHONPATH=/Users/pnb/shared/ADIOS2/build.debug/lib/python3.13/site-packages
~/.virtualenvs/pulse/bin/python -m pytest -q
```

Reader regression tests cover both versions, sparse IDs, soft deletion,
unsupported-version rejection, actual BP array reads, and source preservation.
An HPC Campaign test reads both forks' arrays after merging real ingested BP
metadata. It skips with a clear reason when using an older 0.7-only reader.

## Workflow

Back up an existing archive, then upgrade it **once before making copies**:

```sh
hpc_campaign original.aca upgrade
cp original.aca colleague.aca
```

Make independent changes through Manager on either copy. Preview the exchange,
then apply it when the result has no conflicts:

```sh
hpc_campaign original.aca merge colleague.aca
hpc_campaign original.aca merge colleague.aca --apply
```

The source argument is a filesystem path. The destination follows the usual
archive/campaign-store conventions. From Python:

```python
from hpc_campaign.manager import Manager

manager = Manager(archive="original.aca")
manager.open()
preview = manager.merge("colleague.aca")
if not preview.conflicts:
    result = manager.merge("colleague.aca", apply=True)
manager.close()
```

Apply recomputes the plan under a destination write lock. It never trusts an
old preview. Both preview and apply leave the source unchanged. A conflict
prevents every destination change. Merge requires committed local work, the
same campaign UUID, the same baseline, protocol 1, and the supported schema.
Independently upgraded 0.7 copies cannot be merged. Upgrade cannot recover
historical edits: it records only the existing state, including soft deletions.
Repeated 0.8 upgrades preserve the existing identities and baseline.

Concurrent changes to one object conflict, including delete/update races and
changes which return to an earlier value. Identical final row values can
converge. Independent objects with colliding unique names conflict. All
concurrent time-series membership/order edits, edits to one visualization
aggregate, and edits to provenance documents are handled conservatively.
There is no automatic source-wins policy or JSON graph union. Resolve conflicts
through coordinated edits to a shared copy; unresolved divergent histories
remain conflicts even if someone adjusts their clock.

## Protocol and storage

`entity_identity` maps a table and local rowid to a UUID with an active flag.
Retired mappings remain; the partial unique index prevents two active
identities sharing a rowid. A reused rowid gets a new UUID. Imported objects
already deleted in the source retain retired identities without a local rowid.
Dataset merge UUIDs are separate from application dataset UUIDs. Valid
provenance UUIDs and campaign UUIDs are retained. Key identifiers have
namespace-derived identities because they already identify the same key across
copies. Relationships have portable natural keys; auxiliary identity mappings
also track their local lifecycle.

`change_log` retains operation UUID, writer UUID, sequence, transaction UUID,
a fresh diagnostic timestamp, table, portable key, entity UUID, action,
canonical before/after JSON and causal parents. Baseline entries are synthetic
inserts. `merge_meta` stores the baseline UUID and protocol; `merge_head` stores
the current causal frontier. Every new operation references the entire known
frontier. Ancestry, rather than clocks or source modification times, determines
whether changes are sequential. Imported operations retain their original IDs,
parents and transactions, so round trips and three-copy exchange deduplicate.

The registry in `hpc_campaign.merge` declares every integer reference and
supported column. Null and zero references retain their sentinel meaning.
Local integer primary keys are omitted from portable values. References carry
table and UUID. Every BLOB is retained as base64 of its exact stored bytes with
a SHA-256 integrity digest, including ciphertext and compressed metadata.
History and payloads are never garbage-collected.

Persistent AFTER triggers call a connection-local capture function. Identity,
operation and frontier edits occur inside the same SQLite statement/transaction
as application edits. Rollback and savepoint rollback undo all of them.
Changing a relationship key emits delete and insert operations. Changing local
rowids directly is unsupported. Recursive triggers cover SQL REPLACE deletes.
The connection trace callback tracks transaction boundaries, including outer
savepoint transactions, and records a shared transaction UUID; sequence orders
operations within that transaction. Reopened connections always get a fresh
writer UUID. No writer metadata copied in a file is reused to generate IDs.

Manager initializes every writable connection. Other Python writers must call
`initialize_connection(connection)` before writing. Replay starts a fresh writer
session and holds its capture control in Python; SQL has no capture-disable
function. Do not replace its
trace callback or disable recursive triggers. Uninitialized SQL connections
fail when triggers call the missing capture function. History tables and
capture functions are internal; deliberately bypassing capture or modifying
internal storage is unsupported. There is no SQL JSON-extension requirement.

Merge opens a read-only source transaction and checks each live snapshot
against its captured state. It combines causal histories, detects conflicts,
and rehearses the complete resulting state in private SQLite storage.
Reconstruction allocates destination rowids and translates UUID references in
dependency order, retaining existing destination rowids. Final application and
history import use one transaction, preserving atomic multi-row results even
when original writer operations span several committed transactions. Capture
is suppressed only on the replay connection; imported operations are inserted
with their original IDs. Constraint failures, dangling references, duplicate
time-series positions, payload digest errors, provenance hashes and the active
provenance graph are validated before commit.

This first implementation materializes the combined final state and retains
all intermediate operations. Preview copies the database into memory, and
history scans/ancestry checks favor clarity over large-history performance.
Large embedded payloads require corresponding memory and history storage.
