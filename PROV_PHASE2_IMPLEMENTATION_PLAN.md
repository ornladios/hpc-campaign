# Phase 2 Implementation Plan: Canonical Scientific Provenance

**Status:** Complete. Steps 2.1 through 2.6 were approved, implemented, and
verified on 2026-09-02. See `PROV_PHASE2_COMPLETION_REPORT.md` for the runnable
example, measured scale assessment, and resulting roadmap decision.

**Design authority:** `PROV_CAMPAIGN_SCHEMA_DESIGN.md`

**Phase 1 evidence:** `PROV_MAPPING_SPIKE.md`,
`hpc_campaign/prov_mapping.py`, and `tests/test_prov_mapping.py`

## 1. Objective

Phase 2 makes W3C PROV the canonical representation of the campaign's core
scientific provenance. It integrates the mappings proven in Phase 1 with an
ACA campaign database while keeping the initial implementation small enough to
review and test thoroughly.

At completion, a campaign can persist and retrieve:

- campaign datasets as PROV Entities;
- simulation runs and processing executions as PROV Activities;
- logical variables as stable PROV Entities;
- role-qualified Usage and Generation relationships;
- explicit qualified Derivation relationships;
- Plans and Agents; and
- canonical PROV-JSON documents that survive closing and reopening the ACA.

The SQLite representation introduced in this phase stores canonical documents
and only the minimum metadata required to manage them. It is not the query
index planned for Phase 4.

## 2. Decisions already fixed

The implementation must preserve these decisions:

1. W3C PROV is the canonical relationship model. Custom activity, input,
   output, or derivation tables are not a second provenance model.
2. Python `prov >= 3.1.0, < 4` is the parser, serializer, and in-memory object
   model.
3. Canonical storage is PROV-JSON.
4. Each campaign has a stable UUID independent of its filename and SQLite row
   identifiers.
5. Each logical variable has one stable UUID and one PROV Entity identity.
6. A time-varying logical variable remains the same Entity as its underlying
   dataset grows. An append does not create another Entity, Generation, or
   Derivation.
7. A scientifically distinct replacement or transformed product receives a
   new logical-variable UUID and normal Generation and Derivation records.
8. Campaign-authored Usage and Generation records are identified and carry a
   non-empty `prov:role`.
9. Campaign-authored dependencies are explicit `wasDerivedFrom` records.
   Usage plus Generation is dataflow, but is not inferred to prove derivation.
10. A detailed Derivation identifies the exact Activity, Generation, and Usage
    records that connect its output and input.
11. Dataset, Fides, action-specification, and other context used by an Activity
    does not automatically become scientific lineage.
12. Database integer keys are local implementation details and are never the
    sole exported identity.
13. All campaign-authored identifiers and active relationship endpoints are
    self-contained in the campaign namespace.
14. Unknown PROV records that `prov` can parse are preserved during a
    load/save round trip.

## 3. Scope boundaries

### 3.1 Included

- Persistent campaign identity.
- Persistent canonical PROV documents.
- Stable identifier and vocabulary helpers.
- Dataset Entity registration from existing ACA dataset rows.
- Run, logical-variable, Activity, Plan, and Agent authoring helpers.
- Atomic creation of an Activity and all of its output and relationship
  records.
- Import, retrieval, and export of `ProvDocument` objects.
- Structural and campaign-invariant validation required by the core model.
- Focused unit tests and end-to-end persistence tests.

### 3.2 Not included

- Configurable provenance profiles or run profiles; these are Phase 3.
- The large query/index schema, graph traversal API, pagination, and grouped
  summaries; these are Phase 4.
- Native schema/guide registration helpers; these are Phase 5.
- Collaboration sessions, messages, conclusions, and publication; these are
  Phase 6.
- Internal PROV document partitioning and lazy loading; these are Phase 7
  unless scale measurements require moving them earlier.
- Timestep Entities, timestep-range Usage, snapshots, or logical-variable
  revision chains.
- Unit parsing or coordinate-system interpretation.
- Full PROV-CONSTRAINTS validation.
- Automatic execution of Plans or workflows.
- Ingesting or generating scientific payload files as part of
  `add_activity`; payloads must already be registered as ACA datasets.

## 4. Persistent storage design

### 4.1 Campaign identity

The current ACA `info.id` value identifies the file format (`ACA`); it is not a
campaign object UUID. Add a singleton table dedicated to persistent campaign
identity:

```sql
CREATE TABLE campaign_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    uuid TEXT NOT NULL UNIQUE
);
```

Behavior:

- New ACA files receive a random UUID when their tables are created.
- An existing ACA without this table receives one when upgraded or when the
  provenance API is first used, according to the repository's chosen upgrade
  path.
- Moving or renaming the ACA does not change the UUID.
- Opening an ACA never regenerates an existing UUID.
- The value is validated as a UUID before constructing a PROV namespace.

### 4.2 Canonical document table

Store each canonical document directly in the ACA so its replacement can be
committed atomically with its management metadata:

```sql
CREATE TABLE provenance_document (
    uuid TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    format TEXT NOT NULL CHECK (format = 'prov-json'),
    content TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    modtime INTEGER NOT NULL
);
```

Version-one behavior:

- Create one active authored document named `campaign-provenance` on first
  use.
- Permit additional imported documents so the public model does not assume
  that a campaign always contains one document.
- `active = 1` means the document participates in the campaign graph and must
  satisfy campaign self-containment and supported-core invariants.
- `active = 0` permits preserved documents that are retrievable and
  exportable but are not used by campaign convenience queries or authoring
  operations.
- `sha256` covers the UTF-8 canonical JSON text stored in `content`.
- `modtime` uses the same nanosecond convention as existing campaign tables.
- The table contains canonical PROV, not a derived graph index. Its contents
  must not be discarded and rebuilt from other SQL tables.

Storing the document in SQLite is intentionally the initial implementation.
It gives one crash-safe transaction boundary and keeps the campaign
self-contained. If scale testing later selects external or partitioned PROV
resources, the public document identifiers and PROV record identifiers remain
unchanged.

### 4.3 Canonical JSON

To avoid hashes that change only because of dictionary formatting:

1. Serialize the `ProvDocument` through `prov` as JSON.
2. Parse that JSON with the Python standard library.
3. Encode it with sorted keys and compact separators using UTF-8.
4. Hash and store those exact bytes as text.
5. Verify that `prov` can deserialize the canonical text before committing it.

Add a test proving that serializing an unchanged document twice produces the
same stored text and hash. Do not claim that this is an external W3C canonical
JSON standard; it is the campaign's deterministic storage encoding of
PROV-JSON.

## 5. Code organization

Keep the implementation focused:

- `hpc_campaign/prov_mapping.py`
  - Retain vocabulary constants and `CampaignProvIds`.
  - Replace example-only helpers with reusable, typed record-building helpers
    only when the production code needs them.
  - Keep the Phase 1 example builder available to focused tests or move it to
    test support if it would otherwise become public production behavior.
- `hpc_campaign/prov_store.py` (new)
  - Own campaign identity and provenance-document persistence.
  - Canonicalize, hash, validate, read, write, import, and export documents.
  - Contain no scientific action vocabulary or query traversal policy.
- `hpc_campaign/provenance.py` (new)
  - Author and validate the restricted scientific mappings.
  - Resolve existing ACA dataset UUIDs and live replica locations.
  - Replace the canonical authored document through `ProvStore`.
- `hpc_campaign/manager.py`
  - Add thin campaign-oriented methods that validate existing ACA objects and
    delegate PROV construction/storage.
  - Do not embed SQL or general graph algorithms in each public method.
- `hpc_campaign/manager_funcs.py` or the existing upgrade mechanism
  - Create/upgrade only the two persistent tables above.

Do not add a broad repository layer, ORM, plugin system, or general-purpose
PROV wrapper in Phase 2.

## 6. Dataset mapping

The existing `dataset` table remains authoritative for campaign payload
registration. Its `uuid` supplies the persistent dataset object UUID.

When a dataset is first referenced by provenance:

1. Resolve a live existing dataset by name or UUID.
2. Reject a missing or deleted dataset.
3. Validate its UUID.
4. Create or reuse `hpcid:dataset_<DATASET_UUID>`.
5. Record:
   - `prov:type = hpc:Dataset`;
   - `prov:label` using the campaign dataset name;
   - `hpc:datasetUuid` using the existing UUID;
   - `hpc:format` using the existing file-format value when present; and
   - one or more `prov:location` values resolved from its live replicas.
6. Require at least one resolvable location.

Multiple logical variables may reference the same dataset Entity. Dataset
registration must be idempotent: an identical existing record is reused;
conflicting identity attributes are rejected rather than silently overwritten.

Phase 2 does not copy ADIOS variables, attributes, code parameters, or other
native metadata into campaign SQL or PROV attributes.

## 7. Scientific object mappings

### 7.1 Run

`add_run` creates an Activity with:

- `hpcid:run_<RUN_UUID>`;
- `prov:type = hpc:SimulationRun`;
- non-empty `prov:label`;
- optional Association to an Agent and Plan.

Run names must be unique within the authored campaign graph. The UUID remains
the identity if a display label is later changed.

### 7.2 Logical variable

`add_variable` creates an Entity with:

- `hpcid:variable_<VARIABLE_UUID>`;
- `prov:type = hpc:LogicalVariable`;
- `hpc:logicalVariableId`;
- one Run reference;
- one Dataset Entity reference;
- dataset name;
- non-empty physical variable name;
- non-empty variable-definition string;
- resolved dataset location;
- optional opaque units string; and
- optional opaque coordinate-system string.

The logical-variable Entity has no revision number or `_rN` identifier suffix.
For a source variable, the run may be recorded as its generating Activity. The
Generation is optional only when the source execution is genuinely unknown.

Phase 2 accepts an explicit `primary` flag only if the existing branch's
normalized primary binding is present when implementation begins. It must not
encode mutable primary selection as an attribute on the PROV Entity.

### 7.3 Processing activity

The provisional convenience API recognizes the initial action set:

- `reduction` -> `hpc:Reduction`;
- `projection` -> `hpc:Projection`;
- `quantity_of_interest` -> `hpc:QuantityOfInterest`; and
- `visualization` -> `hpc:Visualization`.

This temporary table is replaced as the authority by the campaign profile in
Phase 3. Imported PROV is not restricted to this set.

Each processing Activity has a stable UUID and may have:

- multiple logical-variable inputs with unique non-empty roles;
- multiple logical-variable outputs with unique non-empty roles;
- non-lineage context inputs such as a Plan or specification;
- an optional Agent association; and
- an optional Plan association.

### 7.4 Plans, action specifications, and agents

Phase 2 supports ordinary PROV Plans and Agents needed by the scientific core.
An optional action specification is immutable canonical JSON represented as an
Entity typed `hpc:ActionSpecification` and, when prescriptive, `prov:Plan`. It
records a content hash. Phase 3 adds per-action key validation.

Agents may be Person, SoftwareAgent, Organization, or an HPC-qualified type
such as Instrument. Only a stable UUID and type are required; label and
descriptive metadata are optional.

## 8. Activity transaction boundary

One `add_activity` call is all-or-nothing for provenance. It creates in memory:

1. the Activity;
2. all output logical-variable Entities;
3. all identified Usage records;
4. all identified Generation records;
5. optional context Usage records;
6. explicit qualified Derivation records;
7. optional action-specification Entity and Usage;
8. optional Agent/Plan Association; and
9. compact source-step mappings when that campaign feature is available.

Implementation sequence:

1. Load the current document and remember its stored hash.
2. Build changes in a separate candidate `ProvDocument` obtained through an
   exact serialize/deserialize copy, so failures cannot mutate cached state.
3. Resolve every referenced Run, Dataset, logical variable, Plan, and Agent.
4. Validate roles, derivation mappings, dataset locations, generation
   uniqueness, qualified endpoints, and self-containment.
5. Canonicalize the candidate document and verify a `prov` round trip.
6. Execute `BEGIN IMMEDIATE`.
7. Update `provenance_document` only if its stored hash still matches the hash
   loaded in step 1. A mismatch reports a concurrent-update error.
8. Commit once. On any exception, roll back and leave the original document
   byte-for-byte unchanged.

Scientific payload generation or archiving occurs before this transaction.
The transaction records provenance for already registered datasets; it cannot
atomically roll back an external simulation, ParaView, or filesystem write.

## 9. Derivation rules

The convenience API accepts an explicit mapping from each output role to the
input roles that scientifically contributed to it.

For the common case, omission means all scientific logical-variable inputs
contributed to all outputs. Context inputs never participate in this default.

For every selected output/input pair, create an identified qualified
Derivation containing:

- generated Entity;
- used Entity;
- Activity;
- exact Generation identifier; and
- exact Usage identifier.

Reject:

- unknown input or output roles;
- a context role named as a derivation input;
- a derivation whose qualified endpoints do not match its Activity;
- a second generating Activity for the same Entity; and
- cross-campaign identifiers in campaign-authored records.

Direct `wasDerivedFrom(output, input)` without an Activity may be imported and
preserved. A dedicated convenience authoring helper is optional in Phase 2.

## 10. Proposed public API for review

These signatures are a concrete starting point, not authorization to change
the public API without review:

```python
manager.campaign_uuid() -> uuid.UUID

manager.add_prov_document(
    document: ProvDocument,
    *,
    name: str,
    activate: bool = False,
) -> uuid.UUID

manager.prov_documents(*, active: bool | None = None) -> list[ProvDocumentInfo]
manager.prov_document(document_id: uuid.UUID | str) -> ProvDocument
manager.set_prov_document_active(
    document_id: uuid.UUID | str,
    *,
    active: bool = True,
) -> ProvDocumentInfo
manager.export_prov(document_id: uuid.UUID | str, path: str | Path) -> None

manager.add_run(
    name: str,
    *,
    run_id: uuid.UUID | None = None,
    plan: QualifiedName | None = None,
    agent: QualifiedName | None = None,
) -> QualifiedName

manager.add_variable(
    *,
    run: QualifiedName,
    dataset: str,
    variable: str,
    definition: str,
    variable_id: uuid.UUID | None = None,
    units: str | None = None,
    coordinate_system: str | None = None,
    generated_by_run: bool = True,
) -> QualifiedName

manager.add_activity(
    action: str,
    *,
    inputs: Mapping[str, QualifiedName],
    outputs: Mapping[str, VariableSpec],
    derivations: Mapping[str, Sequence[str]] | None = None,
    context: Mapping[str, QualifiedName] | None = None,
    action_spec: Mapping[str, object] | None = None,
    agent: QualifiedName | None = None,
    plan: QualifiedName | None = None,
    activity_id: uuid.UUID | None = None,
) -> ActivityResult
```

The approved Step 2.4 implementation uses public frozen `VariableSpec` and
`ActivityResult` dataclasses. `ActivityResult.outputs` is exposed as an
immutable mapping. Public relationship endpoints are `QualifiedName` values;
UUID and string resolution can be considered later without changing the
canonical PROV identities.

The implementation review resolved:

- whether public return values should be `QualifiedName`, small immutable
  campaign reference objects, or UUIDs;
- whether `add_variable` selects a dataset only by name or also by UUID;
- whether Plan and Agent registration are separate public methods in Phase 2;
- how dataset replica locations are encoded as `prov:location`; and
- whether the existing upgrade mechanism or lazy table creation is used for
  old ACA files.

## 11. Import and export behavior

### 11.1 Import

For `activate=False`:

- require successful `prov` parsing and deterministic storage;
- preserve unknown parsed records;
- do not require the document to satisfy the campaign-authored subset; and
- retain unfamiliar records without destructive filtering. A structured
  warning/report result is deferred until its public shape is designed.

For `activate=True`:

- additionally validate campaign namespace ownership, self-containment,
  dataset references, stable identities, and supported-core invariants;
- reject unresolved campaign-qualified endpoints; and
- reject identifier collisions with different existing records.

`set_prov_document_active()` applies the same checks when an inactive import
is promoted later. Deactivation validates the graph that would remain, so it
cannot remove the only declaration of an endpoint still used by another
active document. The reserved authored document `campaign-provenance` cannot
be deactivated.

The Step 2.5 validator deliberately stops short of full PROV-CONSTRAINTS. It
checks the active graph union for:

- references to another campaign namespace;
- unresolved formal relationship endpoints;
- conflicting reuse of identifiers across active documents;
- stable campaign identifier and role consistency for supported records;
- live ACA dataset resolution for `hpc:Dataset` Entities;
- required logical-variable references and attributes;
- canonical JSON and hashes for `hpc:ActionSpecification`; and
- more than one generating Activity for an Entity.

Valid unfamiliar PROV records and unfamiliar Activity types remain active when
those graph-wide campaign constraints are satisfied.

Activation is explicit. Importing a document does not silently alter the
active authored graph.

### 11.2 Export

- Return the stored canonical PROV-JSON without translating it through custom
  SQL provenance tables.
- Verify its hash before export.
- Permit export of active and inactive documents.
- Never expose SQLite row IDs as PROV identifiers.
- Preserve namespaces and unknown parsed records.

## 12. Test plan

All new tests must explain the semantic requirement they protect, especially
where PROV dataflow differs from derivation.

### 12.1 Identifier and entity tests

- Campaign UUID persists across close/reopen and file rename.
- Database rebuild or row-order changes do not alter public UUID-based IDs.
- Dataset, run, variable, Activity, Plan, Agent, Usage, Generation, and
  Derivation identifiers are deterministic and campaign-qualified.
- A logical-variable identifier has no revision suffix.
- A growing dataset leaves the logical-variable identifier and PROV record
  count unchanged.
- Invalid UUID and role tokens are rejected.

### 12.2 Storage tests

- First provenance use creates one active canonical document.
- Canonical text and hash are deterministic.
- Close/reopen returns a semantically equal `ProvDocument`.
- Tampered content or hash mismatch is detected.
- Multiple documents remain distinguishable.
- Inactive imported documents are preserved but excluded from authored graph
  operations.
- PROV-JSON export is readable by `prov`.

### 12.3 Dataset and run tests

- Existing live ACA dataset maps to one Dataset Entity.
- Re-registering the same dataset is idempotent.
- Missing, deleted, invalid-UUID, and locationless datasets are rejected.
- Multiple replica locations are preserved according to the selected encoding.
- Run Activity optionally associates an Agent and Plan.
- Source logical variable is optionally generated by its run.

### 12.4 Activity graph tests

- One input and one output.
- Multiple role-qualified inputs and outputs.
- One output derived from a subset of inputs.
- Default all-scientific-inputs-to-all-outputs derivation.
- Context Usage is present in dataflow but absent from lineage.
- Qualified Derivation resolves the exact Activity, Usage, and Generation.
- Direct imported Derivation without an Activity survives round trip.
- Unknown imported Activity type survives round trip.
- A second generating Activity for one Entity is rejected.
- Cross-campaign references are rejected for authored records.

### 12.5 Transaction tests

For each injected failure below, compare the stored JSON and hash before and
after the call and require exact equality:

- invalid second output;
- unknown derivation role;
- missing input;
- invalid action-specification JSON value;
- serialization failure;
- validation failure; and
- optimistic concurrency/hash conflict.

Also verify that a successful multi-output Activity changes the canonical
document in one committed update.

### 12.6 Compatibility tests

- Full existing test suite remains green.
- An ACA without provenance tables can still be opened and read.
- The selected upgrade/lazy-creation path adds provenance tables without
  modifying existing dataset UUIDs, replicas, or payload content.
- Existing `info`, schema, image, scalar-field, and visualization behavior is
  unchanged.

## 13. Verification commands

Run at minimum:

```bash
poetry run pytest tests/test_prov_mapping.py tests/test_prov_store.py tests/test_manager_provenance.py -q
poetry run pytest -q
poetry run ruff check hpc_campaign tests
poetry run ruff format --check hpc_campaign tests
poetry run mypy hpc_campaign --check-untyped-defs
poetry run pylint hpc_campaign
poetry check --lock
```

Also run `git diff --check` and a PROV-JSON round trip through the exact locked
`prov` version.

## 14. Implementation sequence and review gates

### Step 2.1: Freeze public and persistent boundaries

**Status:** Complete. The public document API, two-table ACA 0.8 storage,
explicit upgrade path, and repeated dataset-location direction were approved.
Replica locations use `protocol://host/path` when the ACA host has a protocol
and `host:path` otherwise; archived replicas include the archive and member.

- Review the proposed public signatures and return types.
- Select upgrade versus lazy table creation.
- Finalize dataset-location encoding.
- Approve the two-table persistent schema.

**Gate:** explicit approval is required because this changes public API and
persistent database structure.

### Step 2.2: Add storage foundation

**Status:** Complete. Implemented in `hpc_campaign/prov_store.py` with focused
coverage in `tests/test_prov_store.py`; the complete suite passes.

- Add campaign identity and document tables.
- Implement canonical JSON, hashes, document read/write, and corruption tests.
- Do not add scientific authoring helpers yet.

**Gate:** storage tests and the complete existing test suite pass.

### Step 2.3: Add dataset, run, variable, Plan, and Agent mappings

**Status:** Complete. Implemented in `hpc_campaign/provenance.py` and the thin
`Manager` facade, with focused coverage in `tests/test_provenance.py`.

- Reuse Phase 1 identifier code.
- Map existing dataset UUIDs and locations.
- Add stable logical variables without revisions.
- Add focused persistence and round-trip tests.

**Gate:** mappings survive close/reopen and exact PROV-JSON round trips.

### Step 2.4: Add atomic Activity authoring

**Status:** Complete. Implemented in `hpc_campaign/provenance.py`, exposed by
the thin `Manager` facade, and covered by
`tests/test_activity_provenance.py`.

- Add role-qualified input/output construction.
- Add explicit qualified derivations.
- Add action specifications and Associations.
- Implement candidate-document validation and one-transaction replacement.
- Add failure-injection rollback tests.

**Gate:** Passed. Focused graph, persistence, controlled-vocabulary,
deduplication, self-containment, and exact-JSON rollback tests pass.

### Step 2.5: Add import/export escape hatch

**Status:** Complete. Implemented by `hpc_campaign/prov_validation.py`, the
transactional activation support in `hpc_campaign/prov_store.py`, and the
public `Manager.set_prov_document_active()` facade. Focused coverage is in
`tests/test_prov_import.py`.

- Preserve unknown parsed records.
- Separate inactive preservation from active campaign validation.
- Add self-containment and collision tests.

**Gate:** Passed. Imported unknown types and direct derivations survive
load/save/export; failed import, activation, and deactivation operations leave
stored content and activation state unchanged.

### Step 2.6: Document and assess

**Status:** Complete. The runnable example is
`examples/provenance_workflow.py`; the reproducible benchmark is
`tools/benchmark_provenance_scale.py`; results and interpretation are recorded
in `PROV_PHASE2_COMPLETION_REPORT.md`.

- Add API examples for simulation output, reduction, QoI, and visualization.
- Document the stable growing-entity rule and timestep limitation.
- Record measured document size, write cost, and load cost for a representative
  graph.
- Identify any finding that must move indexing or partitioning into an earlier
  milestone.

**Gate:** Passed. Full verification succeeds. The measured upper-scale
projection requires batching and partitioned/compressed canonical documents
to move into the production query-index milestone; those architectural changes
are documented but intentionally not implemented in Phase 2.

## 15. Principal risks

### Rewriting one document

Phase 2 rewrites one canonical JSON document for each authored transaction.
This is simple and safe but will not scale indefinitely. Measure it; do not
hide it behind an API that assumes one document forever.

### Concurrent writers

SQLite serialization alone prevents simultaneous commits but does not prevent
a manager from overwriting a document loaded before another manager's commit.
The stored-hash comparison is required.

### Dataset locations

ACA datasets can have multiple replicas. A single ad hoc path would lose
information or become machine-specific. Finalize the repeated-location
encoding before making the mapping public.

### Imported PROV

The `prov` package can preserve more PROV than the campaign API understands.
Activation and preservation must remain separate so a warning does not turn
into data loss or an invalid active graph.

### Time-varying entity semantics

One growing Entity keeps metadata small and matches the current campaign need,
but cannot answer which exact timesteps existed when an Activity ran. This is
an accepted version-one limitation, not an accidental omission.

### Canonical versus derived data

Future SQL indexes must remain rebuildable from stored PROV. Phase 2 must not
introduce convenient SQL fields whose semantics cannot be reconstructed from
the canonical documents.

## 16. Completion criteria

Phase 2 is complete only when:

1. A real ACA can persist the end-to-end simulation -> reduction -> QoI ->
   visualization graph.
2. The graph survives close/reopen and exact PROV-JSON export/import.
3. Dataset, run, logical-variable, Activity, Usage, Generation, Derivation,
   Plan, and Agent identities are stable and self-contained.
4. Logical variables remain unversioned across ordinary timestep appends.
5. Detailed derivations retain their exact input/output roles and relationship
   identifiers.
6. Context and lineage remain distinguishable.
7. A failed multi-record Activity leaves no partial provenance.
8. Unknown imported PROV records are preserved.
9. Existing campaign behavior and the complete test suite remain green.
10. Public API and persistent schema changes have received explicit review.
