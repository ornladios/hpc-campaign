# Scientific Provenance Implementation Notes

**Status:** Current API design, updated 2026-10-14.

This note describes the current provenance model implemented by the campaign
API. The detailed schema, examples, and mapping are also documented in
`PROV_CAMPAIGN_SCHEMA_DESIGN.md`, `PROV_MAPPING_SPIKE.md`, and
`PROV_PHASE2_COMPLETION_REPORT.md`.

## Public API

```python
pressure = manager.add_variable(
    dataset="simulation-output",
    variable="P",
    definition="pressure",
    units="Pa",
    coordinate_system="boozer",
)

manager.add_activity(
    "simulation",
    name="simulation-001",
    outputs={"pressure": pressure},
    agent=xgc,
    plan=simulation_plan,
)

reduced_pressure = manager.add_variable(
    dataset="reduced-products",
    variable="P_reduced",
    definition="pressure",
    units="Pa",
)
reduction = manager.add_activity(
    "reduction",
    inputs={"source": pressure},
    outputs={"result": reduced_pressure},
    metadata={"method": "mgard", "error_bound": 0.001},
    agent=mgard,
)
```

`add_variable()` registers one stable logical-variable Entity backed by an
existing dataset. It does not choose or create a producer. `add_activity()`
records one Activity and connects its causal input products to registered
output products. Its input and output mapping keys are PROV roles. The
returned `ActivityResult` contains the Activity identifier and immutable
output references.

For images and videos, use `add_dataset_entity(name)` to register the existing
dataset UUID as an Entity typed `hpc:Dataset` and `hpc:Image` or `hpc:Video`.
`add_visualization_sequence_entity(name)` registers an ordered collection snapshot.
It reuses a stored UUID for unchanged membership and creates a new snapshot
when membership or order changes. Membership uses `hadMember`; order is a
canonical JSON list of dataset UUIDs in `hpc:memberOrder`. No additional ACA
tables or dependencies are required.

The corresponding `dataset_entity(name)` and
`visualization_sequence_entity(name)` methods perform read-only lookup over
active documents and raise `LookupError` if registration is missing. Sequence
lookup requires a snapshot matching the current ordered membership. Unchanged
registration calls also skip serialization and writes, preserving timestamps.
New replica locations are refreshed by explicit dataset registration.

## Graph rules

- A simulation is an Activity typed `hpc:Simulation` and can have no inputs.
- Inputs are causal product Entities: logical variables, datasets, or
  visualization collection snapshots. Context Entities are recorded as Usage
  but are excluded from causal ancestry. Plans cannot be causal endpoints.
- Every output Entity must already exist and may have at most one producing
  Activity across the active graph.
- Every Activity should describe one coherent transformation. By default,
  every output derives from every causal input. `derivations=` specifies a
  different input set per output when needed.
- Optional JSON metadata is stored directly on its Activity. It creates no
  separate Entity, Plan, or Usage.
- Agent and Plan associations are optional.
- Activity endpoints, Agents, Plans, and context may be resolved from any
  active document or bundle. Imports remain in their original documents;
  inactive imports cannot supply references.
- The active Derivation graph must be acyclic. Qualified Derivation references
  must identify the correct Generation and Usage record types and match their
  Entity and Activity endpoints across the active graph.
- Product and Activity ancestry is formed by following Generation and
  Derivation relationships. Visualization settings and other context are
  outside this ancestry view. Collection membership does not imply generation
  or derivation of individual members.

## Persistent representation

W3C PROV-JSON remains the canonical relationship representation. The ACA
stores campaign-qualified stable UUIDs for datasets, logical variables,
Activities, Agents, and Plans. Usage, Generation, and Derivation records have
stable identifiers that encode the Activity and role names they describe.

Authoring validates references and builds a candidate PROV document before
replacing the stored canonical document. Active imported documents are checked
for campaign namespace ownership, resolvable relationship endpoints, valid
supported core records, and no Entity with multiple generating Activities.
Inactive imports can be retained for later inspection without joining the
active graph.

Activity metadata is a canonical JSON object. Unsupported metadata values and
non-canonical imported metadata are rejected when the document becomes active.
All object keys must be strings at every nesting level, including dictionaries
inside lists or tuples. Non-string keys are rejected before serialization so
their meaning cannot change through implicit conversion.

## Validation and scale

Focused coverage is in `tests/test_activity_provenance.py`,
`tests/test_provenance.py`, `tests/test_prov_import.py`, and
`tests/test_prov_mapping.py`, and `tests/test_product_provenance.py`. The end-to-end example is
`examples/provenance_workflow.py`; the scale builder is
`tools/benchmark_provenance_scale.py`.

The benchmark reports measurements for a complete document construction and
storage rewrite. It does not measure repeated high-level authoring or justify
capacity projections without a current run. Batching, bounded documents,
compression, and the query index remain separate design work.
