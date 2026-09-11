# W3C PROV Mapping Spike Results

**Status:** Phase 1 complete

**Date:** 2026-09-01; updated 2026-09-02 for stable time-varying entities

## Purpose

This spike validates the central mappings in
`PROV_CAMPAIGN_SCHEMA_DESIGN.md` before PROV storage or indexing is integrated
with `Manager`. It is intentionally isolated from the production campaign
database and public API.

## Dependency result

- Added `prov >= 3.1.0, < 4` as a runtime dependency.
- `poetry.lock` resolves the dependency to `prov 3.1.0`.
- No optional RDF, XML, graph, or plotting extras are enabled.
- The package requires Python 3.10 or newer, matching HPC Campaign's declared
  `requires-python = ">=3.10"` range.
- The spike was executed locally with Python 3.14.7. The existing CI uses
  Python 3.13; CI remains the verification point for that supported runtime.

The lockfile change adds only `prov`; it does not update unrelated locked
packages.

## Prototype artifacts

- `hpc_campaign/prov_mapping.py`
  - Creates stable campaign-qualified PROV identifiers from UUIDs.
  - Uses one stable identity for each growing logical-variable entity.
  - Identifies Usage, Generation, and Derivation relationships.
  - Builds a representative simulation-to-investigation `ProvDocument`.
  - Keeps scientific derivation inputs separate from activity context.
- `tests/test_prov_mapping.py`
  - Exercises identifier stability and validation.
  - Verifies logical variables have no revision suffix or attribute.
  - Verifies exact PROV-JSON round trips.
  - Verifies qualified derivation endpoints and roles.
  - Verifies preservation of unknown domain-specific activity types.
  - Verifies Fides usage without incorrectly making Fides scientific lineage.
  - Verifies AI activity association, human conclusion attribution, and
    evidence relationships.

Nothing in this spike is exported from `hpc_campaign.__init__`, and `Manager`
does not read or write these records yet.

## Validated mapping decisions

### Stable identifiers

The prototype uses one object namespace per campaign:

```text
urn:hpc-campaign:CAMPAIGN_UUID:
```

Object UUIDs remain stable while the qualified-name prefix distinguishes the
kind of record:

```text
hpcid:dataset_UUID
hpcid:variable_UUID
hpcid:run_UUID
hpcid:activity_UUID
hpcid:usage_ACTIVITY_UUID_ROLE
hpcid:generation_ACTIVITY_UUID_ROLE
hpcid:derivation_ACTIVITY_UUID_OUTPUTROLE_INPUTROLE
```

This works with `prov 3.1.0` and survives PROV-JSON serialization. The exact
vocabulary and identifier spelling remain provisional until the corresponding
open decisions in the design document are closed.

### Growing logical variables

The prototype uses one PROV Entity and one stable qualified name for each
logical variable. A time-varying variable or image sequence keeps that identity
while the underlying dataset gains timesteps or members. Ordinary appends do
not create revision attributes, new entities, new Generation records, or new
Derivation records.

This deliberately omits provenance for the exact timestep range visible at a
particular historical instant. A scientifically distinct replacement or
transformed product receives a new logical-variable UUID and normal PROV
Generation and Derivation relationships. Snapshot and timestep-specific
provenance are deferred.

### Detailed derivation

Campaign-authored dependencies can use the full qualified PROV derivation:

```text
wasDerivedFrom(derivation; output, input, activity, generation, usage)
```

The package preserves the references to the exact role-qualified Usage and
Generation records. This is sufficient to distinguish the dependencies of
multiple outputs from multiple inputs without a custom provenance edge model.

### Activity context versus lineage

The visualization example records a Fides model as an entity used with the
`data_model` role. The rendered image has explicit derivations from the
scientific color and annotation inputs, but not from the Fides entity. This
confirms that activity dataflow/context and entity lineage can remain separate
as required by PROV-DM.

### Extensible types

An activity with the unrecognized type `hpc:DomainSpecificOperation` survives
the complete PROV-JSON round trip. A future campaign profile can warn about or
decline to index the type without discarding the underlying PROV record.

### Plans, agents, and investigation records

The representative graph includes:

- an XGC simulation activity associated with a software agent and input plan;
- a reduction associated with MGARD and a prescriptive action specification;
- a visualization associated with ParaView and a Fides plan;
- an AI comparison activity using a question and image as inputs; and
- a conclusion attributed to a human agent.

## Representative measurement

The current example contains 50 PROV records:

| Record | Count |
|---|---:|
| Entity | 11 |
| Activity | 6 |
| Agent | 5 |
| Usage | 9 |
| Generation | 7 |
| Derivation | 7 |
| Association | 4 |
| Attribution | 1 |

On the local Python 3.14.7 environment, measured across 100 iterations:

| Measurement | Result |
|---|---:|
| PROV-JSON size | 13,881 bytes |
| gzip-compressed size | 2,208 bytes |
| Median graph construction | 0.460 ms |
| Median serialization | 0.308 ms |
| Median deserialization | 0.616 ms |

These numbers validate basic mechanics only. They are not a substitute for the
1,000-run scale test required by Section 26.9 of the design document.

## Package limitations confirmed

- PROV-JSON is the dependency-light round-trip format.
- PROV-N can be generated for diagnostics, but `prov 3.1.0` raises
  `NotImplementedError` when asked to deserialize PROV-N.
- `ProvDocument` is an in-memory model. The production implementation must not
  assume that a large campaign can always be loaded as one document.
- Parsing and round-trip equality do not provide complete W3C
  PROV-CONSTRAINTS validation. Campaign validation remains a separate layer.

## Verification commands

```bash
poetry run pytest tests/test_prov_mapping.py -q
poetry run ruff check hpc_campaign/prov_mapping.py tests/test_prov_mapping.py
poetry run ruff format --check hpc_campaign/prov_mapping.py tests/test_prov_mapping.py
poetry run mypy hpc_campaign/prov_mapping.py --check-untyped-defs
poetry run pylint hpc_campaign/prov_mapping.py
```

## Phase 2 boundary

The spike supports proceeding to the canonical scientific core, subject to
review of the provisional namespace and identifier spelling. Phase 2 would
integrate PROV entities and relationships with campaign datasets, runs, and
logical variables; define atomic storage behavior; and make the SQLite schema
a rebuildable query index. Those changes are not part of this spike.
