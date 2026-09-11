# Phase 2 Scientific Provenance: Examples and Scale Assessment

**Status:** Complete on 2026-09-02

**Scope:** W3C PROV scientific core only. Collaboration records, configurable
profiles, and the query index are not implemented in Phase 2.

## 1. End-to-end example

`examples/provenance_workflow.py` creates a real ACA and records this graph:

```text
simulation run -> pressure
pressure -> reduction -> reduced pressure
reduced pressure -> quantity-of-interest calculation -> total pressure
reduced pressure + total pressure -> visualization -> image
```

Run it with a new destination directory:

```bash
poetry run python examples/provenance_workflow.py /tmp/provenance-example
```

The script refuses to replace an existing ACA. It creates placeholder payload
datasets because real simulations and visualization tools produce their files
before the provenance transaction. The same API works with registered ADIOS,
HDF5, image, text, or other campaign datasets.

### 1.1 Simulation output

```python
xgc = manager.add_agent("software", "XGC", version="example")
run_plan = manager.add_plan(
    "XGC input configuration",
    location="inputs/run-001.yaml",
)
run = manager.add_run("run-001", agent=xgc, plan=run_plan)
pressure = manager.add_variable(
    run=run,
    dataset="simulation-output",
    variable="P",
    definition="pressure",
    units="Pa",
    coordinate_system="boozer",
)
```

The source logical variable is one PROV Entity. Its Generation identifies the
simulation run Activity.

### 1.2 Reduction

```python
reduced = manager.add_activity(
    "reduction",
    inputs={"source": pressure},
    outputs={
        "result": VariableSpec(
            run=run,
            dataset="reduced-products",
            variable="P_reduced",
            definition="pressure",
            units="Pa",
            coordinate_system="boozer",
        )
    },
    action_spec={"method": "mgard", "error_bound": 0.001},
    agent=mgard,
)
```

The result is a new logical-variable Entity. The graph contains identified,
role-qualified Usage and Generation plus a qualified Derivation that names
the exact Activity, Usage, and Generation.

### 1.3 Quantity of interest

```python
total_pressure = manager.add_activity(
    "quantity_of_interest",
    inputs={"pressure": reduced.outputs["result"]},
    outputs={
        "total": VariableSpec(
            run=run,
            dataset="qoi-products",
            variable="total_pressure",
            definition="total_pressure",
            units="Pa",
        )
    },
    action_spec={"operation": "sum"},
    agent=analysis,
)
```

A QoI product can be an array, field, scalar, or another scientifically named
result. Its physical form remains the responsibility of the referenced native
dataset format.

### 1.4 Visualization and non-lineage context

```python
image = manager.add_activity(
    "visualization",
    inputs={
        "color": reduced.outputs["result"],
        "annotation": total_pressure.outputs["total"],
    },
    context={"data_model": fides},
    outputs={
        "image": VariableSpec(
            run=run,
            dataset="visualizations",
            variable="pressure.png",
            definition="pressure_visualization",
        )
    },
    action_spec={"representation": "isosurface", "colormap": "viridis"},
    agent=paraview,
    plan=fides,
)
```

The Fides Plan is used as context but is not a scientific lineage parent.
Tracing explicit Derivation records from the image reaches reduced pressure
and total pressure, not the visualization configuration.

### 1.5 Broader PROV import

```python
document_id = manager.add_prov_document(
    document,
    name="domain-specific-provenance",
    activate=False,
)

# Promotion is explicit and validates the union of active documents.
manager.set_prov_document_active(document_id, active=True)
manager.export_prov(document_id, "domain-specific-provenance.json")
```

Inactive documents preserve parseable unfamiliar PROV even when campaign
references are unresolved. Activation enforces campaign self-containment and
the supported core invariants. Unknown valid PROV types are not discarded.

## 2. Growing Entity and timestep boundary

A time-varying variable is one logical-variable Entity while its referenced
dataset grows. Appending timesteps or image-sequence members requires no new
provenance call and does not create another Entity, Generation, or Derivation.

```text
before append: variable_PRESSURE_UUID -> output.bp containing steps 0..99
after append:  variable_PRESSURE_UUID -> output.bp containing steps 0..109
```

Create a new logical variable when scientific identity changes: a transformed
product, corrected replacement, new units or coordinate system, or separately
named scientific result. Ordinary append is not such a change.

Accepted limitation: Phase 2 cannot answer which exact timesteps existed when
an Activity executed. Snapshot Entities, timestep-range Usage, and revision
chains remain deferred until a concrete use case requires historical-state
provenance.

## 3. Reproducible scale benchmark

`tools/benchmark_provenance_scale.py` builds a representative graph through
the Python `prov` object model and measures canonical serialization,
deserialization, gzip compression, and one complete SQLite insert/rewrite/load.
It intentionally avoids repeated `Manager.add_activity()` calls so it measures
one full-document transaction rather than cumulative rewrite cost.

Example:

```bash
poetry run python tools/benchmark_provenance_scale.py \
  --runs 20 --variables 50 --reductions 2 --qois 3 \
  --visualizations 1 --repeats 1
```

Each source product contributes an Entity and Generation. Each derived or
visual product contributes seven records: Entity, Activity, two Usages,
Generation, Derivation, and Association. Shared Plans and Agents are counted
once.

### 3.1 Measured samples

Environment:

- Apple M3 Max, 128 GiB RAM
- macOS Darwin 25.6.0 arm64
- Python 3.14.7
- `prov` 3.1.0

Times are medians of three iterations except the largest sample, which used
one iteration. Peak values are Python allocations observed by `tracemalloc`
during each phase; they are not operating-system RSS.

| Runs | Variables/run | Reduction + QoI | Products | Records | PROV-JSON | gzip | Serialize | Deserialize | SQLite rewrite |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 10 | 1 + 1 | 31 | 178 | 50,802 B | 4,661 B | 0.015 s | 0.007 s | 0.005 s |
| 5 | 20 | 2 + 3 | 605 | 3,758 | 1,066,099 B | 139,201 B | 0.320 s | 0.139 s | 0.082 s |
| 10 | 40 | 2 + 3 | 2,410 | 14,908 | 4,235,075 B | 573,849 B | 1.336 s | 0.610 s | 0.388 s |
| 20 | 50 | 2 + 3 | 6,020 | 37,208 | 10,574,005 B | 1,459,375 B | 3.251 s | 1.557 s | 0.978 s |

The largest measured sample used approximately:

- 79.7 MB peak traced allocation while constructing the graph;
- 216.8 MB additional peak allocation while serializing; and
- 163.3 MB peak allocation while deserializing.

The measurements are approximately linear for a single complete operation.
They do not make the current high-level authoring path linear: every authoring
call currently rewrites the growing canonical document, so a long sequence of
individual calls has cumulative quadratic I/O and serialization work.

### 3.2 Upper-scale projection

The roadmap scenario is:

- 1,000 runs;
- 100 source variables per run;
- 5 reductions per source;
- 10 QoIs per source; and
- one selected visualization per run.

This produces approximately 1,601,000 logical-variable products and
10,710,008 PROV records with the representative relationship detail.

The full graph was not materialized because the largest measured sample
already projects beyond the storage target and into tens of gigabytes of
Python memory. Scaling the largest sample by record count gives an approximate
single-document projection:

| Measurement | Linear projection |
|---|---:|
| Canonical PROV-JSON | 3.04 GB |
| gzip-compressed PROV-JSON | 420 MB |
| SQLite file containing uncompressed JSON | 3.05 GB |
| Graph-construction traced allocation | 22.9 GB |
| Serialization traced allocation | 62.4 GB |
| Deserialization traced allocation | 47.0 GB |
| One serialization | 15.6 minutes |
| One deserialization | 7.5 minutes |
| One SQLite rewrite | 4.7 minutes |
| One SQLite load | 3.4 minutes |

These are extrapolations, not measurements of the full scenario. Timing is
likely optimistic once allocator pressure, validation, and storage limits are
included. Compression meets the preferred disk range, but it does not solve
the decompressed in-memory model or repeated-rewrite cost.

## 4. Roadmap decision

Phase 2 is a correct functional core and a suitable small-campaign prototype,
but its monolithic active document is not a production-scale representation
for the upper scenario.

The following work must occur before production-scale ingestion and before a
large query index is finalized:

1. Partition canonical PROV into bounded documents, initially by run or by a
   bounded group of runs, while retaining stable record identifiers.
2. Add a batch authoring boundary so a shard is constructed and committed once
   rather than rewritten after every logical variable or Activity.
3. Add transparent lossless compression for canonical stored documents.
4. Build the derived query index across shards and load detailed PROV only for
   requested subgraphs.
5. Re-run the full 1,000-run benchmark, including index size and representative
   bounded queries, after partitioning and indexing exist.

Therefore the storage-partitioning work previously placed late in the roadmap
must become a prerequisite of the production query-index phase. Configurable
profiles can proceed independently, but collaboration should not be built on
the monolithic storage assumption.

## 5. Phase 2 conclusion

Phase 2 now demonstrates:

- persistent standard PROV for runs, data products, and processing;
- exact scientific lineage with distinguishable context;
- broader PROV import without data loss;
- transactional campaign validation and activation;
- a runnable end-to-end campaign; and
- a measured reason to introduce batching, partitioning, compression, and a
  rebuildable index before production-scale use.
