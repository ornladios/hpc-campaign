# Scientific Provenance: Current API and Scale Assessment

**Status:** Updated for the causal Activity model.

## End-to-end API example

The example in `examples/provenance_workflow.py` records a source simulation,
then reduction, quantity-of-interest, and visualization Activities. Variables
are registered independently from producer relationships:

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
    coordinate_system="boozer",
)
reduction = manager.add_activity(
    "reduction",
    inputs={"source": pressure},
    outputs={"result": reduced_pressure},
    metadata={"method": "mgard", "error_bound": 0.001},
    agent=mgard,
)
```

Activity inputs represent causal products and their mapping keys become PROV
roles. A simulation can have no inputs. Metadata is optional and belongs to
the Activity. Context Entities are recorded separately and do not become
causal-ancestry parents. By default, every output derives from every causal
input; `derivations=` can specify different parents for different outputs.

The visualization writes a small valid PNG, registers it with `image()`, and
uses `add_dataset_entity("pressure-image")` as its output. This Entity is typed
`hpc:Dataset` and `hpc:Image`, with no artificial logical-variable wrapper.
Video datasets work the same way. Registered visualization sequences can be
registered as ordered collection snapshots with `add_visualization_sequence_entity()`.
Sequence changes create new snapshots while earlier graph references remain
valid. Membership alone does not create causal or producer relationships for
the members.

`dataset_entity()` and `visualization_sequence_entity()` are read-only lookup
methods and require prior registration. The latter requires a stored snapshot
matching the current ordered membership. Unchanged registration calls also
perform no write and preserve the document timestamp.

Both runnable examples use product names consistently for Entity references.
They read the stored graph and print each product's producer and causal
parents, tracing back to the simulation. The workflow example includes both
the reduced field and the quantity of interest as image parents, marks shared
ancestors, and excludes the Fides context. The shared example helper follows
Generation and Derivation, rather than treating every Usage as causality.

Run the complete example in a new destination directory:

```bash
poetry run python examples/provenance_workflow.py /tmp/provenance-example
```

## Scale benchmark

`tools/benchmark_provenance_scale.py` builds a representative PROV document
directly through the `prov` object model, then measures construction,
serialization, deserialization, and SQLite storage operations. Use
`--simulations` to set the number of source simulation Activities:

```bash
poetry run python tools/benchmark_provenance_scale.py \
  --simulations 20 --variables 50 --reductions 2 --qois 3 \
  --visualizations 1 --repeats 1
```

Each source variable adds an Entity and Generation. Each derived product adds
an Entity, Activity, causal Usage, Generation, Derivation, and Association.
Activity metadata adds no separate PROV records.

The earlier scale measurements described a different graph shape. They are
not reported here as current measurements; rerun the updated benchmark before
using record counts or performance projections for capacity planning.

## Deferred work

The benchmark still measures one complete document and one SQLite rewrite. It
does not establish that repeated high-level authoring scales linearly. Bounded
documents, batch authoring, compression, and a rebuildable query index remain
separate work that should be informed by measurements of the current graph.
