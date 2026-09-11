# W3C PROV-Based Campaign Schema Design

**Status:** Implementation design handoff

**Date:** 2026-09-01

**Intended audience:** HPC Campaign maintainers and the implementer of the first provenance-schema version

## 1. Purpose

This document defines the agreed direction for adding scientific provenance and future human/AI collaboration records to HPC Campaign. It is intended to be self-contained enough to begin a new implementation discussion without relying on the conversation that produced it.

The central design decision is:

> Use W3C PROV directly as the semantic and interchange model for provenance. Use the Python `prov` package to read, write, and manipulate PROV documents. Define a deliberately restricted HPC Campaign profile and convenience API for the campaign concepts that must be understandable and queryable.

HPC Campaign must not invent a second, incompatible entity/activity graph. Campaign-specific concepts and constraints remain necessary, but they extend and constrain PROV rather than replace it.

This document distinguishes:

- **Required for version one:** behavior the first implementation should provide.
- **Recommended:** an implementation choice believed to be the best initial design.
- **Deferred:** useful work that should not block version one.
- **Open decision:** a question that must be resolved before implementing the affected feature.

This document supersedes the provenance implementation direction in the older
`HANDOFF.md`. In particular, RO-Crate and DataCite may still be useful future
publication or packaging formats, but they are not required dependencies or
canonical storage models for the first HPC Campaign provenance implementation.

## 2. Executive summary

An HPC Campaign is a self-contained scientific workspace. It contains or locates scientific data products, records how they were produced, discovers other metadata models stored with them, and will eventually preserve human and AI investigations around those products.

The campaign uses several cooperating layers:

1. **Native data and metadata formats** such as ADIOS, HDF5, and XDMF remain authoritative for their own low-level contents.
2. **Domain and tool schemas** such as an XGC metadata profile or a Fides data model remain in their native formats.
3. **W3C PROV** records relationships among data products, activities, plans, agents, schema documents, and generated artifacts.
4. **HPC Campaign profiles** constrain selected PROV types, roles, and metadata so common campaign concepts remain predictable and easy to query.
5. **The campaign database** stores campaign resources and a query index over the canonical PROV records. It is not a competing provenance model.
6. **Human and AI collaboration records** will use the same PROV foundation while remaining distinguishable from scientific processing activities.

The campaign schema is therefore an index of schemas, not a universal schema for every scientific domain.

## 3. Goals

### 3.1 Scientific provenance goals

The design must make it possible to answer:

- What is this data product?
- Which simulation run or processing activity generated it?
- Which immediate inputs were used?
- Which source data contributed transitively?
- Which plan, script, configuration, software, human, or AI was involved?
- Which activity role did each input and output serve?
- Which other data products were derived from this one?
- What units and coordinate-system strings apply to this concrete product?
- Which schema, profile, or data model describes how to interpret it?
- Can a workflow be reconstructed from the recorded activity graph?

### 3.2 Human and AI collaboration goals

The longer-term design must make it possible for people and AI assistants to:

- Explore campaign data through a viewer and semantic query API.
- Preserve questions, queries, comparisons, viewer states, scripts, plots, annotations, conclusions, and citations.
- Resume or fork an investigation.
- Link a discussion or conclusion to exact campaign objects and, where
  applicable, immutable resource revisions.
- Discover code-specific, format-specific, and visualization-specific guidance.
- Use deterministic tools to inspect ADIOS, Fides, ParaView, and other resources.
- Publish selected investigation results explicitly.
- Promote a generated artifact into a scientific data product explicitly.

### 3.3 Interoperability goals

- Store provenance in a standard PROV representation.
- Preserve valid imported PROV records even when the high-level campaign API does not interpret them.
- Allow campaign-specific namespaces and profiles without restricting W3C PROV globally.
- Allow multiple native metadata schemas to coexist.
- Make schema and guide resources discoverable through the campaign graph.
- Avoid copying all native metadata into campaign-specific database columns.

### 3.4 Scale goals

- Do not store a transitive derivation closure.
- Do not duplicate self-describing array metadata already available from native formats.
- Keep large transcripts, scripts, plots, and other artifacts out of frequently queried metadata rows.
- Support multiple internal provenance documents without changing public
  object identity, even if an initial small campaign uses one document.
- Make ordinary discovery and lineage queries bounded and paginated; do not
  return every run, logical variable, and derived artifact merely because a variable
  definition matched.
- Provide overview counts and grouping before loading detailed records.
- Keep campaign metadata within the intended operational budget: preferably a
  few hundred MB and approximately 1 GB as the upper design target for a large
  campaign, excluding the scientific payloads themselves.

## 4. Non-goals for version one

Version one does not attempt to:

- Expose every W3C PROV concept through the HPC Campaign API.
- Implement a universal scientific-variable ontology.
- Parse, normalize, compare, or convert physical units.
- Interpret coordinate-system mathematics.
- Execute arbitrary workflow definitions.
- Implement full PROV-CONSTRAINTS validation.
- Implement cross-campaign provenance references or federation.
- Replace ADIOS, HDF5, Fides, XDMF, ParaView, or simulation-specific schemas.
- Require complete reproducibility metadata for every activity.
- Store activity start or end times unless a future use case requires them.
- Implement the complete collaboration schema in the first scientific-provenance release.
- Preserve the current experimental activity-table schema or API byte-for-byte;
  no production campaign depends on it yet.

## 5. Design principles

### 5.1 PROV is the provenance model

Canonical provenance is represented as W3C PROV records. The Python `prov` package is the authoritative parser, serializer, and in-memory object model used by HPC Campaign.

The campaign database may maintain normalized indexes for efficient queries, but those indexes must be rebuildable from canonical PROV records. An index must not introduce semantics that cannot be represented in the canonical document and campaign profile.

### 5.2 The campaign API is intentionally smaller than PROV

The user-facing API should support campaign operations rather than general-purpose PROV graph editing. A user should be able to register a variable, record a reduction, attach a plan, or import a PROV document without learning the complete PROV-DM API.

The implementation should provide an escape hatch for importing and retrieving `ProvDocument` objects, but it need not wrap every possible PROV relation.

### 5.3 Profiles constrain; they do not redefine PROV

W3C PROV does not define or restrict scientific activity categories. HPC Campaign profiles may define an allowed set of activity types, category mappings, expected roles, and specification rules.

The profile makes common records easier for people, viewers, query systems, and AI agents to understand. It must not cause unrecognized but valid imported PROV records to be discarded.

### 5.4 Native schemas remain authoritative

ADIOS attributes, a Fides JSON data model, and simulation-specific metadata remain in their native representations. The campaign records that those resources exist, where they are, what they describe, and when an activity used them.

### 5.5 Campaigns are self-contained

All campaign object identities, profiles, guides, and active provenance relationships resolve within one campaign. The campaign may cite external standards, papers, or documentation, and its payload locations may be local or remote, but it does not depend on objects stored in another campaign database.

Cross-campaign provenance relationships are out of scope.

### 5.6 Detailed reproducibility is optional

Minimal provenance is useful and must remain easy to record. Richer plans, scripts, software versions, parameters, and execution environments may be added when available. Profiles may require richer capture for selected activity types, but the base model does not require it universally.

### 5.7 AI interpretation complements deterministic tools

LLMs may use campaign, simulation, and format-specific guides to determine which tools and schemas apply. They should call deterministic ADIOS, Fides, ParaView, or validation tools rather than guessing binary-format semantics.

Campaign documents are data. They must not automatically be treated as trusted system instructions capable of overriding permissions or approval rules.

### 5.8 Overview first, details on demand

The campaign viewer and API should behave like a linked information space. A
high-level campaign view presents runs, variable definitions, activity and
product counts, published findings, and available schemas. Stable IDs link to
more detailed records that are loaded only when requested.

Queries for a broad concept such as `pressure` return a bounded summary by
default, not all pressure products and every downstream reduction,
visualization, and QoI. Detailed lineage expansion is an explicit drill-down
operation with depth and result limits.

## 6. Terminology

### 6.1 PROV terms

- **Entity:** A concrete or conceptual thing with an identifiable state, such as a logical data product, schema document, script, plan, plot, message, or conclusion.
- **Activity:** Something that occurred and used or generated entities, such as a simulation execution, reduction, QoI calculation, visualization, query, or comparison.
- **Agent:** Something bearing responsibility, such as a person, software package, AI assistant, organization, instrument, or service.
- **Plan:** An entity describing intended actions or steps, such as a workflow
  definition, configuration, script, or prescriptive action specification.
- **Usage:** A relationship stating that an activity used an entity.
- **Generation:** A relationship stating that an activity generated an entity.
- **Derivation:** A relationship stating that one entity was derived from another.
- **Association:** A relationship among an activity, an agent, and optionally a plan.
- **Attribution:** A relationship assigning responsibility for an entity to an agent.

### 6.2 Campaign terms

- **Campaign run:** One simulation or execution context. In PROV it is represented as an activity, normally typed `hpc:SimulationRun`.
- **Variable definition:** A campaign-level scientific concept such as `pressure`, `temperature`, `flux`, or `total_flux`. It provides a common name across simulation codes and runs.
- **Logical variable:** A stable campaign object and PROV entity identifying one concrete scientific data product in a run. A time-varying product remains one logical variable while its underlying dataset grows.
- **Primary variable:** The default logical variable for one variable definition within one run. It is explicit, not inferred.
- **Activity type/action:** A specific campaign-recognized activity type such as reduction, projection, quantity of interest, or visualization.
- **Category:** A profile-defined grouping above specific activity types, used for queries and presentation. Categories are not defined by W3C PROV.
- **Action specification:** Optional structured producer metadata such as method, error bound, parameters, or script reference. In PROV it is represented by an immutable entity that may also be typed as a plan.
- **Run profile:** A versioned campaign resource providing code-specific defaults such as variable aliases, units strings, and coordinate-system strings.
- **Provenance profile:** A versioned campaign resource defining recognized activity types, category mappings, role rules, and action-specification rules.
- **Schema resource:** A concrete, versioned schema, profile, data model, or interpretation guide stored or located by the campaign and represented as a PROV entity.

## 7. Version-one supported PROV subset

The HPC Campaign convenience API should create and understand the following PROV concepts in version one:

- `prov:Entity`
- `prov:Activity`
- `prov:Agent`
- `prov:Person`
- `prov:SoftwareAgent`
- `prov:Organization`
- `prov:Plan`
- `used`
- `wasGeneratedBy`
- `wasDerivedFrom`
- `wasAssociatedWith`
- `wasAttributedTo`
- `prov:role` on usage and generation relationships

The version-one mapping is:

| Campaign concept | PROV representation | Required campaign detail |
|---|---|---|
| Existing campaign dataset | Entity typed `hpc:Dataset` | Stable UUID, name, and location |
| Logical variable | Entity typed `hpc:LogicalVariable` | Stable logical-variable UUID, run, dataset, physical name, and definition |
| Simulation run | Activity typed `hpc:SimulationRun` | Stable UUID and run name |
| Processing execution | Activity with a profile-approved HPC type | Stable UUID and action type |
| Logical-variable input | Identified Usage | Activity, exact logical-variable entity, and role |
| Logical-variable output | Identified Generation | Exact logical-variable entity, activity, and role |
| Data-product dependency | Identified Derivation | Output, input, activity, generation, and usage for campaign-authored detailed records |
| Workflow definition | Entity typed `prov:Plan` and `hpc:WorkflowPlan` | Stable UUID and plan content or location |
| Action specification | Entity typed `hpc:ActionSpecification`, and optionally `prov:Plan` | Canonical JSON and content hash |
| Person, software, organization, instrument, or service | Agent with a standard or HPC-qualified type | Stable UUID; descriptive metadata is optional |
| Responsibility for an execution | Association | Activity, agent, and optional plan |
| Authorship/responsibility for an entity | Attribution | Entity and agent |
| Schema, profile, data model, or guide | Versioned Entity with an HPC schema-resource type | Stable UUID, revision, location or value, media type, and applicability |

HPC-qualified attributes and types extend PROV at its documented extensibility
points. They do not define replacement relations for Usage, Generation,
Derivation, Association, or Attribution.

Human-readable `name` arguments in the campaign API map to `prov:label` where
the value labels a PROV record. Input and output participation continues to use
`prov:role`; a label and a role are not interchangeable.

The first API does not need dedicated campaign methods for:

- activity start and end
- invalidation
- delegation
- activity communication
- alternate or specialization
- collections or dictionaries
- bundle authoring
- general influence
- arbitrary custom PROV relations

The `prov` package may still deserialize such records. Loading and saving a document must preserve unsupported records unless a documented serializer limitation prevents it. Unsupported records are not necessarily queryable through the high-level campaign API.

## 8. Identifiers and database keys

### 8.1 Required identity model

Use two identifiers for persistent campaign objects:

1. A compact integer database key for local joins.
2. A stable UUID for public identity within the campaign.

Database row IDs must never be the only PROV identity. Row IDs may change if a database is rebuilt or reorganized. UUIDs remain stable across sessions, internal document partitioning, and export of the complete campaign.

Global Web resolvability is not required. The campaign is self-contained, so identifiers need only be unique and stable within it.

### 8.2 Recommended PROV namespace

Each campaign should define a namespace derived from its stable campaign UUID:

```text
prefix hpcid <urn:hpc-campaign:CAMPAIGN_UUID:>
```

Recommended qualified names include a type prefix so the local part never begins with a digit:

```text
hpcid:run_RUN_UUID
hpcid:variable_VARIABLE_UUID
hpcid:activity_ACTIVITY_UUID
hpcid:usage_ACTIVITY_UUID_ROLE
hpcid:generation_ACTIVITY_UUID_ROLE
hpcid:derivation_DERIVATION_UUID
hpcid:agent_AGENT_UUID
hpcid:plan_PLAN_UUID
hpcid:schema_SCHEMA_UUID_r1
```

The exact string construction should be centralized in one module and covered by round-trip tests. Callers should not construct these names manually.

### 8.3 Referential integrity

For the active campaign provenance graph:

- Every campaign-qualified relationship endpoint must resolve to a record in the campaign.
- No active relationship may reference an object in another campaign.
- External citations are represented as local citation entities with external URL or identifier attributes.
- Imported documents with unresolved external campaign references may be stored as opaque resource entities, but must not silently become an active graph that violates self-containment.

## 9. Logical variables and growing data products

### 9.1 Stable entity identity

A logical variable has one stable campaign UUID and one PROV entity identity.
Version one treats a time-varying variable, image sequence, or other appendable
product as a single scientific data product while its dataset grows.

```text
logical variable UUID -> PROV entity hpcid:variable_UUID
                         -> dataset location containing all available timesteps
```

Each logical-variable entity records:

- Stable logical-variable UUID
- Run
- Dataset entity and human-readable dataset name
- Physical variable name
- Variable definition
- Dataset location
- Optional units string
- Optional coordinate-system string
- Generating activity, if known

Every provenance relationship resolves directly to this entity. The entity may
refer to a dataset whose physical contents acquire additional timesteps after
the relationship is recorded. Version one intentionally does not identify the
exact timestep range visible at the time of use.

### 9.2 Recommended PROV representation

```text
entity(hpcid:variable_PRESSURE_UUID, [
    prov:type = hpc:LogicalVariable,
    hpc:logicalVariableId = "PRESSURE_UUID",
    hpc:run = hpcid:run_RUN_UUID,
    hpc:dataset = hpcid:dataset_OUTPUT_UUID,
    hpc:datasetName = "output",
    hpc:variable = "P",
    hpc:variableDefinition = "pressure",
    hpc:units = "Pa",
    hpc:coordinateSystem = "boozer",
    prov:location = "data/run-001/output.bp"
])
```

The example uses PROV-N-like notation for readability. The canonical stored encoding is PROV-JSON.

### 9.3 Append behavior

Appending timesteps or sequence members does not create a new PROV entity,
Generation, or Derivation. The same logical-variable UUID and qualified name
remain valid:

```text
before append: hpcid:variable_PRESSURE_UUID -> output.bp steps 0..99
after append:  hpcid:variable_PRESSURE_UUID -> output.bp steps 0..109
```

Compact `source_steps` metadata may describe how a complete output sequence
maps to a complete input sequence. It does not create timestep-specific entity
identities or claim that the campaign can reproduce the historical contents
visible at a particular instant.

### 9.4 When to create a new logical variable

Create a new logical variable with a new UUID when the scientific identity of
the product changes, for example:

- a corrected or replacement product is produced;
- units or the coordinate system change because of a transformation;
- a different activity generates a distinct product; or
- a user needs a separately named scientific result rather than more members
  of the same time-varying product.

Record the normal Generation and Derivation relationships for the new entity.
Formal snapshot entities, timestep-range identities, and revision chains are
deferred until a use case requires provenance for an exact historical state.

## 10. Variable definitions, storage, and discovery metadata

### 10.1 Variable definitions

`variable_definition` remains a campaign concept, not a W3C PROV concept. It is stored as an HPC-qualified attribute on logical-variable entities.

Different physical names may share one definition:

```text
run-001 / output / pressure -> variableDefinition "pressure"
run-002 / output / P        -> variableDefinition "pressure"
run-003 / output / press    -> variableDefinition "pressure"
```

Definitions describe observed products. Version one does not require every run to contain every definition.

Do not map variable definitions to `specializationOf` without a future semantic review. A concrete pressure array is not necessarily a PROV specialization of the abstract scientific concept `pressure`.

### 10.2 Dataset location

The existing campaign dataset remains the storage object or logical namespace
that owns a location. It is not owned by a run. A dataset is represented in
PROV as a stable entity so schemas, guides, activities, and logical variables
can refer to the same object:

```text
entity(hpcid:dataset_OUTPUT_UUID, [
    prov:type = hpc:Dataset,
    prov:label = "output",
    hpc:format = "adios-bp",
    prov:location = "data/run-001/output.bp"
])
```

Every campaign dataset has a location. A logical variable resolves through:

```text
dataset location + physical variable name
```

The logical-variable entity stores an `hpc:dataset` qualified-name reference to
the dataset entity, plus the physical variable name. It should also carry the
resolved `prov:location` and human-readable dataset name when exported so the
entity remains useful outside the campaign index.

This relationship is what allows a query to move from a logical variable to an
ADIOS dataset and then to a Fides model, simulation metadata profile, or guide
that describes the dataset. Do not rely on matching location strings to
discover that relationship.

The location may refer to a campaign-managed local or remote replica; it must
not require another campaign database.

### 10.3 Primary variable and preferred preview

Primary-variable and preferred-preview relationships are campaign discovery metadata rather than core provenance.

- Primary selection remains explicit and unique for one `(run, variable definition)` pair.
- Preferred preview points to another logical variable suitable for quick presentation.
- Store these as separate campaign discovery bindings, retaining the current
  normalized-table approach. Do not place mutable `primary` or
  `preferredPreview` attributes on the logical-variable entity.
- A portable export may represent the current bindings in a versioned campaign
  discovery-configuration entity if consumers need them outside the database.
- They must not be confused with derivation. A preferred preview is not necessarily the source or derivation parent of the original product.

### 10.4 Chunks and step mappings

The campaign may retain its compact chunk and source-step mappings. W3C PROV Collections do not by themselves provide the ordering and compact stride representation required here.

Version one may continue to represent:

```yaml
source_steps:
  start: 0
  count: 200
  stride: 5
```

The mapping belongs to a specific activity usage and output logical variable.
It is campaign-specific relationship metadata and must remain queryable
without expanding one row per timestep.

General PROV Collection support is deferred.

## 11. Runs, plans, workflows, and executions

### 11.1 Simulation run

A simulation run is represented as a PROV activity:

```text
activity(hpcid:run_RUN_UUID, [
    prov:type = hpc:SimulationRun,
    prov:label = "run-001",
    hpc:runProfile = hpcid:schema_XGC_BOOZER_PROFILE_UUID_r1
])
```

Newly captured source variables should be generated by the run activity:

```text
wasGeneratedBy(hpcid:variable_PRESSURE_UUID, hpcid:run_RUN_UUID)
```

Imported historical data may omit the generating run relationship when it is unknown. Do not fabricate an activity solely to satisfy the detailed model.

### 11.2 Plan versus execution

The model distinguishes:

- **Plan:** what is intended to happen.
- **Activity:** what actually happened.
- **Entity:** what was used or produced.

For example:

```text
XGC configuration         -> Plan entity
run-001 executing XGC     -> Activity
mesh and pressure output  -> Generated entities
```

### 11.3 Workflow

A workflow definition is a plan, not an activity category:

```text
entity(hpcid:plan_PRESSURE_WORKFLOW_UUID, [
    prov:type = prov:Plan,
    prov:type = hpc:WorkflowPlan,
    prov:label = "pressure-analysis-v1"
])
```

Each executed reduction, projection, QoI calculation, or visualization remains an activity. The executed dataflow is reconstructed by following usage and generation relationships.

**Version-one recommendation:** do not add a separate parent workflow-execution object. Record the plan and individual activities. Explicit composite-execution grouping and activity containment are deferred until a real query requires them.

### 11.4 No workflow category

Do not add `workflow` to the scientific activity-category vocabulary. A workflow describes intended steps; a category groups activity types for query and presentation. They solve different problems.

## 12. Activity vocabulary and categories

### 12.1 W3C PROV behavior

PROV defines a generic Activity and does not constrain application-specific activity types or categories. HPC Campaign supplies the scientific vocabulary.

### 12.2 Initial recommended vocabulary

The initial profile should contain:

| Campaign action | PROV type | Category |
|---|---|---|
| `reduction` | `hpc:Reduction` | `transformation` |
| `projection` | `hpc:Projection` | `transformation` |
| `quantity_of_interest` | `hpc:QuantityOfInterest` | `analysis` |
| `visualization` | `hpc:Visualization` | `presentation` |

MGARD and ZFP are methods recorded in an action specification. They are not activity types or representation kinds.

### 12.3 Why retain categories

Categories enable stable broad queries such as:

- Find every analysis involving pressure.
- Summarize all transformations.
- Show all presentation products.

They also support viewer grouping, validation defaults, and AI interpretation of unfamiliar specific operations.

### 12.4 Category storage

Store the specific PROV type on the activity. Derive the category from the active profile rather than repeating it on every activity.

This prevents contradictory records such as a reduction activity explicitly labeled as presentation.

### 12.5 Restriction policy

- Campaign-authored activities must use a type allowed by the campaign profile.
- A campaign may require every authored activity to have a recognized type.
- Unknown imported qualified types must be preserved.
- Unknown imported types should produce warnings rather than cause data loss.
- Queries may handle an unknown activity generically even when they cannot assign an HPC category.

### 12.6 Activity granularity and product selection

An activity represents an actual execution boundary, not necessarily one
mathematical operation or one generated product. A single program invocation
may use several inputs and generate several outputs. For example, one
descriptive-statistics execution may use pressure and generate separate
minimum, maximum, and mean logical variables. That execution should normally
be recorded as one activity with three Generation relationships, not as three
activities invented solely because there are three outputs.

Use separate activities when the operations actually execute separately or
when they have independently meaningful provenance, such as different agents,
Plans, parameters, success or failure states, or approval decisions. The
activity boundary should describe what happened rather than being chosen only
to minimize the number of PROV records.

Campaign profiles constrain recognized activity types, roles, and optional
action-specification fields. They do not determine which scientific products a
workflow should generate. Product-selection policies belong to the producing
workflow or Plan. For example, a statistics Plan may request only pressure mean
and maximum rather than six statistics for every numeric variable. The
resulting activity then records only the products that were actually generated.
This is a workflow policy and requires no change to the campaign PROV schema.

Provenance should describe every generated product that is included as a
logical campaign product. Implementations must not generate a large collection
of products and then silently omit most of them from provenance merely to
reduce metadata size. Control graph size primarily by generating meaningful
products, choosing execution-faithful activity boundaries, and reusing shared
Plans and agents. Future profiles may add domain-specific granularity guidance
or constraints after representative workflows establish useful rules.

## 13. Inputs, outputs, and roles

### 13.1 Input usage

Each activity input is represented by a PROV Usage relationship with a role:

```text
used(
    hpcid:activity_REDUCTION_UUID,
    hpcid:variable_PRESSURE_UUID,
    [prov:role = hpc:source]
)
```

### 13.2 Output generation

Each output is represented by a PROV Generation relationship with a role:

```text
wasGeneratedBy(
    hpcid:variable_REDUCED_PRESSURE_UUID,
    hpcid:activity_REDUCTION_UUID,
    [prov:role = hpc:result]
)
```

### 13.3 Multiple inputs and outputs

Multiple inputs and outputs use multiple relationships:

```text
used(visualization, pressure,    [prov:role = hpc:color])
used(visualization, temperature, [prov:role = hpc:contours])
wasGeneratedBy(image, visualization, [prov:role = hpc:image])
```

Version one does not require a global role vocabulary. Profiles may constrain roles for selected activity types.

### 13.4 Role multiplicity and ordering

**Deferred:** repeated ordered uses of the same role. The initial campaign API may require role names to be unique within an activity, matching the existing mapping-based interface. If a real operation requires multiple ordered inputs with the same semantic role, add an occurrence or position field without changing the PROV entity/activity model.

### 13.5 Identified relationship records

Campaign-authored Usage and Generation relationships should have PROV
identifiers. Identified relationships allow a qualified derivation to name the
exact input usage and output generation involved in a multi-input/multi-output
activity.

Because version one requires role names to be unique within an activity, usage
and generation qualified names may be derived deterministically from the
activity UUID, relationship kind, and role. The mapping must still be
centralized and tested; callers do not construct these names.

Short examples in this document sometimes omit relationship IDs for
readability. Canonical campaign-authored records include them.

## 14. Action specifications

### 14.1 Requirements

An action specification is optional. It may contain method names, parameters, error bounds, script references, or producer-specific information.

The base implementation must not require universal keys.

### 14.2 Recommended PROV mapping

Represent an action specification as an immutable entity typed as an HPC action
specification. Add `prov:Plan` only when the specification prescribed how the
activity was intended to execute:

```text
entity(hpcid:plan_REDUCTION_SPEC_UUID, [
    prov:type = prov:Plan,
    prov:type = hpc:ActionSpecification,
    prov:value = "{\"error_bound\":0.0001,\"method\":\"mgard\"}",
    hpc:mediaType = "application/json",
    hpc:sha256 = "..."
])
```

The JSON string is canonicalized before hashing. Equal specifications may be content-hash deduplicated.

Record actual use with a role-qualified Usage relationship:

```text
used(
    hpcid:activity_REDUCTION_UUID,
    hpcid:plan_REDUCTION_SPEC_UUID,
    [prov:role = hpc:actionSpecification]
)
```

This mapping preserves arbitrary JSON without requiring every nested key to become a PROV attribute.

Usage and Association have different meanings and may both be correct:

- `used(activity, specification)` states that the activity utilized the
  specification or parameter record.
- `wasAssociatedWith(activity, agent, plan)` states that the agent followed the
  Plan in the context of the activity.

Every campaign `action_spec` uses the first relationship. Add the second only
when the specification is typed as `prov:Plan` and was actually adopted as a
plan. A descriptive after-the-fact record is not automatically a Plan.

### 14.3 Profile validation

Profiles may continue to define:

- required keys
- optional keys
- whether additional keys are allowed
- optional expected primitive value types in a later version

Validation operates on the decoded JSON object before the activity is committed.

### 14.4 Scripts and external specifications

Executable scripts, notebooks, container descriptions, and external method specifications should be separate entities with locations or values. The action specification may reference them by campaign ID.

Campaign-owned resources required to interpret the record must resolve within the campaign. External standards may be cited by URL but are not active objects in another campaign.

## 15. Direct derivation and detailed provenance

### 15.1 PROV distinction between dataflow and derivation

An activity can use one entity and generate another without the generated
entity necessarily being derived from every entity that was used. W3C PROV
therefore does **not** infer `wasDerivedFrom` from a
`used`/`wasGeneratedBy` path alone.

For example, a rendering activity may use a pressure field, a Fides data model,
an action specification, and a colormap. All four entities are activity
context, but a scientific-source query should not automatically report all four
as equivalent source data.

Campaign-authored provenance must distinguish:

- **Activity dataflow/context:** `used` and `wasGeneratedBy`, including roles.
- **Entity lineage:** explicit `wasDerivedFrom` relationships for entities that
  influenced the generated data product.

Detailed campaign-authored provenance should normally contain both forms:

```text
pressure -> used by reduction -> generated reduced pressure
reduced pressure -> wasDerivedFrom -> pressure
```

In canonical PROV-N-like form, the derivation identifies the exact activity,
generation, and usage:

```text
used(use_source; reduce_pressure, pressure, -, [prov:role = hpc:source])
wasGeneratedBy(gen_result; reduced_pressure, reduce_pressure, -, [prov:role = hpc:result])
wasDerivedFrom(
    derivation_result_source;
    reduced_pressure,
    pressure,
    reduce_pressure,
    gen_result,
    use_source
)
```

Minimal or imported provenance may contain only:

```text
wasDerivedFrom(reduced pressure, pressure)
```

Imported provenance may also contain only usage and generation. Such a graph is
valid activity dataflow, but the campaign must not silently strengthen it into
an entity derivation assertion.

### 15.2 Campaign authoring rule

The campaign-oriented `add_activity` API treats logical variables supplied in
its `inputs` argument as contributing data inputs. By default, each declared
output is derived from every declared logical-variable input. The method writes
one explicit `wasDerivedFrom(output, input)` assertion for each such pair.

For campaign-authored detailed provenance, each assertion also references the
activity and the identified Generation and Usage relationships, as shown
above.

For an activity with outputs that depend on different subsets of its inputs,
the caller supplies an explicit mapping from output roles to input roles. For
example:

```python
derivations={
    "magnitude": ["vector"],
    "quality_report": ["vector", "reference"],
}
```

An empty input-role list explicitly records that the output has no asserted
derivation from the activity's logical-variable inputs. Profiles may forbid an
empty derivation list for selected activity types.

Plans, action specifications, schema documents, guides, and software agents are
not included in the default all-inputs derivation rule. They may still be
recorded as activity context through PROV Usage or Association. If one of these
entities genuinely contributes to the identity or content of an output, the
caller may assert that derivation explicitly through the lower-level PROV API.

### 15.3 Query behavior

The query API must expose two related but distinct traversals:

1. **Lineage traversal:** follows explicit `wasDerivedFrom` relationships only.
2. **Activity dataflow traversal:** follows entity-to-activity-to-entity paths
   formed by `used` and `wasGeneratedBy`, preserving roles and non-data context.

Campaign-authored activities normally make both traversals useful because the
convenience API writes the explicit derivations along with detailed activity
records. Do not invent an activity when importing a direct derivation, and do
not infer a derivation from usage/generation alone.

### 15.4 Multiple parents

An output may have multiple immediate inputs. For example, an image using
pressure for color and temperature for contours has two usage relationships
and normally two explicit derivation relationships. Queries must preserve both
input roles and must not collapse the result to one parent.

## 16. Units and coordinate systems

### 16.1 Units

Units are an optional opaque string:

```text
hpc:units = "Pa"
```

Rules:

- Preserve the exact supplied string.
- Reject an explicitly supplied empty string.
- Do not parse, normalize, compare, or convert units.
- Absence means not recorded.
- Do not invent `unknown`.
- Each logical variable may override the profile default.

A producer that uses normalized units may provide a descriptive string such as `normalized XGC pressure`. HPC Campaign does not interpret it.

### 16.2 Coordinate system

Coordinate system is a separate optional opaque string:

```text
hpc:coordinateSystem = "boozer"
```

Do not treat Cartesian, Boozer, cylindrical, or other coordinate systems as units. Version one does not interpret coordinate transformations, axes, handedness, origins, or metric information.

### 16.3 Run profiles and defaults

A run references one immutable, versioned run profile. The profile may provide default units and coordinate-system strings by variable definition or named output context.

Recommended initial profile structure:

```yaml
schema_version: 1

run_profiles:
  xgc-boozer-v1:
    code: XGC

    variable_defaults:
      pressure:
        units: "Pa"
      temperature:
        units: "eV"

    output_defaults:
      mesh:
        coordinate_system: "boozer"

  xgc-cartesian-v1:
    code: XGC

    output_defaults:
      mesh:
        coordinate_system: "cartesian"
```

### 16.4 Resolution order

Resolve metadata in this order:

1. Explicit value on the logical-variable or mesh entity.
2. Run-specific override.
3. Selected output-context value.
4. Run-profile default.
5. Not recorded.

The exported PROV entity should contain the resolved string. A consumer should not need to retrieve the run profile to determine the units recorded for an exported entity.

### 16.5 Profile versioning

- A run references exactly one run profile in version one.
- Profiles do not inherit from one another in version one.
- A profile becomes immutable once referenced by a run.
- A change creates a new profile name or revision.
- Store a canonical content hash for verification.

## 17. Provenance profiles

### 17.1 Responsibilities

A provenance profile defines the subset of PROV that HPC Campaign can understand deeply. It may specify:

- allowed activity types
- category mapping
- allowed or required input roles
- allowed or required output roles
- action-specification key rules
- unknown-type policy

Illustrative profile:

```yaml
schema_version: 1

provenance_profile:
  unknown_imported_activity: warn
  unknown_authored_activity: error

  activity_types:
    reduction:
      prov_type: "hpc:Reduction"
      category: transformation
      inputs:
        required: [source]
        optional: []
        allow_additional: false
      outputs:
        required: [result]
        optional: []
        allow_additional: false
      action_spec:
        required: []
        optional: [method, error_bound, error_bound_type]
        allow_additional: true

    visualization:
      prov_type: "hpc:Visualization"
      category: presentation
      inputs:
        required: [source]
        optional: [color, contours, annotation]
        allow_additional: true
      outputs:
        required: [image]
        optional: []
        allow_additional: true
      action_spec:
        required: []
        optional: [colormap]
        allow_additional: true
```

This syntax is a recommended starting point. Final field names should be settled before coding and then versioned.

Version one uses one active provenance-profile revision for campaign-authored
records. The profile resource is immutable after first use. Replacing the
active profile means selecting a new resource revision and validating all
existing campaign-authored records before committing the binding change.
Imported records outside the selected vocabulary remain preserved under the
unknown-import policy.

### 17.2 Validation timing

Validate:

- When a campaign-authored activity is added.
- Before committing an atomic activity plus outputs transaction.
- When a profile is changed or assigned.
- During an explicit campaign validation command.
- When imported PROV is promoted from opaque storage into the active campaign graph.

## 18. Schema, profile, data-model, and guide discovery

### 18.1 General rule

Every concrete schema, profile, data model, or AI-readable guide is a versioned PROV entity. Represent the concrete information artifact, not only the abstract idea of a schema.

Recommended type hierarchy:

```text
hpc:SchemaDocument
├── hpc:CampaignProfile
├── hpc:ProvenanceProfile
├── hpc:RunProfile
├── hpc:SimulationMetadataProfile
├── hpc:VisualizationDataModel
└── hpc:InterpretationGuide
```

An entity may have more than one type.

### 18.2 Fides example

A Fides JSON data model is a concrete visualization data model. It may also be a plan because it prescribes how visualization software should map ADIOS variables to meshes and fields.

```text
entity(hpcid:schema_FIDES_UUID_r1, [
    prov:type = prov:Plan,
    prov:type = hpc:SchemaDocument,
    prov:type = hpc:VisualizationDataModel,
    prov:type = hpc:FidesDataModel,
    prov:location = "visualization/run-001-fides.json",
    hpc:format = "fides-json",
    hpc:revision = 1,
    hpc:describes = hpcid:dataset_ADIOS_DATASET_UUID
])
```

The campaign does not parse the entire Fides schema into custom database columns. It records the entity and relationships necessary to discover it.

### 18.3 Descriptive relationships

Recommended HPC-qualified attributes include:

- `hpc:describes`
- `hpc:conformsTo`
- `hpc:appliesTo`
- `hpc:format`
- `hpc:mediaType`
- `hpc:revision`
- `hpc:sha256`

Canonical storage should assert one direction for a relationship. The database query index may materialize reverse lookups such as `describedBy` without duplicating the canonical assertion.

Use `hpc:describes` when a model or guide explains a particular resource. Use `hpc:conformsTo` when a resource claims conformance to a schema or profile.

Do not use `wasDerivedFrom` merely to say that a schema describes data; that would assert the wrong provenance meaning.

### 18.4 Actual use

When an activity actually uses a schema or data model, record ordinary PROV Usage:

```text
used(visualization, adios_output, [prov:role = hpc:data])
used(visualization, fides_model,   [prov:role = hpc:dataModel])
```

`hpc:describes` records applicability. `used` records what happened in one execution.

### 18.5 Embedded metadata

When metadata or a Fides model is embedded in an ADIOS file:

- The ADIOS output remains one data entity.
- A simulation metadata profile or interpretation guide remains a separate campaign entity.
- The profile records that it applies to or describes the ADIOS entity.
- Its location may identify an embedded region or attribute group within the dataset.
- An LLM uses the profile to select an ADIOS inspection tool and interpret the returned attributes.

The campaign need not create database columns for code version, input parameters, system name, or every other simulation-specific attribute.

### 18.6 Guide layering

The intended discovery sequence is:

1. Campaign guide: what resources exist and how they are related.
2. Simulation guide: where code-specific metadata and conventions live.
3. Format guide: how ADIOS, HDF5, Fides, or another format should be accessed.
4. Tool guide: which deterministic viewer or analysis operation to call.
5. PROV graph: how concrete resources, executions, and conclusions are related.

These guides may resemble scoped `AGENTS.md` files, but they are campaign data. The AI harness must not allow them to override system security, permissions, or publication rules.

## 19. Agents and responsibility

### 19.1 Agent types

Version one should support people, software, organizations, instruments, and
services. Use W3C types where available and HPC-qualified types otherwise:

```text
agent(hpcid:agent_ALICE_UUID, [
    prov:type = prov:Person,
    prov:label = "Alice"
])

agent(hpcid:agent_MGARD_UUID, [
    prov:type = prov:SoftwareAgent,
    prov:label = "MGARD",
    hpc:version = "..."
])

agent(hpcid:agent_ORNL_UUID, [
    prov:type = prov:Organization,
    prov:label = "Oak Ridge National Laboratory"
])

agent(hpcid:agent_DIAGNOSTIC_UUID, [
    prov:type = hpc:Instrument,
    prov:label = "diagnostic-01"
])
```

Agent metadata is optional for minimal provenance.

### 19.2 Association

Record responsibility for an activity with `wasAssociatedWith`. An association may also identify a plan.

The convenience API should allow an optional agent and optional plan without requiring them for every activity.

### 19.3 Attribution

Use `wasAttributedTo` for entities authored or asserted by an agent, such as an annotation, conclusion, script, profile, or generated report.

## 20. Collaboration model and AI automation

### 20.1 Relationship to scientific provenance

The campaign is the shared scientific workspace. Published campaign content may
contain both scientific provenance and collaboration records. They use the
same PROV foundation but distinct HPC types and profiles.

A collaboration event does not automatically become a scientific data-processing activity. A temporary plot, script, or message may remain a collaboration entity. Promotion to a scientific product is explicit and gives the artifact the appropriate logical-variable identity and producing scientific activity.

### 20.2 Planned collaboration concepts

- Human and AI participants -> Agents
- Conversation session -> campaign collaboration object; possible future PROV bundle or collection
- Message -> Entity
- Query specification -> Entity
- Saved viewer state or selection -> Entity
- Script, plot, image, or report -> Entity
- Query, comparison, or generation -> Activity
- Annotation or conclusion -> Entity attributed to an Agent
- Citation -> Entity with external identifier or URL
- Evidence -> relationships from a conclusion to supporting campaign entities, activities, artifacts, or citations

### 20.3 Working-session and campaign-record scopes

Collaboration has two logical storage scopes:

1. **Investigation workspace:** private or selectively shared session state used
   while exploring the campaign. The user can retrieve, resume, and fork it. It
   is associated with a campaign and refers to stable campaign object IDs, but
   it is not part of the portable campaign record by default.
2. **Campaign collaboration record:** selected messages, summaries, artifacts,
   annotations, conclusions, and citations deliberately published for everyone
   with access to the campaign.

This is a logical boundary. A future collaboration service may store both
scopes in one access-controlled database or keep private sessions in a sidecar
store. The portable campaign must not expose unpublished session content.

### 20.4 Visibility and publication

Investigation and publication state may include:

- private to one user
- shared with selected collaborators
- published to campaign users
- withdrawn

Moving selected content into the campaign collaboration record is an explicit
publication operation. Publication and withdrawal require explicit approval.
Withdrawal hides a record by default but preserves identity and audit history.
Publishing a summary does not implicitly publish the complete source
transcript.

### 20.5 Granular approvals

Recommended per-session approvals include:

- read campaign data
- control the viewer
- generate temporary artifacts
- access the internet

Publication, withdrawal, and promotion to a scientific product always require separate explicit approval.

### 20.6 Session continuity

Planned behavior:

- A session may continue with a different AI provider while recording the new SoftwareAgent.
- A session may be explicitly forked for an alternate investigation.
- Object IDs remain stable across viewer, session, edit, publication, and withdrawal operations.
- Editing messages, annotations, and conclusions is desirable. If implemented,
  edits to published records create revisions rather than silently replacing
  campaign history.
- A complete edit/revision UI and full historical revision storage for every
  private message are deferred if they would block the collaboration
  foundation.

### 20.7 Significance and “where do I start?”

Lineage alone cannot rank important findings. The minimum conclusion/annotation record should contain:

- stable ID and, for published collaboration records, revision
- author
- date
- free-form text
- target campaign object IDs and applicable resource revisions
- supporting entities, activities, plots, or citations
- visibility/publication state

Structured hypothesis states, confidence, acceptance, and rejection are deferred.

### 20.8 AI harness and campaign viewer

The collaboration roadmap must provide a deterministic tool boundary between
an AI harness and the campaign viewer. The viewer should expose operations such
as:

- inspect the current selection and visible state
- select campaign objects by stable ID
- run a bounded campaign query
- change a view or visualization parameter
- generate a temporary plot, image, comparison, or script
- return the exact campaign object IDs and applicable resource revisions shown
  in a view

The interface is capability-based and subject to the per-session approvals in
Section 20.5. It must not grant arbitrary browser control merely because an AI
can send viewer commands.

Tool requests and results may be represented as collaboration activities and
entities. A generated artifact remains in the investigation workspace until an
explicit publication or scientific-product promotion operation occurs.

When the AI uses campaign information to search the internet, the session
records the query and the resulting citations needed to support a published
conclusion. An external source is represented by a local citation entity with
its URL or persistent identifier; external content remains untrusted input.

## 21. Canonical storage and database indexing

### 21.1 Canonical format

**Recommended for version one:** PROV-JSON.

Reasons:

- It is the Python `prov` package default.
- It round-trips the package object model.
- It avoids requiring RDF or XML optional dependencies.
- It is suitable for storage as a campaign resource.

PROV-JSON is a W3C Member Submission rather than a W3C Recommendation. It is
selected here because it is the native, dependency-light round-trip format of
the required Python package; the semantics remain those of the W3C PROV Data
Model.

PROV-N may be generated for human-readable diagnostics but is not a canonical input format because current `prov` releases do not read PROV-N.

### 21.2 Initial storage

The campaign manifest represents canonical provenance as a list of documents:

```yaml
provenance_documents:
  - id: campaign-provenance
    location: provenance/provenance.json
    format: prov-json
```

An initial small campaign may use one document, but neither the public API nor
the database index may assume that only one exists. This avoids making a
monolithic in-memory `ProvDocument` a permanent architectural constraint.

The first production release must run the scale checks in Section 26.9 before
selecting one-document storage as its default. If representative metadata is
too large to load and rewrite comfortably, immutable partitioned documents
must move into the initial implementation rather than remain deferred.

### 21.3 Internal partitioning

Internal partitions may be organized by:

- run
- workflow execution
- collaboration session
- campaign section

All documents remain inside the same campaign namespace. Partitioning must not change stable identifiers.

Partition boundaries are storage details, not semantic boundaries. Queries use
the database index and load individual PROV documents only when full records
are requested. Updating one partition must not require rewriting unrelated
partitions.

### 21.4 Database index

The campaign database should index at least:

- stable object UUID
- PROV qualified name
- record kind: entity, activity, or agent
- HPC types
- location
- resource revision or version where applicable
- document location
- usage and generation endpoints
- direct derivation endpoints
- roles
- schema/profile applicability relationships
- logical-variable-to-dataset relationships
- selected frequently queried campaign attributes

The index is derived and rebuildable. Do not duplicate entire native schemas or large content in the index.

### 21.5 Large content

Store large content such as transcripts, scripts, reports, images, and schema documents as campaign resources. PROV entities record their location, media type, hash when available, and relationships.

Canonical PROV-JSON documents may be stored with transparent lossless
compression, for example gzip. Compression is a storage encoding and does not
change the canonical PROV semantics. The manifest records the media type,
content encoding, uncompressed hash, and compressed hash when compression is
used. Readers decompress a stream before passing it to `prov`; they should not
require the complete uncompressed document to be copied into another campaign
resource first.

## 22. Import, export, and validation

### 22.1 Import

Import flow:

1. Read the document with the Python `prov` package.
2. Preserve all parsed PROV records.
3. Check self-containment for active campaign-qualified relationships.
4. Validate the supported campaign subset against the selected profile.
5. Report unknown types or unsupported records as warnings unless strict promotion is requested.
6. Build or update the campaign query index transactionally.

### 22.2 Export

Export must:

- Produce valid PROV-JSON accepted by the pinned/tested `prov` package version.
- Include resolved units and coordinate-system strings on exported logical-variable entities.
- Preserve unknown imported records.
- Preserve qualified namespaces.
- Preserve stable IDs and revision relationships for versioned resources.
- Avoid exporting database integer row IDs as public identities.

### 22.3 Validation layers

Validation consists of separate layers:

1. Parser/serializer checks provided by `prov`.
2. Available PROV structural or unification checks.
3. Campaign self-containment and referential-integrity checks.
4. HPC Campaign profile checks.
5. Optional native-schema checks performed by external tools.

The Python `prov` package does not currently implement the complete PROV-CONSTRAINTS validation algorithm. Do not claim that parsing a document proves full PROV validity.

## 23. Proposed campaign-oriented API

The exact names may follow existing project style, but the first API should provide the following capabilities.

### 23.1 Document access

```python
manager.add_prov_document(document, name="imported-provenance", activate=True)
documents = manager.prov_documents()
document = manager.prov_document(document_id)
manager.set_prov_document_active(document_id, active=True)
manager.export_prov(document_id, path)
```

`add_prov_document` is the escape hatch for PROV concepts not exposed through convenience methods.
Inactive import preserves parseable PROV without requiring campaign graph
resolution. Explicit activation validates the union of active documents;
deactivation is rejected when it would leave unresolved active relationships.

### 23.2 Agent, Plan, and run registration

```python
agent = manager.add_agent("software", "XGC", version="...")
run_plan = manager.add_plan(
    "run-001 configuration",
    location="runs/run-001/input.json",
)
run = manager.add_run(
    name="run-001",
    plan=run_plan,
    agent=agent,
)
```

Run-profile selection and default resolution are added in Phase 3.

### 23.3 Variable registration

```python
pressure = manager.add_variable(
    run=run,
    dataset="output",
    variable="P",
    definition="pressure",
    units="Pa",
    coordinate_system="boozer",
)
```

The Phase 2 API returns the stable PROV `QualifiedName`. Profile-derived
defaults and primary/preferred-preview discovery bindings are added later;
omitting units or coordinates in Phase 2 means they are not recorded.

### 23.4 Activity recording

```python
reduced = manager.add_activity(
    action="reduction",
    inputs={"source": pressure},
    outputs={
        "result": VariableSpec(
            run="run-001",
            dataset="products",
            variable="pressure-reduced",
            definition="pressure",
        )
    },
    # Omit this for the common all-logical-inputs-to-all-outputs default.
    derivations={"result": ["source"]},
    action_spec={"method": "mgard", "error_bound": 1e-4},
    agent=mgard_agent,
    plan=workflow_plan,
)
```

The helper must create standard PROV entities, activities, usage, generation, agent association, and specification entities atomically.

### 23.5 Agent and schema resources

```python
agent = manager.add_agent(
    kind="software",
    name="MGARD",
    version="...",
)

fides_model = manager.add_schema_resource(
    kind="fides_data_model",
    location="visualization/run-001-fides.json",
    describes=[output_dataset],
    as_plan=True,
)
```

### 23.6 Direct derivation

A dedicated authoring helper is optional in version one. The query layer must understand direct derivation imported through a `ProvDocument`. If a helper is provided, it should add `wasDerivedFrom` without fabricating an activity.

### 23.7 Query API

Required query capabilities:

- Find a logical variable by run, dataset, and variable name.
- Resolve its stable PROV entity.
- Find its generating activity, if any.
- Find immediate inputs and their roles.
- Find immediate outputs and their roles.
- Find explicit immediate and root derivation sources.
- Traverse activity dataflow separately from entity lineage.
- Find direct or transitive downstream products.
- Filter activities by specific type or derived category.
- Find plans, agents, schemas, and guides used by or applicable to an object.
- Find conclusions and annotations targeting an object when collaboration is implemented.

All list and traversal operations must accept a result limit and continuation
cursor. Graph traversals also accept an explicit maximum depth. Defaults are
bounded and do not include downstream products unless requested.

A variable-definition discovery query should initially return a summary such
as counts grouped by run, action type, category, and product status. The caller
then requests one run, logical variable, activity, or page of products. A
separate explicit export operation may stream the complete matching graph.

## 24. End-to-end worked example

This example demonstrates the intended relationships. Identifiers are shortened for readability.

### 24.1 Profiles and guides

```text
entity(xgc_profile, [
    prov:type = hpc:RunProfile,
    prov:location = "profiles/xgc-boozer-v1.yaml"
])

entity(xgc_guide, [
    prov:type = hpc:SimulationMetadataProfile,
    prov:type = hpc:InterpretationGuide,
    prov:location = "guides/xgc-output-v1.yaml",
    hpc:describes = xgc_output
])
```

### 24.2 Simulation

```text
entity(xgc_run_plan, [
    prov:type = prov:Plan,
    prov:type = hpc:SimulationConfiguration,
    prov:location = "runs/run-001/input.json"
])

agent(xgc, [
    prov:type = prov:SoftwareAgent,
    prov:label = "XGC",
    hpc:version = "..."
])

activity(run_001, [
    prov:type = hpc:SimulationRun,
    hpc:runProfile = xgc_profile
])

wasAssociatedWith(run_001, xgc, xgc_run_plan)
```

### 24.3 ADIOS output and logical variables

```text
entity(xgc_output, [
    prov:type = hpc:Dataset,
    prov:type = hpc:SimulationOutput,
    prov:label = "output",
    prov:location = "data/run-001/output.bp",
    hpc:format = "adios-bp"
])

wasGeneratedBy(xgc_output, run_001)

entity(pressure, [
    prov:type = hpc:LogicalVariable,
    hpc:logicalVariableId = "PRESSURE_UUID",
    hpc:run = run_001,
    hpc:dataset = xgc_output,
    hpc:datasetName = "output",
    hpc:variable = "P",
    hpc:variableDefinition = "pressure",
    hpc:units = "Pa",
    hpc:coordinateSystem = "boozer",
    prov:location = "data/run-001/output.bp"
])

wasGeneratedBy(pressure, run_001)
```

The ADIOS file may contain code version, input parameters, machine information, and other XGC-specific attributes. HPC Campaign does not copy those fields. `xgc_guide` explains how an AI or tool should discover them.

### 24.4 Fides visualization data model

```text
entity(fides_model, [
    prov:type = prov:Plan,
    prov:type = hpc:FidesDataModel,
    prov:location = "visualization/run-001-fides.json",
    hpc:describes = xgc_output
])
```

### 24.5 Reduction

```text
entity(reduction_spec, [
    prov:type = prov:Plan,
    prov:type = hpc:ActionSpecification,
    prov:value = "{\"error_bound\":0.0001,\"method\":\"mgard\"}",
    hpc:mediaType = "application/json"
])

agent(mgard, [
    prov:type = prov:SoftwareAgent,
    prov:label = "MGARD"
])

activity(reduce_pressure, [prov:type = hpc:Reduction])

used(reduce_pressure, pressure,       [prov:role = hpc:source])
used(reduce_pressure, reduction_spec, [prov:role = hpc:actionSpecification])
wasAssociatedWith(reduce_pressure, mgard, reduction_spec)

entity(reduced_pressure, [
    prov:type = hpc:LogicalVariable,
    hpc:variableDefinition = "pressure",
    hpc:units = "Pa",
    hpc:coordinateSystem = "boozer",
    prov:location = "data/run-001/products.bp"
])

wasGeneratedBy(reduced_pressure, reduce_pressure, [prov:role = hpc:result])
wasDerivedFrom(reduced_pressure, pressure)
```

### 24.6 Quantity of interest

```text
activity(compute_flux, [prov:type = hpc:QuantityOfInterest])
used(compute_flux, reduced_pressure, [prov:role = hpc:pressure])
used(compute_flux, temperature,      [prov:role = hpc:temperature])

entity(flux, [
    prov:type = hpc:LogicalVariable,
    hpc:variableDefinition = "flux",
    hpc:units = "kg/(m^2 s)"
])

wasGeneratedBy(flux, compute_flux, [prov:role = hpc:field])
wasDerivedFrom(flux, reduced_pressure)
wasDerivedFrom(flux, temperature)

activity(integrate_flux, [prov:type = hpc:QuantityOfInterest])
used(integrate_flux, flux, [prov:role = hpc:flux])

entity(total_flux, [
    prov:type = hpc:LogicalVariable,
    hpc:variableDefinition = "total_flux",
    hpc:units = "kg/s"
])

wasGeneratedBy(total_flux, integrate_flux, [prov:role = hpc:total])
wasDerivedFrom(total_flux, flux)
```

### 24.7 Visualization

```text
agent(paraview, [
    prov:type = prov:SoftwareAgent,
    prov:label = "ParaView"
])

activity(render_pressure, [prov:type = hpc:Visualization])

used(render_pressure, reduced_pressure, [prov:role = hpc:color])
used(render_pressure, total_flux,       [prov:role = hpc:annotation])
used(render_pressure, fides_model,         [prov:role = hpc:dataModel])
wasAssociatedWith(render_pressure, paraview, fides_model)

entity(pressure_image, [
    prov:type = hpc:LogicalVariable,
    prov:type = hpc:ImageSequence,
    hpc:variableDefinition = "pressure_visualization",
    prov:location = "visualization/pressure/"
])

wasGeneratedBy(pressure_image, render_pressure, [prov:role = hpc:image])
wasDerivedFrom(pressure_image, reduced_pressure)
wasDerivedFrom(pressure_image, total_flux)
```

For every fifth source step, the campaign-specific compact usage/output mapping records:

```yaml
source_steps:
  color:
    start: 0
    count: 200
    stride: 5
```

### 24.8 Human/AI investigation

The collaboration phase may add:

```text
agent(robert, [prov:type = prov:Person])
agent(ai_assistant, [prov:type = prov:SoftwareAgent])

entity(question, [
    prov:type = hpc:Message,
    prov:value = "Why does the reduced-pressure isosurface change here?"
])

activity(compare_runs, [prov:type = hpc:Comparison])
used(compare_runs, question,          [prov:role = hpc:request])
used(compare_runs, pressure_image, [prov:role = hpc:evidence])
wasAssociatedWith(compare_runs, ai_assistant, -)

entity(conclusion, [
    prov:type = hpc:Conclusion,
    prov:value = "...free-form conclusion..."
])

wasGeneratedBy(conclusion, compare_runs)
wasAttributedTo(conclusion, robert)
```

The conclusion may additionally reference the exact logical variables and
activities that support it. Publishing the conclusion is an explicit campaign
operation.

### 24.9 Required query outcomes

From `pressure_image`, the query layer must be able to find:

- `render_pressure` as the generating activity.
- `reduced_pressure`, `total_flux`, and `fides_model` as immediate used entities with roles.
- `pressure` and `temperature` as transitive source entities.
- MGARD and ParaView as associated software agents when recorded.
- `reduction_spec` and `fides_model` as plans/specifications.
- `run_001` and `xgc_run_plan` as the original simulation execution and plan.
- units and coordinate-system strings on each concrete logical variable.
- conclusions or annotations that cite the image when collaboration records exist.

## 25. Required invariants

The implementation must maintain these invariants:

1. Every active campaign object has a stable UUID.
2. Database integer IDs are never exported as the sole public identity.
3. Every logical variable has one stable PROV entity identity; appending data
   does not create another entity or another Generation.
4. Every generated campaign entity has at most one generating activity in the supported profile.
5. A usage or generation role is non-empty.
6. Campaign-authored Usage and Generation relationships have stable PROV IDs.
7. Role uniqueness follows the selected profile and initial mapping-based API.
8. Activity type is allowed for campaign-authored records.
9. Category is derived from the profile, not independently authored.
10. Action specifications are valid JSON, immutable, and content-hash verified.
11. Activity, outputs, uses, specification, derivations, and compact step
    mappings are committed atomically.
12. Campaign-authored logical-variable dependencies have explicit
    `wasDerivedFrom` assertions; usage/generation alone is not treated as proof
    of derivation.
13. Detailed campaign-authored derivations identify their activity,
    generation, and usage relationships.
14. Direct derivation does not require a fabricated activity.
15. Every logical-variable entity references a dataset entity with a
    resolvable location.
16. Every active campaign-qualified relationship endpoint resolves within the campaign.
17. Run profiles and provenance profiles are immutable once used.
18. Explicit metadata overrides profile defaults deterministically.
19. Unknown imported PROV records are preserved unless an explicit strict operation rejects promotion into the active campaign graph.

## 26. Test strategy

### 26.1 PROV round trips

- Create each supported record through the campaign API.
- Serialize to PROV-JSON.
- Deserialize with `prov`.
- Verify semantic equality and stable qualified names.
- Repeat import/export while preserving unknown records.

### 26.2 Profile validation

- Accept every allowed initial activity type.
- Reject unknown campaign-authored types.
- Warn and preserve unknown imported types.
- Validate one required action-specification key.
- Validate multiple required keys.
- Validate `allow_additional: false`.
- Validate required and optional roles.
- Reject category/type inconsistencies by deriving category rather than accepting an authored category.

### 26.3 Graph behavior

- One input and one output.
- A growing time-varying logical variable retains one qualified name and has no
  logical-variable revision attribute.
- Appending timesteps does not add an Entity, Generation, or Derivation.
- Creating a scientifically distinct replacement uses a new logical-variable
  UUID and normal Generation/Derivation relationships.
- Multiple role-qualified inputs.
- Multiple role-qualified outputs.
- Direct `wasDerivedFrom` without an activity.
- A detailed activity containing usage, generation, and explicit derivation.
- A qualified derivation that resolves its exact activity, generation, usage,
  and input/output roles.
- A usage/generation path without derivation that appears in dataflow queries
  but not lineage queries.
- A mixed graph containing direct and detailed activity records.
- Root-source and downstream lineage queries.
- An activity whose outputs derive from different input subsets.
- Cycle handling or cycle rejection according to the supported profile.
- Cross-run activity inputs.

### 26.4 Runs and plans

- Simulation run generates source variables.
- Run references an immutable run profile.
- Activity uses an action specification.
- Activity is associated with an optional software or human agent and plan.
- Named workflow plan does not become an activity category.

### 26.5 Units and coordinate systems

- Resolve a units string from a profile.
- Override units on a run.
- Override units on a logical variable.
- Preserve an arbitrary units string exactly.
- Resolve Boozer versus Cartesian coordinate-system defaults from different run profiles.
- Export resolved strings on the entity.

### 26.6 Schema discovery

- Register a Fides data model entity.
- Find all schemas/guides describing an ADIOS entity.
- Traverse from a logical variable through its dataset entity to the schemas
  and guides that describe that dataset.
- Record actual use of the Fides entity by a visualization activity.
- Distinguish applicability (`hpc:describes`) from execution (`used`).
- Version a schema entity and link revisions.

### 26.7 Self-containment

- Reject or quarantine unresolved active campaign-qualified references.
- Permit external citation URLs on local citation entities.
- Ensure no query requires opening another campaign database.

### 26.8 Transactions and corruption

- Roll back all generated records if one output fails.
- Reject malformed action-specification JSON.
- Detect specification hash mismatch.
- Detect missing dataset locations.
- Rebuild the query index from canonical PROV-JSON.

### 26.9 Scale checks

Before finalizing monolithic storage, generate at least the
representative upper-scale scenario discussed for the roadmap:

- 1,000 runs
- 100 source variables per run
- 5 reduced products per source variable
- 10 QoI products per source variable
- representative plans, specifications, schemas, and selected visualizations

This is approximately 1.6 million logical-variable products before
visualization products. Measure:

- PROV-JSON size
- losslessly compressed PROV-JSON size
- SQLite index size
- in-memory load cost
- peak memory while importing, exporting, and rebuilding the index
- index construction time
- bounded summary-query and root-source-query time
- time and memory required to retrieve one detailed lineage subgraph
- effect of large time-varying datasets without per-timestep provenance records

The test report must state whether provenance plus its query index fits the
preferred few-hundred-MB budget and the approximate 1-GB upper target. If it
does not, the initial production design must adopt partitioning, streaming,
compression, coarser activity granularity where scientifically correct, or a
revised index representation before declaring the schema production-ready.
The size target must not be met by dropping required provenance relationships.

## 27. Recommended implementation phases

### Phase 1: dependency and mapping spike

**Status:** Complete on 2026-09-01. See `PROV_MAPPING_SPIKE.md` for the
prototype, test results, package limitations, and representative measurements.

- Add a reviewed dependency on a tested `prov` 3.x version. The spike selected
  `prov >= 3.1.0, < 4`, resolved to 3.1.0 in `poetry.lock`.
- Create one `ProvDocument` entirely through the package API.
- Implement the namespace and stable-ID mapping.
- Round-trip the end-to-end example through PROV-JSON.
- Verify Python versions supported by HPC Campaign.

### Phase 2: canonical scientific core

**Implementation plan:** See `PROV_PHASE2_IMPLEMENTATION_PLAN.md`. Phase 2 is
complete: the API/storage boundary, ACA 0.8 canonical document storage,
dataset Entities, Agents, Plans, simulation runs, and stable logical variables
are complete. Atomic processing Activity authoring is also complete, including
role-qualified Usage and Generation, explicit qualified Derivation,
non-lineage context, optional Associations, and content-addressed JSON action
specifications. Active-import validation, inactive preservation, and explicit
activation/deactivation are complete. The runnable workflow, benchmark, and
scale decision are recorded in `PROV_PHASE2_COMPLETION_REPORT.md`.

- Implement runs as activities.
- Map existing campaign datasets to stable PROV dataset entities.
- Implement one stable PROV entity per logical variable, including growing
  time-varying products.
- Implement usage, generation, direct derivation, roles, plans, and agents.
- Provide atomic `add_activity` convenience behavior.
- Add canonical PROV-JSON storage.

### Phase 3: profiles

- Replace the hard-coded global activity table as the ultimate authority with a versioned campaign provenance profile.
- Implement action type/category mapping.
- Implement role rules and action-specification validation.
- Implement flat, immutable run profiles with units and coordinate-system defaults.

### Phase 4: scalable storage and query index

- Add bounded canonical PROV documents, initially partitioned by run or a
  bounded group of runs.
- Add batch authoring so one shard is committed once rather than rewritten
  after every record.
- Add transparent lossless compression and document-level lazy loading.
- Build a rebuildable SQLite index over canonical PROV.
- Implement distinct explicit-lineage and activity-dataflow traversals.
- Implement bounded pagination, continuation cursors, traversal depth limits,
  and grouped overview summaries.
- Support schema/guide discovery.
- Preserve existing high-value query helpers where their semantics remain valid.

### Phase 5: native schema resources

- Add schema/profile/guide entity registration.
- Add Fides and simulation-profile examples.
- Add tool-facing discovery queries.

### Phase 6: collaboration foundation

- Add participants, private/shared investigation sessions, messages, viewer
  state, artifacts, annotations, conclusions, citations, visibility, and audit
  records.
- Use the same PROV foundation with a distinct collaboration profile.
- Keep unpublished investigation content out of portable campaign exports.
- Add explicit selective publication, withdrawal, and scientific-product promotion.

### Phase 7: production-scale validation and tuning

- Re-run the full 1,000-run benchmark with partitioned storage and the query
  index.
- Measure bounded summary and detailed-lineage queries, index rebuild, and
  import/export memory.
- Tune shard thresholds and compression while preserving stable identifiers
  and query semantics.

## 28. Impact on the current activity-based branch

The current implementation contains valuable campaign semantics and tests, including:

- stable UUIDs
- run-qualified variables
- variable definitions
- primary variables and preferred previews
- atomic activity/output transactions
- multiple role-qualified inputs and outputs
- action-specification validation and deduplication
- compact source-step mappings
- provenance traversal tests
- complete workflow examples

These concepts should be retained where consistent with this design.

The existing campaign dataset abstraction is also retained. Each dataset gains
a stable PROV entity representation so logical variables and native schema
resources can refer to it directly; this does not introduce a run-owned
`dataset_run` replacement.

The following parts require reconsideration:

- Custom activity/input/output tables as the canonical provenance representation
- The hard-coded activity vocabulary as a global restriction rather than an initial profile
- Rejection and loss of unknown imported PROV types
- Lack of agents and plans
- Requiring all derivation to pass through the custom activity subset
- Any design that creates logical-variable revisions for ordinary timestep
  appends

No legacy-campaign migration is required for this redesign because the current
activity schema is not in production use. Preserve useful campaign concepts and
the general convenience of the API, but do not retain a custom table or method
shape solely for backward compatibility. Any actual public API change still
requires normal review and explicit approval during implementation.

The implementation strategy is to make campaign-oriented convenience methods
create standard PROV records and to treat the SQLite representation as a query
index.

## 29. Explicitly deferred features

- Full PROV-CONSTRAINTS validation
- PROV-N import
- General PROV bundle authoring APIs
- PROV Collections and Dictionaries
- Activity start/end times
- Timestep-specific provenance, logical-variable snapshots, and formal
  logical-variable revision chains
- Universal role vocabulary
- Repeated ordered inputs with the same role
- Profile inheritance
- Value-type validation for every action-specification field
- Unit parsing, conversion, or normalization
- Coordinate-system interpretation or transformation
- Workflow-step containment and explicit composite workflow executions
- Expected variable definitions per run and run completeness validation
- Cross-campaign references, federation, or parent/child campaign linking
- Provenance query services and Web dereferencing
- Full collaboration-record revision history
- Structured hypothesis and conclusion states
- Sophisticated privacy and external-service policy
- Internal provenance partitioning and lazy loading only if the required scale
  spike demonstrates that monolithic storage is acceptable for the first
  production release
- Formal significance ranking for “where do I start?”

## 30. Open decisions to carry into implementation

The broad design is settled. The following implementation-level questions remain:

1. **Final profile syntax:** confirm field names and namespace mapping before making the format public.
2. **Canonical ID spelling:** finalize the campaign namespace and qualified-name construction.
3. **Index schema:** determine the minimal SQLite tables needed for query performance while keeping the index rebuildable.
4. **Unknown imported records:** define the exact warning/report structure and the boundary between preserved opaque records and active indexed records.
5. **Profile storage:** decide whether profiles are stored inline in the campaign schema, as dedicated campaign resources, or both with one canonical source.
6. **Collaboration rollout:** decide which collaboration objects are included in the first release after scientific provenance.
7. **Storage partition threshold:** use scale measurements to decide whether the
   first production version writes one document or immutable partitions, and
   select the partitioning key and target size.

None of these questions changes the decision to use W3C PROV as the relationship model.

## 31. References

- [W3C PROV Overview](https://www.w3.org/TR/prov-overview/)
- [W3C PROV Data Model](https://www.w3.org/TR/prov-dm/)
- [W3C PROV Ontology](https://www.w3.org/TR/prov-o/)
- [W3C PROV Constraints](https://www.w3.org/TR/prov-constraints/)
- [W3C PROV Access and Query](https://www.w3.org/TR/prov-aq/)
- [PROV-JSON W3C Member Submission](https://www.w3.org/submissions/prov-json/)
- [Python `prov` package documentation](https://prov.readthedocs.io/)
- [Python `prov` package on PyPI](https://pypi.org/project/prov/)
- [Fides documentation](https://fides.readthedocs.io/)
- [Fides data-model schema](https://fides.readthedocs.io/en/latest/schema/schema.html)
