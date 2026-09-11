"""Phase-one W3C PROV mapping spike for HPC Campaign.

This module deliberately does not integrate PROV storage with ``Manager``.  It
tests the proposed identifier and relationship mappings in isolation so those
choices can be reviewed before they become part of the campaign database API.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Mapping

from prov.model import PROV, Namespace, ProvDocument, QualifiedName

# This vocabulary URI is intentionally provisional during the mapping spike.
# Campaign object identities use a separate campaign-specific URN namespace.
HPC = Namespace("hpc", "urn:hpc-campaign:vocabulary:")

_TOKEN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _as_uuid(value: uuid.UUID | str, label: str) -> uuid.UUID:
    """Return a UUID and provide a field-specific error for invalid input."""

    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a UUID") from exc


def _token(value: str, label: str) -> str:
    """Validate a token before embedding it in a PROV qualified name."""

    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValueError(f"{label} must begin with a letter and contain only letters, digits, or underscores")
    return value


@dataclass(frozen=True)
class CampaignProvIds:
    """Create stable, campaign-qualified PROV identifiers.

    Database integer keys are intentionally absent.  A complete campaign can
    be exported or reindexed without changing any identifier produced here.
    """

    campaign_id: uuid.UUID | str
    namespace: Namespace = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        campaign_id = _as_uuid(self.campaign_id, "campaign_id")
        object.__setattr__(self, "campaign_id", campaign_id)
        object.__setattr__(self, "namespace", Namespace("hpcid", f"urn:hpc-campaign:{campaign_id}:"))

    def _object(self, kind: str, object_id: uuid.UUID | str, suffix: str = "") -> QualifiedName:
        kind = _token(kind, "kind")
        object_id = _as_uuid(object_id, f"{kind}_id")
        return self.namespace[f"{kind}_{object_id.hex}{suffix}"]

    def dataset(self, dataset_id: uuid.UUID | str) -> QualifiedName:
        return self._object("dataset", dataset_id)

    def variable(self, variable_id: uuid.UUID | str) -> QualifiedName:
        """Return the stable identity of one logical data product.

        A time-varying variable remains one PROV Entity while new timesteps are
        appended. Version one therefore does not encode a content revision in
        the qualified name.
        """

        return self._object("variable", variable_id)

    def run(self, run_id: uuid.UUID | str) -> QualifiedName:
        return self._object("run", run_id)

    def activity(self, activity_id: uuid.UUID | str) -> QualifiedName:
        return self._object("activity", activity_id)

    def agent(self, agent_id: uuid.UUID | str) -> QualifiedName:
        return self._object("agent", agent_id)

    def plan(self, plan_id: uuid.UUID | str) -> QualifiedName:
        return self._object("plan", plan_id)

    def entity(self, entity_id: uuid.UUID | str) -> QualifiedName:
        return self._object("entity", entity_id)

    def usage(self, activity_id: uuid.UUID | str, role: str) -> QualifiedName:
        role = _token(role, "usage role")
        return self._object("usage", activity_id, f"_{role}")

    def generation(self, activity_id: uuid.UUID | str, role: str) -> QualifiedName:
        role = _token(role, "generation role")
        return self._object("generation", activity_id, f"_{role}")

    def derivation(self, activity_id: uuid.UUID | str, output_role: str, input_role: str) -> QualifiedName:
        output_role = _token(output_role, "output role")
        input_role = _token(input_role, "input role")
        return self._object("derivation", activity_id, f"_{output_role}_{input_role}")


@dataclass(frozen=True)
class ExampleProvGraph:
    """The representative PROV document plus named references used by tests."""

    document: ProvDocument
    identifiers: CampaignProvIds
    records: Mapping[str, QualifiedName]


def _example_uuid(campaign_id: uuid.UUID, name: str) -> uuid.UUID:
    """Generate repeatable UUIDs for the example without hard-coded constants."""

    return uuid.uuid5(campaign_id, name)


@dataclass(frozen=True)
class _LogicalVariableSpec:  # pylint: disable=too-many-instance-attributes
    """Values used to create one stable logical-variable entity."""

    # These fields mirror the deliberately explicit logical-variable mapping.
    # Grouping unrelated values only to satisfy an attribute-count heuristic
    # would make the example harder to compare with the design document.

    variable_id: uuid.UUID
    run: QualifiedName
    dataset: QualifiedName
    dataset_name: str
    variable: str
    definition: str
    location: str
    units: str | None = None
    coordinate_system: str | None = None

    def attributes(self) -> list[tuple[QualifiedName, object]]:
        """Return the standard and campaign-qualified PROV attributes."""

        attributes: list[tuple[QualifiedName, object]] = [
            (PROV["type"], HPC["LogicalVariable"]),
            (HPC["logicalVariableId"], str(self.variable_id)),
            (HPC["run"], self.run),
            (HPC["dataset"], self.dataset),
            (HPC["datasetName"], self.dataset_name),
            (HPC["variable"], self.variable),
            (HPC["variableDefinition"], self.definition),
            (PROV["location"], self.location),
        ]
        if self.units is not None:
            attributes.append((HPC["units"], self.units))
        if self.coordinate_system is not None:
            attributes.append((HPC["coordinateSystem"], self.coordinate_system))
        return attributes


@dataclass(frozen=True)
class _DerivedActivitySpec:
    """Inputs needed to record one detailed data-producing activity."""

    activity_id: uuid.UUID
    activity_type: QualifiedName
    inputs: Mapping[str, QualifiedName]
    outputs: Mapping[str, QualifiedName]
    context: Mapping[str, QualifiedName] = field(default_factory=dict)
    derivations: Mapping[str, tuple[str, ...]] | None = None


def _add_derived_activity(
    document: ProvDocument,
    identifiers: CampaignProvIds,
    spec: _DerivedActivitySpec,
) -> QualifiedName:
    """Record detailed activity dataflow and exact entity derivations.

    Contributing inputs participate in the default all-inputs-to-all-outputs
    derivation rule. Context entities are used by the activity but do not
    silently become lineage parents.
    """

    duplicate_roles = set(spec.inputs).intersection(spec.context)
    if duplicate_roles:
        raise ValueError(f"input and context roles must be unique: {', '.join(sorted(duplicate_roles))}")

    activity_ref = identifiers.activity(spec.activity_id)
    document.activity(activity_ref, other_attributes=[(PROV["type"], spec.activity_type)])

    usages = {}
    for role, entity_ref in {**spec.inputs, **spec.context}.items():
        usage_ref = identifiers.usage(spec.activity_id, role)
        document.used(
            activity_ref,
            entity_ref,
            identifier=usage_ref,
            other_attributes=[(PROV["role"], HPC[role])],
        )
        usages[role] = usage_ref

    generations = {}
    for role, entity_ref in spec.outputs.items():
        generation_ref = identifiers.generation(spec.activity_id, role)
        document.wasGeneratedBy(
            entity_ref,
            activity_ref,
            identifier=generation_ref,
            other_attributes=[(PROV["role"], HPC[role])],
        )
        generations[role] = generation_ref

    derivations = spec.derivations
    if derivations is None:
        derivations = {output_role: tuple(spec.inputs) for output_role in spec.outputs}

    if not set(derivations).issubset(spec.outputs):
        raise ValueError(
            "derivations reference unknown output roles: "
            + ", ".join(sorted(set(derivations).difference(spec.outputs)))
        )

    for output_role, input_roles in derivations.items():
        for input_role in input_roles:
            if input_role not in spec.inputs:
                raise ValueError(f"derivations reference unknown input role: {input_role}")
            document.wasDerivedFrom(
                spec.outputs[output_role],
                spec.inputs[input_role],
                activity_ref,
                generations[output_role],
                usages[input_role],
                identifier=identifiers.derivation(spec.activity_id, output_role, input_role),
            )

    return activity_ref


def build_pressure_workflow_example(campaign_id: uuid.UUID | str) -> ExampleProvGraph:
    """Build the representative simulation-to-investigation PROV document.

    The example intentionally exercises plans, agents, native-schema discovery,
    multiple data inputs, non-derivational context, qualified derivations, and
    a domain-specific activity type that the campaign profile may not know.
    """

    identifiers = CampaignProvIds(campaign_id)
    campaign_uuid = _as_uuid(campaign_id, "campaign_id")
    document = ProvDocument()
    document.add_namespace(HPC)
    document.add_namespace(identifiers.namespace)

    object_ids = {
        name: _example_uuid(campaign_uuid, name)
        for name in (
            "dataset-output",
            "pressure",
            "temperature",
            "reduced-pressure",
            "flux",
            "image",
            "run",
            "run-plan",
            "reduction",
            "reduction-spec",
            "qoi",
            "visualization",
            "fides-model",
            "xgc-agent",
            "mgard-agent",
            "paraview-agent",
            "question",
            "comparison",
            "conclusion",
            "robert-agent",
            "ai-agent",
            "unknown-activity",
        )
    }

    refs: dict[str, QualifiedName] = {
        "dataset": identifiers.dataset(object_ids["dataset-output"]),
        "pressure": identifiers.variable(object_ids["pressure"]),
        "temperature": identifiers.variable(object_ids["temperature"]),
        "reduced_pressure": identifiers.variable(object_ids["reduced-pressure"]),
        "flux": identifiers.variable(object_ids["flux"]),
        "image": identifiers.variable(object_ids["image"]),
        "run": identifiers.run(object_ids["run"]),
        "run_plan": identifiers.plan(object_ids["run-plan"]),
        "reduction": identifiers.activity(object_ids["reduction"]),
        "reduction_spec": identifiers.plan(object_ids["reduction-spec"]),
        "qoi": identifiers.activity(object_ids["qoi"]),
        "visualization": identifiers.activity(object_ids["visualization"]),
        "fides_model": identifiers.plan(object_ids["fides-model"]),
        "xgc_agent": identifiers.agent(object_ids["xgc-agent"]),
        "mgard_agent": identifiers.agent(object_ids["mgard-agent"]),
        "paraview_agent": identifiers.agent(object_ids["paraview-agent"]),
        "question": identifiers.entity(object_ids["question"]),
        "comparison": identifiers.activity(object_ids["comparison"]),
        "conclusion": identifiers.entity(object_ids["conclusion"]),
        "robert_agent": identifiers.agent(object_ids["robert-agent"]),
        "ai_agent": identifiers.agent(object_ids["ai-agent"]),
        "unknown_activity": identifiers.activity(object_ids["unknown-activity"]),
    }

    document.entity(
        refs["run_plan"],
        [
            (PROV["type"], PROV["Plan"]),
            (PROV["type"], HPC["SimulationConfiguration"]),
            (PROV["location"], "runs/run-001/input.json"),
        ],
    )
    document.agent(
        refs["xgc_agent"],
        [(PROV["type"], PROV["SoftwareAgent"]), (PROV["label"], "XGC")],
    )
    document.activity(
        refs["run"],
        other_attributes=[(PROV["type"], HPC["SimulationRun"]), (PROV["label"], "run-001")],
    )
    document.wasAssociatedWith(refs["run"], refs["xgc_agent"], refs["run_plan"])

    document.entity(
        refs["dataset"],
        [
            (PROV["type"], HPC["Dataset"]),
            (PROV["type"], HPC["SimulationOutput"]),
            (PROV["label"], "output"),
            (HPC["format"], "adios-bp"),
            (PROV["location"], "data/run-001/output.bp"),
        ],
    )
    document.wasGeneratedBy(
        refs["dataset"],
        refs["run"],
        identifier=identifiers.generation(object_ids["run"], "dataset"),
        other_attributes=[(PROV["role"], HPC["dataset"])],
    )

    for name, physical_name, definition, units in (
        ("pressure", "P", "pressure", "Pa"),
        ("temperature", "T", "temperature", "eV"),
    ):
        document.entity(
            refs[name],
            _LogicalVariableSpec(
                variable_id=object_ids[name],
                run=refs["run"],
                dataset=refs["dataset"],
                dataset_name="output",
                variable=physical_name,
                definition=definition,
                location="data/run-001/output.bp",
                units=units,
                coordinate_system="boozer",
            ).attributes(),
        )
        document.wasGeneratedBy(
            refs[name],
            refs["run"],
            identifier=identifiers.generation(object_ids["run"], name),
            other_attributes=[(PROV["role"], HPC[name])],
        )

    document.entity(
        refs["fides_model"],
        [
            (PROV["type"], PROV["Plan"]),
            (PROV["type"], HPC["SchemaDocument"]),
            (PROV["type"], HPC["FidesDataModel"]),
            (PROV["location"], "visualization/run-001-fides.json"),
            (HPC["describes"], refs["dataset"]),
        ],
    )

    specification = json.dumps({"error_bound": 0.0001, "method": "mgard"}, sort_keys=True, separators=(",", ":"))
    document.entity(
        refs["reduction_spec"],
        [
            (PROV["type"], PROV["Plan"]),
            (PROV["type"], HPC["ActionSpecification"]),
            (PROV["value"], specification),
            (HPC["mediaType"], "application/json"),
            (HPC["sha256"], hashlib.sha256(specification.encode("utf-8")).hexdigest()),
        ],
    )
    document.agent(
        refs["mgard_agent"],
        [(PROV["type"], PROV["SoftwareAgent"]), (PROV["label"], "MGARD")],
    )
    document.entity(
        refs["reduced_pressure"],
        _LogicalVariableSpec(
            variable_id=object_ids["reduced-pressure"],
            run=refs["run"],
            dataset=refs["dataset"],
            dataset_name="products",
            variable="pressure-reduced",
            definition="pressure",
            location="data/run-001/products.bp",
            units="Pa",
            coordinate_system="boozer",
        ).attributes(),
    )
    _add_derived_activity(
        document,
        identifiers,
        _DerivedActivitySpec(
            activity_id=object_ids["reduction"],
            activity_type=HPC["Reduction"],
            inputs={"source": refs["pressure"]},
            outputs={"result": refs["reduced_pressure"]},
            context={"action_specification": refs["reduction_spec"]},
        ),
    )
    document.wasAssociatedWith(refs["reduction"], refs["mgard_agent"], refs["reduction_spec"])

    document.entity(
        refs["flux"],
        _LogicalVariableSpec(
            variable_id=object_ids["flux"],
            run=refs["run"],
            dataset=refs["dataset"],
            dataset_name="products",
            variable="flux",
            definition="flux",
            location="data/run-001/products.bp",
            units="kg/(m^2 s)",
            coordinate_system="boozer",
        ).attributes(),
    )
    _add_derived_activity(
        document,
        identifiers,
        _DerivedActivitySpec(
            activity_id=object_ids["qoi"],
            activity_type=HPC["QuantityOfInterest"],
            inputs={"pressure": refs["reduced_pressure"], "temperature": refs["temperature"]},
            outputs={"field": refs["flux"]},
        ),
    )

    document.agent(
        refs["paraview_agent"],
        [(PROV["type"], PROV["SoftwareAgent"]), (PROV["label"], "ParaView")],
    )
    document.entity(
        refs["image"],
        _LogicalVariableSpec(
            variable_id=object_ids["image"],
            run=refs["run"],
            dataset=refs["dataset"],
            dataset_name="visualizations",
            variable="pressure-flux",
            definition="pressure_visualization",
            location="visualization/pressure/",
        ).attributes()
        + [(PROV["type"], HPC["ImageSequence"])],
    )
    _add_derived_activity(
        document,
        identifiers,
        _DerivedActivitySpec(
            activity_id=object_ids["visualization"],
            activity_type=HPC["Visualization"],
            inputs={"color": refs["reduced_pressure"], "annotation": refs["flux"]},
            outputs={"image": refs["image"]},
            context={"data_model": refs["fides_model"]},
        ),
    )
    document.wasAssociatedWith(refs["visualization"], refs["paraview_agent"], refs["fides_model"])

    document.agent(
        refs["robert_agent"],
        [(PROV["type"], PROV["Person"]), (PROV["label"], "Robert")],
    )
    document.agent(
        refs["ai_agent"],
        [(PROV["type"], PROV["SoftwareAgent"]), (PROV["label"], "AI assistant")],
    )
    document.entity(
        refs["question"],
        [
            (PROV["type"], HPC["Message"]),
            (PROV["value"], "Why does the reduced-pressure isosurface change here?"),
        ],
    )
    document.entity(
        refs["conclusion"],
        [
            (PROV["type"], HPC["Conclusion"]),
            (PROV["value"], "The example conclusion is supported by the rendered pressure image."),
        ],
    )
    _add_derived_activity(
        document,
        identifiers,
        _DerivedActivitySpec(
            activity_id=object_ids["comparison"],
            activity_type=HPC["Comparison"],
            inputs={"request": refs["question"], "evidence": refs["image"]},
            outputs={"result": refs["conclusion"]},
        ),
    )
    document.wasAssociatedWith(refs["comparison"], refs["ai_agent"])
    document.wasAttributedTo(refs["conclusion"], refs["robert_agent"])

    # The package must preserve valid, domain-specific PROV types even when the
    # high-level campaign profile does not understand them.
    document.activity(
        refs["unknown_activity"],
        other_attributes=[(PROV["type"], HPC["DomainSpecificOperation"])],
    )

    return ExampleProvGraph(document=document, identifiers=identifiers, records=refs)
