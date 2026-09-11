"""Campaign-oriented authoring of core scientific PROV records.

The public ``Manager`` methods delegate here so W3C PROV construction,
campaign validation, and canonical-document replacement remain out of the
already large manager implementation. It covers the Phase 2 scientific core:
datasets, Agents, Plans, runs, logical variables, and processing Activities.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType

from prov.model import (
    PROV,
    ProvActivity,
    ProvAgent,
    ProvDocument,
    ProvEntity,
    ProvRecord,
    QualifiedName,
)

from .prov_mapping import HPC, CampaignProvIds
from .prov_store import ProvDocumentInfo, ProvStore


@dataclass(frozen=True)
class _DatasetDescription:
    """Existing ACA dataset information needed by the PROV mapping."""

    dataset_id: int
    dataset_uuid: uuid.UUID
    name: str
    file_format: str
    locations: tuple[str, ...]


@dataclass(frozen=True)
class _LogicalVariableDescription:
    """Validated values that define one logical-variable Entity."""

    variable_id: uuid.UUID
    physical_name: str
    definition: str
    units: str | None
    coordinate_system: str | None


@dataclass(frozen=True)
class VariableSpec:
    """Describe one logical-variable output created by an Activity.

    The referenced run and dataset determine the output's campaign context and
    physical storage. ``variable`` is the name used by that dataset, while
    ``definition`` is the shared scientific concept recorded by the campaign.
    """

    run: QualifiedName
    dataset: str
    variable: str
    definition: str
    variable_id: uuid.UUID | None = None
    units: str | None = None
    coordinate_system: str | None = None


@dataclass(frozen=True)
class ActivityResult:
    """Stable PROV references produced by :meth:`Manager.add_activity`."""

    activity: QualifiedName
    outputs: Mapping[str, QualifiedName]
    action_specification: QualifiedName | None = None

    def __post_init__(self) -> None:
        # Do not expose a mutable dictionary from an otherwise frozen result.
        object.__setattr__(self, "outputs", MappingProxyType(dict(self.outputs)))


class CampaignProvenance:
    """Author the restricted Phase 2 scientific subset into canonical PROV."""

    _AGENT_TYPES = {
        "person": PROV["Person"],
        "software": PROV["SoftwareAgent"],
        "organization": PROV["Organization"],
        "instrument": HPC["Instrument"],
    }
    # This deliberately small vocabulary is the first campaign authoring
    # profile. Imported PROV may contain additional valid Activity types.
    _ACTIVITY_TYPES = {
        "reduction": HPC["Reduction"],
        "projection": HPC["Projection"],
        "quantity_of_interest": HPC["QuantityOfInterest"],
        "visualization": HPC["Visualization"],
    }
    _ACTION_SPECIFICATION_ROLE = "action_specification"

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.store = ProvStore(connection)

    def add_agent(
        self,
        kind: str,
        name: str | None = None,
        *,
        version: str | None = None,
        agent_id: uuid.UUID | None = None,
    ) -> QualifiedName:
        """Create a Person, software, organization, or instrument Agent."""

        agent_type = self._agent_type(kind)
        label = self._optional_string(name, "agent name")
        agent_version = self._optional_string(version, "agent version", preserve=True)
        resolved_id = self._optional_uuid(agent_id, "agent_id")

        info, document, identifiers = self._load_authored_document()
        reference = identifiers.agent(resolved_id)
        self._require_unused_identifier(document, reference)

        attributes: list[tuple[QualifiedName, object]] = [(PROV["type"], agent_type)]
        if label is not None:
            attributes.append((PROV["label"], label))
        if agent_version is not None:
            attributes.append((HPC["version"], agent_version))
        document.agent(reference, attributes)
        self._commit(info, document)
        return reference

    def add_plan(
        self,
        name: str,
        *,
        location: str | None = None,
        value: str | None = None,
        plan_id: uuid.UUID | None = None,
    ) -> QualifiedName:
        """Create an immutable Plan identified inside this campaign."""

        label = self._required_string(name, "plan name")
        plan_location = self._optional_string(location, "plan location", preserve=True)
        plan_value = self._optional_string(value, "plan value", preserve=True)
        resolved_id = self._optional_uuid(plan_id, "plan_id")

        info, document, identifiers = self._load_authored_document()
        reference = identifiers.plan(resolved_id)
        self._require_unused_identifier(document, reference)

        attributes: list[tuple[QualifiedName, object]] = [
            (PROV["type"], PROV["Plan"]),
            (PROV["label"], label),
        ]
        if plan_location is not None:
            attributes.append((PROV["location"], plan_location))
        if plan_value is not None:
            attributes.append((PROV["value"], plan_value))
        document.entity(reference, attributes)
        self._commit(info, document)
        return reference

    def add_run(
        self,
        name: str,
        *,
        run_id: uuid.UUID | None = None,
        plan: QualifiedName | None = None,
        agent: QualifiedName | None = None,
    ) -> QualifiedName:
        """Create one simulation execution and its optional association."""

        label = self._required_string(name, "run name")
        resolved_id = self._optional_uuid(run_id, "run_id")

        info, document, identifiers = self._load_authored_document()
        reference = identifiers.run(resolved_id)
        self._require_unused_identifier(document, reference)
        self._require_unique_run_name(document, label)

        if plan is not None:
            self._require_campaign_reference(identifiers, plan, "plan")
            self._require_record(document, plan, ProvEntity, PROV["Plan"], "plan")
        if agent is not None:
            self._require_campaign_reference(identifiers, agent, "agent")
            self._require_record(document, agent, ProvAgent, None, "agent")

        document.activity(
            reference,
            other_attributes=[(PROV["type"], HPC["SimulationRun"]), (PROV["label"], label)],
        )
        if plan is not None or agent is not None:
            document.wasAssociatedWith(reference, agent, plan)
        self._commit(info, document)
        return reference

    def add_variable(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        *,
        run: QualifiedName,
        dataset: str,
        variable: str,
        definition: str,
        variable_id: uuid.UUID | None = None,
        units: str | None = None,
        coordinate_system: str | None = None,
        generated_by_run: bool = True,
    ) -> QualifiedName:
        """Create one stable logical-variable Entity backed by an ACA dataset."""

        dataset_name = self._required_string(dataset, "dataset")
        if not isinstance(generated_by_run, bool):
            raise TypeError("generated_by_run must be a bool")
        description = _LogicalVariableDescription(
            variable_id=self._optional_uuid(variable_id, "variable_id"),
            physical_name=self._required_string(variable, "variable"),
            definition=self._required_token(definition, "definition"),
            units=self._optional_string(units, "units", preserve=True),
            coordinate_system=self._optional_string(
                coordinate_system,
                "coordinate_system",
                preserve=True,
            ),
        )

        info, document, identifiers = self._load_authored_document()
        self._require_campaign_reference(identifiers, run, "run")
        self._require_record(document, run, ProvActivity, HPC["SimulationRun"], "run")
        dataset_description = self._resolve_dataset(dataset_name)
        dataset_reference = self._ensure_dataset_entity(document, identifiers, dataset_description)
        reference = self._add_logical_variable_entity(
            document,
            identifiers,
            run,
            dataset_description,
            dataset_reference,
            description,
        )
        if generated_by_run:
            self._add_run_generation(document, identifiers, run, reference, description.definition)

        self._commit(info, document)
        return reference

    def add_activity(  # pylint: disable=too-many-arguments,too-many-locals
        self,
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
    ) -> ActivityResult:
        """Create one processing Activity and its qualified PROV relations.

        ``inputs`` are scientific lineage inputs. ``context`` records Entities
        used by the Activity without making them derivation parents. Unless an
        explicit ``derivations`` map is supplied, every output is derived from
        every scientific input.
        """

        activity_type = self._activity_type(action)
        resolved_activity_id = self._optional_uuid(activity_id, "activity_id")
        input_map = self._validated_role_mapping(inputs, "inputs", require_values=True)
        output_map = self._validated_output_mapping(outputs)
        context_map = self._validated_role_mapping(context or {}, "context", require_values=False)
        duplicate_roles = set(input_map).intersection(context_map)
        if duplicate_roles:
            raise ValueError("input and context roles must be unique: " + ", ".join(sorted(duplicate_roles)))
        if self._ACTION_SPECIFICATION_ROLE in input_map or self._ACTION_SPECIFICATION_ROLE in context_map:
            raise ValueError(f"role {self._ACTION_SPECIFICATION_ROLE!r} is reserved")
        derivation_map = self._validated_derivations(derivations, input_map, output_map)
        canonical_spec = self._canonical_action_specification(action_spec)

        info, document, identifiers = self._load_authored_document()
        activity_reference = identifiers.activity(resolved_activity_id)
        self._require_unused_identifier(document, activity_reference)
        self._require_logical_inputs(document, identifiers, input_map)
        self._require_context(document, identifiers, context_map)
        self._require_optional_association_records(document, identifiers, agent, plan)

        document.activity(activity_reference, other_attributes=[(PROV["type"], activity_type)])
        usage_references = self._add_usages(
            document,
            identifiers,
            resolved_activity_id,
            activity_reference,
            {**input_map, **context_map},
        )
        action_specification = self._add_action_specification(
            document,
            identifiers,
            resolved_activity_id,
            activity_reference,
            canonical_spec,
            usage_references,
        )
        output_references, generation_references = self._add_activity_outputs(
            document,
            identifiers,
            resolved_activity_id,
            activity_reference,
            output_map,
        )
        self._add_qualified_derivations(
            document,
            identifiers,
            resolved_activity_id,
            activity_reference,
            input_map,
            output_references,
            usage_references,
            generation_references,
            derivation_map,
        )
        if agent is not None or plan is not None:
            document.wasAssociatedWith(activity_reference, agent, plan)

        self._commit(info, document)
        return ActivityResult(activity_reference, output_references, action_specification)

    @staticmethod
    def _add_logical_variable_entity(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        document: ProvDocument,
        identifiers: CampaignProvIds,
        run: QualifiedName,
        dataset: _DatasetDescription,
        dataset_reference: QualifiedName,
        description: _LogicalVariableDescription,
    ) -> QualifiedName:
        reference = identifiers.variable(description.variable_id)
        CampaignProvenance._require_unused_identifier(document, reference)
        attributes: list[tuple[QualifiedName, object]] = [
            (PROV["type"], HPC["LogicalVariable"]),
            (HPC["logicalVariableId"], str(description.variable_id)),
            (HPC["run"], run),
            (HPC["dataset"], dataset_reference),
            (HPC["datasetName"], dataset.name),
            (HPC["variable"], description.physical_name),
            (HPC["variableDefinition"], description.definition),
        ]
        attributes.extend((PROV["location"], location) for location in dataset.locations)
        if description.units is not None:
            attributes.append((HPC["units"], description.units))
        if description.coordinate_system is not None:
            attributes.append((HPC["coordinateSystem"], description.coordinate_system))
        document.entity(reference, attributes)
        return reference

    @staticmethod
    def _add_run_generation(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        run: QualifiedName,
        variable: QualifiedName,
        definition: str,
    ) -> None:
        run_id = CampaignProvenance._reference_uuid(run, "run")
        generation_reference = identifiers.generation(run_id, definition)
        if document.get_record(generation_reference):
            raise ValueError(
                f"run already has a generated variable with definition role {definition!r}; "
                "set generated_by_run=False for an additional alias or derived product"
            )
        document.wasGeneratedBy(
            variable,
            run,
            identifier=generation_reference,
            other_attributes=[(PROV["role"], HPC[definition])],
        )

    @staticmethod
    def _require_logical_inputs(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        inputs: Mapping[str, QualifiedName],
    ) -> None:
        """Require every scientific input to be a local logical variable."""

        for role, reference in inputs.items():
            CampaignProvenance._require_campaign_reference(identifiers, reference, f"input {role!r}")
            CampaignProvenance._require_record(
                document,
                reference,
                ProvEntity,
                HPC["LogicalVariable"],
                f"input {role!r}",
            )

    @staticmethod
    def _require_context(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        context: Mapping[str, QualifiedName],
    ) -> None:
        """Require context to be a local Entity, without implying lineage."""

        for role, reference in context.items():
            CampaignProvenance._require_campaign_reference(identifiers, reference, f"context {role!r}")
            CampaignProvenance._require_record(
                document,
                reference,
                ProvEntity,
                None,
                f"context {role!r}",
            )

    @staticmethod
    def _require_optional_association_records(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        agent: QualifiedName | None,
        plan: QualifiedName | None,
    ) -> None:
        """Validate optional responsibility records before changing the graph."""

        if agent is not None:
            CampaignProvenance._require_campaign_reference(identifiers, agent, "agent")
            CampaignProvenance._require_record(document, agent, ProvAgent, None, "agent")
        if plan is not None:
            CampaignProvenance._require_campaign_reference(identifiers, plan, "plan")
            CampaignProvenance._require_record(document, plan, ProvEntity, PROV["Plan"], "plan")

    @staticmethod
    def _add_usages(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        activity_id: uuid.UUID,
        activity: QualifiedName,
        entities: Mapping[str, QualifiedName],
    ) -> dict[str, QualifiedName]:
        """Add identified, role-qualified Usage records."""

        usages: dict[str, QualifiedName] = {}
        for role, entity in entities.items():
            usage = identifiers.usage(activity_id, role)
            document.used(
                activity,
                entity,
                identifier=usage,
                other_attributes=[(PROV["role"], HPC[role])],
            )
            usages[role] = usage
        return usages

    @staticmethod
    def _add_action_specification(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        document: ProvDocument,
        identifiers: CampaignProvIds,
        activity_id: uuid.UUID,
        activity: QualifiedName,
        canonical_spec: tuple[str, str] | None,
        usages: dict[str, QualifiedName],
    ) -> QualifiedName | None:
        """Add or reuse a content-addressed JSON specification Plan."""

        if canonical_spec is None:
            return None
        value, digest = canonical_spec
        campaign_id = uuid.UUID(str(identifiers.campaign_id))
        specification_id = uuid.uuid5(campaign_id, f"action-specification:{digest}")
        reference = identifiers.plan(specification_id)
        records = document.get_record(reference)
        if records:
            record = CampaignProvenance._require_record(
                document,
                reference,
                ProvEntity,
                PROV["Plan"],
                "action specification",
            )
            if HPC["ActionSpecification"] not in record.get_asserted_types():
                raise ValueError(f"action specification has the wrong PROV type: {reference}")
            CampaignProvenance._require_exact_attribute(record, PROV["value"], {value}, "JSON value")
            CampaignProvenance._require_exact_attribute(
                record,
                HPC["mediaType"],
                {"application/json"},
                "media type",
            )
            CampaignProvenance._require_exact_attribute(record, HPC["sha256"], {digest}, "SHA-256")
        else:
            document.entity(
                reference,
                [
                    (PROV["type"], PROV["Plan"]),
                    (PROV["type"], HPC["ActionSpecification"]),
                    (PROV["value"], value),
                    (HPC["mediaType"], "application/json"),
                    (HPC["sha256"], digest),
                ],
            )

        role = CampaignProvenance._ACTION_SPECIFICATION_ROLE
        usage = identifiers.usage(activity_id, role)
        document.used(
            activity,
            reference,
            identifier=usage,
            other_attributes=[(PROV["role"], HPC[role])],
        )
        usages[role] = usage
        return reference

    def _add_activity_outputs(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        document: ProvDocument,
        identifiers: CampaignProvIds,
        activity_id: uuid.UUID,
        activity: QualifiedName,
        outputs: Mapping[str, VariableSpec],
    ) -> tuple[dict[str, QualifiedName], dict[str, QualifiedName]]:
        """Create output variables and their identified Generation records."""

        output_references: dict[str, QualifiedName] = {}
        generations: dict[str, QualifiedName] = {}
        for role, spec in outputs.items():
            self._require_campaign_reference(identifiers, spec.run, f"output {role!r} run")
            self._require_record(
                document,
                spec.run,
                ProvActivity,
                HPC["SimulationRun"],
                f"output {role!r} run",
            )
            dataset = self._resolve_dataset(spec.dataset)
            dataset_reference = self._ensure_dataset_entity(document, identifiers, dataset)
            description = _LogicalVariableDescription(
                variable_id=self._optional_uuid(spec.variable_id, f"output {role!r} variable_id"),
                physical_name=spec.variable,
                definition=spec.definition,
                units=spec.units,
                coordinate_system=spec.coordinate_system,
            )
            output = self._add_logical_variable_entity(
                document,
                identifiers,
                spec.run,
                dataset,
                dataset_reference,
                description,
            )
            generation = identifiers.generation(activity_id, role)
            document.wasGeneratedBy(
                output,
                activity,
                identifier=generation,
                other_attributes=[(PROV["role"], HPC[role])],
            )
            output_references[role] = output
            generations[role] = generation
        return output_references, generations

    @staticmethod
    def _add_qualified_derivations(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        document: ProvDocument,
        identifiers: CampaignProvIds,
        activity_id: uuid.UUID,
        activity: QualifiedName,
        inputs: Mapping[str, QualifiedName],
        outputs: Mapping[str, QualifiedName],
        usages: Mapping[str, QualifiedName],
        generations: Mapping[str, QualifiedName],
        derivations: Mapping[str, tuple[str, ...]],
    ) -> None:
        """Link exact Usage and Generation records for every lineage edge."""

        for output_role, input_roles in derivations.items():
            for input_role in input_roles:
                document.wasDerivedFrom(
                    outputs[output_role],
                    inputs[input_role],
                    activity,
                    generations[output_role],
                    usages[input_role],
                    identifier=identifiers.derivation(activity_id, output_role, input_role),
                )

    @classmethod
    def _activity_type(cls, action: str) -> QualifiedName:
        normalized = cls._required_token(action, "action").lower()
        try:
            return cls._ACTIVITY_TYPES[normalized]
        except KeyError as exc:
            choices = ", ".join(sorted(cls._ACTIVITY_TYPES))
            raise ValueError(f"action must be one of: {choices}") from exc

    @classmethod
    def _validated_role_mapping(
        cls,
        values: Mapping[str, QualifiedName],
        label: str,
        *,
        require_values: bool,
    ) -> dict[str, QualifiedName]:
        if not isinstance(values, Mapping):
            raise TypeError(f"{label} must be a mapping from roles to PROV QualifiedNames")
        if require_values and not values:
            raise ValueError(f"{label} must contain at least one role")

        normalized: dict[str, QualifiedName] = {}
        for role, reference in values.items():
            normalized_role = cls._required_token(role, f"{label} role")
            if normalized_role in normalized:
                raise ValueError(f"{label} contains duplicate normalized role: {normalized_role}")
            if not isinstance(reference, QualifiedName):
                raise TypeError(f"{label} role {normalized_role!r} must reference a PROV QualifiedName")
            normalized[normalized_role] = reference
        return normalized

    @classmethod
    def _validated_output_mapping(cls, outputs: Mapping[str, VariableSpec]) -> dict[str, VariableSpec]:
        if not isinstance(outputs, Mapping):
            raise TypeError("outputs must be a mapping from roles to VariableSpec values")
        if not outputs:
            raise ValueError("outputs must contain at least one role")

        normalized: dict[str, VariableSpec] = {}
        for role, spec in outputs.items():
            normalized_role = cls._required_token(role, "outputs role")
            if normalized_role in normalized:
                raise ValueError(f"outputs contains duplicate normalized role: {normalized_role}")
            if not isinstance(spec, VariableSpec):
                raise TypeError(f"output role {normalized_role!r} must contain a VariableSpec")
            # Validate all scalar fields before constructing a candidate graph.
            cls._required_string(spec.dataset, f"output {normalized_role!r} dataset")
            cls._required_string(spec.variable, f"output {normalized_role!r} variable")
            cls._required_token(spec.definition, f"output {normalized_role!r} definition")
            cls._optional_uuid(spec.variable_id, f"output {normalized_role!r} variable_id")
            cls._optional_string(spec.units, f"output {normalized_role!r} units", preserve=True)
            cls._optional_string(
                spec.coordinate_system,
                f"output {normalized_role!r} coordinate_system",
                preserve=True,
            )
            normalized[normalized_role] = VariableSpec(
                run=spec.run,
                dataset=spec.dataset.strip(),
                variable=spec.variable.strip(),
                definition=spec.definition.strip(),
                variable_id=spec.variable_id,
                units=spec.units,
                coordinate_system=spec.coordinate_system,
            )
        return normalized

    @classmethod
    def _validated_derivations(
        cls,
        derivations: Mapping[str, Sequence[str]] | None,
        inputs: Mapping[str, QualifiedName],
        outputs: Mapping[str, VariableSpec],
    ) -> dict[str, tuple[str, ...]]:
        if derivations is None:
            return {output_role: tuple(inputs) for output_role in outputs}
        if not isinstance(derivations, Mapping):
            raise TypeError("derivations must be a mapping from output roles to input-role sequences")

        normalized: dict[str, tuple[str, ...]] = {}
        for raw_output_role, raw_input_roles in derivations.items():
            output_role = cls._required_token(raw_output_role, "derivations output role")
            if output_role in normalized:
                raise ValueError(f"derivations contains duplicate normalized output role: {output_role}")
            if isinstance(raw_input_roles, (str, bytes)) or not isinstance(raw_input_roles, Sequence):
                raise TypeError(f"derivations for {output_role!r} must be a sequence of input roles")
            input_roles = tuple(
                cls._required_token(input_role, f"derivations for {output_role!r}") for input_role in raw_input_roles
            )
            if not input_roles:
                raise ValueError(f"derivations for {output_role!r} must contain at least one input role")
            if len(set(input_roles)) != len(input_roles):
                raise ValueError(f"derivations for {output_role!r} contains duplicate input roles")
            normalized[output_role] = input_roles

        missing_outputs = set(outputs).difference(normalized)
        unknown_outputs = set(normalized).difference(outputs)
        if missing_outputs or unknown_outputs:
            details = []
            if missing_outputs:
                details.append("missing output roles: " + ", ".join(sorted(missing_outputs)))
            if unknown_outputs:
                details.append("unknown output roles: " + ", ".join(sorted(unknown_outputs)))
            raise ValueError("derivations must describe every output (" + "; ".join(details) + ")")

        for output_role, input_roles in normalized.items():
            unknown_inputs = set(input_roles).difference(inputs)
            if unknown_inputs:
                raise ValueError(
                    f"derivations for {output_role!r} reference unknown input roles: "
                    + ", ".join(sorted(unknown_inputs))
                )
        return normalized

    @staticmethod
    def _canonical_action_specification(action_spec: Mapping[str, object] | None) -> tuple[str, str] | None:
        if action_spec is None:
            return None
        if not isinstance(action_spec, Mapping):
            raise TypeError("action_spec must be a JSON-compatible mapping")
        try:
            value = json.dumps(action_spec, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("action_spec must contain only JSON-compatible values") from exc
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
        return value, digest

    def _load_authored_document(self) -> tuple[ProvDocumentInfo, ProvDocument, CampaignProvIds]:
        info = self.store.ensure_authored_document()
        document = self.store.document(info.document_id)
        return info, document, CampaignProvIds(self.store.campaign_uuid())

    def _commit(self, info: ProvDocumentInfo, document: ProvDocument) -> None:
        self.store.replace_document(
            info.document_id,
            document,
            expected_sha256=info.sha256,
        )

    def _resolve_dataset(self, name: str) -> _DatasetDescription:
        dataset_row = self.connection.execute(
            """
            SELECT rowid, uuid, name, fileformat
            FROM dataset
            WHERE name = ? AND deltime = 0
            """,
            (name,),
        ).fetchone()
        if dataset_row is None:
            raise LookupError(f"dataset not found or deleted: {name}")
        try:
            dataset_uuid = uuid.UUID(str(dataset_row["uuid"]))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(f"dataset has an invalid UUID: {name}") from exc

        replica_rows = self.connection.execute(
            """
            SELECT
                r.name AS replica_name,
                r.archiveid AS archive_id,
                h.hostname AS hostname,
                h.longhostname AS longhostname,
                h.default_protocol AS protocol,
                d.name AS directory_name,
                a.tarname AS archive_name
            FROM replica AS r
            JOIN host AS h ON h.rowid = r.hostid
            JOIN directory AS d ON d.rowid = r.dirid
            LEFT JOIN archive AS a ON a.rowid = r.archiveid
            WHERE r.datasetid = ?
              AND r.deltime = 0
              AND h.deltime = 0
              AND d.deltime = 0
            ORDER BY r.rowid
            """,
            (int(dataset_row["rowid"]),),
        ).fetchall()
        locations = tuple(dict.fromkeys(self._replica_location(row) for row in replica_rows))
        if not locations:
            raise ValueError(f"dataset has no live replica location: {name}")

        return _DatasetDescription(
            dataset_id=int(dataset_row["rowid"]),
            dataset_uuid=dataset_uuid,
            name=str(dataset_row["name"]),
            file_format=str(dataset_row["fileformat"]),
            locations=locations,
        )

    @staticmethod
    def _replica_location(row: sqlite3.Row) -> str:
        host = str(row["longhostname"] or row["hostname"] or "").strip()
        directory = str(row["directory_name"] or "").strip()
        replica = str(row["replica_name"] or "").strip()
        if not host or not directory or not replica:
            raise ValueError("live dataset replica has incomplete host, directory, or name")

        archive_id = int(row["archive_id"] or 0)
        archive_name = str(row["archive_name"] or "").strip()
        if archive_id > 0:
            if not archive_name:
                raise ValueError("archived dataset replica has no archive name")
            path = str(PurePosixPath(directory) / archive_name)
            path += f"#{replica}"
        else:
            path = str(PurePosixPath(directory) / replica)

        protocol = str(row["protocol"] or "").strip().lower()
        if protocol:
            return f"{protocol}://{host}/{path.lstrip('/')}"
        return f"{host}:{path}"

    @staticmethod
    def _ensure_dataset_entity(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        dataset: _DatasetDescription,
    ) -> QualifiedName:
        reference = identifiers.dataset(dataset.dataset_uuid)
        records = document.get_record(reference)
        if len(records) > 1:
            raise ValueError(f"dataset identifier has multiple PROV assertions: {reference}")
        if records:
            record = records[0]
            if not isinstance(record, ProvEntity) or HPC["Dataset"] not in record.get_asserted_types():
                raise ValueError(f"dataset identifier conflicts with an existing PROV record: {reference}")
            CampaignProvenance._require_exact_attribute(record, PROV["label"], {dataset.name}, "dataset name")
            CampaignProvenance._require_exact_attribute(
                record,
                HPC["datasetUuid"],
                {str(dataset.dataset_uuid)},
                "dataset UUID",
            )
            CampaignProvenance._require_exact_attribute(
                record,
                HPC["format"],
                {dataset.file_format},
                "dataset format",
            )
            existing_locations = record.get_attribute(PROV["location"])
            missing_locations = set(dataset.locations).difference(existing_locations)
            if missing_locations:
                record.add_attributes((PROV["location"], location) for location in sorted(missing_locations))
            return reference

        attributes: list[tuple[QualifiedName, object]] = [
            (PROV["type"], HPC["Dataset"]),
            (PROV["label"], dataset.name),
            (HPC["datasetUuid"], str(dataset.dataset_uuid)),
            (HPC["format"], dataset.file_format),
        ]
        attributes.extend((PROV["location"], location) for location in dataset.locations)
        document.entity(reference, attributes)
        return reference

    @staticmethod
    def _require_exact_attribute(
        record: ProvRecord,
        attribute: QualifiedName,
        expected: set[object],
        label: str,
    ) -> None:
        if record.get_attribute(attribute) != expected:
            raise ValueError(f"existing PROV record has conflicting {label}: {record.identifier}")

    @staticmethod
    def _require_unused_identifier(document: ProvDocument, reference: QualifiedName) -> None:
        if document.get_record(reference):
            raise ValueError(f"PROV identifier already exists: {reference}")

    @staticmethod
    def _require_unique_run_name(document: ProvDocument, name: str) -> None:
        for record in document.get_records(ProvActivity):
            if HPC["SimulationRun"] not in record.get_asserted_types():
                continue
            if name in record.get_attribute(PROV["label"]):
                raise ValueError(f"run name already exists: {name}")

    @staticmethod
    def _require_campaign_reference(
        identifiers: CampaignProvIds,
        reference: QualifiedName,
        label: str,
    ) -> None:
        if not isinstance(reference, QualifiedName):
            raise TypeError(f"{label} must be a PROV QualifiedName")
        if reference.namespace.uri != identifiers.namespace.uri:
            raise ValueError(f"{label} belongs to another campaign: {reference}")

    @staticmethod
    def _require_record(
        document: ProvDocument,
        reference: QualifiedName,
        record_type: type[ProvRecord],
        required_type: QualifiedName | None,
        label: str,
    ) -> ProvRecord:
        records = document.get_record(reference)
        if len(records) != 1 or not isinstance(records[0], record_type):
            raise LookupError(f"{label} does not resolve to one {record_type.__name__}: {reference}")
        record = records[0]
        if required_type is not None and required_type not in record.get_asserted_types():
            raise ValueError(f"{label} has the wrong PROV type: {reference}")
        return record

    @classmethod
    def _agent_type(cls, kind: str) -> QualifiedName:
        normalized = cls._required_string(kind, "agent kind").lower()
        try:
            return cls._AGENT_TYPES[normalized]
        except KeyError as exc:
            choices = ", ".join(sorted(cls._AGENT_TYPES))
            raise ValueError(f"agent kind must be one of: {choices}") from exc

    @staticmethod
    def _reference_uuid(reference: QualifiedName, kind: str) -> uuid.UUID:
        prefix = f"{kind}_"
        if not reference.localpart.startswith(prefix):
            raise ValueError(f"expected {kind} identifier, got: {reference}")
        try:
            return uuid.UUID(reference.localpart[len(prefix) :])
        except ValueError as exc:
            raise ValueError(f"invalid {kind} UUID in PROV identifier: {reference}") from exc

    @staticmethod
    def _required_string(value: str, label: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be a non-empty string")
        return value.strip()

    @staticmethod
    def _required_token(value: str, label: str) -> str:
        token = CampaignProvenance._required_string(value, label)
        if not token[0].isalpha() or not all(character.isalnum() or character == "_" for character in token):
            raise ValueError(f"{label} must begin with a letter and contain only letters, digits, or underscores")
        return token

    @staticmethod
    def _optional_string(value: str | None, label: str, *, preserve: bool = False) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be a non-empty string when supplied")
        return value if preserve else value.strip()

    @staticmethod
    def _optional_uuid(value: uuid.UUID | None, label: str) -> uuid.UUID:
        if value is None:
            return uuid.uuid4()
        if not isinstance(value, uuid.UUID):
            raise TypeError(f"{label} must be a UUID")
        return value
