"""Campaign-oriented authoring of core scientific PROV records.

The public ``Manager`` methods delegate here so W3C PROV construction,
campaign validation, and canonical-document replacement remain out of the
already large manager implementation. It covers the Phase 2 scientific core:
datasets, Agents, Plans, logical variables, and Activities.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType

from prov.model import (
    PROV,
    ProvAgent,
    ProvDocument,
    ProvEntity,
    ProvGeneration,
    ProvRecord,
    QualifiedName,
)

from .prov_mapping import HPC, CampaignProvIds
from .prov_store import DEFAULT_PROV_DOCUMENT_NAME, ProvDocumentInfo, ProvenanceStorageError, ProvStore


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
class ActivityResult:
    """Stable PROV references produced by :meth:`Manager.add_activity`."""

    activity: QualifiedName
    outputs: Mapping[str, QualifiedName]

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
    # Imported PROV may contain additional valid Activity types.
    _ACTIVITY_TYPES = {
        "simulation": HPC["Simulation"],
        "reduction": HPC["Reduction"],
        "projection": HPC["Projection"],
        "quantity_of_interest": HPC["QuantityOfInterest"],
        "visualization": HPC["Visualization"],
    }

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

    def add_variable(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        *,
        dataset: str,
        variable: str,
        definition: str,
        variable_id: uuid.UUID | None = None,
        units: str | None = None,
        coordinate_system: str | None = None,
    ) -> QualifiedName:
        """Create one stable logical-variable Entity backed by an ACA dataset."""

        dataset_name = self._required_string(dataset, "dataset")
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
        dataset_description = self._resolve_dataset(dataset_name)
        dataset_reference, _ = self._ensure_dataset_entity(document, identifiers, dataset_description)
        reference = self._add_logical_variable_entity(
            document,
            identifiers,
            dataset_description,
            dataset_reference,
            description,
        )
        self._commit(info, document)
        return reference

    def add_dataset_entity(self, name: str) -> QualifiedName:
        """Register a typed dataset Entity, writing only if its declaration changes."""

        dataset = self._resolve_dataset(self._required_string(name, "dataset name"))
        identifiers = CampaignProvIds(self.store.campaign_uuid())
        references = self._active_reference_document()
        reference = identifiers.dataset(dataset.dataset_uuid)
        already_registered = bool(references.get_record(reference))
        reference, changed = self._ensure_dataset_entity(references, identifiers, dataset)
        if not changed:
            return reference
        info, document, identifiers = self._load_authored_document()
        if already_registered and not document.get_record(reference):
            raise ValueError(f"dataset registration conflicts with an imported Entity: {reference}")
        reference, changed = self._ensure_dataset_entity(document, identifiers, dataset)
        if changed:
            self._commit(info, document)
        return reference

    def dataset_entity(self, name: str) -> QualifiedName:
        """Look up an active dataset Entity without creating or updating records."""

        dataset = self._resolve_dataset(self._required_string(name, "dataset name"))
        reference = CampaignProvIds(self.store.campaign_uuid()).dataset(dataset.dataset_uuid)
        references = self._active_reference_document()
        if not references.get_record(reference):
            raise LookupError(f"dataset Entity not registered: {name}; use add_dataset_entity()")
        self._require_record(references, reference, ProvEntity, HPC["Dataset"], "dataset Entity")
        return reference

    def add_visualization_sequence_entity(self, name: str) -> QualifiedName:
        """Register the current ordered membership, reusing an unchanged snapshot."""

        name = self._required_string(name, "sequence name")
        datasets, member_order = self._resolve_visualization_sequence(name)
        references = self._active_reference_document()
        reference = self._find_sequence_entity(references, name, member_order)
        if reference is not None:
            return reference

        info, document, identifiers = self._load_authored_document()
        members = []
        for dataset in datasets:
            member = identifiers.dataset(dataset.dataset_uuid)
            if document.get_record(member) or not references.get_record(member):
                member, _ = self._ensure_dataset_entity(document, identifiers, dataset)
            else:
                member, changed = self._ensure_dataset_entity(references, identifiers, dataset)
                if changed:
                    raise ValueError(f"dataset registration conflicts with an imported Entity: {member}")
            members.append(member)
        reference = identifiers.entity(uuid.uuid4())
        sequence_type = HPC["ImageSequence"] if datasets[0].file_format == "IMAGE" else HPC["VideoSequence"]
        document.entity(
            reference,
            [
                (PROV["type"], PROV["Collection"]),
                (PROV["type"], HPC["VisualizationSequence"]),
                (PROV["type"], sequence_type),
                (PROV["label"], name),
                (HPC["sequenceName"], name),
                (HPC["memberOrder"], member_order),
            ],
        )
        for member in dict.fromkeys(members):
            document.hadMember(reference, member)
        self._commit(info, document)
        return reference

    def visualization_sequence_entity(self, name: str) -> QualifiedName:
        """Look up the registered current snapshot, without creating one."""

        name = self._required_string(name, "sequence name")
        _, member_order = self._resolve_visualization_sequence(name)
        reference = self._find_sequence_entity(self._active_reference_document(), name, member_order)
        if reference is None:
            raise LookupError(
                f"sequence snapshot not registered for current membership: {name}; "
                "use add_visualization_sequence_entity()"
            )
        return reference

    def _resolve_visualization_sequence(self, name: str) -> tuple[list[_DatasetDescription], str]:
        if not self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'visualization_sequence'"
        ).fetchone():
            raise LookupError(f"visualization sequence not found: {name}")
        sequence = self.connection.execute(
            "SELECT visid FROM visualization_sequence WHERE name = ?", (name,)
        ).fetchone()
        if sequence is None:
            raise LookupError(f"visualization sequence not found: {name}")
        rows = self.connection.execute(
            "SELECT i.item_uuid, d.name FROM visualization_item AS i "
            "LEFT JOIN dataset AS d ON d.uuid = i.item_uuid AND d.deltime = 0 "
            "WHERE i.visid = ? ORDER BY i.item_order",
            (sequence[0],),
        ).fetchall()
        if not rows or any(row["name"] is None for row in rows):
            raise ValueError("sequence must contain live image or video datasets")
        datasets = [self._resolve_dataset(row["name"]) for row in rows]
        formats = {dataset.file_format for dataset in datasets}
        if formats not in ({"IMAGE"}, {"VIDEO"}):
            raise ValueError("sequence must contain only images or only videos")
        member_order = json.dumps([str(dataset.dataset_uuid) for dataset in datasets], separators=(",", ":"))

        return datasets, member_order

    @staticmethod
    def _find_sequence_entity(document: ProvDocument, name: str, member_order: str) -> QualifiedName | None:
        for record in document.get_records(ProvEntity):
            if (
                HPC["VisualizationSequence"] in record.get_asserted_types()
                and record.get_attribute(HPC["sequenceName"]) == {name}
                and record.get_attribute(HPC["memberOrder"]) == {member_order}
            ):
                return record.identifier
        return None

    def add_activity(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        action: str,
        *,
        inputs: Mapping[str, QualifiedName] | None = None,
        outputs: Mapping[str, QualifiedName],
        name: str | None = None,
        derivations: Mapping[str, Sequence[str]] | None = None,
        context: Mapping[str, QualifiedName] | None = None,
        metadata: Mapping[str, object] | None = None,
        agent: QualifiedName | None = None,
        plan: QualifiedName | None = None,
        activity_id: uuid.UUID | None = None,
    ) -> ActivityResult:
        """Create one Activity, using and generating existing product Entities.

        ``inputs`` lists causal data products. ``context`` records other
        Entities used by the Activity without making them causal ancestors.
        Unless ``derivations`` is supplied, every output is derived from every
        causal input.
        """

        activity_type = self._activity_type(action)
        resolved_activity_id = self._optional_uuid(activity_id, "activity_id")
        activity_name = self._optional_string(name, "activity name")
        input_map = self._validated_role_mapping(
            inputs if inputs is not None else {},
            "inputs",
            require_values=False,
        )
        output_map = self._validated_output_mapping(outputs)
        context_map = self._validated_role_mapping(context or {}, "context", require_values=False)
        duplicate_roles = set(input_map).intersection(context_map)
        if duplicate_roles:
            raise ValueError("input and context roles must be unique: " + ", ".join(sorted(duplicate_roles)))
        if set(input_map.values()).intersection(output_map.values()):
            raise ValueError("an Activity must generate a new product instead of reusing an input Entity")
        derivation_map = self._validated_derivations(derivations, input_map, output_map)
        canonical_metadata = self._canonical_metadata(metadata)

        info, document, identifiers = self._load_authored_document()
        references = self._active_reference_document(info, document)
        activity_reference = identifiers.activity(resolved_activity_id)
        self._require_unused_identifier(references, activity_reference)
        self._require_products(references, identifiers, input_map, "input")
        self._require_products(references, identifiers, output_map, "output")
        self._require_context(references, identifiers, context_map)
        self._require_optional_association_records(references, identifiers, agent, plan)
        for output in output_map.values():
            if self._entity_has_generation(references, output):
                raise ValueError(f"output Entity already has a producing Activity: {output}")

        activity_attributes: list[tuple[QualifiedName, object]] = [(PROV["type"], activity_type)]
        if activity_name is not None:
            activity_attributes.append((PROV["label"], activity_name))
        if canonical_metadata is not None:
            activity_attributes.append((HPC["metadata"], canonical_metadata))
        document.activity(activity_reference, other_attributes=activity_attributes)
        usage_references = self._add_usages(
            document,
            identifiers,
            resolved_activity_id,
            activity_reference,
            {**input_map, **context_map},
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
        return ActivityResult(activity_reference, output_references)

    @staticmethod
    def _add_logical_variable_entity(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        document: ProvDocument,
        identifiers: CampaignProvIds,
        dataset: _DatasetDescription,
        dataset_reference: QualifiedName,
        description: _LogicalVariableDescription,
    ) -> QualifiedName:
        reference = identifiers.variable(description.variable_id)
        CampaignProvenance._require_unused_identifier(document, reference)
        attributes: list[tuple[QualifiedName, object]] = [
            (PROV["type"], HPC["LogicalVariable"]),
            (HPC["logicalVariableId"], str(description.variable_id)),
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
    def _require_products(
        document: ProvDocument,
        identifiers: CampaignProvIds,
        products: Mapping[str, QualifiedName],
        label: str,
    ) -> None:
        """Require causal endpoints to be local data products, excluding Plans."""

        for role, reference in products.items():
            description = f"{label} {role!r}"
            CampaignProvenance._require_campaign_reference(identifiers, reference, description)
            record = CampaignProvenance._require_record(document, reference, ProvEntity, None, description)
            types = record.get_asserted_types()
            if PROV["Plan"] in types or not types.intersection(
                {HPC["LogicalVariable"], HPC["Dataset"], HPC["VisualizationSequence"]}
            ):
                raise ValueError(f"{description} must reference a data product Entity: {reference}")

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

    def _add_activity_outputs(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        document: ProvDocument,
        identifiers: CampaignProvIds,
        activity_id: uuid.UUID,
        activity: QualifiedName,
        outputs: Mapping[str, QualifiedName],
    ) -> tuple[dict[str, QualifiedName], dict[str, QualifiedName]]:
        """Generate existing product Entities and identify each relation."""

        output_references: dict[str, QualifiedName] = {}
        generations: dict[str, QualifiedName] = {}
        for role, output in outputs.items():
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
    def _entity_has_generation(document: ProvDocument, entity: QualifiedName) -> bool:
        return any(record.args[0] == entity for record in document.get_records(ProvGeneration))

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
    def _validated_output_mapping(cls, outputs: Mapping[str, QualifiedName]) -> dict[str, QualifiedName]:
        if not isinstance(outputs, Mapping):
            raise TypeError("outputs must be a mapping from roles to product Entity QualifiedNames")
        if not outputs:
            raise ValueError("outputs must contain at least one role")

        normalized: dict[str, QualifiedName] = {}
        for role, reference in outputs.items():
            normalized_role = cls._required_token(role, "outputs role")
            if normalized_role in normalized:
                raise ValueError(f"outputs contains duplicate normalized role: {normalized_role}")
            if not isinstance(reference, QualifiedName):
                raise TypeError(f"output role {normalized_role!r} must reference a PROV QualifiedName")
            normalized[normalized_role] = reference
        if len(set(normalized.values())) != len(normalized):
            raise ValueError("each output role must reference a different product Entity")
        return normalized

    @classmethod
    def _validated_derivations(
        cls,
        derivations: Mapping[str, Sequence[str]] | None,
        inputs: Mapping[str, QualifiedName],
        outputs: Mapping[str, QualifiedName],
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
    def _canonical_metadata(metadata: Mapping[str, object] | None) -> str | None:
        if metadata is None:
            return None
        if not isinstance(metadata, Mapping):
            raise TypeError("metadata must be a JSON-compatible mapping")
        pending: list[object] = [metadata]
        seen = set()
        while pending:
            item = pending.pop()
            if id(item) in seen:
                continue
            if isinstance(item, Mapping):
                if any(not isinstance(key, str) for key in item):
                    raise ValueError("metadata keys must be strings at every nesting level")
                pending.extend(item.values())
            elif isinstance(item, (list, tuple)):
                pending.extend(item)
            else:
                continue
            # Shared containers need checking only once. Leave circular-value
            # rejection to json.dumps rather than looping during key checks.
            seen.add(id(item))
        try:
            value = json.dumps(dict(metadata), sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("metadata must contain only JSON-compatible values") from exc
        return value

    def _load_authored_document(self) -> tuple[ProvDocumentInfo | None, ProvDocument, CampaignProvIds]:
        """Load existing storage or start an unsaved document for the first edit."""
        identifiers = CampaignProvIds(self.store.campaign_uuid())
        info = next((item for item in self.store.documents() if item.name == DEFAULT_PROV_DOCUMENT_NAME), None)
        if info is not None:
            if not info.active:
                raise ProvenanceStorageError(
                    f"reserved document {DEFAULT_PROV_DOCUMENT_NAME!r} exists but is not active"
                )
            return info, self.store.document(info.document_id), identifiers

        document = ProvDocument()
        document.add_namespace(HPC)
        document.add_namespace(identifiers.namespace)
        return None, document, identifiers

    def _active_reference_document(
        self,
        info: ProvDocumentInfo | None = None,
        authored: ProvDocument | None = None,
    ) -> ProvDocument:
        """Build a reference view without copying imports into authored storage."""

        documents = [] if authored is None else [authored]
        documents.extend(
            self.store.document(item.document_id)
            for item in self.store.documents(active=True)
            if info is None or item.document_id != info.document_id
        )
        references = ProvDocument()
        seen = set()
        for document in documents:
            for bundle in (document, *document.bundles):
                for record in bundle.get_records():
                    if record not in seen:
                        references.add_record(record)
                        seen.add(record)
        return references

    def _commit(self, info: ProvDocumentInfo | None, document: ProvDocument) -> None:
        if info is None:
            self.store.add_document(document, name=DEFAULT_PROV_DOCUMENT_NAME, active=True)
            return
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
    ) -> tuple[QualifiedName, bool]:
        """Return the reference and whether the supplied document was changed."""

        reference = identifiers.dataset(dataset.dataset_uuid)
        records = document.get_record(reference)
        if len(records) > 1:
            raise ValueError(f"dataset identifier has multiple PROV assertions: {reference}")
        if records:
            changed = False
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
                record.add_attributes([(PROV["location"], location) for location in sorted(missing_locations)])
                changed = True
            media_type = {"IMAGE": HPC["Image"], "VIDEO": HPC["Video"]}.get(dataset.file_format)
            if media_type is not None and media_type not in record.get_asserted_types():
                record.add_attributes([(PROV["type"], media_type)])
                changed = True
            return reference, changed

        attributes: list[tuple[QualifiedName, object]] = [
            (PROV["type"], HPC["Dataset"]),
            (PROV["label"], dataset.name),
            (HPC["datasetUuid"], str(dataset.dataset_uuid)),
            (HPC["format"], dataset.file_format),
        ]
        attributes.extend((PROV["location"], location) for location in dataset.locations)
        media_type = {"IMAGE": HPC["Image"], "VIDEO": HPC["Video"]}.get(dataset.file_format)
        if media_type is not None:
            attributes.append((PROV["type"], media_type))
        document.entity(reference, attributes)
        return reference, True

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
