"""Validation for promoting imported PROV into the active campaign graph.

Inactive documents are intentionally outside this validator: they are parsed,
canonicalized, hashed, and preserved even when the campaign cannot interpret
or resolve them. Activation is the explicit boundary at which campaign
self-containment and the supported scientific-core invariants apply.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from prov.model import (
    PROV,
    ProvActivity,
    ProvAgent,
    ProvDerivation,
    ProvDocument,
    ProvEntity,
    ProvGeneration,
    ProvRecord,
    ProvRelation,
    ProvUsage,
    QualifiedName,
)

from .prov_mapping import HPC, CampaignProvIds

_UUID_HEX = r"[0-9a-f]{32}"
_ROLE = r"[A-Za-z][A-Za-z0-9_]*"
_OBJECT_ID = re.compile(rf"^(dataset|variable|run|activity|agent|plan|entity)_({_UUID_HEX})$")
_USAGE_ID = re.compile(rf"^usage_({_UUID_HEX})_({_ROLE})$")
_GENERATION_ID = re.compile(rf"^generation_({_UUID_HEX})_({_ROLE})$")
_DERIVATION_PREFIX = re.compile(rf"^derivation_({_UUID_HEX})_.+$")


class ProvenanceValidationError(ValueError):
    """Raised when a document cannot join the active campaign graph."""


@dataclass(frozen=True)
class ActiveProvDocument:
    """One document and the stable storage label used in diagnostics."""

    label: str
    document: ProvDocument


class CampaignProvValidator:  # pylint: disable=too-few-public-methods
    """Validate the union of all documents selected as active."""

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.campaign_id = self._campaign_uuid()
        self.identifiers = CampaignProvIds(self.campaign_id)
        self.namespace_uri = self.identifiers.namespace.uri

    def validate(self, documents: Sequence[ActiveProvDocument]) -> None:
        """Validate a complete proposed active-document set.

        The method deliberately does not require the campaign to understand
        every PROV record. Unknown records remain valid when their graph
        references are self-contained and do not misuse another campaign's
        namespace.
        """

        records_by_document = {item.label: tuple(self._records(item.document)) for item in documents}
        bundle_ids_by_document = {
            item.label: tuple(bundle.identifier for bundle in item.document.bundles if bundle.identifier is not None)
            for item in documents
        }
        self._validate_namespaces(records_by_document, bundle_ids_by_document)
        self._validate_identifier_collisions(records_by_document, bundle_ids_by_document)

        all_records = tuple(record for records in records_by_document.values() for record in records)
        declared = {record.identifier for record in all_records if record.identifier is not None}
        declared.update(identifier for identifiers in bundle_ids_by_document.values() for identifier in identifiers)
        self._validate_relationship_endpoints(all_records, declared)
        self._validate_supported_core(all_records, declared)
        self._validate_generation_uniqueness(all_records)

    @staticmethod
    def _records(document: ProvDocument) -> Iterable[ProvRecord]:
        yield from document.get_records()
        for bundle in document.bundles:
            yield from bundle.get_records()

    def _validate_namespaces(
        self,
        records_by_document: dict[str, tuple[ProvRecord, ...]],
        bundle_ids_by_document: dict[str, tuple[QualifiedName, ...]],
    ) -> None:
        """Reject object references that explicitly name another campaign."""

        for label, records in records_by_document.items():
            references = [value for record in records for value in self._qualified_names(record)]
            references.extend(bundle_ids_by_document[label])
            for reference in references:
                foreign_campaign = self._campaign_uuid_from_namespace(reference)
                if foreign_campaign is not None and foreign_campaign != self.campaign_id:
                    raise ProvenanceValidationError(
                        f"active document {label!r} references another campaign: {reference}"
                    )

    @staticmethod
    def _qualified_names(record: ProvRecord) -> Iterable[QualifiedName]:
        if record.identifier is not None:
            yield record.identifier
        for _, value in record.attributes:
            if isinstance(value, QualifiedName):
                yield value

    @staticmethod
    def _campaign_uuid_from_namespace(reference: QualifiedName) -> uuid.UUID | None:
        uri = reference.namespace.uri
        prefix = "urn:hpc-campaign:"
        if not uri.startswith(prefix) or not uri.endswith(":"):
            return None
        candidate = uri[len(prefix) : -1]
        try:
            return uuid.UUID(candidate)
        except ValueError:
            # Vocabulary and other non-object namespaces are not campaign IDs.
            return None

    @staticmethod
    def _validate_identifier_collisions(
        records_by_document: dict[str, tuple[ProvRecord, ...]],
        bundle_ids_by_document: dict[str, tuple[QualifiedName, ...]],
    ) -> None:
        """Allow identical repeated assertions but reject conflicting reuse."""

        prior_records: dict[QualifiedName, tuple[str, frozenset[ProvRecord]]] = {}
        prior_bundles: dict[QualifiedName, str] = {}
        for label, records in records_by_document.items():
            current: dict[QualifiedName, set[ProvRecord]] = defaultdict(set)
            for record in records:
                if record.identifier is not None:
                    current[record.identifier].add(record)
            for identifier, assertions in current.items():
                frozen = frozenset(assertions)
                prior = prior_records.get(identifier)
                if prior is not None and prior[1] != frozen:
                    raise ProvenanceValidationError(
                        f"active documents {prior[0]!r} and {label!r} contain "
                        f"conflicting records for identifier {identifier}"
                    )
                prior_records[identifier] = (label, frozen)

            for identifier in bundle_ids_by_document[label]:
                prior_label = prior_bundles.get(identifier)
                if prior_label is not None and prior_label != label:
                    raise ProvenanceValidationError(
                        f"active documents {prior_label!r} and {label!r} reuse bundle identifier {identifier}"
                    )
                prior_bundles[identifier] = label

    @staticmethod
    def _validate_relationship_endpoints(
        records: Sequence[ProvRecord],
        declared: set[QualifiedName],
    ) -> None:
        for record in records:
            if not isinstance(record, ProvRelation):
                continue
            for attribute, endpoint in record.formal_attributes:
                if isinstance(endpoint, QualifiedName) and endpoint not in declared:
                    raise ProvenanceValidationError(f"unresolved active relationship endpoint {attribute}={endpoint}")

    def _validate_supported_core(
        self,
        records: Sequence[ProvRecord],
        declared: set[QualifiedName],
    ) -> None:
        records_by_id: dict[QualifiedName, list[ProvRecord]] = defaultdict(list)
        for record in records:
            if record.identifier is not None:
                records_by_id[record.identifier].append(record)

        live_dataset_ids = self._live_dataset_ids()
        for record in records:
            self._validate_local_identifier(record)
            asserted_types = record.get_asserted_types()
            if isinstance(record, ProvEntity) and HPC["Dataset"] in asserted_types:
                self._validate_dataset(record, live_dataset_ids)
            if isinstance(record, ProvEntity) and HPC["LogicalVariable"] in asserted_types:
                self._validate_logical_variable(record, declared, records_by_id)
            if isinstance(record, ProvEntity) and HPC["ActionSpecification"] in asserted_types:
                self._validate_action_specification(record)
            if isinstance(record, ProvGeneration):
                self._validate_generation(record)
            if isinstance(record, ProvUsage):
                self._validate_usage(record)
            if isinstance(record, ProvDerivation):
                self._validate_derivation(record)

    def _validate_local_identifier(self, record: ProvRecord) -> None:
        reference = record.identifier
        if reference is None or reference.namespace.uri != self.namespace_uri:
            return

        localpart = reference.localpart
        object_match = _OBJECT_ID.fullmatch(localpart)
        if object_match:
            kind = object_match.group(1)
            if isinstance(record, ProvEntity) and kind not in {"dataset", "variable", "plan", "entity"}:
                raise ProvenanceValidationError(f"Entity has incompatible campaign identifier: {reference}")
            if isinstance(record, ProvActivity) and kind not in {"run", "activity"}:
                raise ProvenanceValidationError(f"Activity has incompatible campaign identifier: {reference}")
            if isinstance(record, ProvAgent) and kind != "agent":
                raise ProvenanceValidationError(f"Agent has incompatible campaign identifier: {reference}")
            return
        if isinstance(record, ProvUsage) and _USAGE_ID.fullmatch(localpart):
            return
        if isinstance(record, ProvGeneration) and _GENERATION_ID.fullmatch(localpart):
            return
        # Exact derivation validation happens after the referenced Generation
        # and Usage provide the two potentially underscore-containing roles.
        if isinstance(record, ProvDerivation) and _DERIVATION_PREFIX.fullmatch(localpart):
            return
        raise ProvenanceValidationError(f"unsupported campaign-qualified identifier: {reference}")

    def _validate_dataset(self, record: ProvEntity, live_dataset_ids: set[uuid.UUID]) -> None:
        identifier_id = self._object_uuid(record, "dataset")
        values = self._one_attribute(record, HPC["datasetUuid"], "dataset UUID")
        try:
            attribute_id = uuid.UUID(str(values))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProvenanceValidationError(f"Dataset has an invalid hpc:datasetUuid: {record.identifier}") from exc
        if identifier_id != attribute_id:
            raise ProvenanceValidationError(f"Dataset identifier and hpc:datasetUuid disagree: {record.identifier}")
        if attribute_id not in live_dataset_ids:
            raise ProvenanceValidationError(f"Dataset does not resolve to a live ACA dataset: {record.identifier}")
        self._require_nonempty_values(record, PROV["location"], "Dataset location")

    def _validate_logical_variable(  # pylint: disable=too-many-locals
        self,
        record: ProvEntity,
        declared: set[QualifiedName],
        records_by_id: dict[QualifiedName, list[ProvRecord]],
    ) -> None:
        identifier_id = self._object_uuid(record, "variable")
        logical_id = self._one_attribute(record, HPC["logicalVariableId"], "logical-variable UUID")
        try:
            attribute_id = uuid.UUID(str(logical_id))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProvenanceValidationError(
                f"LogicalVariable has an invalid hpc:logicalVariableId: {record.identifier}"
            ) from exc
        if identifier_id != attribute_id:
            raise ProvenanceValidationError(f"LogicalVariable identifier and UUID disagree: {record.identifier}")

        run = self._one_qname_attribute(record, HPC["run"], "LogicalVariable run")
        dataset = self._one_qname_attribute(record, HPC["dataset"], "LogicalVariable dataset")
        for label, reference, record_type, required_type in (
            ("run", run, ProvActivity, HPC["SimulationRun"]),
            ("dataset", dataset, ProvEntity, HPC["Dataset"]),
        ):
            if reference not in declared:
                raise ProvenanceValidationError(f"LogicalVariable {label} reference is unresolved: {reference}")
            candidates = records_by_id[reference]
            if not any(
                isinstance(candidate, record_type) and required_type in candidate.get_asserted_types()
                for candidate in candidates
            ):
                raise ProvenanceValidationError(f"LogicalVariable {label} reference has the wrong type: {reference}")

        for attribute, label in (
            (HPC["datasetName"], "dataset name"),
            (HPC["variable"], "physical variable name"),
            (HPC["variableDefinition"], "variable definition"),
        ):
            value = self._one_attribute(record, attribute, f"LogicalVariable {label}")
            if not isinstance(value, str) or not value.strip():
                raise ProvenanceValidationError(
                    f"LogicalVariable {label} must be a non-empty string: {record.identifier}"
                )
        self._require_nonempty_values(record, PROV["location"], "LogicalVariable location")

    def _validate_action_specification(self, record: ProvEntity) -> None:
        if PROV["Plan"] not in record.get_asserted_types():
            raise ProvenanceValidationError(f"ActionSpecification must also be a prov:Plan: {record.identifier}")
        value = self._one_attribute(record, PROV["value"], "ActionSpecification value")
        media_type = self._one_attribute(
            record,
            HPC["mediaType"],
            "ActionSpecification media type",
        )
        digest = self._one_attribute(record, HPC["sha256"], "ActionSpecification SHA-256")
        if not isinstance(value, str) or media_type != "application/json":
            raise ProvenanceValidationError(f"ActionSpecification must contain canonical JSON: {record.identifier}")
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ProvenanceValidationError(f"ActionSpecification contains invalid JSON: {record.identifier}") from exc
        if not isinstance(decoded, dict):
            raise ProvenanceValidationError(f"ActionSpecification JSON must be an object: {record.identifier}")
        canonical = json.dumps(decoded, sort_keys=True, separators=(",", ":"), allow_nan=False)
        expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if value != canonical or digest != expected:
            raise ProvenanceValidationError(
                f"ActionSpecification canonical JSON or SHA-256 is invalid: {record.identifier}"
            )

    def _validate_usage(self, record: ProvUsage) -> None:
        match = self._local_relation_match(record, _USAGE_ID)
        if match is None:
            return
        activity_id, role = match.groups()
        self._require_role(record, role)
        self._require_activity_uuid(record.args[0], activity_id, "Usage")

    def _validate_generation(self, record: ProvGeneration) -> None:
        match = self._local_relation_match(record, _GENERATION_ID)
        if match is None:
            return
        activity_id, role = match.groups()
        self._require_role(record, role)
        self._require_activity_uuid(record.args[1], activity_id, "Generation")

    def _validate_derivation(self, record: ProvDerivation) -> None:
        reference = record.identifier
        if reference is None or reference.namespace.uri != self.namespace_uri:
            return

        # A derivation identifier contains two role tokens separated by an
        # underscore. Splitting that text is ambiguous because underscores are
        # valid inside either role. The referenced Generation and Usage each
        # contain exactly one role, so derive the expected identifier from
        # those unambiguous records instead.
        activity, generation, usage = record.args[2:5]
        generation_match = self._local_reference_match(generation, _GENERATION_ID, "Derivation Generation")
        usage_match = self._local_reference_match(usage, _USAGE_ID, "Derivation Usage")
        generation_activity, output_role = generation_match.groups()
        usage_activity, input_role = usage_match.groups()
        if generation_activity != usage_activity:
            raise ProvenanceValidationError(
                f"Derivation Generation and Usage reference different Activities: {record.identifier}"
            )

        self._require_activity_uuid(activity, generation_activity, "Derivation")
        expected = self.identifiers.derivation(
            uuid.UUID(hex=generation_activity),
            output_role,
            input_role,
        )
        if reference != expected:
            raise ProvenanceValidationError(
                f"Derivation identifier does not match its Generation and Usage roles: {record.identifier}"
            )

    @staticmethod
    def _validate_generation_uniqueness(records: Sequence[ProvRecord]) -> None:
        generators: dict[QualifiedName, set[QualifiedName | None]] = defaultdict(set)
        for record in records:
            if isinstance(record, ProvGeneration) and isinstance(record.args[0], QualifiedName):
                generators[record.args[0]].add(record.args[1])
        for entity, activities in generators.items():
            if len(activities) > 1:
                raise ProvenanceValidationError(f"active Entity has more than one generating Activity: {entity}")

    def _object_uuid(self, record: ProvRecord, expected_kind: str) -> uuid.UUID:
        reference = record.identifier
        if reference is None or reference.namespace.uri != self.namespace_uri:
            raise ProvenanceValidationError(f"{expected_kind} record must use this campaign's namespace: {reference}")
        match = _OBJECT_ID.fullmatch(reference.localpart)
        if match is None or match.group(1) != expected_kind:
            raise ProvenanceValidationError(f"{expected_kind} record has an invalid stable identifier: {reference}")
        return uuid.UUID(hex=match.group(2))

    @staticmethod
    def _one_attribute(record: ProvRecord, attribute: QualifiedName, label: str):
        values = record.get_attribute(attribute)
        if len(values) != 1:
            raise ProvenanceValidationError(f"{label} must occur exactly once: {record.identifier}")
        return next(iter(values))

    @classmethod
    def _one_qname_attribute(
        cls,
        record: ProvRecord,
        attribute: QualifiedName,
        label: str,
    ) -> QualifiedName:
        value = cls._one_attribute(record, attribute, label)
        if not isinstance(value, QualifiedName):
            raise ProvenanceValidationError(f"{label} must be a PROV QualifiedName")
        return value

    @staticmethod
    def _require_nonempty_values(
        record: ProvRecord,
        attribute: QualifiedName,
        label: str,
    ) -> None:
        values = record.get_attribute(attribute)
        if not values or any(not isinstance(value, str) or not value.strip() for value in values):
            raise ProvenanceValidationError(f"{label} must contain non-empty strings: {record.identifier}")

    def _live_dataset_ids(self) -> set[uuid.UUID]:
        rows = self.connection.execute("SELECT uuid FROM dataset WHERE deltime = 0").fetchall()
        result: set[uuid.UUID] = set()
        for row in rows:
            try:
                result.add(uuid.UUID(str(row[0])))
            except (AttributeError, TypeError, ValueError):
                # Invalid unrelated dataset rows are diagnosed when referenced.
                continue
        return result

    def _local_relation_match(self, record: ProvRecord, pattern: re.Pattern[str]):
        reference = record.identifier
        if reference is None or reference.namespace.uri != self.namespace_uri:
            return None
        match = pattern.fullmatch(reference.localpart)
        if match is None:
            raise ProvenanceValidationError(f"campaign relation has an invalid identifier: {reference}")
        return match

    def _local_reference_match(
        self,
        reference: object,
        pattern: re.Pattern[str],
        label: str,
    ) -> re.Match[str]:
        """Validate and parse one campaign-qualified relation reference."""
        if not isinstance(reference, QualifiedName) or reference.namespace.uri != self.namespace_uri:
            raise ProvenanceValidationError(f"{label} is not campaign-qualified: {reference}")
        match = pattern.fullmatch(reference.localpart)
        if match is None:
            raise ProvenanceValidationError(f"{label} has an invalid identifier: {reference}")
        return match

    @staticmethod
    def _require_role(record: ProvRecord, role: str) -> None:
        if record.get_attribute(PROV["role"]) != {HPC[role]}:
            raise ProvenanceValidationError(
                f"campaign relation role does not match its identifier: {record.identifier}"
            )

    def _require_activity_uuid(
        self,
        reference: object,
        expected_hex: str,
        label: str,
    ) -> None:
        if not isinstance(reference, QualifiedName) or reference.namespace.uri != self.namespace_uri:
            raise ProvenanceValidationError(f"{label} Activity is not campaign-qualified: {reference}")
        match = _OBJECT_ID.fullmatch(reference.localpart)
        if match is None or match.group(1) not in {"run", "activity"} or match.group(2) != expected_hex:
            raise ProvenanceValidationError(f"{label} identifier does not match its Activity: {reference}")

    def _campaign_uuid(self) -> uuid.UUID:
        row = self.connection.execute("SELECT uuid FROM campaign_identity WHERE singleton = 1").fetchone()
        if row is None:
            raise ProvenanceValidationError("campaign identity is missing")
        try:
            return uuid.UUID(str(row[0]))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProvenanceValidationError("campaign identity is invalid") from exc
