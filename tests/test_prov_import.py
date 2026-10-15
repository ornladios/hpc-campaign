import uuid
from pathlib import Path

import pytest
from prov.model import PROV, Namespace, ProvActivity, ProvDerivation, ProvDocument

from hpc_campaign.manager import Manager
from hpc_campaign.prov_mapping import HPC, CampaignProvIds
from hpc_campaign.prov_store import (
    DEFAULT_PROV_DOCUMENT_NAME,
    ProvenanceStorageError,
    canonical_prov_json,
)
from hpc_campaign.prov_validation import ProvenanceValidationError


def _new_manager(tmp_path: Path, name: str = "campaign.aca") -> Manager:
    manager = Manager(archive=name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    return manager


def _document_state(manager: Manager, name: str):
    row = manager.con.execute(
        "SELECT content, sha256, active, modtime FROM provenance_document WHERE name = ?",
        (name,),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _unfamiliar_complete_document() -> ProvDocument:
    """Create valid dataflow using a type the campaign API does not know."""

    document = ProvDocument()
    example = Namespace("example", "urn:example:")
    document.add_namespace(example)
    document.entity(example["source"])
    document.entity(example["result"])
    document.activity(
        example["domain-operation"],
        other_attributes=[(PROV["type"], example["UnfamiliarScientificOperation"])],
    )
    document.used(example["domain-operation"], example["source"])
    document.wasGeneratedBy(example["result"], example["domain-operation"])
    # A direct Derivation is legal imported PROV. The campaign must not invent
    # a different Activity merely because its convenience API is more strict.
    document.wasDerivedFrom(example["result"], example["source"])
    return document


def test_unknown_activity_and_direct_derivation_survive_active_round_trip(tmp_path: Path):
    manager = _new_manager(tmp_path)
    document = _unfamiliar_complete_document()

    document_id = manager.add_prov_document(document, name="domain-specific", activate=True)
    loaded = manager.prov_document(document_id)
    export_path = tmp_path / "domain-specific.json"
    manager.export_prov(document_id, export_path)

    assert loaded == document
    assert any(
        Namespace("example", "urn:example:")["UnfamiliarScientificOperation"] in record.get_asserted_types()
        for record in loaded.get_records(ProvActivity)
    )
    assert len(list(loaded.get_records(ProvDerivation))) == 1
    assert export_path.read_text(encoding="utf-8") == canonical_prov_json(document)
    manager.close()


def test_unknown_campaign_activity_type_is_allowed_with_stable_activity_id(tmp_path: Path):
    manager = _new_manager(tmp_path)
    identifiers = CampaignProvIds(manager.campaign_uuid())
    activity_id = uuid.uuid4()
    document = ProvDocument()
    document.add_namespace(HPC)
    document.add_namespace(identifiers.namespace)
    document.activity(
        identifiers.activity(activity_id),
        other_attributes=[(PROV["type"], HPC["DomainSpecificOperation"])],
    )

    document_id = manager.add_prov_document(document, name="unknown-action", activate=True)

    loaded = manager.prov_document(document_id)
    record = loaded.get_record(identifiers.activity(activity_id))[0]
    assert HPC["DomainSpecificOperation"] in record.get_asserted_types()
    manager.close()


def test_obsolete_run_identifier_is_rejected_on_activation(tmp_path: Path):
    manager = _new_manager(tmp_path)
    identifiers = CampaignProvIds(manager.campaign_uuid())
    document = ProvDocument()
    document.add_namespace(identifiers.namespace)
    obsolete_activity = identifiers.namespace[f"run_{uuid.uuid4().hex}"]
    document.activity(obsolete_activity, other_attributes=[(PROV["type"], HPC["SimulationRun"])])

    with pytest.raises(ProvenanceValidationError, match="unsupported campaign-qualified identifier"):
        manager.add_prov_document(document, name="obsolete-run-model", activate=True)
    assert all(item.name != "obsolete-run-model" for item in manager.prov_documents())
    manager.close()


def test_unresolved_document_is_preserved_inactive_then_activated_with_support(tmp_path: Path):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:example:")
    unresolved = ProvDocument()
    unresolved.add_namespace(example)
    unresolved.activity(example["activity"])
    unresolved.used(example["activity"], example["missing-input"])

    document_id = manager.add_prov_document(unresolved, name="unresolved", activate=False)
    before = _document_state(manager, "unresolved")
    with pytest.raises(ProvenanceValidationError, match="unresolved active relationship endpoint"):
        manager.set_prov_document_active(document_id)

    # Failed promotion cannot rewrite the preserved bytes, hash, flag, or
    # modification time of the inactive document.
    assert _document_state(manager, "unresolved") == before
    assert manager.prov_document(document_id) == unresolved

    support = ProvDocument()
    support.add_namespace(example)
    support.entity(example["missing-input"])
    manager.add_prov_document(support, name="support", activate=True)
    activated = manager.set_prov_document_active("unresolved")
    assert activated.active
    manager.close()

    # Activation is persistent campaign state, not an in-memory selection.
    reopened = Manager(archive="campaign.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert {item.name for item in reopened.prov_documents(active=True)} == {
        "support",
        "unresolved",
    }
    reopened.close()


def test_foreign_campaign_namespace_is_preserved_but_cannot_be_activated(tmp_path: Path):
    manager = _new_manager(tmp_path)
    foreign_ids = CampaignProvIds(uuid.uuid4())
    document = ProvDocument()
    document.add_namespace(foreign_ids.namespace)
    document.entity(foreign_ids.entity(uuid.uuid4()))

    document_id = manager.add_prov_document(document, name="foreign", activate=False)
    before = _document_state(manager, "foreign")
    with pytest.raises(ProvenanceValidationError, match="references another campaign"):
        manager.set_prov_document_active(document_id)

    assert _document_state(manager, "foreign") == before
    assert manager.prov_document(document_id) == document
    manager.close()


def test_conflicting_identifier_rejects_activation_without_changing_documents(tmp_path: Path):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:example:")
    first = ProvDocument()
    first.add_namespace(example)
    first.entity(example["shared"], [(PROV["label"], "first assertion")])
    manager.add_prov_document(first, name="first", activate=True)

    conflicting = ProvDocument()
    conflicting.add_namespace(example)
    conflicting.entity(example["shared"], [(PROV["label"], "conflicting assertion")])
    conflict_id = manager.add_prov_document(conflicting, name="conflicting", activate=False)
    first_before = _document_state(manager, "first")
    conflict_before = _document_state(manager, "conflicting")

    with pytest.raises(ProvenanceValidationError, match="conflicting records"):
        manager.set_prov_document_active(conflict_id)

    assert _document_state(manager, "first") == first_before
    assert _document_state(manager, "conflicting") == conflict_before
    manager.close()


def test_duplicate_generating_activities_are_rejected_before_active_insert(tmp_path: Path):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:example:")
    document = ProvDocument()
    document.add_namespace(example)
    document.entity(example["result"])
    document.activity(example["first"])
    document.activity(example["second"])
    document.wasGeneratedBy(example["result"], example["first"])
    document.wasGeneratedBy(example["result"], example["second"])

    with pytest.raises(ProvenanceValidationError, match="more than one generating Activity"):
        manager.add_prov_document(document, name="two-generators", activate=True)

    # Validation happens inside the insertion transaction, so a failed active
    # import does not leave a partially stored inactive document behind.
    assert all(item.name != "two-generators" for item in manager.prov_documents())
    manager.close()


def test_deactivation_cannot_leave_dangling_active_relationships(tmp_path: Path):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:example:")
    support = ProvDocument()
    support.add_namespace(example)
    support.entity(example["input"])
    support_id = manager.add_prov_document(support, name="support", activate=True)

    dependent = ProvDocument()
    dependent.add_namespace(example)
    dependent.activity(example["activity"])
    dependent.used(example["activity"], example["input"])
    dependent_id = manager.add_prov_document(dependent, name="dependent", activate=True)
    support_before = _document_state(manager, "support")

    with pytest.raises(ProvenanceValidationError, match="unresolved active relationship endpoint"):
        manager.set_prov_document_active(support_id, active=False)
    assert _document_state(manager, "support") == support_before

    assert not manager.set_prov_document_active(dependent_id, active=False).active
    assert not manager.set_prov_document_active(support_id, active=False).active
    assert manager.prov_documents(active=True) == []
    manager.close()


def test_reserved_authored_document_cannot_be_deactivated(tmp_path: Path):
    manager = _new_manager(tmp_path)
    manager.add_agent("software", "example")
    before = _document_state(manager, DEFAULT_PROV_DOCUMENT_NAME)

    with pytest.raises(ProvenanceStorageError, match="cannot be deactivated"):
        manager.set_prov_document_active(DEFAULT_PROV_DOCUMENT_NAME, active=False)

    assert _document_state(manager, DEFAULT_PROV_DOCUMENT_NAME) == before
    manager.close()


def test_active_campaign_dataset_must_resolve_to_existing_aca_dataset(tmp_path: Path):
    manager = _new_manager(tmp_path)
    identifiers = CampaignProvIds(manager.campaign_uuid())
    missing_dataset_id = uuid.uuid4()
    document = ProvDocument()
    document.add_namespace(HPC)
    document.add_namespace(identifiers.namespace)
    document.entity(
        identifiers.dataset(missing_dataset_id),
        [
            (PROV["type"], HPC["Dataset"]),
            (HPC["datasetUuid"], str(missing_dataset_id)),
            (PROV["location"], "data/missing.bp"),
        ],
    )

    document_id = manager.add_prov_document(document, name="missing-dataset", activate=False)
    with pytest.raises(ProvenanceValidationError, match="live ACA dataset"):
        manager.set_prov_document_active(document_id)

    assert manager.prov_documents(active=False)[0].name == "missing-dataset"
    manager.close()


def test_campaign_qualified_usage_role_must_match_stable_identifier(tmp_path: Path):
    manager = _new_manager(tmp_path)
    identifiers = CampaignProvIds(manager.campaign_uuid())
    activity_id = uuid.uuid4()
    activity = identifiers.activity(activity_id)
    source = identifiers.entity(uuid.uuid4())
    document = ProvDocument()
    document.add_namespace(HPC)
    document.add_namespace(identifiers.namespace)
    document.activity(activity, other_attributes=[(PROV["type"], HPC["DomainSpecificOperation"])])
    document.entity(source)
    document.used(
        activity,
        source,
        identifier=identifiers.usage(activity_id, "source"),
        other_attributes=[(PROV["role"], HPC["different_role"])],
    )

    document_id = manager.add_prov_document(document, name="wrong-role", activate=False)
    with pytest.raises(ProvenanceValidationError, match="role does not match"):
        manager.set_prov_document_active(document_id)

    assert not manager.prov_documents(active=False)[0].active
    manager.close()


@pytest.mark.parametrize("self_cycle", [False, True])
def test_cyclic_import_is_preserved_inactive_and_activation_is_atomic(tmp_path: Path, self_cycle):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:cycle:")
    support = ProvDocument()
    support.add_namespace(example)
    support.entity(example["a"])
    support.entity(example["b"])
    if not self_cycle:
        support.wasDerivedFrom(example["b"], example["a"])
    manager.add_prov_document(support, name="support", activate=True)
    support_before = _document_state(manager, "support")
    cyclic = ProvDocument()
    cyclic.add_namespace(example)
    cyclic.wasDerivedFrom(example["a"], example["a"] if self_cycle else example["b"])
    imported_id = manager.add_prov_document(cyclic, name="cycle", activate=False)
    before = _document_state(manager, "cycle")

    with pytest.raises(ProvenanceValidationError, match="derivation cycle"):
        manager.set_prov_document_active(imported_id)
    assert _document_state(manager, "cycle") == before
    assert _document_state(manager, "support") == support_before
    with pytest.raises(ProvenanceValidationError, match="derivation cycle"):
        manager.add_prov_document(cyclic, name="active-cycle", activate=True)
    assert "active-cycle" not in {item.name for item in manager.prov_documents()}
    manager.close()


def test_long_acyclic_import_and_repeated_derivations_are_accepted(tmp_path: Path):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:chain:")
    document = ProvDocument()
    document.add_namespace(example)
    entities = [example[f"item_{index}"] for index in range(1200)]
    for entity in entities:
        document.entity(entity)
    for parent, child in zip(entities, entities[1:], strict=False):
        document.wasDerivedFrom(child, parent)
    document_id = manager.add_prov_document(document, name="chain", activate=True)
    repeated = ProvDocument()
    repeated.add_namespace(example)
    repeated.wasDerivedFrom(entities[-1], entities[0])
    repeated.wasDerivedFrom(entities[-1], entities[0])
    manager.add_prov_document(repeated, name="shared-ancestor", activate=True)
    manager.add_prov_document(repeated, name="repeated-assertions", activate=True)

    assert len(list(manager.prov_document(document_id).get_records(ProvDerivation))) == 1199
    manager.close()


def _qualified_derivation_document(manager, defect=None, campaign_ids=False):
    """Build a minimal graph whose references can disagree independently."""

    identifiers = CampaignProvIds(manager.campaign_uuid())
    example = Namespace("example", "urn:qualified:")
    document = ProvDocument()
    document.add_namespace(example)
    document.add_namespace(HPC)
    document.add_namespace(identifiers.namespace)
    entities = [
        identifiers.entity(uuid.uuid4()) if campaign_ids else example[name] for name in ("source", "result", "other")
    ]
    for entity in entities:
        document.entity(entity)
    source, result, other = entities
    activity_id = uuid.uuid4()
    activity = identifiers.activity(activity_id) if campaign_ids else example["activity"]
    other_activity = identifiers.activity(uuid.uuid4()) if campaign_ids else example["other_activity"]
    document.activity(activity)
    document.activity(other_activity)
    generation = identifiers.generation(activity_id, "result") if campaign_ids else example["generation"]
    usage = identifiers.usage(activity_id, "source") if campaign_ids else example["usage"]
    if defect == "generation_type":
        document.entity(generation)
    else:
        document.wasGeneratedBy(
            result, activity, identifier=generation, other_attributes=[(PROV["role"], HPC["result"])]
        )
    if defect == "usage_type":
        document.entity(usage)
    else:
        document.used(activity, source, identifier=usage, other_attributes=[(PROV["role"], HPC["source"])])
    derivation = identifiers.derivation(activity_id, "result", "source") if campaign_ids else example["derivation"]
    document.wasDerivedFrom(
        other if defect == "generated_entity" else result,
        other if defect == "used_entity" else source,
        other_activity if defect == "activity" else activity,
        generation,
        usage,
        identifier=derivation,
    )
    return document


@pytest.mark.parametrize("defect", ["generated_entity", "used_entity", "activity"])
@pytest.mark.parametrize("campaign_ids", [False, True])
def test_qualified_derivation_must_match_actual_relationship_endpoints(tmp_path: Path, defect, campaign_ids):
    manager = _new_manager(tmp_path)
    document = _qualified_derivation_document(manager, defect, campaign_ids)
    document_id = manager.add_prov_document(document, name="inconsistent", activate=False)
    before = _document_state(manager, "inconsistent")

    with pytest.raises(ProvenanceValidationError, match="Derivation (Entity|Activity) disagrees"):
        manager.set_prov_document_active(document_id)
    assert _document_state(manager, "inconsistent") == before
    manager.close()


@pytest.mark.parametrize("defect", ["generation_type", "usage_type"])
def test_qualified_derivation_rejects_references_to_wrong_record_types(tmp_path: Path, defect):
    manager = _new_manager(tmp_path)
    document = _qualified_derivation_document(manager, defect)
    with pytest.raises(ProvenanceValidationError, match="must identify a Prov(Generation|Usage)"):
        manager.add_prov_document(document, name="wrong-record-type", activate=True)
    assert "wrong-record-type" not in {item.name for item in manager.prov_documents()}
    manager.close()


@pytest.mark.parametrize("campaign_ids", [False, True])
def test_qualified_derivation_can_resolve_relationships_in_another_active_document(tmp_path: Path, campaign_ids):
    manager = _new_manager(tmp_path)
    complete = _qualified_derivation_document(manager, campaign_ids=campaign_ids)
    support = ProvDocument()
    derivation_only = ProvDocument()
    for namespace in complete.get_registered_namespaces():
        support.add_namespace(namespace)
        derivation_only.add_namespace(namespace)
    for record in complete.get_records():
        (derivation_only if isinstance(record, ProvDerivation) else support).add_record(record)
    manager.add_prov_document(support, name="support", activate=True)
    imported_id = manager.add_prov_document(derivation_only, name="derivation", activate=True)

    assert manager.prov_document(imported_id) == derivation_only
    manager.close()


def test_qualified_import_allows_unspecified_generation_activity(tmp_path: Path):
    manager = _new_manager(tmp_path)
    example = Namespace("example", "urn:partial:")
    document = ProvDocument()
    document.add_namespace(example)
    document.entity(example["source"])
    document.entity(example["result"])
    document.activity(example["activity"])
    document.wasGeneratedBy(example["result"], identifier=example["generation"])
    document.used(example["activity"], example["source"], identifier=example["usage"])
    document.wasDerivedFrom(
        example["result"],
        example["source"],
        example["activity"],
        example["generation"],
        example["usage"],
    )

    document_id = manager.add_prov_document(document, name="partial-qualification", activate=True)

    assert manager.prov_document(document_id) == document
    manager.close()
