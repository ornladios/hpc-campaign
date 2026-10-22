import json
import uuid
from pathlib import Path

import pytest
from PIL import Image
from prov.model import PROV, ProvDerivation, ProvDocument, ProvEntity, ProvGeneration, ProvMembership

from hpc_campaign import Manager
from hpc_campaign.prov_mapping import HPC, CampaignProvIds
from hpc_campaign.prov_validation import CampaignProvValidator, ProvenanceValidationError
from hpc_campaign.provenance import CampaignProvenance


def _manager_with_source(tmp_path: Path):
    manager = Manager("products.aca", campaign_store=str(tmp_path))
    manager.open(create=True)
    payload = tmp_path / "source.txt"
    payload.write_text("numerical placeholder\n")
    manager.text(payload, name="source", store=True)
    source = manager.add_variable(dataset="source", variable="P", definition="pressure")
    manager.add_activity("simulation", outputs={"pressure": source})
    return manager, source


def _image(manager: Manager, tmp_path: Path, name: str):
    path = tmp_path / f"{name}.png"
    Image.new("RGB", (4, 4), color="blue").save(path)
    manager.image(path, name=name, store=True)
    return manager.add_dataset_entity(name)


def _document(manager: Manager):
    return manager.prov_document("campaign-provenance")


def _stored_json(manager: Manager):
    return manager.con.execute("SELECT content FROM provenance_document WHERE name = 'campaign-provenance'").fetchone()[
        0
    ]


def test_image_uses_dataset_identity_and_persists_qualified_ancestry(tmp_path: Path):
    manager, source = _manager_with_source(tmp_path)
    image = _image(manager, tmp_path, "pressure")
    dataset_uuid = manager.con.execute("SELECT uuid FROM dataset WHERE name = 'pressure'").fetchone()[0]
    assert image == CampaignProvIds(manager.campaign_uuid()).dataset(uuid.UUID(dataset_uuid))
    assert manager.dataset_entity("pressure") == image
    result = manager.add_activity("visualization", inputs={"color_by": source}, outputs={"image": image})
    document = _document(manager)
    record = document.get_record(image)[0]
    assert record.get_asserted_types() == {HPC["Dataset"], HPC["Image"]}
    derivation = next(document.get_records(ProvDerivation))
    assert derivation.args[:3] == (image, source, result.activity)
    assert document.get_record(derivation.args[3])[0].args[:2] == (image, result.activity)
    assert document.get_record(derivation.args[4])[0].args[:2] == (result.activity, source)
    export = tmp_path / "products.json"
    manager.export_prov("campaign-provenance", export)
    assert ProvDocument.deserialize(source=str(export), format="json") == document
    manager.close()

    reopened = Manager("products.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.dataset_entity("pressure") == image
    assert _document(reopened) == document
    reopened.close()


def test_video_can_be_generated_from_an_image_product(tmp_path: Path):
    manager, source = _manager_with_source(tmp_path)
    image = _image(manager, tmp_path, "frame")
    manager.add_activity("visualization", inputs={"color_by": source}, outputs={"image": image})
    movie = tmp_path / "pressure.mp4"
    # Provenance does not decode video; explicit timing avoids probing media.
    movie.write_bytes(b"opaque video placeholder")
    manager.video(
        movie,
        name="movie",
        variables=["P"],
        source_dataset="source",
        vis_type="movie",
        frame_count=2,
        time_range=(0.0, 1.0),
        store=True,
    )
    video = manager.add_dataset_entity("movie")
    manager.add_activity("visualization", inputs={"frame": image}, outputs={"video": video})
    document = _document(manager)
    assert document.get_record(video)[0].get_asserted_types() == {HPC["Dataset"], HPC["Video"]}
    assert {(relation.args[0], relation.args[1]) for relation in document.get_records(ProvDerivation)} == {
        (image, source),
        (video, image),
    }
    collection = manager.add_visualization_sequence_entity("movie")
    assert HPC["VideoSequence"] in _document(manager).get_record(collection)[0].get_asserted_types()
    manager.close()


def test_invalid_product_endpoints_and_second_producer_preserve_document(tmp_path: Path):
    manager, source = _manager_with_source(tmp_path)
    image = _image(manager, tmp_path, "pressure")
    manager.add_activity("visualization", inputs={"color_by": source}, outputs={"image": image})
    plan = manager.add_plan("rendering recipe")
    before = _stored_json(manager)
    with pytest.raises(ValueError, match="already has a producing Activity"):
        manager.add_activity("visualization", inputs={"source": source}, outputs={"image": image})
    assert _stored_json(manager) == before
    for inputs, outputs in (({"recipe": plan}, {"image": image}), ({"source": source}, {"plan": plan})):
        with pytest.raises(ValueError, match="data product Entity"):
            manager.add_activity("visualization", inputs=inputs, outputs=outputs)
        assert _stored_json(manager) == before
    manager.close()


@pytest.mark.parametrize("media_type", ["Image", "Video"])
def test_active_import_rejects_media_type_that_disagrees_with_live_dataset(tmp_path: Path, media_type):
    manager, _source = _manager_with_source(tmp_path)
    payload = tmp_path / "other.txt"
    payload.write_text("not an image or video\n")
    manager.text(payload, name="other", store=True)
    dataset_uuid = manager.con.execute("SELECT uuid FROM dataset WHERE name = 'other'").fetchone()[0]
    identifiers = CampaignProvIds(manager.campaign_uuid())
    invalid = ProvDocument()
    invalid.add_namespace(identifiers.namespace)
    invalid.add_namespace(HPC)
    invalid.entity(
        identifiers.dataset(uuid.UUID(dataset_uuid)),
        [
            (PROV["type"], HPC["Dataset"]),
            (PROV["type"], HPC[media_type]),
            (HPC["datasetUuid"], dataset_uuid),
            (HPC["format"], "TEXT"),
            (PROV["location"], str(payload)),
        ],
    )
    before = _stored_json(manager)
    with pytest.raises(ProvenanceValidationError, match="media type"):
        manager.add_prov_document(invalid, name="invalid-media", activate=True)
    assert _stored_json(manager) == before
    manager.close()


def test_sequence_snapshots_keep_order_membership_and_prior_producer(tmp_path: Path):
    manager, source = _manager_with_source(tmp_path)
    first = _image(manager, tmp_path, "first")
    second = _image(manager, tmp_path, "second")

    def register(names, replace=False):
        manager.visualization_sequence(
            name="frames", vis_type="heatmap", variables=["P"], source_dataset="source", items=names, replace=replace
        )

    register(["second", "first", "second"])
    sequence = manager.add_visualization_sequence_entity("frames")
    result = manager.add_activity("visualization", inputs={"color_by": source}, outputs={"sequence": sequence})
    document = _document(manager)
    entity = document.get_record(sequence)[0]
    assert {PROV["Collection"], HPC["VisualizationSequence"], HPC["ImageSequence"]}.issubset(
        entity.get_asserted_types()
    )
    assert json.loads(next(iter(entity.get_attribute(HPC["memberOrder"])))) == [
        str(uuid.UUID(second.localpart.removeprefix("dataset_"))),
        str(uuid.UUID(first.localpart.removeprefix("dataset_"))),
        str(uuid.UUID(second.localpart.removeprefix("dataset_"))),
    ]
    assert {relation.args[1] for relation in document.get_records(ProvMembership)} == {first, second}
    assert manager.visualization_sequence_entity("frames") == sequence
    register(["first", "second"], replace=True)
    before = _stored_json(manager)
    with pytest.raises(LookupError, match="snapshot not registered"):
        manager.visualization_sequence_entity("frames")
    assert _stored_json(manager) == before
    updated = manager.add_visualization_sequence_entity("frames")
    assert updated != sequence
    assert manager.visualization_sequence_entity("frames") == updated
    manager.close()

    reopened = Manager("products.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.visualization_sequence_entity("frames") == updated
    document = _document(reopened)
    assert document.get_record(sequence)[0] == entity
    assert any(relation.args[:2] == (sequence, result.activity) for relation in document.get_records(ProvGeneration))
    reopened.close()


def test_active_import_rejects_sequence_order_inconsistent_with_membership(tmp_path: Path):
    manager, _source = _manager_with_source(tmp_path)
    _image(manager, tmp_path, "first")
    manager.visualization_sequence(
        name="frames", vis_type="heatmap", variables=["P"], source_dataset="source", items=["first"]
    )
    sequence = manager.add_visualization_sequence_entity("frames")
    invalid = _document(manager)
    record = invalid.get_record(sequence)[0]
    # Build a second snapshot to test import validation without modifying a
    # package record through private attributes.
    other = CampaignProvIds(manager.campaign_uuid()).entity(uuid.uuid4())
    invalid.entity(
        other,
        [
            (attribute, json.dumps([str(uuid.uuid4())]) if attribute == HPC["memberOrder"] else value)
            for attribute, value in record.attributes
        ],
    )
    with pytest.raises(ProvenanceValidationError, match="membership and order disagree"):
        manager.add_prov_document(invalid, name="invalid-sequence", activate=False)
        manager.set_prov_document_active("invalid-sequence", active=True)
    manager.close()


def _catalog_with_sequence(tmp_path: Path):
    """Register physical products without creating provenance declarations."""

    manager = Manager("products.aca", campaign_store=str(tmp_path))
    manager.open(create=True)
    source = tmp_path / "source.txt"
    source.write_text("numerical placeholder\n")
    manager.text(source, name="source", store=True)
    for name in ("first", "second"):
        path = tmp_path / f"{name}.png"
        Image.new("RGB", (4, 4), color="blue").save(path)
        manager.image(path, name=name, store=True)
    manager.visualization_sequence(
        name="frames", vis_type="heatmap", variables=["P"], source_dataset="source", items=["first", "second"]
    )
    return manager


def _provenance_state(manager: Manager):
    return tuple(tuple(row) for row in manager.con.execute("SELECT * FROM provenance_document ORDER BY name"))


@pytest.mark.parametrize("operation", ["variable", "activity"])
def test_failed_first_authoring_call_does_not_create_provenance(tmp_path: Path, operation):
    manager = _catalog_with_sequence(tmp_path)
    before = _provenance_state(manager)
    changes = manager.con.total_changes
    try:
        with pytest.raises(LookupError):
            if operation == "variable":
                manager.add_variable(dataset="missing", variable="P", definition="pressure")
            else:
                missing = CampaignProvIds(manager.campaign_uuid()).variable(uuid.uuid4())
                manager.add_activity("simulation", outputs={"pressure": missing})
        assert _provenance_state(manager) == before == ()
        assert manager.con.total_changes == changes
    finally:
        manager.close()


@pytest.mark.parametrize("operation", ["agent", "plan", "variable", "dataset", "sequence"])
def test_first_document_validation_failure_leaves_no_empty_document(tmp_path: Path, monkeypatch, operation):
    manager = _catalog_with_sequence(tmp_path)
    changes = manager.con.total_changes

    def reject_nonempty_document(_self, documents):
        if any(tuple(item.document.get_records()) for item in documents):
            raise ProvenanceValidationError("injected initial validation failure")

    monkeypatch.setattr(CampaignProvValidator, "validate", reject_nonempty_document)
    operations = {
        "agent": lambda: manager.add_agent("software", "solver"),
        "plan": lambda: manager.add_plan("recipe"),
        "variable": lambda: manager.add_variable(dataset="source", variable="P", definition="pressure"),
        "dataset": lambda: manager.add_dataset_entity("first"),
        "sequence": lambda: manager.add_visualization_sequence_entity("frames"),
    }
    try:
        with pytest.raises(ProvenanceValidationError, match="injected initial validation failure"):
            operations[operation]()
        assert _provenance_state(manager) == ()
        assert manager.con.total_changes == changes
    finally:
        manager.close()


def test_unregistered_entity_lookups_do_not_create_provenance(tmp_path: Path):
    manager = _catalog_with_sequence(tmp_path)
    assert manager.prov_documents() == []
    changes = manager.con.total_changes
    manager.con.execute("PRAGMA query_only = ON")

    with pytest.raises(LookupError, match="use add_dataset_entity"):
        manager.dataset_entity("first")
    with pytest.raises(LookupError, match="use add_visualization_sequence_entity"):
        manager.visualization_sequence_entity("frames")

    assert _provenance_state(manager) == ()
    assert manager.con.total_changes == changes
    manager.close()


def test_entity_lookups_and_unchanged_registration_perform_no_writes(tmp_path: Path, monkeypatch):
    manager = _catalog_with_sequence(tmp_path)
    image = manager.add_dataset_entity("first")
    sequence = manager.add_visualization_sequence_entity("frames")
    before = _provenance_state(manager)
    changes = manager.con.total_changes
    statements = []
    manager.con.execute("PRAGMA query_only = ON")
    manager.con.set_trace_callback(statements.append)

    def unexpected_serialization(*_args, **_kwargs):
        pytest.fail("an unchanged registration or lookup must not serialize a document")

    monkeypatch.setattr("hpc_campaign.prov_store.canonical_prov_json", unexpected_serialization)
    for _ in range(2):
        assert manager.dataset_entity("first") == image
        assert manager.visualization_sequence_entity("frames") == sequence
        assert manager.add_dataset_entity("first") == image
        assert manager.add_visualization_sequence_entity("frames") == sequence

    manager.con.set_trace_callback(None)
    assert manager.con.total_changes == changes
    assert _provenance_state(manager) == before
    assert not any(sql.strip().upper().startswith(("BEGIN", "INSERT", "UPDATE", "DELETE")) for sql in statements)
    manager.close()


def test_dataset_registration_refreshes_locations_but_lookup_does_not(tmp_path: Path):
    manager, _source = _manager_with_source(tmp_path)
    image = _image(manager, tmp_path, "pressure")
    before = _provenance_state(manager)
    before_info = manager.prov_documents()[0]
    manager.con.execute(
        "INSERT INTO replica (datasetid, hostid, dirid, archiveid, name, modtime, deltime, keyid, size) "
        "SELECT datasetid, hostid, dirid, archiveid, 'additional.png', modtime, deltime, keyid, size "
        "FROM replica WHERE datasetid = (SELECT rowid FROM dataset WHERE name = 'pressure') LIMIT 1"
    )
    manager.con.commit()
    manager.con.execute("PRAGMA query_only = ON")

    assert manager.dataset_entity("pressure") == image
    assert _provenance_state(manager) == before

    manager.con.execute("PRAGMA query_only = OFF")
    assert manager.add_dataset_entity("pressure") == image
    after = manager.prov_documents()[0]
    assert after.sha256 != before_info.sha256
    assert after.modified_ns > before_info.modified_ns
    records = _document(manager).get_record(image)
    assert len(records) == 1
    assert len(records[0].get_attribute(PROV["location"])) == 2
    refreshed = _provenance_state(manager)
    assert manager.add_dataset_entity("pressure") == image
    assert _provenance_state(manager) == refreshed
    manager.close()


@pytest.mark.parametrize("method", ["dataset_entity", "visualization_sequence_entity"])
def test_entity_lookup_does_not_create_a_missing_archive(tmp_path: Path, method):
    manager = Manager("missing.aca", campaign_store=str(tmp_path))

    with pytest.raises(FileNotFoundError):
        getattr(manager, method)("missing")

    assert not (tmp_path / "missing.aca").exists()


@pytest.mark.parametrize("active", [False, True])
def test_entity_lookups_use_only_active_imports_without_creating_authored_storage(tmp_path: Path, active):
    manager = _catalog_with_sequence(tmp_path)
    datasets = [CampaignProvenance(manager.con)._resolve_dataset(name) for name in ("first", "second")]
    identifiers = CampaignProvIds(manager.campaign_uuid())
    reference = identifiers.dataset(datasets[0].dataset_uuid)
    imported = ProvDocument()
    imported.add_namespace(HPC)
    imported.add_namespace(identifiers.namespace)
    for dataset in datasets:
        imported.entity(
            identifiers.dataset(dataset.dataset_uuid),
            [
                (PROV["type"], HPC["Dataset"]),
                (PROV["type"], HPC["Image"]),
                (PROV["label"], dataset.name),
                (HPC["datasetUuid"], str(dataset.dataset_uuid)),
                (HPC["format"], "IMAGE"),
                *((PROV["location"], location) for location in dataset.locations),
            ],
        )
    sequence = identifiers.entity(uuid.uuid4())
    imported.entity(
        sequence,
        [
            (PROV["type"], PROV["Collection"]),
            (PROV["type"], HPC["VisualizationSequence"]),
            (PROV["type"], HPC["ImageSequence"]),
            (HPC["sequenceName"], "frames"),
            (
                HPC["memberOrder"],
                json.dumps([str(dataset.dataset_uuid) for dataset in datasets], separators=(",", ":")),
            ),
        ],
    )
    for dataset in datasets:
        imported.hadMember(sequence, identifiers.dataset(dataset.dataset_uuid))
    manager.add_prov_document(imported, name="imported", activate=active)
    before = _provenance_state(manager)
    manager.con.execute("PRAGMA query_only = ON")

    if active:
        assert manager.dataset_entity("first") == reference
        assert manager.add_dataset_entity("first") == reference
        assert manager.visualization_sequence_entity("frames") == sequence
        assert manager.add_visualization_sequence_entity("frames") == sequence
    else:
        with pytest.raises(LookupError, match="not registered"):
            manager.dataset_entity("first")
        with pytest.raises(LookupError, match="snapshot not registered"):
            manager.visualization_sequence_entity("frames")
    assert _provenance_state(manager) == before
    assert [item.name for item in manager.prov_documents()] == ["imported"]
    manager.close()
