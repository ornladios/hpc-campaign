import hashlib
import json
import sqlite3
import uuid
from pathlib import Path

import pytest
from prov.model import PROV, Namespace, ProvDocument

from hpc_campaign.manager import Manager
from hpc_campaign.prov_store import (
    ProvenanceConflictError,
    ProvenanceCorruptionError,
    ProvenanceStorageError,
    ProvStore,
    canonical_prov_json,
)


def _example_document(label: str = "pressure") -> ProvDocument:
    """Build a small document whose complete contents are easy to compare."""

    document = ProvDocument()
    example = Namespace("example", "urn:example:")
    document.add_namespace(example)
    document.entity(
        example["variable"],
        [(PROV["type"], example["LogicalVariable"]), (PROV["label"], label)],
    )
    return document


def _new_manager(tmp_path: Path, name: str = "campaign.aca") -> Manager:
    manager = Manager(archive=name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    return manager


def test_new_campaign_identity_survives_reopen_and_file_rename(tmp_path: Path):
    # The campaign UUID belongs to the campaign, not its current path or a
    # SQLite row number. Moving the complete ACA must therefore preserve it.
    manager = _new_manager(tmp_path)
    original_id = manager.campaign_uuid()
    manager.close()

    original_path = tmp_path / "campaign.aca"
    renamed_path = tmp_path / "renamed.aca"
    original_path.rename(renamed_path)

    reopened = Manager(archive=renamed_path.name, campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.campaign_uuid() == original_id
    reopened.close()


def test_canonical_prov_json_is_deterministic_and_round_trips():
    document = _example_document()

    first = canonical_prov_json(document)
    second = canonical_prov_json(document)

    # Compact sorted JSON gives the ACA a stable byte representation and hash
    # even though W3C PROV-JSON itself does not define canonical JSON bytes.
    assert first == second
    assert first == json.dumps(
        json.loads(first),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert ProvDocument.deserialize(content=first, format="json") == document


def test_document_persists_across_close_reopen_and_exports_exact_text(tmp_path: Path):
    manager = _new_manager(tmp_path)
    document = _example_document()
    document_id = manager.add_prov_document(document, name="imported", activate=False)
    stored_info = manager.prov_documents()
    manager.close()

    reopened = Manager(archive="campaign.aca", campaign_store=str(tmp_path))
    reopened.open()
    assert reopened.prov_document(document_id) == document
    assert reopened.prov_document("imported") == document
    assert len(stored_info) == 1
    assert stored_info[0].document_id == document_id
    assert stored_info[0].sha256 == hashlib.sha256(canonical_prov_json(document).encode("utf-8")).hexdigest()

    export_path = tmp_path / "exported-prov.json"
    reopened.export_prov(document_id, export_path)
    assert export_path.read_text(encoding="utf-8") == canonical_prov_json(document)
    reopened.close()


def test_document_listing_filters_active_without_loading_content(tmp_path: Path):
    manager = _new_manager(tmp_path)
    active_id = manager.add_prov_document(_example_document("active"), name="active", activate=True)
    inactive_id = manager.add_prov_document(_example_document("inactive"), name="inactive", activate=False)

    assert [item.document_id for item in manager.prov_documents(active=True)] == [active_id]
    assert [item.document_id for item in manager.prov_documents(active=False)] == [inactive_id]
    assert {item.document_id for item in manager.prov_documents()} == {active_id, inactive_id}
    manager.close()


def test_authored_document_is_created_once_with_campaign_namespaces(tmp_path: Path):
    manager = _new_manager(tmp_path)
    store = ProvStore(manager.con)

    first = store.ensure_authored_document()
    second = store.ensure_authored_document()
    document = store.document(first.document_id)

    # Later scientific authoring methods can call this idempotently without
    # creating one canonical document per operation.
    assert first == second
    assert first.active
    assert first.name == "campaign-provenance"
    assert len(store.documents()) == 1
    namespaces = {namespace.prefix: namespace for namespace in document.get_registered_namespaces()}
    assert namespaces["hpc"].uri == "urn:hpc-campaign:vocabulary:"
    assert namespaces["hpcid"].uri == f"urn:hpc-campaign:{manager.campaign_uuid()}:"
    manager.close()


def test_hash_mismatch_is_reported_as_corruption(tmp_path: Path):
    manager = _new_manager(tmp_path)
    document_id = manager.add_prov_document(_example_document(), name="tampered")
    manager.con.execute(
        "UPDATE provenance_document SET content = ? WHERE uuid = ?",
        ('{"tampered":true}', str(document_id)),
    )
    manager.con.commit()

    # The reader verifies bytes before asking `prov` to parse them, so an
    # accidental or external SQL edit cannot masquerade as canonical content.
    with pytest.raises(ProvenanceCorruptionError, match="hash mismatch"):
        manager.prov_document(document_id)
    manager.close()


def test_unreadable_json_with_matching_hash_is_reported_as_corruption(tmp_path: Path):
    manager = _new_manager(tmp_path)
    document_id = manager.add_prov_document(_example_document(), name="invalid-json")
    content = "not JSON"
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    manager.con.execute(
        "UPDATE provenance_document SET content = ?, sha256 = ? WHERE uuid = ?",
        (content, digest, str(document_id)),
    )
    manager.con.commit()

    # A matching database hash proves only byte integrity; the content must
    # independently remain readable by the pinned `prov` package.
    with pytest.raises(ProvenanceCorruptionError, match="not readable PROV-JSON"):
        manager.prov_document(document_id)
    manager.close()


def test_replace_document_uses_optimistic_hash_and_rolls_back_conflict(tmp_path: Path):
    manager = _new_manager(tmp_path)
    store = ProvStore(manager.con)
    original = store.add_document(_example_document("original"), name="authored", active=True)

    updated_document = _example_document("updated")
    updated = store.replace_document(
        original.document_id,
        updated_document,
        expected_sha256=original.sha256,
    )
    text_after_success = manager.con.execute(
        "SELECT content FROM provenance_document WHERE uuid = ?",
        (str(original.document_id),),
    ).fetchone()[0]

    # A stale manager must not overwrite the first successful writer. The
    # failed UPDATE is rolled back and leaves the exact canonical text intact.
    with pytest.raises(ProvenanceConflictError, match="changed since it was loaded"):
        store.replace_document(
            original.document_id,
            _example_document("stale writer"),
            expected_sha256=original.sha256,
        )
    text_after_conflict = manager.con.execute(
        "SELECT content FROM provenance_document WHERE uuid = ?",
        (str(original.document_id),),
    ).fetchone()[0]

    assert updated.sha256 != original.sha256
    assert text_after_conflict == text_after_success
    assert store.document(original.document_id) == updated_document
    manager.close()


def test_adding_provenance_to_0_7_preserves_existing_dataset_uuid(tmp_path: Path):
    manager = _new_manager(tmp_path, "upgrade.aca")
    dataset_id = uuid.uuid4()
    manager.con.execute(
        """
        INSERT INTO dataset (name, uuid, modtime, deltime, fileformat, tsid, tsorder)
        VALUES (?, ?, 1, 0, 'ADIOS', NULL, NULL)
        """,
        ("output.bp", str(dataset_id)),
    )

    # Build a representative legacy 0.7 ACA from the current test database.
    # The normal upgrade API adds provenance storage without changing the ACA
    # version that ADIOS CampaignReader supports.
    manager.con.execute("DROP TABLE provenance_document")
    manager.con.execute("DROP TABLE campaign_identity")
    manager.con.execute("UPDATE info SET version = '0.7' WHERE id = 'ACA'")
    manager.con.commit()

    with pytest.raises(ProvenanceStorageError, match="upgrade this ACA"):
        manager.campaign_uuid()
    with pytest.raises(ProvenanceStorageError, match="upgrade this ACA"):
        manager.prov_document(uuid.uuid4())

    assert manager.upgrade() == "0.7"
    assert isinstance(manager.campaign_uuid(), uuid.UUID)
    stored_dataset = manager.con.execute(
        "SELECT uuid, name, fileformat FROM dataset WHERE name = 'output.bp'"
    ).fetchone()
    stored_version = manager.con.execute("SELECT version FROM info WHERE id = 'ACA'").fetchone()[0]

    assert tuple(stored_dataset) == (str(dataset_id), "output.bp", "ADIOS")
    assert stored_version == "0.7"
    assert manager.prov_documents() == []
    manager.close()
