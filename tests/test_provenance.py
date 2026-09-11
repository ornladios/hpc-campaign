import uuid
from pathlib import Path

import pytest
from prov.model import (
    PROV,
    ProvActivity,
    ProvAgent,
    ProvAssociation,
    ProvDocument,
    ProvEntity,
    ProvGeneration,
)

from hpc_campaign.manager import Manager
from hpc_campaign.prov_mapping import HPC, CampaignProvIds
from hpc_campaign.prov_store import ProvStore


def _new_manager(tmp_path: Path, name: str = "campaign.aca") -> Manager:
    manager = Manager(archive=name, campaign_store=str(tmp_path))
    manager.open(create=True, truncate=True)
    return manager


def _add_text_dataset(manager: Manager, tmp_path: Path, name: str = "output") -> str:
    source = tmp_path / f"{name}.txt"
    source.write_text(f"payload for {name}\n", encoding="utf-8")
    manager.text(source, name=name, store=True)
    return str(manager.con.execute("SELECT uuid FROM dataset WHERE name = ?", (name,)).fetchone()[0])


def _active_document(manager: Manager) -> ProvDocument:
    active = manager.prov_documents(active=True)
    assert len(active) == 1
    return manager.prov_document(active[0].document_id)


def _stored_text(manager: Manager) -> str:
    row = manager.con.execute("SELECT content FROM provenance_document WHERE name = 'campaign-provenance'").fetchone()
    assert row is not None
    return str(row[0])


def _one_record(document: ProvDocument, reference):
    records = document.get_record(reference)
    assert len(records) == 1
    return records[0]


def test_agent_plan_run_and_variable_persist_as_standard_prov(tmp_path: Path):
    manager = _new_manager(tmp_path)
    dataset_uuid = uuid.UUID(_add_text_dataset(manager, tmp_path))

    agent = manager.add_agent("software", "XGC", version="1.2.3")
    plan = manager.add_plan("run configuration", location="runs/run-001/input.json")
    run = manager.add_run("run-001", agent=agent, plan=plan)
    pressure = manager.add_variable(
        run=run,
        dataset="output",
        variable="P",
        definition="pressure",
        units="Pa",
        coordinate_system="boozer",
    )
    campaign_id = manager.campaign_uuid()
    document_before_close = _active_document(manager)
    manager.close()

    # Close/reopen verifies that the public helpers changed canonical storage,
    # rather than returning references to an in-memory-only graph.
    reopened = Manager(archive="campaign.aca", campaign_store=str(tmp_path))
    reopened.open()
    document = _active_document(reopened)
    identifiers = CampaignProvIds(campaign_id)
    dataset = identifiers.dataset(dataset_uuid)

    assert document == document_before_close
    assert isinstance(_one_record(document, agent), ProvAgent)
    assert PROV["SoftwareAgent"] in _one_record(document, agent).get_asserted_types()
    assert _one_record(document, agent).get_attribute(HPC["version"]) == {"1.2.3"}
    assert isinstance(_one_record(document, plan), ProvEntity)
    assert PROV["Plan"] in _one_record(document, plan).get_asserted_types()
    assert isinstance(_one_record(document, run), ProvActivity)
    assert HPC["SimulationRun"] in _one_record(document, run).get_asserted_types()
    assert HPC["Dataset"] in _one_record(document, dataset).get_asserted_types()
    assert HPC["LogicalVariable"] in _one_record(document, pressure).get_asserted_types()

    associations = [record for record in document.get_records(ProvAssociation) if record.args[0] == run]
    assert len(associations) == 1
    assert associations[0].args[1:] == (agent, plan)

    generations = [record for record in document.get_records(ProvGeneration) if record.args[0] == pressure]
    assert len(generations) == 1
    assert generations[0].args[1] == run
    assert generations[0].get_attribute(PROV["role"]) == {HPC["pressure"]}
    reopened.close()


def test_dataset_entity_is_idempotent_and_collects_live_replica_locations(tmp_path: Path):
    manager = _new_manager(tmp_path)
    dataset_uuid = uuid.UUID(_add_text_dataset(manager, tmp_path))
    dataset_rowid = int(manager.con.execute("SELECT rowid FROM dataset WHERE name = 'output'").fetchone()[0])

    # Add a second live replica with an explicit protocol. Dataset registration
    # must preserve both locations without making a second Entity assertion.
    remote_host = manager.con.execute(
        """
        INSERT INTO host (hostname, longhostname, modtime, deltime, default_protocol)
        VALUES ('remote', 'remote.example', 1, 0, 'ssh')
        RETURNING rowid
        """
    ).fetchone()[0]
    remote_directory = manager.con.execute(
        """
        INSERT INTO directory (hostid, name, modtime, deltime)
        VALUES (?, '/data', 1, 0)
        RETURNING rowid
        """,
        (remote_host,),
    ).fetchone()[0]
    manager.con.execute(
        """
        INSERT INTO replica
            (datasetid, hostid, dirid, archiveid, name, modtime, deltime, keyid, size)
        VALUES (?, ?, ?, 0, 'remote-output', 1, 0, 0, 1)
        """,
        (dataset_rowid, remote_host, remote_directory),
    )
    manager.con.commit()

    run = manager.add_run("run-001")
    manager.add_variable(run=run, dataset="output", variable="P", definition="pressure")
    manager.add_variable(
        run=run,
        dataset="output",
        variable="T",
        definition="temperature",
        generated_by_run=False,
    )

    document = _active_document(manager)
    dataset_reference = CampaignProvIds(manager.campaign_uuid()).dataset(dataset_uuid)
    dataset_records = document.get_record(dataset_reference)

    assert len(dataset_records) == 1
    locations = dataset_records[0].get_attribute(PROV["location"])
    assert len(locations) == 2
    assert "ssh://remote.example/data/remote-output" in locations
    assert dataset_records[0].get_attribute(HPC["datasetUuid"]) == {str(dataset_uuid)}
    manager.close()


def test_stable_logical_variables_have_no_revisions_and_optional_generation(tmp_path: Path):
    manager = _new_manager(tmp_path)
    _add_text_dataset(manager, tmp_path)
    run = manager.add_run("run-001")

    pressure = manager.add_variable(
        run=run,
        dataset="output",
        variable="P",
        definition="pressure",
        units=" normalized pressure ",
        coordinate_system=" boozer ",
    )
    pressure_alias = manager.add_variable(
        run=run,
        dataset="output",
        variable="pressure_alias",
        definition="pressure",
        generated_by_run=False,
    )
    document = _active_document(manager)
    pressure_record = _one_record(document, pressure)

    # Opaque scientific strings are preserved exactly, and neither the entity
    # identity nor its attributes introduce the deferred revision model.
    assert "_r" not in pressure.localpart
    assert pressure_record.get_attribute(HPC["revision"]) == set()
    assert pressure_record.get_attribute(HPC["units"]) == {" normalized pressure "}
    assert pressure_record.get_attribute(HPC["coordinateSystem"]) == {" boozer "}
    assert any(record.args[0] == pressure for record in document.get_records(ProvGeneration))
    assert all(record.args[0] != pressure_alias for record in document.get_records(ProvGeneration))
    manager.close()


def test_run_names_are_unique_and_failed_mutation_preserves_exact_json(tmp_path: Path):
    manager = _new_manager(tmp_path)
    manager.add_run("run-001")
    before = _stored_text(manager)

    with pytest.raises(ValueError, match="run name already exists"):
        manager.add_run("run-001")

    assert _stored_text(manager) == before
    manager.close()


def test_invalid_dataset_cases_leave_canonical_document_unchanged(tmp_path: Path):
    manager = _new_manager(tmp_path)
    run = manager.add_run("run-001")
    before = _stored_text(manager)

    with pytest.raises(LookupError, match="dataset not found or deleted"):
        manager.add_variable(run=run, dataset="missing", variable="P", definition="pressure")
    assert _stored_text(manager) == before

    manager.con.execute(
        """
        INSERT INTO dataset (name, uuid, modtime, deltime, fileformat, tsid, tsorder)
        VALUES ('locationless', ?, 1, 0, 'TEXT', 0, 0)
        """,
        (str(uuid.uuid4()),),
    )
    manager.con.commit()
    with pytest.raises(ValueError, match="no live replica location"):
        manager.add_variable(run=run, dataset="locationless", variable="P", definition="pressure")
    assert _stored_text(manager) == before

    _add_text_dataset(manager, tmp_path, "deleted")
    manager.con.execute("UPDATE dataset SET deltime = 1 WHERE name = 'deleted'")
    manager.con.commit()
    with pytest.raises(LookupError, match="dataset not found or deleted"):
        manager.add_variable(run=run, dataset="deleted", variable="P", definition="pressure")
    assert _stored_text(manager) == before

    _add_text_dataset(manager, tmp_path, "invalid-uuid")
    manager.con.execute("UPDATE dataset SET uuid = 'not-a-uuid' WHERE name = 'invalid-uuid'")
    manager.con.commit()
    with pytest.raises(ValueError, match="dataset has an invalid UUID"):
        manager.add_variable(run=run, dataset="invalid-uuid", variable="P", definition="pressure")
    assert _stored_text(manager) == before
    manager.close()


def test_conflicting_existing_dataset_entity_is_rejected_without_rewrite(tmp_path: Path):
    manager = _new_manager(tmp_path)
    dataset_uuid = uuid.UUID(_add_text_dataset(manager, tmp_path))
    run = manager.add_run("run-001")
    store = ProvStore(manager.con)
    info = store.ensure_authored_document()
    document = store.document(info.document_id)
    identifiers = CampaignProvIds(manager.campaign_uuid())

    # Simulate an imported assertion that reused the campaign dataset UUID but
    # assigned a contradictory name. The convenience API must not silently
    # overwrite identity-bearing attributes to make the record fit.
    document.entity(
        identifiers.dataset(dataset_uuid),
        [
            (PROV["type"], HPC["Dataset"]),
            (PROV["label"], "different-name"),
            (HPC["datasetUuid"], str(dataset_uuid)),
            (HPC["format"], "TEXT"),
            (PROV["location"], "conflicting-location"),
        ],
    )
    store.replace_document(info.document_id, document, expected_sha256=info.sha256)
    before = _stored_text(manager)

    with pytest.raises(ValueError, match="conflicting dataset name"):
        manager.add_variable(run=run, dataset="output", variable="P", definition="pressure")

    assert _stored_text(manager) == before
    manager.close()


def test_cross_campaign_agent_and_plan_references_are_rejected(tmp_path: Path):
    first = _new_manager(tmp_path, "first.aca")
    second = _new_manager(tmp_path, "second.aca")
    foreign_agent = second.add_agent("software", "foreign")
    foreign_plan = second.add_plan("foreign plan")

    first.add_run("baseline")
    before = _stored_text(first)
    with pytest.raises(ValueError, match="belongs to another campaign"):
        first.add_run("bad-agent", agent=foreign_agent)
    assert _stored_text(first) == before
    with pytest.raises(ValueError, match="belongs to another campaign"):
        first.add_run("bad-plan", plan=foreign_plan)
    assert _stored_text(first) == before

    first.close()
    second.close()


@pytest.mark.parametrize(
    ("kind", "expected_type"),
    [
        ("person", PROV["Person"]),
        ("software", PROV["SoftwareAgent"]),
        ("organization", PROV["Organization"]),
        ("instrument", HPC["Instrument"]),
    ],
)
def test_supported_agent_kinds(kind, expected_type, tmp_path: Path):
    manager = _new_manager(tmp_path, f"{kind}.aca")
    agent = manager.add_agent(kind, kind)
    assert expected_type in _one_record(_active_document(manager), agent).get_asserted_types()
    manager.close()
