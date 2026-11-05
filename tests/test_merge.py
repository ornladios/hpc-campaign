"""Fork exchange tests use real Manager connections and SQLite transactions."""

import hashlib
import io
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from PIL import Image
from prov.model import Namespace, ProvDocument

from hpc_campaign.manager import Manager
from hpc_campaign.merge import MergeError, initialize_connection
from hpc_campaign.prov_store import canonical_prov_json


def archive(tmp_path, name="a.aca"):
    manager = Manager(archive=name, campaign_store=str(tmp_path))
    manager.open(create=True)
    return manager


def fork(manager, tmp_path, name="b.aca"):
    manager.con.commit()
    shutil.copyfile(manager.args.campaign_file_name, tmp_path / name)
    return archive(tmp_path, name)


def host(manager, name):
    return manager.con.execute("INSERT INTO host VALUES (?, ?, 1, 0, 'file') RETURNING rowid", (name, name)).fetchone()[
        0
    ]


def _png(color):
    buffer = io.BytesIO()
    Image.new("RGB", (16, 12), color).save(buffer, format="PNG")
    return buffer.getvalue()


def test_collision_replay_roundtrip_and_third_copy(tmp_path):
    a = archive(tmp_path)
    b = fork(a, tmp_path)
    c = fork(a, tmp_path, "c.aca")
    assert host(a, "local") == host(b, "remote") == 1
    b.con.execute("INSERT INTO directory VALUES (1, '/remote', 1, 0)")
    a.con.commit()
    b.con.commit()
    before = Path(b.args.campaign_file_name).read_bytes()
    preview = a.merge(b.args.campaign_file_name)
    assert not preview.conflicts and not preview.applied
    assert a.con.execute("SELECT count(*) FROM host").fetchone()[0] == 1
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert (
        a.con.execute("SELECT hostname FROM host JOIN directory ON host.rowid=directory.hostid").fetchone()[0]
        == "remote"
    )
    assert Path(b.args.campaign_file_name).read_bytes() == before
    assert a.merge(b.args.campaign_file_name, apply=True).operations == 0
    assert b.merge(a.args.campaign_file_name, apply=True).applied
    assert a.merge(b.args.campaign_file_name).operations == 0
    assert c.merge(b.args.campaign_file_name, apply=True).applied
    assert c.con.execute("SELECT count(*) FROM host").fetchone()[0] == 2
    assert a.merge(c.args.campaign_file_name).operations == 0


def test_capture_rollback_savepoint_rowid_reuse_and_blob(tmp_path):
    a = archive(tmp_path)
    b = fork(a, tmp_path)
    host(a, "rolled-back")
    a.con.rollback()
    assert a.con.execute("SELECT count(*) FROM change_log").fetchone()[0] == 0
    host(a, "kept")
    a.con.execute("SAVEPOINT inner_edit")
    host(a, "discarded")
    a.con.execute("ROLLBACK TO inner_edit")
    a.con.execute("RELEASE inner_edit")
    a.con.commit()
    old_uuid = a.con.execute("SELECT uuid FROM entity_identity WHERE kind='host'").fetchone()[0]
    a.con.execute("DELETE FROM host")
    host(a, "new")
    a.con.execute("INSERT INTO file(name,data) VALUES ('bytes', ?)", (b"\x00\xff\x80",))
    a.con.execute("DELETE FROM file")
    a.con.commit()
    assert a.con.execute("SELECT uuid FROM entity_identity WHERE kind='host' AND active=1").fetchone()[0] != old_uuid
    logs = a.con.execute("SELECT after_value FROM change_log WHERE kind='file' AND action='insert'").fetchone()[0]
    assert "AP+A" in logs
    assert b.merge(a.args.campaign_file_name, apply=True).applied
    assert b.con.execute("SELECT hostname FROM host").fetchone()[0] == "new"
    assert b.con.execute("SELECT count(*) FROM file").fetchone()[0] == 0


def test_concurrent_repeated_value_and_delete_update_conflict(tmp_path):
    a = archive(tmp_path)
    host(a, "shared")
    a.con.commit()
    b = fork(a, tmp_path)
    a.con.execute("UPDATE host SET longhostname='temporary'")
    a.con.execute("UPDATE host SET longhostname='shared'")
    b.con.execute("UPDATE host SET longhostname='other'")
    a.con.commit()
    b.con.commit()
    snapshot = a.con.serialize()
    result = a.merge(b.args.campaign_file_name, apply=True)
    assert result.conflicts and not result.applied
    assert a.con.serialize() == snapshot
    a.con.execute("DELETE FROM host")
    a.con.commit()
    assert a.merge(b.args.campaign_file_name).conflicts


def test_names_relationships_and_external_connections(tmp_path):
    a = archive(tmp_path)
    b = fork(a, tmp_path)
    host(a, "same")
    host(b, "same")
    a.con.commit()
    b.con.commit()
    assert a.merge(b.args.campaign_file_name).conflicts
    with sqlite3.connect(a.args.campaign_file_name) as raw:
        with pytest.raises(sqlite3.OperationalError, match="aca_capture"):
            raw.execute("DELETE FROM host")
        initialize_connection(raw)
        with pytest.raises(sqlite3.OperationalError, match="no such function"):
            raw.execute("SELECT aca_replay(1)")
        raw.execute("UPDATE host SET longhostname='captured'")
    assert a.con.execute("SELECT count(*) FROM change_log").fetchone()[0] == 2


def test_unrelated_and_legacy_rejected(tmp_path):
    a = archive(tmp_path)
    b = archive(tmp_path, "b.aca")
    with pytest.raises(MergeError, match="baseline"):
        a.merge(b.args.campaign_file_name)
    b.con.execute("UPDATE info SET version='0.7'")
    b.con.commit()
    with pytest.raises(MergeError, match="0.8"):
        a.merge(b.args.campaign_file_name)


def test_upgrade_legacy_additive_tables_blob_and_idempotence(tmp_path):
    a = archive(tmp_path)
    host(a, "original")
    a.con.execute("INSERT INTO file(name, data) VALUES ('embedded', ?)", (b"\xff\x00",))
    a.con.commit()
    for row in a.con.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall():
        a.con.execute(f'DROP TRIGGER "{row[0]}"')
    for table in (
        "merge_head",
        "merge_meta",
        "entity_identity",
        "change_log",
        "visualization_item",
        "visualization_variable",
        "visualization_sequence",
        "scalar_field",
    ):
        a.con.execute(f'DROP TABLE "{table}"')
    a.con.execute("UPDATE info SET version='0.7'")
    a.con.commit()
    campaign = a.campaign_uuid()
    assert a.upgrade() == "0.8"
    assert a.campaign_uuid() == campaign
    baseline = a.con.execute("SELECT baseline FROM merge_meta").fetchone()[0]
    assert a.upgrade() == "0.8"
    assert a.con.execute("SELECT baseline FROM merge_meta").fetchone()[0] == baseline
    assert a.con.execute("SELECT data FROM file").fetchone()[0] == b"\xff\x00"
    b = fork(a, tmp_path)
    host(b, "added")
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert a.con.execute("SELECT data FROM file").fetchone()[0] == b"\xff\x00"


def test_relationship_key_updates_are_delete_insert_operations(tmp_path):
    a = archive(tmp_path)
    host(a, "one")
    host(a, "two")
    a.con.execute("INSERT INTO directory VALUES (1, '/data', 0, 0)")
    # Keys such as repfiles and visualization relationships may be changed by
    # existing archive writers; represent those changes portably as two ops.
    a.con.execute("INSERT INTO dataset(name) VALUES ('dataset')")
    for rowid in (1, 2):
        a.con.execute("INSERT INTO replica(rowid,datasetid,hostid,dirid) VALUES (?,1,1,1)", (rowid,))
    a.con.execute("INSERT INTO file(name,data) VALUES ('blob', ?)", (b"\x00\xff",))
    a.con.execute("INSERT INTO repfiles VALUES (1,1)")
    a.con.commit()
    b = fork(a, tmp_path)
    b.con.execute("UPDATE repfiles SET replicaid=2")
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert a.con.execute("SELECT replicaid FROM repfiles").fetchone()[0] == 2
    assert a.con.execute("SELECT data FROM file").fetchone()[0] == b"\x00\xff"
    actions = [r[0] for r in b.con.execute("SELECT action FROM change_log WHERE kind='repfiles'")]
    assert actions == ["insert", "delete", "insert"]


def test_visualization_aggregate_child_parent_conflict(tmp_path):
    a = archive(tmp_path)
    a.con.execute("INSERT INTO visualization_sequence(name) VALUES ('sequence')")
    a.con.execute("INSERT INTO visualization_item VALUES (1,0,'IMAGE','image',NULL)")
    a.con.commit()
    b = fork(a, tmp_path)
    a.con.execute("UPDATE visualization_sequence SET metadata='changed'")
    b.con.execute("UPDATE visualization_item SET metadata='other'")
    a.con.commit()
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).conflicts


def test_identical_concurrent_updates_and_sequential_reversion(tmp_path):
    a = archive(tmp_path)
    host(a, "shared")
    a.con.commit()
    b = fork(a, tmp_path)
    for manager in (a, b):
        manager.con.execute("UPDATE host SET longhostname='equal',modtime=0")
        manager.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert b.merge(a.args.campaign_file_name, apply=True).applied
    b.con.execute("UPDATE host SET longhostname='shared'")
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert a.con.execute("SELECT longhostname FROM host").fetchone()[0] == "shared"


def test_apply_failure_rolls_back_history_and_data(tmp_path):
    a = archive(tmp_path)
    b = fork(a, tmp_path)
    host(b, "imported")
    b.con.commit()
    original = a.con.serialize()
    # Rehearsal succeeds, but the real write fails. Exercise rollback after
    # data replay, before imported history has been committed.
    a.con.set_authorizer(
        lambda action, table, *_: (
            sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_INSERT and table == "change_log" else sqlite3.SQLITE_OK
        )
    )
    with pytest.raises(sqlite3.DatabaseError):
        a.merge(b.args.campaign_file_name, apply=True)
    a.con.set_authorizer(None)
    assert a.con.serialize() == original
    assert a.merge(b.args.campaign_file_name, apply=True).applied


def test_savepoint_outer_transactions_and_replace(tmp_path):
    a = archive(tmp_path)
    for name in ("one", "two"):
        a.con.execute("SAVEPOINT outer_transaction")
        host(a, name)
        a.con.execute("RELEASE outer_transaction")
    transactions = a.con.execute("SELECT DISTINCT transaction_uuid FROM change_log").fetchall()
    assert len(transactions) == 2
    a.con.execute("INSERT OR REPLACE INTO host VALUES ('one','replacement',1,0,'file')")
    a.con.commit()
    assert [r[0] for r in a.con.execute("SELECT action FROM change_log")][-2:] == ["delete", "insert"]


def test_provenance_document_replay_and_concurrent_documents(tmp_path):
    from prov.model import Namespace, ProvDocument

    a = archive(tmp_path)
    b = fork(a, tmp_path)
    document = ProvDocument()
    namespace = Namespace("example", "urn:example:")
    document.add_namespace(namespace)
    document.entity(namespace["value"])
    identifier = b.add_prov_document(document, name="imported", activate=True)
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert a.prov_document(identifier) == document
    assert b.merge(a.args.campaign_file_name, apply=True).operations == 0
    a.add_prov_document(document, name="local", activate=False)
    b.add_prov_document(document, name="remote", activate=False)
    result = a.merge(b.args.campaign_file_name, apply=True)
    assert result.conflicts and any("provenance" in conflict for conflict in result.conflicts)


def test_timeseries_delete_parent_before_members_and_conflict(tmp_path):
    a = archive(tmp_path)
    a.con.execute("INSERT INTO timeseries(name) VALUES ('steps')")
    a.con.execute("INSERT INTO dataset(name,tsid,tsorder) VALUES ('first',1,0)")
    a.con.commit()
    b = fork(a, tmp_path)
    b.con.execute("DELETE FROM timeseries")
    b.con.execute("UPDATE dataset SET tsid=0,tsorder=0")
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert a.con.execute("SELECT tsid FROM dataset").fetchone()[0] == 0
    # Concurrent membership changes share a conservative aggregate boundary.
    c = fork(a, tmp_path, "c.aca")
    for manager, name in ((a, "local"), (c, "remote")):
        manager.con.execute("INSERT INTO timeseries(name) VALUES (?)", (name,))
        manager.con.commit()
    assert a.merge(c.args.campaign_file_name).conflicts


def test_migration_failure_is_atomic(tmp_path):
    a = archive(tmp_path)
    a.con.commit()
    for row in a.con.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall():
        a.con.execute(f'DROP TRIGGER "{row[0]}"')
    for table in (
        "merge_head",
        "merge_meta",
        "change_log",
        "entity_identity",
        "visualization_item",
        "visualization_variable",
        "visualization_sequence",
        "scalar_field",
    ):
        a.con.execute(f'DROP TABLE "{table}"')
    a.con.execute("UPDATE info SET version='0.7'")
    a.con.execute("INSERT INTO directory(hostid,name) VALUES (99,'dangling')")
    a.con.commit()
    snapshot = list(a.con.iterdump())
    with pytest.raises(MergeError, match="Missing identity"):
        a.upgrade()
    assert list(a.con.iterdump()) == snapshot
    assert a.con.execute("SELECT version FROM info").fetchone()[0] == "0.7"


def test_baseline_reconstruction_includes_bytes_and_soft_deletions(tmp_path):
    from hpc_campaign.merge import _apply_state, _history, _state

    a = archive(tmp_path)
    host(a, "deleted")
    a.con.execute("UPDATE host SET deltime=10")
    a.con.execute("INSERT INTO file(name,data) VALUES ('retained',?)", (b"\x00\xff",))
    a.con.commit()
    # Reconstruct from portable operations with all local rows absent.
    clone = sqlite3.connect(":memory:")
    clone.executescript("\n".join(a.con.iterdump()))
    clone.row_factory = sqlite3.Row
    initialize_connection(clone)
    _apply_state(clone, _state(_history(a.con)))
    assert clone.execute("SELECT deltime FROM host").fetchone()[0] == 10
    assert clone.execute("SELECT data FROM file").fetchone()[0] == b"\x00\xff"
    clone.close()


def test_key_identity_can_reappear_after_imported_tombstone(tmp_path):
    a = archive(tmp_path)
    b = fork(a, tmp_path)
    b.con.execute("INSERT INTO key VALUES ('shared-key')")
    b.con.execute("DELETE FROM key")
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    b.con.execute("INSERT INTO key VALUES ('shared-key')")
    b.con.commit()
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    assert a.con.execute("SELECT keyid FROM key").fetchone()[0] == "shared-key"


def test_cli_preview_and_apply(tmp_path, capsys):
    from hpc_campaign.manager import main

    a = archive(tmp_path)
    b = fork(a, tmp_path)
    host(b, "from-cli")
    b.con.commit()
    arguments = ["--campaign_store", str(tmp_path), "a.aca", "merge", b.args.campaign_file_name]
    main(args=arguments)
    assert "applied=False" in capsys.readouterr().out
    assert a.con.execute("SELECT count(*) FROM host").fetchone()[0] == 0
    main(args=[*arguments, "--apply"])
    assert "applied=True" in capsys.readouterr().out
    assert a.con.execute("SELECT hostname FROM host").fetchone()[0] == "from-cli"


def test_adios_reader_reads_merged_archive(tmp_path, monkeypatch):
    import adios2
    import numpy as np

    first = np.arange(5, dtype=np.float64)
    second = first + 100
    paths = []
    for name, values in (("local.bp", first), ("remote.bp", second)):
        path = tmp_path / name
        with adios2.Stream(str(path), "w") as writer:
            writer.write("values", values, [5], [0], [5])
        paths.append(path)
    monkeypatch.chdir(tmp_path)
    a = archive(tmp_path)
    # Leave gaps in the retained IDs before copying; the reader must map actual
    # host/directory rowids, rather than subtracting one to address a vector.
    a.con.execute("INSERT INTO host(rowid,hostname) VALUES (16,'retired')")
    a.con.execute("DELETE FROM host")
    a.con.execute("INSERT INTO host(rowid,hostname,longhostname) VALUES (17,?,'test')", (a.args.hostname,))
    a.con.execute("INSERT INTO directory(rowid,hostid,name) VALUES (29,17,?)", (str(tmp_path),))
    a.con.commit()
    a.data(paths[0].name, name="local")
    b = fork(a, tmp_path)
    (tmp_path / "note").write_text("local addition")
    a.text("note", name="local-note", store=True)
    b.data(paths[1].name, name="remote")
    assert a.merge(b.args.campaign_file_name, apply=True).applied
    try:
        adios = adios2.Adios()
        io = adios.declare_io("merged-reader")
        io.set_parameter("hostname", a.args.hostname)
        reader = adios2.FileReader(io, a.args.campaign_file_name)
    except ValueError as error:
        if "reads only ACA version 0.7" in str(error):
            pytest.skip("Installed ADIOS2 reader predates ACA 0.8; use the aca08_support build")
        raise
    with reader:
        np.testing.assert_array_equal(reader.read("local/values"), first)
        np.testing.assert_array_equal(reader.read("remote/values"), second)


def test_merge_published_image_movie_and_provenance_payloads(tmp_path):
    if not shutil.which("ffmpeg"):
        pytest.skip("requires FFmpeg")
    movie_path = tmp_path / "movie.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=green:s=16x12:r=2:d=1",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(movie_path),
        ],
        check=True,
    )
    movie_bytes = movie_path.read_bytes()
    preview_bytes = _png("green")
    times = [0.25, 1.0]
    timing_source = {"kind": "explicit", "source": "regression-test"}

    a = archive(tmp_path)
    b = fork(a, tmp_path)
    b.args.hostname = "remote-test-host"
    image_id = str(uuid4())
    image_bytes = _png("blue")
    image_record = a.publish_image(image_bytes, publication_id=image_id, title="Fork image")

    movie_id = str(uuid4())
    movie_record = b.publish_video(
        movie_bytes,
        preview=preview_bytes,
        publication_id=movie_id,
        title="Fork movie",
        video_metadata={
            "width": 16,
            "height": 12,
            "fps": 2.0,
            "frame_count": 2,
            "duration": 1.0,
            "codec": "h264",
            "time_mode": "physical_time",
            "time_values": times,
            "time_unit": "s",
            "time_source": timing_source,
        },
    )

    document = ProvDocument()
    example = Namespace("example", "urn:example:")
    document.add_namespace(example)
    document.entity(example["movie"], {example["sha256"]: hashlib.sha256(movie_bytes).hexdigest()})
    document.activity(example["render"])
    document.wasGeneratedBy(example["movie"], example["render"])
    document_id = b.add_prov_document(document, name="movie provenance", activate=True)
    b.con.commit()

    result = a.merge(b.args.campaign_file_name, apply=True)
    assert result.applied and not result.conflicts

    def stored_file(dataset_name, file_name):
        row = a.con.execute(
            "SELECT f.data FROM dataset d "
            "JOIN replica r ON r.datasetid=d.rowid "
            "JOIN repfiles rf ON rf.replicaid=r.rowid "
            "JOIN file f ON f.fileid=rf.fileid "
            "WHERE d.name=? AND f.name=?",
            (dataset_name, file_name),
        ).fetchone()
        assert row is not None
        return bytes(row[0])

    merged_image = stored_file(image_record["image_name"], "16x12.png")
    merged_preview = stored_file(movie_record["image_name"], "16x12.png")
    merged_movie = stored_file(movie_record["video_name"], "movie.mp4")
    assert merged_image == image_bytes
    assert merged_preview == preview_bytes
    assert merged_movie == movie_bytes
    assert hashlib.sha256(merged_image).hexdigest() == image_record["image_sha256"]
    assert hashlib.sha256(merged_movie).hexdigest() == movie_record["video_sha256"]

    record_bytes = stored_file(f"__published_videos/{movie_id}.json", "publication.json")
    merged_record = json.loads(record_bytes)
    assert merged_record["video"]["time_values"] == times
    assert merged_record["video"]["time_unit"] == "s"
    assert merged_record["video"]["time_source"] == timing_source

    provenance_info = next(info for info in a.prov_documents(active=True) if info.document_id == document_id)
    assert provenance_info.sha256 == hashlib.sha256(canonical_prov_json(document).encode("utf-8")).hexdigest()
    assert a.prov_document(document_id) == document
