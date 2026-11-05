"""ACA 0.8 portable history, transaction capture, and conservative fork merge.

Only initialized connections can write application rows: persistent triggers call
connection-local functions, so ordinary sqlite connections fail closed.
"""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from typing import Any, Callable

# Dependency order; every local integer reference is declared here.
REFERENCES = {
    "key": {},
    "host": {},
    "timeseries": {},
    "directory": {"hostid": "host"},
    "dataset": {"tsid": "timeseries"},
    "archive": {"dirid": "directory"},
    "replica": {
        "datasetid": "dataset",
        "hostid": "host",
        "dirid": "directory",
        "archiveid": "archive",
        "keyid": "key",
    },
    "file": {},
    "repfiles": {"replicaid": "replica", "fileid": "file"},
    "accuracy": {"replicaid": "replica"},
    "resolution": {"replicaid": "replica"},
    "archiveidx": {"archiveid": "archive", "replicaid": "replica"},
    "scalar_field": {"datasetid": "dataset"},
    "visualization_sequence": {},
    "visualization_variable": {"visid": "visualization_sequence", "datasetid": "dataset"},
    "visualization_item": {"visid": "visualization_sequence"},
    "provenance_document": {},
}
COLUMNS = {
    "key": "keyid",
    "host": "hostname longhostname modtime deltime default_protocol",
    "directory": "hostid name modtime deltime",
    "timeseries": "tsid name",
    "dataset": "name uuid modtime deltime fileformat tsid tsorder",
    "replica": "datasetid hostid dirid archiveid name modtime deltime keyid size",
    "file": "fileid name compression lenorig lencompressed modtime checksum data",
    "repfiles": "replicaid fileid",
    "accuracy": "replicaid accuracy norm relative",
    "resolution": "replicaid x y",
    "archive": "dirid tarname system notes",
    "archiveidx": "archiveid replicaid filename offset offset_data size",
    "scalar_field": "datasetid metadata",
    "visualization_sequence": "visid name vistype thumbnail_itemuuid metadata",
    "visualization_variable": "visid datasetid variable_name role",
    "visualization_item": "visid item_order item_type item_uuid metadata",
    "provenance_document": "uuid name format content sha256 active modtime",
}

LOCAL_KEYS = {"timeseries": "tsid", "file": "fileid", "visualization_sequence": "visid"}
RELATION_KEYS = {
    "key": ("keyid",),
    "repfiles": ("replicaid", "fileid"),
    "accuracy": ("replicaid",),
    "resolution": ("replicaid",),
    "archiveidx": ("archiveid", "replicaid", "filename"),
    "scalar_field": ("datasetid",),
    "visualization_variable": ("visid", "datasetid", "variable_name", "role"),
    "visualization_item": ("visid", "item_order"),
}


class MergeError(ValueError):
    """An unsupported, corrupt, or conflicting archive exchange."""


@dataclass(frozen=True)
class MergeResult:
    """Preview or application result. Conflicts prevent every change."""

    operations: int
    conflicts: tuple[str, ...]
    applied: bool = False


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in con.execute(f'PRAGMA table_info("{table}")')]


def _check_schema(con: sqlite3.Connection) -> None:
    for table, columns in COLUMNS.items():
        if _columns(con, table) != columns.split():
            raise MergeError(f"Unsupported application schema: {table}")
    allowed = set(REFERENCES) | {
        "info",
        "campaign_identity",
        "merge_meta",
        "entity_identity",
        "change_log",
        "merge_head",
    }
    tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if any(table not in allowed and not table.startswith("sqlite_") for table in tables):
        raise MergeError("Unsupported application tables")


def _identity(con: sqlite3.Connection, table: str, rowid: int, *, retired: bool = False) -> str:
    row = con.execute(
        "SELECT uuid FROM entity_identity WHERE kind=? AND local_id=? AND active=1", (table, rowid)
    ).fetchone()
    if row is None and retired:
        row = con.execute(
            "SELECT uuid FROM entity_identity WHERE kind=? AND local_id=? ORDER BY rowid DESC LIMIT 1",
            (table, rowid),
        ).fetchone()
    if row is None:
        raise MergeError(f"Missing identity: {table}/{rowid}")
    return str(row[0])


def _portable(con: sqlite3.Connection, table: str, values: dict) -> dict:
    result = {}
    for column, value in values.items():
        if column == LOCAL_KEYS.get(table):
            continue
        target = REFERENCES[table].get(column)
        if target and value not in (None, 0):
            value = {"ref": [target, _identity(con, target, value, retired=True)]}
        elif isinstance(value, bytes):
            value = {"blob": base64.b64encode(value).decode("ascii"), "sha256": hashlib.sha256(value).hexdigest()}
        result[column] = value
    return result


def _row_key(table: str, values: dict, entity_uuid: str) -> str:
    if table in RELATION_KEYS:
        return _json([values[column] for column in RELATION_KEYS[table]])
    return entity_uuid


def initialize_connection(con: sqlite3.Connection) -> Callable[[bool], None]:
    """Register mandatory capture functions on a writable connection.

    Register before writes. Reinitializing starts a fresh writer session.
    The trace hook tracks actual SQLite BEGIN and COMMIT/ROLLBACK boundaries;
    savepoints retain their transaction UUID. Identity and history writes
    themselves participate in SQLite rollback. The returned private replay
    control is for internal reconstruction only; SQL cannot disable capture.
    """
    writer = str(uuid.uuid4())
    context = {"transaction": str(uuid.uuid4()), "replay": False}

    def trace(statement: str) -> None:
        first = statement.strip().upper()
        boundary = first.startswith(("BEGIN", "COMMIT", "END", "ROLLBACK")) and not first.startswith("ROLLBACK TO")
        implicit = not con.in_transaction and first.startswith(("SAVEPOINT", "INSERT", "UPDATE", "DELETE", "REPLACE"))
        if boundary or implicit:
            context["transaction"] = str(uuid.uuid4())

    # pylint: disable=too-many-locals
    def capture(table: str, action: str, rowid: int, oldid: int, *values: Any) -> int:
        if context["replay"]:
            return 0
        columns = _columns(con, table)
        count = len(columns)
        before_raw = dict(zip(columns, values[:count], strict=True))
        after_raw = dict(zip(columns, values[count:], strict=True))
        if action == "update" and rowid != oldid:
            raise MergeError("Changing local row IDs is unsupported")
        if action == "insert":
            entity_uuid = str(uuid.uuid4())
            if table == "key":
                entity_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, "aca-key:" + str(after_raw["keyid"])))
            if table == "provenance_document":
                entity_uuid = str(uuid.UUID(after_raw["uuid"]))
            identity_sql = "INSERT INTO entity_identity(kind, local_id, uuid, active) VALUES (?, ?, ?, 1)"
            if table == "key":
                identity_sql += " ON CONFLICT(kind, uuid) DO UPDATE SET local_id=excluded.local_id, active=1"
            con.execute(identity_sql, (table, rowid, entity_uuid))
        else:
            entity_uuid = _identity(con, table, oldid)
        before = None if action == "insert" else _portable(con, table, before_raw)
        after = None if action == "delete" else _portable(con, table, after_raw)
        if before == after:
            return 0
        key_values = after if after is not None else before
        assert key_values is not None
        key = _row_key(table, key_values, entity_uuid)
        if before is not None and after is not None and _row_key(table, before, entity_uuid) != key:
            empty = [None] * count
            capture(table, "delete", oldid, oldid, *values[:count], *empty)
            capture(table, "insert", rowid, rowid, *empty, *values[count:])
            return 0
        parents = [row[0] for row in con.execute("SELECT uuid FROM merge_head")]
        operation = str(uuid.uuid4())
        con.execute(
            "INSERT INTO change_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                operation,
                writer,
                con.execute("SELECT count(*) FROM change_log").fetchone()[0] + 1,
                context["transaction"],
                time_ns(),
                table,
                key,
                entity_uuid,
                action,
                _json(before),
                _json(after),
                _json(parents),
            ),
        )
        con.execute("DELETE FROM merge_head")
        con.execute("INSERT INTO merge_head VALUES (?)", (operation,))
        if action == "delete":
            con.execute("UPDATE entity_identity SET active=0 WHERE kind=? AND uuid=?", (table, entity_uuid))
        return 0

    con.create_function("aca_capture", -1, capture)
    con.set_trace_callback(trace)
    con.execute("PRAGMA recursive_triggers=ON")

    def replay(enabled: bool) -> None:
        context["replay"] = enabled

    return replay


def install_history(con: sqlite3.Connection) -> None:
    """Allocate a baseline and triggers. Caller owns the migration transaction."""
    if con.execute("SELECT 1 FROM sqlite_master WHERE name='merge_meta'").fetchone():
        return
    _check_schema(con)
    initialize_connection(con)
    con.execute("CREATE TABLE merge_meta (baseline TEXT PRIMARY KEY, protocol INTEGER NOT NULL)")
    con.execute("INSERT INTO merge_meta VALUES (?, 1)", (str(uuid.uuid4()),))
    con.execute(
        "CREATE TABLE entity_identity (kind TEXT, local_id INTEGER, uuid TEXT, active INTEGER NOT NULL, "
        "PRIMARY KEY(kind, uuid))"
    )
    con.execute("CREATE UNIQUE INDEX entity_active ON entity_identity(kind, local_id) WHERE active=1")
    con.execute(
        "CREATE TABLE change_log (uuid TEXT PRIMARY KEY, writer TEXT, sequence INTEGER, transaction_uuid TEXT, "
        "timestamp_ns INTEGER, kind TEXT, object_key TEXT, entity_uuid TEXT, action TEXT, "
        "before_value TEXT, after_value TEXT, parents TEXT)"
    )
    con.execute("CREATE TABLE merge_head (uuid TEXT PRIMARY KEY)")
    # Allocate every identity before encoding references, including forward refs.
    for table in REFERENCES:
        for row in con.execute(f'SELECT rowid, * FROM "{table}"').fetchall():
            entity_uuid = str(uuid.uuid4())
            if table == "key":
                entity_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, "aca-key:" + str(row[1])))
            if table == "provenance_document":
                entity_uuid = str(uuid.UUID(row[1]))
            con.execute("INSERT INTO entity_identity VALUES (?, ?, ?, 1)", (table, row[0], entity_uuid))
    for table in REFERENCES:
        columns = _columns(con, table)
        for row in con.execute(f'SELECT rowid, * FROM "{table}"').fetchall():
            entity_uuid = _identity(con, table, row[0])
            values = _portable(con, table, dict(zip(columns, tuple(row)[1:], strict=True)))
            operation = str(uuid.uuid4())
            parents = [r[0] for r in con.execute("SELECT uuid FROM merge_head")]
            con.execute(
                "INSERT INTO change_log VALUES (?, 'baseline', 0, ?, ?, ?, ?, ?, 'baseline', 'null', ?, ?)",
                (
                    operation,
                    con.execute("SELECT baseline FROM merge_meta").fetchone()[0],
                    time_ns(),
                    table,
                    _row_key(table, values, entity_uuid),
                    entity_uuid,
                    _json(values),
                    _json(parents),
                ),
            )
            con.execute("DELETE FROM merge_head")
            con.execute("INSERT INTO merge_head VALUES (?)", (operation,))
        old = ",".join(f'OLD."{c}"' for c in columns)
        new = ",".join(f'NEW."{c}"' for c in columns)
        nulls = ",".join("NULL" for _ in columns)
        for action in ("insert", "update", "delete"):
            args = {
                "insert": f"NEW.rowid, NEW.rowid, {nulls}, {new}",
                "update": f"NEW.rowid, OLD.rowid, {old}, {new}",
                "delete": f"OLD.rowid, OLD.rowid, {old}, {nulls}",
            }[action]
            con.execute(
                f'CREATE TRIGGER "aca_{table}_{action}" AFTER {action} ON "{table}" '
                f"BEGIN SELECT aca_capture('{table}', '{action}', {args}); END"
            )

    previous_factory = con.row_factory
    try:
        con.row_factory = sqlite3.Row
        _verify_snapshot(con, _history(con))
    finally:
        con.row_factory = previous_factory


def _decode_blob(value: dict) -> bytes:
    payload = base64.b64decode(value["blob"], validate=True)
    if hashlib.sha256(payload).hexdigest() != value["sha256"]:
        raise MergeError("History BLOB digest mismatch")
    return payload


def _history(con: sqlite3.Connection) -> dict[str, dict]:
    columns = _columns(con, "change_log")
    history = {row[0]: dict(zip(columns, row, strict=True)) for row in con.execute("SELECT * FROM change_log")}
    for operation in history.values():
        if operation["kind"] not in REFERENCES:
            raise MergeError("Unknown operation table")
        for field in ("before_value", "after_value"):
            values = json.loads(operation[field])
            if values:
                for value in values.values():
                    if isinstance(value, dict) and "blob" in value:
                        _decode_blob(value)
    return history


def _state(history: dict[str, dict]) -> dict[tuple[str, str], dict]:
    state = {}
    # SQLite insertion order is topological, including imports. Validate parents.
    seen: set[str] = set()
    for opid, operation in history.items():
        if not set(json.loads(operation["parents"])).issubset(seen):
            raise MergeError("Incomplete or unordered causal history")
        seen.add(opid)
        state[(operation["kind"], operation["object_key"])] = operation
    return state


def _ancestors(history: dict[str, dict], opid: str) -> set[str]:
    result: set[str] = set()
    pending = [opid]
    while pending:
        current = pending.pop()
        if current not in result:
            result.add(current)
            pending.extend(json.loads(history[current]["parents"]))
    return result


def _groups(operation: dict) -> set[str]:
    table = operation["kind"]
    groups = {f"{table}/{operation['object_key']}"}
    for field in ("before_value", "after_value"):
        values = json.loads(operation[field])
        if values is None:
            continue
        if table == "provenance_document":
            groups.add("active provenance graph")
        if table == "timeseries" or (table == "dataset" and values.get("tsid") not in (None, 0)):
            groups.add("time-series membership and order")
        if table.startswith("visualization_"):
            parent = values.get("visid", operation["entity_uuid"])
            if isinstance(parent, dict):
                parent = parent["ref"][1]
            groups.add("visualization/" + _json(parent))
    return groups


def _validate(con: sqlite3.Connection) -> None:
    for table, references in REFERENCES.items():
        for row in con.execute(f'SELECT rowid, * FROM "{table}"').fetchall():
            _identity(con, table, row[0])
            raw = dict(zip(_columns(con, table), tuple(row)[1:], strict=True))
            for column, target in references.items():
                if raw[column] not in (None, 0):
                    _identity(con, target, raw[column])
            values = _portable(con, table, dict(zip(_columns(con, table), tuple(row)[1:], strict=True)))
            if table == "dataset" and values.get("tsid") not in (None, 0):
                duplicate = con.execute(
                    "SELECT count(*) FROM dataset WHERE tsid=? AND tsorder=?", (row[6], row[7])
                ).fetchone()[0]
                if duplicate > 1:
                    raise MergeError("Duplicate time-series position")
    # Existing storage validation checks exact hashes, UUIDs, and the combined graph.
    from .prov_store import ProvStore  # pylint: disable=import-outside-toplevel

    previous_factory = con.row_factory
    try:
        con.row_factory = sqlite3.Row
        store = ProvStore(con)
        for document in store.documents():
            store.document(document.document_id)
        store._validate_active_graph()  # pylint: disable=protected-access
    finally:
        con.row_factory = previous_factory


def _verify_snapshot(con: sqlite3.Connection, history: dict[str, dict]) -> None:
    """Detect unlogged changes or incomplete snapshots before planning a merge."""
    expected = {
        key: json.loads(operation["after_value"])
        for key, operation in _state(history).items()
        if operation["after_value"] != "null"
    }
    actual = {}
    for table in REFERENCES:
        columns = _columns(con, table)
        for row in con.execute(f'SELECT rowid, * FROM "{table}"').fetchall():
            values = _portable(con, table, dict(zip(columns, tuple(row)[1:], strict=True)))
            key = _row_key(table, values, _identity(con, table, row[0]))
            actual[(table, key)] = values
    if actual != expected:
        raise MergeError("Live application state differs from captured history")
    _validate(con)


def _check_format(con: sqlite3.Connection) -> tuple:
    try:
        if con.execute("SELECT version FROM info WHERE id='ACA'").fetchone()[0] != "0.8":
            raise MergeError("Merge requires ACA 0.8; upgrade once before copying")
        meta = tuple(con.execute("SELECT baseline, protocol FROM merge_meta").fetchone())
        campaign = con.execute("SELECT uuid FROM campaign_identity").fetchone()[0]
        if meta[1] != 1:
            raise MergeError("Unsupported merge protocol")
        _check_schema(con)
        return campaign, meta
    except (sqlite3.Error, TypeError, IndexError) as exc:
        raise MergeError("Missing or invalid ACA 0.8 merge storage") from exc


# pylint: disable=too-many-locals,too-many-nested-blocks
def _apply_state(con: sqlite3.Connection, chosen: dict[tuple[str, str], dict]) -> None:
    """Materialize the combined final state with dependency-aware UUID remapping."""
    replay = initialize_connection(con)
    replay(True)
    try:
        for table in reversed(REFERENCES):
            con.execute(f'DELETE FROM "{table}"')
        con.execute("UPDATE entity_identity SET active=0")
        mapping: dict[tuple[str, str], int] = {}
        for table in REFERENCES:
            operations = [op for (kind, _), op in chosen.items() if kind == table and op["after_value"] != "null"]
            for operation in operations:
                values = json.loads(operation["after_value"])
                for column, value in values.items():
                    if isinstance(value, dict):
                        if "ref" in value:
                            try:
                                values[column] = mapping[tuple(value["ref"])]
                            except KeyError as exc:
                                raise MergeError(f"Dangling reference in {table}.{column}") from exc
                        elif "blob" in value:
                            values[column] = _decode_blob(value)
                entity_uuid = operation["entity_uuid"]
                existing = con.execute(
                    "SELECT local_id FROM entity_identity WHERE kind=? AND uuid=?", (table, entity_uuid)
                ).fetchone()
                # Preserve destination IDs for existing objects. Allocate new IDs
                # above every historical local ID to avoid collisions on import.
                rowid = (
                    existing[0]
                    if existing and existing[0] is not None
                    else con.execute(
                        "SELECT coalesce(max(local_id), 0)+1 FROM entity_identity WHERE kind=?", (table,)
                    ).fetchone()[0]
                )
                columns = ["rowid", *values]
                marks = ",".join("?" for _ in columns)
                quoted = ",".join(f'"{column}"' for column in columns)
                con.execute(f'INSERT INTO "{table}" ({quoted}) VALUES ({marks})', [rowid, *values.values()])
                con.execute(
                    "INSERT INTO entity_identity VALUES (?, ?, ?, 1) "
                    "ON CONFLICT(kind, uuid) DO UPDATE SET local_id=excluded.local_id, active=1",
                    (table, rowid, entity_uuid),
                )
                mapping[(table, entity_uuid)] = rowid
        _validate(con)
    finally:
        replay(False)


# pylint: disable=too-many-statements
def merge_archive(destination: sqlite3.Connection, source: str | Path, *, apply: bool = False) -> MergeResult:
    """Preview or atomically apply a compatible fork. Source is always read-only.

    Preview rehearses replay in a private in-memory SQLite backup, including
    unique constraints, reference checks and provenance validation.
    """
    if destination.in_transaction:
        raise MergeError("Commit or roll back pending edits before merging")
    source_uri = Path(source).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as origin:
        origin.row_factory = sqlite3.Row
        origin.execute("BEGIN")
        destination.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        try:
            if _check_format(destination) != _check_format(origin):
                raise MergeError("Campaign identity or baseline differs; upgrade once before copying")
            for table in REFERENCES:
                if _columns(destination, table) != _columns(origin, table):
                    raise MergeError(f"Incompatible schema: {table}")
            local, remote = _history(destination), _history(origin)
            _verify_snapshot(destination, local)
            _verify_snapshot(origin, remote)
            for opid in local.keys() & remote.keys():
                if local[opid] != remote[opid]:
                    raise MergeError("Operation UUID has inconsistent contents")
            incoming = {key: value for key, value in remote.items() if key not in local}
            local_only = {key: value for key, value in local.items() if key not in remote}
            combined = {**local, **incoming}
            local_state, remote_state = _state(local), _state(remote)
            _state(combined)
            chosen = dict(local_state)
            conflicts = set()
            for key, operation in remote_state.items():
                previous = chosen.get(key)
                if previous is None or previous["uuid"] in _ancestors(combined, operation["uuid"]):
                    chosen[key] = operation
                elif operation["uuid"] not in _ancestors(combined, previous["uuid"]):
                    if previous["after_value"] != operation["after_value"]:
                        conflicts.add(f"Concurrent changes: {key[0]}/{key[1]}")
            for incoming_op in incoming.values():
                for local_op in local_only.values():
                    if local_op["uuid"] not in _ancestors(combined, incoming_op["uuid"]):
                        overlap = _groups(incoming_op) & _groups(local_op)
                        # Equal final row values can safely converge, while aggregate
                        # edits retain conservative conflict handling.
                        overlap = {
                            group
                            for group in overlap
                            if group not in {f"{incoming_op['kind']}/{incoming_op['object_key']}"}
                        }
                        conflicts.update(f"Concurrent aggregate changes: {group}" for group in overlap)
            if not conflicts and incoming:
                rehearsal = sqlite3.connect(":memory:")
                try:
                    # A SQL snapshot also supports Python 3.10 and avoids the
                    # backup API blocking on our own write transaction.
                    rehearsal.executescript("\n".join(destination.iterdump()))
                    rehearsal.row_factory = sqlite3.Row
                    initialize_connection(rehearsal)
                    _apply_state(rehearsal, chosen)
                except (sqlite3.Error, ValueError, RuntimeError) as exc:
                    conflicts.add(f"Combined state is invalid: {exc}")
                finally:
                    rehearsal.close()
            if conflicts:
                return MergeResult(len(incoming), tuple(sorted(conflicts)))
            if apply and incoming:
                _apply_state(destination, chosen)
                for operation in incoming.values():
                    destination.execute(
                        "INSERT OR IGNORE INTO entity_identity VALUES (?, NULL, ?, 0)",
                        (operation["kind"], operation["entity_uuid"]),
                    )
                for operation in incoming.values():
                    destination.execute(
                        "INSERT INTO change_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", tuple(operation.values())
                    )
                parents = {parent for op in combined.values() for parent in json.loads(op["parents"])}
                destination.execute("DELETE FROM merge_head")
                destination.executemany(
                    "INSERT INTO merge_head VALUES (?)", [(key,) for key in combined if key not in parents]
                )
            destination.commit()
            return MergeResult(len(incoming), (), apply)
        finally:
            if destination.in_transaction:
                destination.rollback()
