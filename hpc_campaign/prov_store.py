"""Persistent storage for canonical W3C PROV documents in an ACA file.

This module owns the Phase 2 storage boundary only. Scientific campaign
helpers build records elsewhere; this layer canonicalizes complete
``ProvDocument`` objects, verifies their hashes, and commits replacements
atomically in SQLite.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from time import time_ns

from prov.model import Namespace, ProvDocument, ProvException

from .prov_mapping import HPC
from .prov_validation import ActiveProvDocument, CampaignProvValidator

PROV_JSON_FORMAT = "prov-json"
DEFAULT_PROV_DOCUMENT_NAME = "campaign-provenance"


class ProvenanceStorageError(RuntimeError):
    """Base error for unavailable or invalid provenance storage."""


class ProvenanceCorruptionError(ProvenanceStorageError):
    """Raised when stored provenance content fails integrity checks."""


class ProvenanceConflictError(ProvenanceStorageError):
    """Raised when a document changed after it was loaded for update."""


@dataclass(frozen=True)
class ProvDocumentInfo:
    """Small immutable description of one stored canonical document."""

    document_id: uuid.UUID
    name: str
    format: str
    sha256: str
    active: bool
    modified_ns: int


def create_provenance_tables(cursor: sqlite3.Cursor, campaign_id: uuid.UUID | None = None) -> uuid.UUID:
    """Create Phase 2 tables and assign the ACA a persistent campaign UUID.

    The caller owns the surrounding database transaction. This function is
    used both for a new ACA and when adding provenance storage to a legacy 0.7
    archive.
    """

    resolved_id = campaign_id or uuid.uuid4()
    cursor.execute(
        """
        CREATE TABLE campaign_identity (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            uuid TEXT NOT NULL UNIQUE
        )
        """
    )
    cursor.execute(
        "INSERT INTO campaign_identity (singleton, uuid) VALUES (1, ?)",
        (str(resolved_id),),
    )
    cursor.execute(
        """
        CREATE TABLE provenance_document (
            uuid TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            format TEXT NOT NULL CHECK (format = 'prov-json'),
            content TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            active INTEGER NOT NULL CHECK (active IN (0, 1)),
            modtime INTEGER NOT NULL
        )
        """
    )
    return resolved_id


def canonical_prov_json(document: ProvDocument) -> str:
    """Return deterministic UTF-8 text for the package's PROV-JSON model.

    This is HPC Campaign's deterministic storage encoding, not a claim that
    PROV-JSON defines a general canonical JSON representation.
    """

    if not isinstance(document, ProvDocument):
        raise TypeError("document must be a ProvDocument")

    serialized = document.serialize(format="json")
    if not isinstance(serialized, str):
        raise TypeError("prov JSON serialization did not return text")
    decoded = json.loads(serialized)
    canonical = json.dumps(decoded, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))

    # Do not persist text that the pinned package cannot read back. This also
    # catches a future serializer behavior change before it reaches the ACA.
    ProvDocument.deserialize(content=canonical, format="json")
    return canonical


def provenance_sha256(content: str) -> str:
    """Hash the exact UTF-8 text stored in ``provenance_document``."""

    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class ProvStore:
    """Read and atomically write canonical PROV documents in one ACA."""

    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def campaign_uuid(self) -> uuid.UUID:
        """Return the persistent campaign UUID without creating storage."""

        try:
            rows = self.connection.execute("SELECT uuid FROM campaign_identity WHERE singleton = 1").fetchall()
        except sqlite3.OperationalError as exc:
            raise ProvenanceStorageError(
                "provenance storage is unavailable; upgrade this ACA to the current format first"
            ) from exc

        if len(rows) != 1:
            raise ProvenanceCorruptionError("campaign_identity must contain exactly one singleton row")
        try:
            return uuid.UUID(str(rows[0][0]))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProvenanceCorruptionError("campaign_identity contains an invalid UUID") from exc

    def ensure_authored_document(self) -> ProvDocumentInfo:
        """Return or create the single active document used by authoring APIs."""

        existing = self._row_by_name(DEFAULT_PROV_DOCUMENT_NAME)
        if existing is not None:
            info = self._row_to_info(existing)
            if not info.active:
                raise ProvenanceStorageError(
                    f"reserved document {DEFAULT_PROV_DOCUMENT_NAME!r} exists but is not active"
                )
            return info

        document = ProvDocument()
        document.add_namespace(HPC)
        campaign_id = self.campaign_uuid()
        document.add_namespace(Namespace("hpcid", f"urn:hpc-campaign:{campaign_id}:"))
        return self.add_document(document, name=DEFAULT_PROV_DOCUMENT_NAME, active=True)

    def add_document(
        self,
        document: ProvDocument,
        *,
        name: str,
        active: bool = False,
        document_id: uuid.UUID | None = None,
    ) -> ProvDocumentInfo:
        """Store one new canonical document in a single SQLite transaction."""

        resolved_name = self._validate_name(name)
        if not isinstance(active, bool):
            raise TypeError("active must be a bool")
        resolved_id = document_id or uuid.uuid4()
        if not isinstance(resolved_id, uuid.UUID):
            raise TypeError("document_id must be a UUID")

        content = canonical_prov_json(document)
        digest = provenance_sha256(content)
        modified_ns = self._next_modtime()

        self._begin_write()
        try:
            if active:
                self._validate_active_graph(
                    ActiveProvDocument(resolved_name, document),
                )
            self.connection.execute(
                """
                INSERT INTO provenance_document
                    (uuid, name, format, content, sha256, active, modtime)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(resolved_id),
                    resolved_name,
                    PROV_JSON_FORMAT,
                    content,
                    digest,
                    int(active),
                    modified_ns,
                ),
            )
            self.connection.commit()
        except sqlite3.IntegrityError as exc:
            self.connection.rollback()
            raise ProvenanceStorageError(f"provenance document name or UUID already exists: {resolved_name}") from exc
        except Exception:
            self.connection.rollback()
            raise

        return ProvDocumentInfo(resolved_id, resolved_name, PROV_JSON_FORMAT, digest, active, modified_ns)

    def replace_document(
        self,
        document_id: uuid.UUID | str,
        document: ProvDocument,
        *,
        expected_sha256: str,
    ) -> ProvDocumentInfo:
        """Replace a document only if the caller loaded the current version."""

        resolved_id = self._as_uuid(document_id, "document_id")
        if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
            raise ValueError("expected_sha256 must be a 64-character SHA-256 hex digest")

        content = canonical_prov_json(document)
        digest = provenance_sha256(content)
        modified_ns = self._next_modtime()

        self._begin_write()
        try:
            current = self.connection.execute(
                "SELECT * FROM provenance_document WHERE uuid = ?",
                (str(resolved_id),),
            ).fetchone()
            if current is None:
                raise LookupError(f"provenance document not found: {resolved_id}")
            if str(current["sha256"]) != expected_sha256:
                raise ProvenanceConflictError(f"provenance document changed since it was loaded: {resolved_id}")
            if bool(current["active"]):
                self._validate_active_graph(
                    ActiveProvDocument(str(current["name"]), document),
                    exclude=resolved_id,
                )
            result = self.connection.execute(
                """
                UPDATE provenance_document
                SET content = ?, sha256 = ?, modtime = ?
                WHERE uuid = ? AND sha256 = ?
                """,
                (content, digest, modified_ns, str(resolved_id), expected_sha256),
            )
            if result.rowcount != 1:
                exists = self.connection.execute(
                    "SELECT 1 FROM provenance_document WHERE uuid = ?",
                    (str(resolved_id),),
                ).fetchone()
                if exists is None:
                    raise LookupError(f"provenance document not found: {resolved_id}")
                raise ProvenanceConflictError(f"provenance document changed since it was loaded: {resolved_id}")
            row = self.connection.execute(
                """
                SELECT uuid, name, format, sha256, active, modtime
                FROM provenance_document WHERE uuid = ?
                """,
                (str(resolved_id),),
            ).fetchone()
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        if row is None:  # Defensive: the UPDATE above proved this row exists.
            raise ProvenanceCorruptionError(f"updated provenance document disappeared: {resolved_id}")
        return self._row_to_info(row)

    def set_document_active(
        self,
        document_id: uuid.UUID | str,
        *,
        active: bool,
    ) -> ProvDocumentInfo:
        """Activate or deactivate an imported document transactionally.

        Activation validates the complete proposed active graph. Deactivation
        performs the same check after removing the document so it cannot leave
        dangling relationships in the remaining active documents.
        """

        resolved_id = self._as_uuid_or_name(document_id)
        if not isinstance(active, bool):
            raise TypeError("active must be a bool")

        self._begin_write()
        try:
            row = self.connection.execute(
                "SELECT * FROM provenance_document WHERE uuid = ?",
                (str(resolved_id),),
            ).fetchone()
            if row is None:
                raise LookupError(f"provenance document not found: {document_id}")
            current_active = bool(row["active"])
            if current_active == active:
                self.connection.commit()
                return self._row_to_info(row)
            if not active and str(row["name"]) == DEFAULT_PROV_DOCUMENT_NAME:
                raise ProvenanceStorageError(f"reserved document {DEFAULT_PROV_DOCUMENT_NAME!r} cannot be deactivated")

            if active:
                candidate = ActiveProvDocument(
                    str(row["name"]),
                    self._document_from_row(row),
                )
                self._validate_active_graph(candidate, exclude=resolved_id)
            else:
                # No candidate is added; validation covers the graph that will
                # remain after this document is removed.
                self._validate_active_graph(exclude=resolved_id)

            modified_ns = self._next_modtime()
            self.connection.execute(
                "UPDATE provenance_document SET active = ?, modtime = ? WHERE uuid = ?",
                (int(active), modified_ns, str(resolved_id)),
            )
            updated = self.connection.execute(
                "SELECT * FROM provenance_document WHERE uuid = ?",
                (str(resolved_id),),
            ).fetchone()
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        if updated is None:  # Defensive: the UPDATE target was read above.
            raise ProvenanceCorruptionError(f"updated provenance document disappeared: {resolved_id}")
        return self._row_to_info(updated)

    def documents(self, *, active: bool | None = None) -> list[ProvDocumentInfo]:
        """List stored documents without parsing their potentially large content."""

        if active is not None and not isinstance(active, bool):
            raise TypeError("active must be a bool or None")
        try:
            if active is None:
                rows = self.connection.execute(
                    """
                    SELECT uuid, name, format, sha256, active, modtime
                    FROM provenance_document ORDER BY name
                    """
                ).fetchall()
            else:
                rows = self.connection.execute(
                    """
                    SELECT uuid, name, format, sha256, active, modtime
                    FROM provenance_document WHERE active = ? ORDER BY name
                    """,
                    (int(active),),
                ).fetchall()
        except sqlite3.OperationalError as exc:
            raise ProvenanceStorageError(
                "provenance storage is unavailable; upgrade this ACA to the current format first"
            ) from exc
        return [self._row_to_info(row) for row in rows]

    def document(self, document_id: uuid.UUID | str) -> ProvDocument:
        """Load one document after verifying its format, hash, and PROV-JSON."""

        row = self._document_row(document_id)
        content = self._verified_content(row)
        return self._deserialize_content(content, str(row["name"]))

    def export_document(self, document_id: uuid.UUID | str, path: str | Path) -> None:
        """Export the exact verified canonical text without SQL translation."""

        row = self._document_row(document_id)
        content = self._verified_content(row)
        Path(path).write_text(content, encoding="utf-8")

    def _document_row(self, document_id: uuid.UUID | str) -> sqlite3.Row:
        if isinstance(document_id, uuid.UUID):
            row = self._row_by_uuid(document_id)
        elif isinstance(document_id, str) and document_id:
            try:
                row = self._row_by_uuid(uuid.UUID(document_id))
            except ValueError:
                row = self._row_by_name(document_id)
        else:
            raise TypeError("document_id must be a UUID or non-empty document name")
        if row is None:
            raise LookupError(f"provenance document not found: {document_id}")
        return row

    def _as_uuid_or_name(self, document_id: uuid.UUID | str) -> uuid.UUID:
        """Resolve either public document identifier form to its UUID."""

        row = self._document_row(document_id)
        try:
            return uuid.UUID(str(row["uuid"]))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProvenanceCorruptionError("provenance_document contains an invalid UUID") from exc

    def _validate_active_graph(
        self,
        candidate: ActiveProvDocument | None = None,
        *,
        exclude: uuid.UUID | None = None,
    ) -> None:
        documents = self._active_documents(exclude=exclude)
        if candidate is not None:
            documents.append(candidate)
        CampaignProvValidator(self.connection).validate(documents)

    def _active_documents(self, *, exclude: uuid.UUID | None = None) -> list[ActiveProvDocument]:
        rows = self.connection.execute("SELECT * FROM provenance_document WHERE active = 1 ORDER BY name").fetchall()
        documents = []
        for row in rows:
            if exclude is not None and str(row["uuid"]) == str(exclude):
                continue
            documents.append(
                ActiveProvDocument(
                    str(row["name"]),
                    self._document_from_row(row),
                )
            )
        return documents

    def _document_from_row(self, row: sqlite3.Row) -> ProvDocument:
        content = self._verified_content(row)
        return self._deserialize_content(content, str(row["name"]))

    @staticmethod
    def _deserialize_content(content: str, name: str) -> ProvDocument:
        try:
            return ProvDocument.deserialize(content=content, format="json")
        except (json.JSONDecodeError, ProvException, TypeError, ValueError) as exc:
            raise ProvenanceCorruptionError(f"stored provenance is not readable PROV-JSON: {name}") from exc

    def _row_by_uuid(self, document_id: uuid.UUID) -> sqlite3.Row | None:
        try:
            return self.connection.execute(
                "SELECT * FROM provenance_document WHERE uuid = ?",
                (str(document_id),),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise ProvenanceStorageError(
                "provenance storage is unavailable; upgrade this ACA to the current format first"
            ) from exc

    def _row_by_name(self, name: str) -> sqlite3.Row | None:
        try:
            return self.connection.execute(
                "SELECT * FROM provenance_document WHERE name = ?",
                (name,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise ProvenanceStorageError(
                "provenance storage is unavailable; upgrade this ACA to the current format first"
            ) from exc

    @staticmethod
    def _row_to_info(row: sqlite3.Row) -> ProvDocumentInfo:
        try:
            document_id = uuid.UUID(str(row["uuid"]))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ProvenanceCorruptionError("provenance_document contains an invalid UUID") from exc
        return ProvDocumentInfo(
            document_id=document_id,
            name=str(row["name"]),
            format=str(row["format"]),
            sha256=str(row["sha256"]),
            active=bool(row["active"]),
            modified_ns=int(row["modtime"]),
        )

    @staticmethod
    def _verified_content(row: sqlite3.Row) -> str:
        if str(row["format"]) != PROV_JSON_FORMAT:
            raise ProvenanceCorruptionError(f"unsupported stored provenance format: {row['format']}")
        content = row["content"]
        if not isinstance(content, str):
            raise ProvenanceCorruptionError("stored provenance content is not text")
        expected = str(row["sha256"])
        actual = provenance_sha256(content)
        if actual != expected:
            raise ProvenanceCorruptionError(f"stored provenance hash mismatch: {row['name']}")
        return content

    def _begin_write(self) -> None:
        if self.connection.in_transaction:
            raise ProvenanceStorageError("cannot start a provenance write inside another SQLite transaction")
        try:
            self.connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise ProvenanceStorageError("unable to start provenance write transaction") from exc

    def _next_modtime(self) -> int:
        """Return a nanosecond timestamp newer than any stored document."""

        current = time_ns()
        try:
            row = self.connection.execute("SELECT MAX(modtime) FROM provenance_document").fetchone()
        except sqlite3.OperationalError as exc:
            raise ProvenanceStorageError(
                "provenance storage is unavailable; upgrade this ACA to the current format first"
            ) from exc
        previous = int(row[0]) if row is not None and row[0] is not None else 0
        return max(current, previous + 1)

    @staticmethod
    def _validate_name(name: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("provenance document name must be a non-empty string")
        return name.strip()

    @staticmethod
    def _as_uuid(value: uuid.UUID | str, label: str) -> uuid.UUID:
        try:
            return value if isinstance(value, uuid.UUID) else uuid.UUID(value)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(f"{label} must be a UUID") from exc
