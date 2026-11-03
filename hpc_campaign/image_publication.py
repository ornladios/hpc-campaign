"""Atomic, idempotent publication of embedded PNG and MP4 products."""

import hashlib
import json
import math
import zlib
from datetime import datetime, timezone
from io import BytesIO
from time import time_ns
from uuid import NAMESPACE_URL, UUID, uuid5

from PIL import Image

from .manager_funcs import (
    add_dataset_to_archive,
    add_file_to_archive,
    add_image_data,
    add_replica_to_archive,
    add_visualization_sequence,
)


def publish_image(manager, data, *, publication_id, title, variables=None):
    return _publish(manager, data, publication_id=publication_id, title=title, variables=variables)


def publish_video(  # pylint: disable=too-many-arguments
    manager, data, *, preview, publication_id, title, video_metadata, variables=None
):
    return _publish(
        manager,
        preview,
        publication_id=publication_id,
        title=title,
        variables=variables,
        video_data=data,
        video_metadata=video_metadata,
    )


def _video_info(data: bytes, metadata: object, normalize_times) -> dict:
    if len(data) < 12 or data[4:8] != b"ftyp":
        raise ValueError("publish_video requires MP4 bytes")
    if not isinstance(metadata, dict):
        raise ValueError("video_metadata must contain validated playback metadata")
    result = {key: metadata.get(key) for key in ("width", "height", "frame_count", "fps", "duration")}
    for key in ("width", "height", "frame_count"):
        value = result[key]
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"Invalid video metadata: {key}")
    for key in ("fps", "duration"):
        value = result[key]
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid video metadata: {key}")
    result.update(mime_type="video/mp4", time_mode="timestep")
    mode = metadata.get("time_mode", "physical_time" if "time_values" in metadata else "timestep")
    if mode == "physical_time":
        values = metadata.get("time_values")
        if not isinstance(values, (list, tuple)):
            raise ValueError("Movie timing requires one physical time per encoded frame")
        times = normalize_times(values, result["frame_count"])
        unit = metadata.get("time_unit", "s")
        if not isinstance(unit, str) or not unit.strip() or len(unit) > 128:
            raise ValueError("Movie time_unit must be nonempty text")
        source = metadata.get("time_source", {"kind": "explicit"})
        if not isinstance(source, dict):
            raise ValueError("Movie time_source must be a JSON object")
        try:
            source = json.loads(json.dumps(source, allow_nan=False))
        except (TypeError, ValueError) as error:
            raise ValueError("Movie time_source must contain finite JSON values") from error
        result.update(time_mode=mode, time_values=times, time_unit=unit.strip(), time_source=source)
    elif mode != "timestep" or any(key in metadata for key in ("time_values", "time_unit", "time_source")):
        raise ValueError("Movie physical-time metadata requires physical_time mode")
    if metadata.get("codec"):
        result["codec"] = str(metadata["codec"])
    return result


def _existing_publication(manager, publication_id, kind):
    cur = manager.cur
    record_name = f"__published_{kind}s/{publication_id}.json"
    other_name = f"__published_{'image' if kind == 'video' else 'video'}s/{publication_id}.json"
    existing = cur.execute(
        """select f.data, f.compression from dataset d
           join replica r on r.datasetid=d.rowid
           join repfiles rf on rf.replicaid=r.rowid
           join file f on f.fileid=rf.fileid
           where d.name in (?, ?) and d.deltime=0 and r.deltime=0
           order by f.fileid desc limit 1""",
        (record_name, other_name),
    ).fetchone()
    if existing:
        payload = bytes(existing[0])
        if existing[1]:
            payload = zlib.decompress(payload)
        record = json.loads(payload)
        if (
            not isinstance(record, dict)
            or record.get("format") != f"hpc-campaign-published-{kind}"
            or record.get("version") != 1
            or record.get("publication_id") != publication_id
        ):
            raise ValueError("Invalid existing publication record")
        return record
    prefix = f"published-{kind}s/{publication_id}"
    if cur.execute(
        "select 1 from dataset where name in (?, ?, ?, ?)",
        (prefix + ".png", prefix + ".mp4", record_name, other_name),
    ).fetchone():
        raise ValueError("Publication dataset name is already in use")
    return None


def _store_image(manager, data, record):
    args = manager._build_command_args(  # pylint: disable=protected-access
        "image_data",
        {
            "image_data": data,
            "image_format": "png",
            "name": record["image_name"],
            "thumbnail": None,
            "replica_name": record["image_name"],
            "store": True,
        },
    )
    add_image_data(args, manager.cur, manager.con, commit=False)
    row = manager.cur.execute(
        """select d.uuid, r.hostid, r.dirid from dataset d
           join replica r on r.datasetid=d.rowid where d.name=? and r.deltime=0""",
        (record["image_name"],),
    ).fetchone()
    record["image_uuid"] = row[0]
    return args, (row[1], row[2])


def _store_video(manager, args, location, record, data):
    now = time_ns()
    item_uuid = uuid5(NAMESPACE_URL, record["video_name"]).hex
    video_id = add_dataset_to_archive(record["video_name"], manager.cur, item_uuid, "VIDEO", now, verbose=False)
    replica_id = add_replica_to_archive(
        location[0], location[1], 0, 0, record["video_name"], manager.cur, video_id, now, len(data), verbose=False
    )
    add_file_to_archive(args, "", manager.cur, replica_id, now, "movie.mp4", compress=False, content=data)
    record.update(video_uuid=item_uuid, video_sha256=hashlib.sha256(data).hexdigest())


def _store_associations(manager, record):
    item = {"type": "IMAGE", "uuid": record["image_uuid"], "metadata": None}
    if "video_uuid" in record:
        item = {
            "type": "VIDEO",
            "uuid": record["video_uuid"],
            "metadata": {**record["video"], "poster_uuid": record["image_uuid"]},
        }
    sequence = manager._build_command_args(  # pylint: disable=protected-access
        "visualization_sequence",
        {
            "name": record["image_name"].removesuffix(".png"),
            "vis_type": "plot",
            "variables": record["associations"],
            "items": [item],
            "source_dataset": None,
            "thumbnail_uuid": record["image_uuid"],
            "metadata": {"publication_id": record["publication_id"], "title": record["title"]},
            "replace": False,
        },
    )
    add_visualization_sequence(sequence, manager.cur, manager.con, commit=False)


def _store_record(manager, args, location, record):
    payload = json.dumps(record, allow_nan=False, sort_keys=True).encode("utf-8")
    if len(payload) > 128 * 1024:
        raise ValueError("Publication catalogue record exceeds 128 KiB")
    kind = "video" if "video_uuid" in record else "image"
    name = f"__published_{kind}s/{record['publication_id']}.json"
    now = time_ns()
    dataset_id = add_dataset_to_archive(name, manager.cur, uuid5(NAMESPACE_URL, name).hex, "TEXT", now, verbose=False)
    replica_id = add_replica_to_archive(
        location[0], location[1], 0, 0, name, manager.cur, dataset_id, now, len(payload), verbose=False
    )
    add_file_to_archive(args, "", manager.cur, replica_id, now, "publication.json", compress=False, content=payload)


def _publish(  # pylint: disable=too-many-arguments
    manager, data, *, publication_id, title, variables, video_data=None, video_metadata=None
):
    publication_id = str(UUID(publication_id))
    if not isinstance(title, str) or not title.strip() or len(title) > 1024:
        raise ValueError("title must be nonempty text of at most 1024 characters")
    if manager.args.encryption_key:
        raise ValueError("Image publication does not support encrypted campaigns")
    con = manager.con
    if con.in_transaction:
        raise ValueError("publish_image requires a connection without a pending transaction")
    kind = "video" if video_data is not None else "image"
    # Acquire the write reservation before checking the idempotency key.
    with con:
        con.execute("BEGIN IMMEDIATE")
        existing = _existing_publication(manager, publication_id, kind)
        if existing:
            return existing
        data = bytes(data)
        with Image.open(BytesIO(data)) as image:
            if image.format != "PNG":
                raise ValueError("publish_image requires a PNG image")
            image.verify()
        associations = (
            manager._normalize_visualization_variable_specs(variables, None)  # pylint: disable=protected-access
            if variables
            else []
        )
        record = {
            "format": f"hpc-campaign-published-{kind}",
            "version": 1,
            "publication_id": publication_id,
            "title": title.strip(),
            "image_name": f"published-{kind}s/{publication_id}.png",
            "image_sha256": hashlib.sha256(data).hexdigest(),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "associations": associations,
        }
        if video_data is not None:
            record.update(
                video_name=f"published-videos/{publication_id}.mp4",
                video=_video_info(
                    bytes(video_data),
                    video_metadata,
                    manager._normalize_video_times,  # pylint: disable=protected-access
                ),
            )
        args, location = _store_image(manager, data, record)
        if video_data is not None:
            _store_video(manager, args, location, record, bytes(video_data))
        if record["associations"]:
            _store_associations(manager, record)
        _store_record(manager, args, location, record)
    return record
