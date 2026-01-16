import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from os.path import exists, isdir

from .config import Config
from .utils import sizeof_fmt, sql_execute, timestamp_to_str


@dataclass
class ArchiveInfo:
    """Archive metadata stored in the info table."""

    id: str
    name: str
    version: str
    mod_time: int


@dataclass
class ArchiveEntry:
    """Archive entry tied to a directory."""

    id: int
    tar_name: str
    system: str


@dataclass
class DirectoryInfo:
    """Directory metadata with archive entries."""

    id: int
    name: str
    mod_time: int
    del_time: int
    archives: list[ArchiveEntry] = field(default_factory=list)
    has_archive: bool = False


@dataclass
class HostInfo:
    """Host metadata with its directories."""

    id: int
    hostname: str
    long_hostname: str
    directories: list[DirectoryInfo] = field(default_factory=list)


@dataclass
class KeyInfo:
    """Encryption key metadata."""

    id: int
    key: str


@dataclass
class FileInfo:
    """Replica file metadata."""

    name: str
    len_orig: int
    len_compressed: int
    mod_time: int
    checksum: str


@dataclass
class ResolutionInfo:
    """Image resolution metadata."""

    x: int
    y: int


@dataclass
class ReplicaFlags:
    """Replica state flags."""

    deleted: bool
    encrypted: bool
    accuracy: bool
    archive: bool
    embedded: bool


@dataclass
class ReplicaInfo:  # pylint: disable=too-many-instance-attributes
    """Replica metadata entry."""

    id: int
    host_id: int
    dir_id: int
    archive_id: int
    name: str
    mod_time: int
    del_time: int
    key_id: int
    size: int
    flags: ReplicaFlags
    files: list[FileInfo] = field(default_factory=list)
    resolution: ResolutionInfo | None = None


@dataclass
class DatasetInfo:
    """Dataset metadata entry."""

    id: int
    uuid: str
    name: str
    mod_time: int
    del_time: int
    file_format: str
    replicas: list[ReplicaInfo] = field(default_factory=list)


@dataclass
class TimeSeriesInfo:
    """Time series metadata with datasets."""

    id: int
    name: str
    datasets: list[DatasetInfo] = field(default_factory=list)


@dataclass
class InfoResult:
    """Aggregated archive information."""

    archive: ArchiveInfo
    hosts: list[HostInfo] = field(default_factory=list)
    keys: list[KeyInfo] = field(default_factory=list)
    time_series: list[TimeSeriesInfo] = field(default_factory=list)
    datasets: list[DatasetInfo] = field(default_factory=list)


def info_dataset(  # pylint: disable=too-many-locals
    args: argparse.Namespace,
    dataset: tuple,
    cur: sqlite3.Cursor,
    delete_condition_and: str,
    dirs_archived: dict[int, bool],
) -> DatasetInfo:
    dataset_info = DatasetInfo(
        id=dataset[0],
        uuid=dataset[1],
        name=dataset[2],
        mod_time=dataset[3],
        del_time=dataset[4],
        file_format=dataset[5],
    )

    if not args.list_replicas and not args.list_files:
        return dataset_info

    res2 = sql_execute(
        cur,
        "select rowid, hostid, dirid, archiveid, name, modtime, deltime, keyid, size from replica "
        + 'where datasetid = "'
        + str(dataset_info.id)
        + '"'
        + delete_condition_and
        + " order by rowid",
    )
    replicas = res2.fetchall()
    for rep in replicas:
        replica_id = rep[0]
        dir_id = rep[2]
        del_time = rep[6]
        key_id = rep[7]
        if del_time > 0 and not args.show_deleted:
            continue

        flags = ReplicaFlags(
            deleted=del_time > 0,
            encrypted=key_id > 0,
            accuracy=False,
            archive=dirs_archived.get(dir_id, False),
            embedded=False,
        )

        if dataset_info.file_format in ("ADIOS", "HDF5"):
            res3 = sql_execute(
                cur,
                f"select rowid from accuracy where replicaid = {replica_id} order by rowid",
            )
            if res3.fetchall():
                flags.accuracy = True

        if dataset_info.file_format in ("IMAGE", "TEXT"):
            res3 = sql_execute(
                cur,
                f"select fileid from repfiles where replicaid = {replica_id} order by fileid",
            )
            if res3.fetchall():
                flags.embedded = True

        replica_info = ReplicaInfo(
            id=replica_id,
            host_id=rep[1],
            dir_id=dir_id,
            archive_id=rep[3],
            name=rep[4],
            mod_time=rep[5],
            del_time=del_time,
            key_id=key_id,
            size=rep[8],
            flags=flags,
        )

        if dataset_info.file_format == "IMAGE":
            res3 = sql_execute(
                cur,
                'select rowid, x, y from resolution where replicaid = "' + str(replica_id) + '"' + " order by rowid",
            )
            res = res3.fetchall()
            if len(res) > 0:
                replica_info.resolution = ResolutionInfo(x=res[0][1], y=res[0][2])

        if args.list_files:
            res3 = sql_execute(
                cur,
                "select file.name, file.lenorig, file.lencompressed, file.modtime, file.checksum "
                "from file join repfiles on file.fileid = repfiles.fileid "
                f"where repfiles.replicaid = {replica_id} order by file.fileid",
            )
            file_rows = res3.fetchall()
            for file_row in file_rows:
                replica_info.files.append(
                    FileInfo(
                        name=file_row[0],
                        len_orig=file_row[1],
                        len_compressed=file_row[2],
                        mod_time=file_row[3],
                        checksum=file_row[4],
                    )
                )

        dataset_info.replicas.append(replica_info)

    return dataset_info


def collect_info(args: argparse.Namespace, cur: sqlite3.Cursor) -> InfoResult:  # pylint: disable=too-many-locals
    res = sql_execute(cur, "select id, name, version, modtime from info")
    info_row = res.fetchone()
    info_data = InfoResult(
        archive=ArchiveInfo(
            id=info_row[0],
            name=info_row[1],
            version=info_row[2],
            mod_time=info_row[3],
        )
    )

    #
    # Hosts and directories
    #
    delete_condition_where = " where deltime = 0"
    delete_condition_and = " and deltime = 0"
    if args.show_deleted:
        delete_condition_where = ""
        delete_condition_and = ""
    res = sql_execute(
        cur,
        "select rowid, hostname, longhostname from host" + delete_condition_where + " order by rowid",
    )
    hosts = res.fetchall()
    dirs_archived: dict[int, bool] = {}
    for host in hosts:
        host_info = HostInfo(
            id=host[0],
            hostname=host[1],
            long_hostname=host[2],
        )
        res2 = sql_execute(
            cur,
            "select rowid, name, modtime, deltime from directory "
            + 'where hostid = "'
            + str(host[0])
            + '"'
            + delete_condition_and
            + " order by rowid",
        )
        dirs = res2.fetchall()
        for dirrec in dirs:
            if dirrec[3] == 0 or args.show_deleted:
                # check if it's archive dir
                res3 = sql_execute(
                    cur,
                    f"select rowid, tarname, system from archive where dirid = {dirrec[0]} order by rowid",
                )
                archs = res3.fetchall()
                archive_entries: list[ArchiveEntry] = []
                for arch in archs:
                    archive_entries.append(ArchiveEntry(id=arch[0], tar_name=arch[1], system=arch[2]))
                has_archive = bool(archive_entries)
                dirs_archived[dirrec[0]] = has_archive
                host_info.directories.append(
                    DirectoryInfo(
                        id=dirrec[0],
                        name=dirrec[1],
                        mod_time=dirrec[2],
                        del_time=dirrec[3],
                        archives=archive_entries,
                        has_archive=has_archive,
                    )
                )
        info_data.hosts.append(host_info)

    #
    # Keys
    #
    res = sql_execute(cur, "select rowid, keyid from key order by rowid")
    keys = res.fetchall()
    for key in keys:
        info_data.keys.append(KeyInfo(id=key[0], key=key[1]))

    #
    # Time Series
    #
    res = sql_execute(cur, "select tsid, name from timeseries order by tsid")
    timeseries = res.fetchall()
    for ts in timeseries:
        ts_info = TimeSeriesInfo(id=ts[0], name=ts[1])
        res = sql_execute(
            cur,
            "select rowid, uuid, name, modtime, deltime, fileformat from dataset "
            f"where tsid = {ts[0]} " + delete_condition_and,
        )
        datasets = res.fetchall()
        for dataset in datasets:
            ts_info.datasets.append(info_dataset(args, dataset, cur, delete_condition_and, dirs_archived))
        info_data.time_series.append(ts_info)

    #
    # Datasets
    #
    res = sql_execute(
        cur,
        "select rowid, uuid, name, modtime, deltime, fileformat from dataset "
        "where tsid = 0 " + delete_condition_and + " order by rowid",
    )
    datasets = res.fetchall()
    for dataset in datasets:
        info_data.datasets.append(info_dataset(args, dataset, cur, delete_condition_and, dirs_archived))

    return info_data


def format_info_dataset_lines(  # pylint: disable=too-many-locals
    dataset_info: DatasetInfo, args: argparse.Namespace
) -> list[str]:
    lines = []
    time_str = timestamp_to_str(dataset_info.mod_time)
    dataset_line = f"    {dataset_info.uuid}  {dataset_info.file_format:5}  {time_str}   {dataset_info.name}"
    if dataset_info.del_time > 0:
        dataset_line += f"  - deleted {timestamp_to_str(dataset_info.del_time)}"
    lines.append(dataset_line)

    if not args.list_replicas and not args.list_files:
        return lines

    for replica_info in dataset_info.replicas:
        flags = replica_info.flags
        flag_del = "D" if flags.deleted else "-"
        flag_encrypted = "k" if flags.encrypted else "-"
        flag_accuracy = "a" if flags.accuracy else "-"
        flag_archive = "A" if flags.archive else "-"
        flag_remote = "e" if flags.embedded else "r"
        replica_line = (
            f"  {replica_info.id:>7} {flag_remote}{flag_encrypted}{flag_accuracy}{flag_archive}{flag_del} "
            f"{replica_info.dir_id}"
        )
        if replica_info.archive_id > 0:
            replica_line += f".{replica_info.archive_id}"

        if dataset_info.file_format == "IMAGE" and replica_info.resolution is not None:
            res = replica_info.resolution
            resolution_text = f" {res.x} x {res.y}".rjust(14)
        else:
            resolution_text = " ".rjust(14)
        replica_line += f"{resolution_text}"

        replica_line += f" {sizeof_fmt(replica_info.size):>11}  {timestamp_to_str(replica_info.mod_time)}"
        replica_line += f"      {replica_info.name}"
        if flags.deleted:
            replica_line += f"  - deleted {timestamp_to_str(replica_info.del_time)}"
        lines.append(replica_line)

        if not args.list_files:
            continue

        for file_info in replica_info.files:
            if replica_info.key_id > 0:
                prefix = " " * 28 + f"k{replica_info.key_id:<3}"
            else:
                prefix = " " * 32
            file_line = prefix + f"{sizeof_fmt(file_info.len_compressed):>11}  {timestamp_to_str(file_info.mod_time)}"
            if args.show_checksum:
                file_line += f"         {file_info.checksum}  {file_info.name}"
            else:
                file_line += f"         {file_info.name}"
            lines.append(file_line)

    return lines


def format_info(info_data: InfoResult, args: argparse.Namespace) -> str:
    lines = []
    archive_info = info_data.archive
    created_time = timestamp_to_str(archive_info.mod_time)
    lines.append(f"{archive_info.name}, version {archive_info.version}, created on {created_time}")
    lines.append("")

    lines.append("Hosts and directories:")
    for host_info in info_data.hosts:
        lines.append(f"  {host_info.hostname}   longhostname = {host_info.long_hostname}")
        for dir_info in host_info.directories:
            archive_system = "  "
            if dir_info.archives:
                archive_system = f"  - Archive: {dir_info.archives[0].system}"
            lines.append(f"     {dir_info.id}. {dir_info.name}{archive_system}")
            for archive_entry in dir_info.archives:
                tar_name = archive_entry.tar_name if archive_entry.tar_name else "."
                lines.append(f"       {dir_info.id}.{archive_entry.id} {tar_name}")
    lines.append("")

    if info_data.keys:
        lines.append("Encryption keys:")
        for key_info in info_data.keys:
            lines.append(f"  k{key_info.id}. {key_info.key}")
        lines.append("")

    if info_data.time_series:
        lines.append("Time-series and their datasets:")
        for ts_info in info_data.time_series:
            lines.append(f"  {ts_info.name}")
            for dataset_info in ts_info.datasets:
                lines.extend(format_info_dataset_lines(dataset_info, args))
        lines.append("")

    if info_data.datasets:
        lines.append("Other Datasets:")
        for dataset_info in info_data.datasets:
            lines.extend(format_info_dataset_lines(dataset_info, args))

    return "\n".join(lines)


def print_info(info_data: InfoResult, args: argparse.Namespace):
    output_text = format_info(info_data, args)
    if output_text:
        print(output_text)


def _resolve_campaign_file_name(archive: str, campaign_store: str | None) -> str:
    if campaign_store is None:
        user_options = Config()
        campaign_store = user_options.campaign_store_path
    if campaign_store is not None:
        while campaign_store.endswith("/"):
            campaign_store = campaign_store[:-1]
        if not isdir(campaign_store):
            print(
                "ERROR: Campaign directory " + campaign_store + " does not exist",
                flush=True,
            )
            sys.exit(1)

    campaign_file_name = archive
    if not campaign_file_name.endswith(".aca"):
        campaign_file_name += ".aca"
    if not exists(campaign_file_name) and not campaign_file_name.startswith("/") and campaign_store is not None:
        campaign_file_name = campaign_store + "/" + campaign_file_name
    return campaign_file_name


def info(  # pylint: disable=too-many-arguments,too-many-positional-arguments,unused-argument
    args_or_archive,
    cur: sqlite3.Cursor | None = None,
    list_replicas: bool = False,
    list_files: bool = False,
    show_deleted: bool = False,
    show_checksum: bool = False,
    hostname: str | None = None,
    campaign_store: str | None = None,
    keyfile: str | None = None,
) -> InfoResult:
    if isinstance(args_or_archive, argparse.Namespace):
        if cur is None:
            raise ValueError("info requires a cursor when args are provided")
        return collect_info(args_or_archive, cur)
    campaign_file_name = _resolve_campaign_file_name(str(args_or_archive), campaign_store)
    if not exists(campaign_file_name):
        print(f"ERROR: archive {campaign_file_name} does not exist")
        sys.exit(1)
    args = argparse.Namespace(
        list_replicas=list_replicas,
        list_files=list_files,
        show_deleted=show_deleted,
        show_checksum=show_checksum,
    )
    con = sqlite3.connect(campaign_file_name)
    cur = con.cursor()
    info_data = collect_info(args, cur)
    cur.close()
    con.close()
    return info_data


def campaign_info(filename):
    info_data = info(filename)
    datasets = []
    for ts_info in info_data.time_series:
        datasets.extend(ts_info.datasets)
    datasets.extend(info_data.datasets)
    results = []
    for dataset_info in datasets:
        results.append(
            {
                "uuid": dataset_info.uuid,
                "type": dataset_info.file_format,
                "path": dataset_info.name,
            }
        )
    return results
