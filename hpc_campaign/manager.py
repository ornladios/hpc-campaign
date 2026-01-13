#!/usr/bin/env python3

import argparse
import glob
import sqlite3
import zlib
import uuid
import nacl.secret
import nacl.utils
import csv
from dateutil.parser import parse
from hashlib import sha1
from os import chdir, getcwd, remove, stat
from os.path import exists, isdir, basename, join
from pathlib import Path
from PIL import Image
from re import sub
from socket import getfqdn
from time import time_ns, sleep

from .key import read_key
from .config import ACA_VERSION
from .utils import (
    timestamp_to_str,
    SQLCommit,
    SQLExecute,
    SQLErrorList,
    get_folder_size,
    sizeof_fmt,
)
from .upgrade import UpgradeACA
from .taridx import TARTYPES
from .hdf5_metadata import copy_hdf5_file_without_data, IsHDF5Dataset
from .manager_args import ArgParser

CURRENT_TIME = time_ns()


def CheckCampaignStore(args):
    if args.campaign_store is not None and not isdir(args.campaign_store):
        print(
            "ERROR: Campaign directory " + args.campaign_store + " does not exist",
            flush=True,
        )
        exit(1)


def CheckLocalCampaignDir(args):
    if not isdir(args.LocalCampaignDir):
        print(
            "ERROR: Shot campaign data '"
            + args.LocalCampaignDir
            + "' does not exist. Run this command where the code was executed.",
            flush=True,
        )
        exit(1)


def parse_date_to_utc(date, fmt=None):
    if fmt is None:
        fmt = "%Y-%m-%d %H:%M:%S %z"  # Defaults to : 2022-08-31 07:47:30 -0000
    get_date_obj = parse(str(date))
    return get_date_obj.timestamp()


def IsADIOSDataset(dataset):
    if not isdir(dataset):
        return False
    if not exists(dataset + "/" + "md.idx"):
        return False
    if not exists(dataset + "/" + "data.0"):
        return False
    return True


def compressBytes(b: bytes) -> tuple[bytes, int, int, str]:
    compObj = zlib.compressobj()
    compressed = bytearray()
    len_orig = len(b)
    len_compressed = 0
    checksum = sha1(b)

    cBlock = compObj.compress(b)
    compressed += cBlock
    len_compressed += len(cBlock)

    cBlock = compObj.flush()
    compressed += cBlock
    len_compressed += len(cBlock)

    return bytes(memoryview(compressed)), len_orig, len_compressed, checksum.hexdigest()


def compressFile(f) -> tuple[bytes, int, int, str]:
    compObj = zlib.compressobj()
    compressed = bytearray()
    blocksize = 1073741824  # 1GB #1024*1048576
    len_orig = 0
    len_compressed = 0
    checksum = sha1()
    block = f.read(blocksize)
    while block:
        len_orig += len(block)
        cBlock = compObj.compress(block)
        compressed += cBlock
        len_compressed += len(cBlock)
        checksum.update(block)
        block = f.read(blocksize)
    cBlock = compObj.flush()
    compressed += cBlock
    len_compressed += len(cBlock)

    return bytes(memoryview(compressed)), len_orig, len_compressed, checksum.hexdigest()


def decompressBuffer(buf: bytearray):
    data = zlib.decompress(buf)
    return data


def encryptBuffer(args: argparse.Namespace, buf: bytes):
    if args.encryption_key:
        box = nacl.secret.SecretBox(args.encryption_key)
        nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
        e = box.encrypt(buf, nonce)
        print("Encoded buffer size: ", len(e))
        return e
    else:
        return buf


def lastrowid_or_zero(curDS: sqlite3.Cursor) -> int:
    rowID = curDS.lastrowid
    if not rowID:
        rowID = 0
    return rowID


def AddFileToArchive(
    args: argparse.Namespace,
    filename: str,
    cur: sqlite3.Cursor,
    repID: int,
    mt: float = 0.0,
    filename_as_recorded: str = "",
    compress: bool = True,
    content: bytes = bytes(),
    indent: str = "",
):
    if compress:
        compressed = 1
        if content:
            compressed_data, len_orig, len_compressed, checksum = compressBytes(content)
        else:
            try:
                with open(filename, "rb") as f:
                    compressed_data, len_orig, len_compressed, checksum = compressFile(
                        f
                    )

            except IOError:
                print(f"{indent}ERROR While reading file {filename}")
                return
    else:
        compressed = 0
        if content:
            compressed_data = content
        else:
            try:
                with open(filename, "rb") as f:
                    compressed_data = f.read()
            except IOError:
                print(f"{indent}ERROR While reading file {filename}")
                return
        len_orig = len(compressed_data)
        len_compressed = len_orig
        checksum = sha1(compressed_data).hexdigest()

    encrypted_data = encryptBuffer(args, compressed_data)

    if mt == 0.0:
        statres = stat(filename)
        mt = statres.st_mtime_ns

    if len(filename_as_recorded) == 0:
        filename_as_recorded = filename

    SQLExecute(
        cur,
        "insert into file "
        "(replicaid, name, compression, lenorig, lencompressed, modtime, checksum, data) "
        "values (?, ?, ?, ?, ?, ?, ?, ?) "
        "on conflict (replicaid, name) do update "
        "set compression = ?, lenorig = ?, lencompressed = ?, modtime = ?, checksum = ?, data = ?",
        (
            repID,
            filename_as_recorded,
            compressed,
            len_orig,
            len_compressed,
            mt,
            checksum,
            encrypted_data,
            compressed,
            len_orig,
            len_compressed,
            mt,
            checksum,
            encrypted_data,
        ),
    )


def AddReplicaToArchive(
    args: argparse.Namespace,
    hostID: int,
    dirID: int,
    archiveID: int,
    keyID: int,
    dataset: str,
    cur: sqlite3.Cursor,
    datasetid: int,
    mt: float,
    size: int,
    indent: str = "",
) -> int:
    print(f"{indent}Add replica {dataset} to archive")
    print(
        f"{indent}AddReplicaToArchive(host={hostID}, dir={dirID}, archive={archiveID}, key={keyID}, name={dataset}"
        f" dsid={datasetid}, time={mt}, size={size})"
    )
    curDS = SQLExecute(
        cur,
        "insert into replica (datasetid, hostid, dirid, archiveid, name, modtime, deltime, keyid, size) "
        "values  (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "on conflict (datasetid, hostid, dirid, archiveid, name) "
        "do update set modtime = ?, deltime = ?, keyid = ?, size = ? "
        "returning rowid",
        (
            datasetid,
            hostID,
            dirID,
            archiveID,
            dataset,
            mt,
            0,
            keyID,
            size,
            mt,
            0,
            keyID,
            size,
        ),
    )
    rowID = curDS.fetchone()[0]
    print(f"{indent}  Replica rowid = {rowID}")
    return rowID


def AddDatasetToArchive(
    args: argparse.Namespace,
    name: str,
    cur: sqlite3.Cursor,
    uniqueID: str,
    format: str,
    mt: float,
    indent: str = "",
) -> int:
    print(f"{indent}Add dataset {name} to archive")
    curDS = SQLExecute(
        cur,
        "insert into dataset (name, uuid, modtime, deltime, fileformat, tsid, tsorder) "
        "values  (?, ?, ?, ?, ?, ?, ?) "
        "on conflict (name) do update set deltime = ? "
        "returning rowid",
        (name, uniqueID, mt, 0, format, 0, 0, 0),
    )
    datasetID = curDS.fetchone()[0]
    return datasetID


def AddResolutionToArchive(
    args: argparse.Namespace,
    repID: int,
    x: int,
    y: int,
    cur: sqlite3.Cursor,
    indent: str = "",
) -> int:
    print(f"{indent}Add resolution {x} {y} for replica {repID} to archive")
    curDS = SQLExecute(
        cur,
        "insert into resolution (replicaid, x, y) "
        "values  (?, ?, ?) "
        "on conflict (replicaid) do update set x = ?, y = ? returning rowid",
        (repID, x, y, x, y),
    )
    rowID = curDS.fetchone()[0]
    return rowID


def ProcessDatasets(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    hostID: int,
    dirID: int,
    keyID: int,
    dirpath: str,
    location: str,
):
    for entry in args.files:
        dataset = entry
        if args.name is not None:
            dataset = args.name
        uniqueID = uuid.uuid3(uuid.NAMESPACE_URL, location + "/" + entry).hex
        dsID = 0

        if args.remote_data:
            filesize = 0
            if args.s3_datetime:
                mt = parse_date_to_utc(args.s3_datetime)
            else:
                mt = 0
        else:
            statres = stat(entry)
            mt = statres.st_mtime_ns
            filesize = statres.st_size

        if args.remote_data:
            dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "ADIOS", mt)
            repID = AddReplicaToArchive(
                args,
                hostID,
                dirID,
                0,
                keyID,
                entry,
                cur,
                dsID,
                mt,
                filesize,
                indent="  ",
            )
        elif IsADIOSDataset(entry):
            dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "ADIOS", mt)
            filesize = get_folder_size(entry)
            repID = AddReplicaToArchive(
                args,
                hostID,
                dirID,
                0,
                keyID,
                entry,
                cur,
                dsID,
                mt,
                filesize,
                indent="  ",
            )
            cwd = getcwd()
            chdir(entry)
            mdFileList = glob.glob("*md.*")
            profileList = glob.glob("profiling.json")
            files = mdFileList + profileList
            for f in files:
                AddFileToArchive(args, f, cur, repID)
            chdir(cwd)
        elif IsHDF5Dataset(entry):
            mdfilename = "/tmp/md_" + basename(entry)
            copy_hdf5_file_without_data(entry, mdfilename)
            dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "HDF5", mt)
            repID = AddReplicaToArchive(
                args,
                hostID,
                dirID,
                0,
                keyID,
                entry,
                cur,
                dsID,
                mt,
                filesize,
                indent="  ",
            )
            AddFileToArchive(args, mdfilename, cur, repID, mt, basename(entry))
            remove(mdfilename)
        else:
            print(
                f"WARNING: Dataset {dataset} is neither an ADIOS nor an HDF5 dataset. Skip"
            )


def ProcessTextFiles(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    hostID: int,
    dirID: int,
    keyID: int,
    dirpath: str,
    location: str,
):
    for entry in args.files:
        print(f"Process entry {entry}:")
        dataset = entry
        if args.name is not None:
            dataset = args.name
        statres = stat(entry)
        ct = statres.st_mtime_ns
        filesize = statres.st_size
        uniqueID = uuid.uuid3(uuid.NAMESPACE_URL, location + "/" + entry).hex
        dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "TEXT", ct)
        repID = AddReplicaToArchive(
            args, hostID, dirID, 0, keyID, entry, cur, dsID, ct, filesize, indent="  "
        )
        if args.store:
            AddFileToArchive(args, entry, cur, repID, ct, basename(entry))


def ProcessImage(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    hostID: int,
    dirID: int,
    keyID: int,
    dirpath: str,
    location: str,
):
    dataset = args.file
    if args.name is not None:
        dataset = args.name

    statres = stat(args.file)
    mt = statres.st_mtime_ns
    filesize = statres.st_size
    uniqueID = uuid.uuid3(uuid.NAMESPACE_URL, location + "/" + args.file).hex
    print(f"Process image {location}/{args.file}")

    img = Image.open(args.file)
    imgres = img.size

    dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "IMAGE", mt, indent="  ")
    repID = AddReplicaToArchive(
        args, hostID, dirID, 0, keyID, args.file, cur, dsID, mt, filesize, indent="  "
    )
    AddResolutionToArchive(args, repID, imgres[0], imgres[1], cur, indent="  ")

    if args.store or args.thumbnail is not None:
        imgsuffix = Path(args.file).suffix
        if args.store:
            print("Storing the image in the archive")
            resname = f"{imgres[0]}x{imgres[1]}{imgsuffix}"
            AddFileToArchive(
                args, args.file, cur, repID, mt, resname, compress=False, indent="  "
            )

        else:
            print(f"  Make thumbnail image with resolution {args.thumbnail}")
            img.thumbnail(args.thumbnail)
            imgres = img.size
            resname = f"{imgres[0]}x{imgres[1]}{imgsuffix}"
            now = time_ns()
            thumbfilename = "/tmp/" + basename(resname)
            img.save(thumbfilename)
            statres = stat(thumbfilename)
            mt = statres.st_mtime_ns
            filesize = statres.st_size
            thumbrepID = AddReplicaToArchive(
                args,
                hostID,
                dirID,
                0,
                keyID,
                join("thumbnails", args.file),
                cur,
                dsID,
                now,
                filesize,
                indent="  ",
            )
            AddFileToArchive(
                args,
                thumbfilename,
                cur,
                thumbrepID,
                now,
                resname,
                compress=False,
                indent="  ",
            )
            AddResolutionToArchive(
                args, thumbrepID, imgres[0], imgres[1], cur, indent="  "
            )
            remove(thumbfilename)


def ArchiveDataset(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
):
    # Find dataset
    res = SQLExecute(
        cur, f'select rowid, fileformat from dataset where name = "{args.name}"'
    )
    rows = res.fetchall()
    if len(rows) == 0:
        raise Exception(f"Dataset not found: {args.name} ")

    datasetid: int = rows[0][0]
    format: str = rows[0][1]

    # Find archive dir
    res = SQLExecute(
        cur, f"select hostid, name from directory where rowid = {args.dirid}"
    )
    rows = res.fetchall()
    if len(rows) == 0:
        raise Exception(f"Directory ID not found: {args.dirid} ")

    hostID: int = rows[0][0]
    dir_name: str = rows[0][1]

    if args.archiveid is None:
        res = SQLExecute(cur, f"select rowid from archive where dirid = {args.dirid}")
        rows = res.fetchall()
        if len(rows) == 0:
            raise Exception(
                f"Directory {dir_name} with ID {args.dirid} is not an archival storage directory"
            )
        archiveID = rows[0][0]
    else:
        res = SQLExecute(
            cur, f"select rowid, dirid from archive where rowid = {args.archiveid}"
        )
        rows = res.fetchall()
        if len(rows) == 0:
            raise Exception(
                f"Archive ID {args.archiveid} is not found in the archive list"
            )
        archiveID = args.archiveid
        dirID = rows[0][1]
        if dirID != args.dirid:
            raise Exception(
                f"Archive ID {args.archiveid} belongs to dir ID {dirID}, not to {args.dirid}"
            )

    # Check replicas of dataset and see if there is conflict (need --replica option)
    orig_repID: int = args.replica
    if args.replica is None:
        res = SQLExecute(
            cur,
            f"select rowid, archiveid, deltime from replica where datasetid = {datasetid}",
        )
        rows = res.fetchall()
        delrows = []
        live_nonarch_rows = []
        live_arch_rows = []
        for row in rows:
            if row[2] == 0:
                if row[1] == 0:
                    live_nonarch_rows.append(row)
                else:
                    live_arch_rows.append(row)
            else:
                delrows.append(row)
        if len(live_nonarch_rows) > 1:
            raise Exception(
                f"There are {len(live_nonarch_rows)} non-deleted, not-in-archive, replicas for this dataset. "
                f"Use --replica to identify which is archived now. Replicas: {[r[0] for r in live_nonarch_rows]}"
            )
        elif len(live_nonarch_rows) + len(live_arch_rows) == 0:
            if format == "ADIOS" or format == "HDF5":
                raise Exception(
                    f"There are no replicas for a {format} dataset. Cannot archive without "
                    "access to the embedded metadata files of a replica"
                )
            if len(delrows) == 1:
                orig_repID = delrows[0][0]
            else:
                raise Exception(
                    f"There are no replicas but {len(delrows)} deleted replicas for this {format} dataset. "
                    "Use --replica to identify which deleted replica is archived."
                    f"Deleted replicas: {[r[0] for r in delrows]}"
                )
        else:
            if len(live_nonarch_rows) > 0:
                orig_repID = live_nonarch_rows[0][0]
            elif len(live_arch_rows) > 1:
                raise Exception(
                    f"There are {len(live_arch_rows)} archived replicas for this dataset. "
                    f"Use --replica to identify which is archived now. Replicas: {[r[0] for r in live_arch_rows]}"
                )
            else:
                orig_repID = live_arch_rows[0][0]

    # get name and KeyID for selected replica
    res = SQLExecute(
        cur,
        f"select datasetid, name, modtime, keyid, size from replica where rowid = {orig_repID}",
    )
    row = res.fetchone()
    if datasetid != row[0]:
        res = SQLExecute(cur, f'select name from dataset where rowid = "{row[0]}"')
        wrong_dsname = res.fetchone()[0]
        raise Exception(f"Replica belongs to dataset {wrong_dsname}, not this dataset")
    replicaName: str = row[1]
    mt: int = row[2]
    keyID: int = row[3]
    filesize: int = row[4]

    # Create new replica for this dataset
    dsname = replicaName
    if args.newpath:
        dsname = args.newpath

    repID = AddReplicaToArchive(
        args,
        hostID,
        args.dirid,
        archiveID,
        keyID,
        dsname,
        cur,
        datasetid,
        mt,
        filesize,
        indent=indent,
    )

    # if replica has Resolution, copy that to new replica
    res = SQLExecute(cur, f"select x, y from resolution where replicaid = {orig_repID}")
    rows = res.fetchall()
    if len(rows) > 0:
        x = rows[0][0]
        y = rows[0][1]
        AddResolutionToArchive(args, repID, x, y, cur, indent=indent)

    # # if replica has Accuracy, copy that to new replica
    # res = SQLExecute(cur, f"select accuracy, norm, relative from accuracy where replicaid = {orig_repID}")
    # rows = res.fetchall()
    # if len(rows) > 0:
    #     accuracy = rows[0][0]
    #     norm = rows[0][1]
    #     relative = rows[0][2]
    #     AddAccuracyToArchive(args, repID, accuracy, norm, relative, cur)

    # if --move, delete the original replica but assign embedded files to archived replica
    # otherwise, make a copy of all embedded files
    if args.move:
        SQLExecute(
            cur, f"update file set replicaid = {repID} where replicaid = {orig_repID}"
        )
        DeleteReplica(args, cur, con, orig_repID, False, indent=indent)
    else:
        res = SQLExecute(
            cur,
            "select name, compression, lenorig, lencompressed, modtime, checksum, data "
            f"from file where replicaid = {orig_repID}",
        )
        files = res.fetchall()
        print(
            f"{indent}Copying {len(files)} files from original replica to archived one"
        )
        for f in files:
            SQLExecute(
                cur,
                "insert into file values (?, ?, ?, ?, ?, ?, ?, ?) "
                "on conflict (replicaid, name) do update set "
                "compression = ?, lenorig = ?, lencompressed = ?, modtime = ?, checksum = ?, data = ?",
                (
                    repID,
                    f[0],
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    f[5],
                    f[6],
                ),
            )

    SQLCommit(con)
    return repID


def AddTimeSeries(
    args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection
):
    if args.remove:
        res = SQLExecute(cur, f'select tsid from timeseries where name = "{args.name}"')
        rows = res.fetchall()
        if len(rows) > 0:
            tsID = rows[-1][0]
            print(f"Remove {args.name} from time-series but leave datasets alone")
            res = SQLExecute(cur, f'delete from timeseries where name = "{args.name}"')
            curDS = SQLExecute(
                cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{tsID}"'
            )
        else:
            print(f"Time series {args.name} was not found")
        SQLCommit(con)
        return

    print(f"Add {args.name} to time-series")
    # we need to know if it already exists
    ts_exists = False
    res = SQLExecute(cur, f'select tsid from timeseries where name = "{args.name}"')
    rows = res.fetchall()
    if len(rows) > 0:
        ts_exists = True

    # insert/update timeseries
    curTS = SQLExecute(
        cur,
        "insert into timeseries (name) values  (?) "
        "on conflict (name) do update set name = ? returning rowid",
        (args.name, args.name),
    )
    tsID = curTS.fetchone()[0]
    print(f"Time series ID = {tsID}, already existed = {ts_exists}")

    # if --replace, "delete" the existing dataset connections
    tsorder = 0
    if args.replace:
        curDS = SQLExecute(
            cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{tsID}"'
        )
    else:
        # otherwise we need to know how many datasets we have already
        res = SQLExecute(
            cur, f"select tsorder from dataset where tsid = {tsID} order by tsorder"
        )
        rows = res.fetchall()
        if len(rows) > 0:
            tsorder = rows[-1][0] + 1

    for dsname in args.dataset:
        curDS = SQLExecute(
            cur,
            f"update dataset set tsid = {tsID}, tsorder = {tsorder} "
            + f'where name = "{dsname}" returning rowid, name',
        )
        ret = curDS.fetchone()
        if ret is None:
            print(f"    {dsname}  Error: dataset is not in the database, skipping")
        else:
            rowID = ret[0]
            name = ret[1]
            print(f"    {name} (dataset {rowID}) tsorder = {tsorder}")
            tsorder += 1

    SQLCommit(con)


def GetHostName(args: argparse.Namespace):
    if args.s3_endpoint:
        longhost = args.s3_endpoint
    else:
        longhost = getfqdn()
        if longhost.startswith("login"):
            longhost = sub("^login[0-9]*\\.", "", longhost)
        if longhost.startswith("batch"):
            longhost = sub("^batch[0-9]*\\.", "", longhost)

    if args.hostname is None:
        shorthost = longhost.split(".")[0]
    else:
        shorthost = args.hostname
    return longhost, shorthost


def AddHostName(
    longHostName,
    shortHostName,
    cur: sqlite3.Cursor,
    default_protocol: str = "",
    indent: str = "",
) -> int:
    res = SQLExecute(
        cur, 'select rowid from host where hostname = "' + shortHostName + '"'
    )
    row = res.fetchone()
    if row is not None:
        hostID = row[0]
        print(f"{indent}Found host {shortHostName} in database, rowid = {hostID}")
    else:
        curHost = SQLExecute(
            cur,
            "insert into host values (?, ?, ?, ?, ?)",
            (shortHostName, longHostName, CURRENT_TIME, 0, default_protocol),
        )
        hostID = lastrowid_or_zero(curHost)
        print(
            f"{indent}Inserted host {shortHostName} into database, rowid = {hostID}, longhostname = {longHostName}"
        )
    return hostID


def AddDirectory(hostID: int, path: str, cur: sqlite3.Cursor, indent: str = "") -> int:
    res = SQLExecute(
        cur,
        "select rowid from directory where hostid = "
        + str(hostID)
        + ' and name = "'
        + path
        + '"',
    )
    row = res.fetchone()
    if row is not None:
        dirID = row[0]
        print(
            f"{indent}Found directory {path} with hostID {hostID} in database, rowid = {dirID}"
        )
    else:
        curDirectory = SQLExecute(
            cur,
            "insert into directory values (?, ?, ?, ?)",
            (hostID, path, CURRENT_TIME, 0),
        )
        dirID = lastrowid_or_zero(curDirectory)
        print(f"{indent}Inserted directory {path} into database, rowid = {dirID}")
    return dirID


def AddKeyID(key_id: str, cur: sqlite3.Cursor) -> int:
    if key_id:
        res = SQLExecute(cur, 'select rowid from key where keyid = "' + key_id + '"')
        row = res.fetchone()
        if row is not None:
            keyID = row[0]
            print(f"Found key {key_id} in database, rowid = {keyID}")
        else:
            cmd = f'insert into key values ("{(key_id)}")'
            curKey = SQLExecute(cur, cmd)
            # curKey = SQLExecute(cur,"insert into key values (?)", (key_id))
            keyID = lastrowid_or_zero(curKey)
            print(f"Inserted key {key_id} into database, rowid = {keyID}")
        return keyID
    else:
        return 0  # an invalid row id


def ArchiveIdxReplica(
    dsname: str,
    dirID: int,
    archiveID: int,
    replicaID: int,
    entries: dict[str, list[int]],
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
):
    # Archive replica
    args = argparse.Namespace()
    args.name = dsname
    args.dirid = dirID
    args.archiveid = archiveID
    args.replica = replicaID
    args.move = False
    args.newpath = ""

    archived_replicaID = ArchiveDataset(args, cur, con, indent=indent + "  ")
    if archived_replicaID > 0:
        for fname, info in entries.items():
            # add replica and register offsets
            offset = info[0]
            data_offset = info[1]
            size = info[2]
            SQLExecute(
                cur,
                "insert into archiveidx (archiveid, replicaid, filename, offset, offset_data, size)"
                " values  (?, ?, ?, ?, ?, ?) "
                "on conflict (archiveid, replicaid, filename) do update set offset = ?, offset_data = ?, size = ?",
                (
                    archiveID,
                    archived_replicaID,
                    fname,
                    offset,
                    data_offset,
                    size,
                    offset,
                    data_offset,
                    size,
                ),
            )
        SQLCommit(con)


def ArchiveIdx(
    args: argparse.Namespace,
    archiveID: int,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
):
    try:
        csvfile = open(args.tarfileidx, newline="")
        reader = csv.reader(csvfile)
    except FileNotFoundError:
        raise Exception(f"File '{args.tarfileidx}' not found.")
    except Exception as e:
        raise Exception(f"Error occurred when opening '{args.tarfileidx}': {e}")

    # Find archive dir
    res = SQLExecute(
        cur, f"select dirid, tarname from archive where rowid = {archiveID}"
    )
    rows = res.fetchall()
    if len(rows) == 0:
        raise Exception(f"Archive ID not found: {archiveID}")

    dirID: int = rows[0][0]
    tarname: str = rows[0][1]
    if not tarname:
        raise Exception(f"Directory.Archive {dirID}.{archiveID} is not a TAR archive.")

    line_number = 0
    readnext = True
    while True:
        if readnext:
            row = next(reader, None)
            if row is not None:
                line_number += 1
        else:
            readnext = True
        if row is None:
            break

        # print(f"{line_number}: {row}")
        if len(row) != 5:
            print(
                f"{indent}  Warning: Line {line_number} in {args.tarfileidx} does not have 5 elements. "
                f"Found {len(row)}. Skip."
            )
            continue
        entrytype = int(row[0].strip())
        if (
            entrytype != 0 and entrytype != 5
        ):  # process only Regular and Directory entries
            continue
        offset = int(row[1].strip())
        data_offset = int(row[2].strip())
        size = int(row[3].strip())
        archivename = row[4].strip()

        # find (first non-deleted) replica of dataset that matches the name
        res = SQLExecute(
            cur,
            f"select rowid, datasetid, hostid, dirid, size from replica where "
            f"name = '{archivename}' and deltime = 0",
        )
        replica_row = res.fetchone()
        if replica_row is None:
            if args.verbose:
                print(f"{indent}  No suitable replica of {archivename} found. Skip")
            continue
        replicaID: int = replica_row[0]
        replica_datasetID: int = replica_row[1]
        replica_hostID: int = replica_row[2]
        replica_dirID: int = replica_row[3]
        replica_size: int = replica_row[4]
        print(
            f"{indent}Replica id = {replicaID} on host {replica_hostID}, dir {replica_dirID}"
        )

        # find dataset of this replica
        res = SQLExecute(
            cur,
            f"select name, fileformat from dataset where rowid = '{replica_datasetID}'",
        )
        dsrow = res.fetchone()
        dsname = dsrow[0]
        format: str = dsrow[1]
        print(f"{indent}  Dataset {replica_datasetID:<5} {dsname}")

        entries: dict = {"": [offset, data_offset, size]}
        if entrytype == TARTYPES["reg"]:
            if size == replica_size:
                ArchiveIdxReplica(
                    dsname,
                    dirID,
                    archiveID,
                    replicaID,
                    entries,
                    cur,
                    con,
                    indent=indent + "  ",
                )
            else:
                print(
                    f"{indent}  The replica size ({replica_size}) does not match the size "
                    f"in the TAR file ({size}). Skip"
                )

        elif entrytype == TARTYPES["dir"] and format == "ADIOS":
            # it's a directory for ADIOS datasets, process its entries
            while True:
                row = next(reader, None)
                if row is None:
                    break
                line_number += 1
                entrytype = int(row[0].strip())
                offset = int(row[1].strip())
                data_offset = int(row[2].strip())
                size = int(row[3].strip())
                entryname: str = row[4].strip()
                if not entryname.startswith(archivename):
                    break
                if entrytype == TARTYPES["reg"]:
                    # a file inside the ADIOS dataset
                    fname = entryname[len(archivename) + 1 :]
                    entries[fname] = [offset, data_offset, size]
            # we have a row unprocessed or None, skip reading at the beginning of the loop
            readnext = False
            ArchiveIdxReplica(
                dsname,
                dirID,
                archiveID,
                replicaID,
                entries,
                cur,
                con,
                indent=indent + "  ",
            )
    csvfile.close()


def AddArchivalStorage(
    args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection
):
    protocol = args.system.lower()
    if (
        protocol != "https"
        and protocol != "http"
        and protocol != "ftp"
        and protocol != "s3"
    ):
        protocol = ""

    print(
        f"Add archival storage host = {args.host}, directory = {args.directory}, archive system {args.system}"
    )
    print(
        f"                     tarfile = {args.tarfilename} taridx = {args.tarfileidx}"
    )

    hostID = AddHostName(args.longhostname, args.host, cur, protocol, indent="  ")
    dirID = AddDirectory(hostID, args.directory, cur, indent="  ")
    notes = None
    if args.note:
        try:
            with open(args.note, "rb") as f:
                notes = f.read()
        except IOError as e:
            print(f"WARNING: Failed to read notes from {args.notes}: {e.strerror}.")
            notes = None
    tarname = ""
    if args.tarfilename:
        tarname = args.tarfilename
        print(f"  Adding a TAR file: {tarname}")

    res = SQLExecute(
        cur,
        "select rowid from archive where dirid = "
        + str(dirID)
        + ' and tarname = "'
        + tarname
        + '"',
    )
    row = res.fetchone()
    if row is not None:
        archiveID = row[0]
        print(f"  Found archive already in the database, rowid = {archiveID}")
    else:
        curArchive = SQLExecute(
            cur,
            "insert into archive (dirid, tarname, system, notes) values  (?, ?, ?, ?) ",
            (dirID, tarname, args.system, notes),
        )
        archiveID = lastrowid_or_zero(curArchive)
        SQLCommit(con)

    if archiveID == 0:
        print(
            "  ERROR: Could not insert information into table 'archive' for some reason"
        )
    elif args.tarfileidx:
        ArchiveIdx(args, archiveID, cur, con, indent="  ")


def Update(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    longHostName, shortHostName = GetHostName(args)

    hostID = AddHostName(longHostName, shortHostName, cur)
    keyID = AddKeyID(args.encryption_key_id, cur)

    if args.remote_data and args.s3_bucket is not None:
        rootdir = args.s3_bucket
    else:
        rootdir = getcwd()

    dirID = AddDirectory(hostID, rootdir, cur)
    SQLCommit(con)

    if args.command == "dataset":
        ProcessDatasets(
            args, cur, hostID, dirID, keyID, longHostName + rootdir, rootdir
        )
    elif args.command == "text":
        ProcessTextFiles(
            args, cur, hostID, dirID, keyID, longHostName + rootdir, rootdir
        )
    elif args.command == "image":
        ProcessImage(args, cur, hostID, dirID, keyID, longHostName + rootdir, rootdir)

    SQLCommit(con)


def Create(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    print(f"Create new archive {args.CampaignFileName}")
    SQLExecute(cur, "create table info(id TEXT, name TEXT, version TEXT, modtime INT)")
    SQLCommit(con)
    SQLExecute(
        cur,
        "insert into info values (?, ?, ?, ?)",
        ("ACA", "ADIOS Campaign Archive", ACA_VERSION, CURRENT_TIME),
    )

    SQLExecute(cur, "create table key" + "(keyid TEXT PRIMARY KEY)")
    SQLExecute(
        cur,
        "create table host"
        + "(hostname TEXT PRIMARY KEY, longhostname TEXT, modtime INT, deltime INT, default_protocol TEXT)",
    )
    SQLExecute(
        cur,
        "create table directory"
        + "(hostid INT, name TEXT, modtime INT, deltime INT, PRIMARY KEY (hostid, name))",
    )
    SQLExecute(
        cur,
        "create table timeseries" + "(tsid INTEGER PRIMARY KEY, name TEXT UNIQUE)",
    )
    SQLExecute(
        cur,
        "create table dataset"
        + "(name TEXT, uuid TEXT, modtime INT, deltime INT, fileformat TEXT, tsid INT, tsorder INT"
        + ", PRIMARY KEY (name))",
    )
    SQLExecute(
        cur,
        "create table replica"
        + "(datasetid INT, hostid INT, dirid INT, archiveid INT, name TEXT, modtime INT, deltime INT"
        + ", keyid INT, size INT"
        + ", PRIMARY KEY (datasetid, hostid, dirid, archiveid, name))",
    )
    SQLExecute(
        cur,
        "create table file"
        + "(replicaid INT, name TEXT, compression INT, lenorig INT"
        + ", lencompressed INT, modtime INT, checksum TEXT, data BLOB"
        + ", PRIMARY KEY (replicaid, name))",
    )
    SQLExecute(
        cur,
        "create table accuracy"
        + "(replicaid INT, accuracy REAL, norm REAL, relative INT, PRIMARY KEY (replicaid))",
    )
    SQLExecute(
        cur,
        "create table resolution"
        + "(replicaid INT, x INT, y INT, PRIMARY KEY (replicaid))",
    )
    SQLExecute(
        cur,
        "create table archive"
        + "(dirid INT, tarname TEXT,system TEXT, notes BLOB, PRIMARY KEY (dirid, tarname))",
    )
    SQLExecute(
        cur,
        "create table archiveidx"
        + "(archiveid INT, replicaid INT, filename TEXT, offset INT, offset_data INT, size INT"
        + ", PRIMARY KEY (archiveid, replicaid, filename))",
    )
    SQLCommit(con)
    cur.close()
    con.close()
    while not exists(args.CampaignFileName):
        sleep(0.1)


def DeleteDatasetIfEmpty(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    datasetid: int,
    indent: str,
):
    print(f"{indent}Check if dataset {datasetid} still has replicas")
    res = SQLExecute(
        cur,
        "select rowid from replica "
        + f" where datasetid = {datasetid} and deltime = 0",
    )
    replicas = res.fetchall()
    if len(replicas) == 0:
        print("{indent}  Dataset without replicas found. Deleting.")
        SQLExecute(
            cur,
            f"update dataset set deltime = {CURRENT_TIME} "
            + f"where rowid = {datasetid}",
        )


def DeleteReplica(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    repid: int,
    delete_empty_dataset: bool,
    indent: str = "",
):
    print(f"{indent}Delete replica with id {repid}")
    res = SQLExecute(
        cur, "select datasetid, hostid, dirid from replica " + f"where rowid = {repid}"
    )
    replicas = res.fetchall()
    datasetid = 0
    for rep in replicas:
        datasetid = rep[0]
        SQLExecute(
            cur,
            f"update replica set deltime = {CURRENT_TIME} " + f"where rowid = {repid}",
        )
    if delete_empty_dataset:
        SQLExecute(cur, f"delete from file where replicaid = {repid}")
        DeleteDatasetIfEmpty(args, cur, con, datasetid, indent=indent + "  ")


def DeleteDataset(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    name: str = "",
    uniqueid: str = "",
):
    if len(name) > 0:
        print(f"Delete dataset with name {name}")
        curDS = SQLExecute(
            cur,
            f"update dataset set deltime = {CURRENT_TIME} "
            f'where name = "{name}" returning rowid',
        )
    elif len(uniqueid) > 0:
        print(f"Delete dataset with uuid = {uniqueid}")
        curDS = SQLExecute(
            cur,
            f"update dataset set deltime = {CURRENT_TIME} "
            f'where uuid = "{uniqueid}" returning rowid',
        )
    else:
        raise Exception("DeleteDataset() requires name or unique id")

    rowID = curDS.fetchone()[0]
    res = SQLExecute(
        curDS,
        "select rowid from replica " + f" where datasetid = {rowID} and deltime = 0",
    )
    replicas = res.fetchall()
    for rep in replicas:
        DeleteReplica(args, cur, con, rep[0], False)


def Delete(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    if args.uuid is not None:
        for uid in args.uuid:
            DeleteDataset(args, cur, con, uniqueid=uid)
            SQLCommit(con)

    if args.name is not None:
        for name in args.name:
            DeleteDataset(args, cur, con, name=name)
            SQLCommit(con)

    if args.replica is not None:
        for repid in args.replica:
            DeleteReplica(args, cur, con, repid, True)
            SQLCommit(con)


def InfoDataset(
    args: argparse.Namespace,
    dataset: list,
    cur: sqlite3.Cursor,
    delete_condition_and: str,
    dirs_archived: list[bool],
):
    datasetID = dataset[0]
    timestr = timestamp_to_str(dataset[3])
    print(f"    {dataset[1]}  {dataset[5]:5}  {timestr}   {dataset[2]}", end="")
    if dataset[4] > 0:
        print(f"  - deleted {timestamp_to_str(dataset[4])}")
    else:
        print()

    if not args.list_replicas and not args.list_files:
        return

    res2 = SQLExecute(
        cur,
        "select rowid, hostid, dirid, archiveid, name, modtime, deltime, keyid, size from replica "
        + 'where datasetid = "'
        + str(datasetID)
        + '"'
        + delete_condition_and
        + " order by rowid",
    )
    replicas = res2.fetchall()
    for rep in replicas:
        replicaid = rep[0]
        # hostid = rep[1]
        dirid = rep[2]
        archiveid = rep[3]
        name = rep[4]
        modtime = rep[5]
        deltime = rep[6]
        keyid = rep[7]
        size = rep[8]
        if deltime > 0 and not args.show_deleted:
            return

        flagDel = "-"
        flagRemote = "r"
        flagEncrypted = "-"
        flagAccuracy = "-"
        flagArchive = "-"
        if deltime > 0:
            flagDel = "D"

        if keyid > 0:
            flagEncrypted = "k"

        if dirs_archived[dirid]:
            flagArchive = "A"

        if dataset[5] == "ADIOS" or dataset[5] == "HDF5":
            res3 = SQLExecute(
                cur,
                f"select rowid from accuracy where replicaid = {replicaid} order by rowid",
            )
            acc = res3.fetchall()
            if len(acc) > 0:
                flagAccuracy = "a"

        if dataset[5] == "IMAGE" or dataset[5] == "TEXT":
            res3 = SQLExecute(
                cur,
                f"select rowid from file where replicaid = {replicaid} order by rowid",
            )
            res = res3.fetchall()
            if len(res) > 0:
                flagRemote = "e"

        timestr = timestamp_to_str(modtime)
        print(
            f"  {replicaid:>7} {flagRemote}{flagEncrypted}{flagAccuracy}{flagArchive}{flagDel} {dirid}",
            end="",
        )
        if archiveid > 0:
            print(f".{archiveid}", end="")

        if dataset[5] == "IMAGE":
            res3 = SQLExecute(
                cur,
                'select rowid, x, y from resolution where replicaid = "'
                + str(replicaid)
                + '"'
                + " order by rowid",
            )
            res = res3.fetchall()
            if len(res) > 0:
                print(f" {res[0][1]} x {res[0][2]}".rjust(14), end="")
        else:
            print(" ".rjust(14), end="")

        print(
            f" {sizeof_fmt(size):>11}  {timestr}",
            end="",
        )

        print(f"      {name}", end="")

        if deltime > 0:
            print(f"  - deleted {timestamp_to_str(deltime)}", end="")

        print()
        if not args.list_files:
            continue

        res3 = SQLExecute(
            cur,
            "select name, lenorig, lencompressed, modtime, checksum from file "
            + f"where replicaid = {replicaid}",
        )
        files = res3.fetchall()
        for file in files:
            if keyid > 0:
                print("".rjust(28), end="")
                print(f"k{keyid:<3}", end="")
            else:
                print("".rjust(32), end="")
            print(f"{sizeof_fmt(file[2]):>11}  {timestamp_to_str(file[3])}", end="")
            if args.show_checksum:
                print(f"         {file[4]}  {file[0]}", end="")
            else:
                print(f"         {file[0]}", end="")
            print()


def Info(args: argparse.Namespace, cur: sqlite3.Cursor):
    res = SQLExecute(cur, "select id, name, version, modtime from info")
    info = res.fetchone()
    t = timestamp_to_str(info[3])
    print(f"{info[1]}, version {info[2]}, created on {t}")
    print()

    #
    # Hosts and directories
    #
    delete_condition_where = " where deltime = 0"
    delete_condition_and = " and deltime = 0"
    if args.show_deleted:
        delete_condition_where = ""
        delete_condition_and = ""
    print("Hosts and directories:")
    res = SQLExecute(
        cur,
        "select rowid, hostname, longhostname from host"
        + delete_condition_where
        + " order by rowid",
    )
    hosts = res.fetchall()
    dirs_archived = [False]  # [0] is never accessed but needed since dir IDs run 1...n
    for host in hosts:
        # hostID = host[0]
        print(f"  {host[1]}   longhostname = {host[2]}")
        res2 = SQLExecute(
            cur,
            "select rowid, name, modtime, deltime from directory "
            + 'where hostid = "'
            + str(host[0])
            + '"'
            + delete_condition_and
            + " order by rowid",
        )
        dirs = res2.fetchall()
        for dir in dirs:
            if dir[3] == 0 or args.show_deleted:
                # check if it's archive dir
                archive_system = "  "
                res3 = SQLExecute(
                    cur,
                    f"select rowid, tarname, system from archive where dirid = {dir[0]} order by rowid",
                )
                archs = res3.fetchall()
                if len(archs) > 0:
                    archive_system = f"  - Archive: {archs[0][2]}"
                    dirs_archived.append(True)
                else:
                    dirs_archived.append(False)
                print(f"     {dir[0]}. {dir[1]}{archive_system}")
                for idx in range(len(archs)):
                    if archs[idx][1]:
                        print(f"       {dir[0]}.{archs[idx][0]} {archs[idx][1]}")
                    else:
                        print(f"       {dir[0]}.{archs[idx][0]} .")
    print()

    #
    # Keys
    #
    res = SQLExecute(cur, "select rowid, keyid from key order by rowid")
    keys = res.fetchall()
    if len(keys) > 0:
        print("Encryption keys:")
    for key in keys:
        print(f"  k{key[0]}. {key[1]}")
    if len(keys) > 0:
        print()

    #
    # Time Series
    #
    res = SQLExecute(cur, "select tsid, name from timeseries order by tsid")
    timeseries = res.fetchall()
    if len(timeseries) > 0:
        print("Time-series and their datasets:")
    for ts in timeseries:
        print(f"  {ts[1]}")
        res = SQLExecute(
            cur,
            "select rowid, uuid, name, modtime, deltime, fileformat from dataset "
            f"where tsid = {ts[0]} " + delete_condition_and,
        )
        datasets = res.fetchall()
        for dataset in datasets:
            InfoDataset(args, dataset, cur, delete_condition_and, dirs_archived)
    if len(timeseries) > 0:
        print("")

    #
    # Datasets
    #
    res = SQLExecute(
        cur,
        "select rowid, uuid, name, modtime, deltime, fileformat from dataset "
        "where tsid = 0 " + delete_condition_and + " order by rowid",
    )
    datasets = res.fetchall()
    if len(datasets) > 0:
        print("Other Datasets:")
    for dataset in datasets:
        InfoDataset(args, dataset, cur, delete_condition_and, dirs_archived)


def DeleteCampaignFile(args: argparse.Namespace):
    if exists(args.CampaignFileName):
        print(f"Delete campaign archive {args.CampaignFileName}")
        remove(args.CampaignFileName)
        while exists(args.CampaignFileName):
            sleep(0.1)
        return 0
    else:
        print(f"ERROR: archive {args.CampaignFileName} does not exist")
        return 1


def CampaignInfo(filename):
    import io
    import sys
    import re

    output = io.StringIO()
    sys.stdout = output
    main(
        args=[filename, "info"],
        prog=None,
    )
    output_string = output.getvalue()
    sys.stdout = sys.__stdout__
    pattern = re.compile(
        r"^\s*(?P<uuid>[0-9a-f]{32})\s+(?P<type>ADIOS|HDF5|TEXT|IMAGE)\s+.*?\s+.*?\s+.*?\s+(?P<path>.*)",
        re.MULTILINE,
    )
    return [m.groupdict() for m in pattern.finditer(output_string)]


def main(args=None, prog=None):
    parser = ArgParser(args=args, prog=prog)
    CheckCampaignStore(parser.args)

    if parser.args.keyfile:
        key = read_key(parser.args.keyfile)
        # ask for password at this point
        parser.args.encryption_key = key.get_decrypted_key()
        parser.args.encryption_key_id = key.id
    else:
        parser.args.encryption_key = None
        parser.args.encryption_key_id = None

    con: sqlite3.Connection
    cur: sqlite3.Cursor
    connected = False

    while parser.parse_next_command():
        print("=" * 70)
        # print(parser.args)
        # print("--------------------------")
        if parser.args.command == "delete" and parser.args.campaign is True:
            DeleteCampaignFile(parser.args)
            continue

        if parser.args.command == "create":
            # print("Create archive")
            if exists(parser.args.CampaignFileName):
                print(f"ERROR: archive {parser.args.CampaignFileName} already exist")
                exit(1)
        else:
            # print(f"{parser.args.command} archive")
            if not exists(parser.args.CampaignFileName):
                print(f"ERROR: archive {parser.args.CampaignFileName} does not exist")
                exit(1)

        if not connected:
            con = sqlite3.connect(parser.args.CampaignFileName)
            cur = con.cursor()
            connected = True

        if parser.args.command == "info":
            Info(parser.args, cur)
            continue
        elif parser.args.command == "create":
            Create(parser.args, cur, con)
            connected = False
            continue
        elif (
            parser.args.command == "dataset"
            or parser.args.command == "text"
            or parser.args.command == "image"
        ):
            Update(parser.args, cur, con)
            continue
        elif parser.args.command == "delete":
            Delete(parser.args, cur, con)
            continue
        elif parser.args.command == "add-archival-storage":
            AddArchivalStorage(parser.args, cur, con)
        elif parser.args.command == "archived":
            ArchiveDataset(parser.args, cur, con)
        elif parser.args.command == "time-series":
            AddTimeSeries(parser.args, cur, con)
        elif parser.args.command == "upgrade":
            UpgradeACA(parser.args, cur, con)
        else:
            print(
                "This should not happen. "
                f"Unknown command accepted by argparser: {parser.args.command}"
            )

    if connected:
        cur.close()
        con.close()

    if len(SQLErrorList) > 0:
        print()
        print("!!!! SQL Errors encountered")
        for e in SQLErrorList:
            print(f"  {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
        print("!!!!")
        print()


if __name__ == "__main__":
    main()
