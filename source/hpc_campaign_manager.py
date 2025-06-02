#!/usr/bin/env python3

import argparse
import glob
import sqlite3
import zlib
import uuid
import nacl.secret
import nacl.utils
from dateutil.parser import parse
from hashlib import sha1
from os import chdir, getcwd, remove, stat
from os.path import exists, isdir, basename, join
from pathlib import Path
from PIL import Image
from re import sub
from socket import getfqdn
from time import time_ns, sleep

from hpc_campaign_key import read_key
from hpc_campaign_config import ADIOS_ACA_VERSION
from hpc_campaign_utils import timestamp_to_str, SQLCommit, SQLExecute, SQLErrorList, get_folder_size, sizeof_fmt
from hpc_campaign_hdf5_metadata import copy_hdf5_file_without_data, IsHDF5Dataset
from hpc_campaign_manager_args import ArgParser

CURRENT_TIME = time_ns()


def CheckCampaignStore(args):
    if args.campaign_store is not None and not isdir(args.campaign_store):
        print("ERROR: Campaign directory " + args.campaign_store + " does not exist", flush=True)
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
):
    if compress:
        compressed = 1
        if content:
            compressed_data, len_orig, len_compressed, checksum = compressBytes(content)
        else:
            try:
                with open(filename, "rb") as f:
                    compressed_data, len_orig, len_compressed, checksum = compressFile(f)

            except IOError:
                print(f"ERROR While reading file {filename}")
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
                print(f"ERROR While reading file {filename}")
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
    keyID: int,
    dataset: str,
    cur: sqlite3.Cursor,
    datasetid: int,
    mt: float,
    size: int,
) -> int:

    print(f"Add replica {dataset} to archive")
    print(
        f"AddReplicaToArchive(host={hostID}, dir={dirID}, key={keyID}, name={dataset}"
        f" dsid={datasetid}, time={mt}, size={size})"
    )
    curDS = SQLExecute(
        cur,
        "insert into replica (datasetid, hostid, dirid, name, modtime, deltime, keyid, size) "
        "values  (?, ?, ?, ?, ?, ?, ?, ?) "
        "on conflict (datasetid, hostid, dirid, name) "
        "do update set modtime = ?, deltime = ?, keyid = ?, size = ? "
        "returning rowid",
        (datasetid, hostID, dirID, dataset, mt, 0, keyID, size, mt, 0, keyID, size),
    )
    rowID = curDS.fetchone()[0]
    print(f"Replica rowid = {rowID}")
    return rowID


def AddDatasetToArchive(
    args: argparse.Namespace, name: str, cur: sqlite3.Cursor, uniqueID: str, format: str, mt: float
) -> int:

    print(f"Add dataset {name} to archive")
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
) -> int:

    print(f"Add resolution {x} {y} for replica {repID} to archive")
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
        print(f"Process entry {entry}:")
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
            repID = AddReplicaToArchive(args, hostID, dirID, keyID, entry, cur, dsID, mt, filesize)
        elif IsADIOSDataset(entry):
            dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "ADIOS", mt)
            filesize = get_folder_size(entry)
            repID = AddReplicaToArchive(args, hostID, dirID, keyID, entry, cur, dsID, mt, filesize)
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
            repID = AddReplicaToArchive(args, hostID, dirID, keyID, entry, cur, dsID, mt, filesize)
            AddFileToArchive(args, mdfilename, cur, repID, mt, basename(entry))
            remove(mdfilename)
        else:
            print(f"WARNING: Dataset {dataset} is neither an ADIOS nor an HDF5 dataset. Skip")


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
        repID = AddReplicaToArchive(args, hostID, dirID, keyID, entry, cur, dsID, ct, filesize)
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
    print(f"  -- Img path = {location}/{args.file}   uuid = {uniqueID}")

    img = Image.open(args.file)
    imgres = img.size

    dsID = AddDatasetToArchive(args, dataset, cur, uniqueID, "IMAGE", mt)
    repID = AddReplicaToArchive(args, hostID, dirID, keyID, args.file, cur, dsID, mt, filesize)
    AddResolutionToArchive(args, repID, imgres[0], imgres[1], cur)

    if args.store or args.thumbnail is not None:
        imgsuffix = Path(args.file).suffix
        if args.store:
            print("Storing the image in the archive")
            resname = f"{imgres[0]}x{imgres[1]}{imgsuffix}"
            AddFileToArchive(args, args.file, cur, repID, mt, resname, compress=False)

        else:
            print(f"Resize image to {args.thumbnail}")
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
                args, hostID, dirID, keyID, join("thumbnails", args.file), cur, dsID, now, filesize
            )
            AddFileToArchive(args, thumbfilename, cur, thumbrepID, now, resname, compress=False)
            AddResolutionToArchive(args, thumbrepID, imgres[0], imgres[1], cur)
            remove(thumbfilename)


def ArchiveDataset(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    # Find dataset
    res = SQLExecute(cur, f'select rowid, fileformat from dataset where name = "{args.name}"')
    rows = res.fetchall()
    if len(rows) == 0:
        raise Exception(f"Dataset not found: {args.name} ")

    datasetid: int = rows[0][0]
    format: str = rows[0][1]

    # Find archive dir
    res = SQLExecute(cur, f"select hostid, name from directory where rowid = {args.dirid}")
    rows = res.fetchall()
    if len(rows) == 0:
        raise Exception(f"Directory ID not found: {args.dirid} ")

    hostID: int = rows[0][0]
    dir_name: str = rows[0][1]

    res = SQLExecute(cur, f"select rowid, system from archive where dirid = {args.dirid}")
    rows = res.fetchall()
    if len(rows) == 0:
        raise Exception(f"Directory {dir_name} with ID {args.dirid} is not an archival storage directory")

    # Check replicas of dataset and see if there is conflict (need --replica option)
    orig_repID: int = args.replica
    if args.replica is None:
        res = SQLExecute(cur, f"select rowid, deltime from replica where datasetid = {datasetid}")
        rows = res.fetchall()
        liverows = []
        delrows = []
        for row in rows:
            if row[1] == 0:
                liverows.append(row)
            else:
                delrows.append(row)
        if len(liverows) > 1:
            raise Exception(
                f"There are {len(liverows)} non-deleted replicas for this dataset. "
                "Use --replica to identify which is archived"
            )
        if len(liverows) == 0:
            if format == "ADIOS" or format == "HDF5":
                raise Exception(
                    f"There are no non-deleted replicas for a {format} dataset. Cannot archive without "
                    "access to the embedded metadata files of a non-deleted replica"
                )
            if len(delrows) == 1:
                orig_repID = delrows[0][0]
            else:
                raise Exception(
                    f"There are no non-deleted replicas and {len(delrows)} deleted replicas for this {format} dataset. "
                    "Use --replica to identify which is archived"
                )
        orig_repID = liverows[0][0]

    # get name and KeyID for selected replica
    res = SQLExecute(cur, f"select datasetid, name, modtime, keyid, size from replica where rowid = {orig_repID}")
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

    repID = AddReplicaToArchive(args, hostID, args.dirid, keyID, dsname, cur, datasetid, mt, filesize)

    # if replica has Resolution, copy that to new replica
    res = SQLExecute(cur, f"select x, y from resolution where replicaid = {orig_repID}")
    rows = res.fetchall()
    if len(rows) > 0:
        x = rows[0][0]
        y = rows[0][1]
        AddResolutionToArchive(args, repID, x, y, cur)

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
        SQLExecute(cur, f"update file set replicaid = {repID} where replicaid = {orig_repID}")
        DeleteReplica(args, cur, con, orig_repID, False)
    else:
        res = SQLExecute(
            cur,
            "select name, compression, lenorig, lencompressed, modtime, checksum, data "
            f"from file where replicaid = {orig_repID}",
        )
        files = res.fetchall()
        print(f"Copying {len(files)} files from original replica to archived one")
        for f in files:
            SQLExecute(
                cur,
                "insert into file values (?, ?, ?, ?, ?, ?, ?, ?) "
                "on conflict (replicaid, name) do update set "
                "compression = ?, lenorig = ?, lencompressed = ?, modtime = ?, checksum = ?, data = ?",
                (repID, f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[1], f[2], f[3], f[4], f[5], f[6]),
            )

    SQLCommit(con)


def AddTimeSeries(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    if args.remove:
        res = SQLExecute(cur, f'select tsid from timeseries where name = "{args.name}"')
        rows = res.fetchall()
        if len(rows) > 0:
            tsID = rows[-1][0]
            print(f"Remove {args.name} from time-series but leave datasets alone")
            res = SQLExecute(cur, f'delete from timeseries where name = "{args.name}"')
            curDS = SQLExecute(cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{tsID}"')
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
        "insert into timeseries (name) values  (?) " "on conflict (name) do update set name = ? returning rowid",
        (args.name, args.name),
    )
    tsID = curTS.fetchone()[0]
    print(f"Time series ID = {tsID}, already existed = {ts_exists}")

    # if --replace, "delete" the existing dataset connections
    tsorder = 0
    if args.replace:
        curDS = SQLExecute(cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{tsID}"')
    else:
        # otherwise we need to know how many datasets we have already
        res = SQLExecute(cur, f"select tsorder from dataset where tsid = {tsID} order by tsorder")
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


def AddHostName(longHostName, shortHostName, cur: sqlite3.Cursor) -> int:
    res = SQLExecute(cur, 'select rowid from host where hostname = "' + shortHostName + '"')
    row = res.fetchone()
    if row is not None:
        hostID = row[0]
        print(f"Found host {shortHostName} in database, rowid = {hostID}")
    else:
        curHost = SQLExecute(
            cur,
            "insert into host values (?, ?, ?, ?)",
            (shortHostName, longHostName, CURRENT_TIME, 0),
        )
        hostID = lastrowid_or_zero(curHost)
        print(f"Inserted host {shortHostName} into database, rowid = {hostID}, longhostname = {longHostName}")
    return hostID


def AddDirectory(hostID: int, path: str, cur: sqlite3.Cursor) -> int:
    res = SQLExecute(
        cur,
        "select rowid from directory where hostid = " + str(hostID) + ' and name = "' + path + '"',
    )
    row = res.fetchone()
    if row is not None:
        dirID = row[0]
        print(f"Found directory {path} with hostID {hostID} in database, rowid = {dirID}")
    else:
        curDirectory = SQLExecute(cur, "insert into directory values (?, ?, ?, ?)", (hostID, path, CURRENT_TIME, 0))
        dirID = lastrowid_or_zero(curDirectory)
        print(f"Inserted directory {path} into database, rowid = {dirID}")
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


def AddArchivalStorage(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    hostID = AddHostName(args.longhostname, args.host, cur)
    dirID = AddDirectory(hostID, args.directory, cur)
    print(f"Note the archive system {args.system} for directory {dirID}")
    notes = None
    if args.note:
        try:
            with open(args.note, "rb") as f:
                notes = f.read()
        except IOError as e:
            print(f"WARNING: Failed to read notes from {args.notes}: {e.strerror}.")
            notes = None

    SQLExecute(
        cur,
        "insert into archive (dirid, system, notes) values  (?, ?, ?) " "on conflict (dirid) do update set system = ?",
        (dirID, args.system, notes, args.system),
    )
    SQLCommit(con)


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
        ProcessDatasets(args, cur, hostID, dirID, keyID, longHostName + rootdir, rootdir)
    elif args.command == "text":
        ProcessTextFiles(args, cur, hostID, dirID, keyID, longHostName + rootdir, rootdir)
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
        ("ACA", "ADIOS Campaign Archive", ADIOS_ACA_VERSION, CURRENT_TIME),
    )

    SQLExecute(cur, "create table key" + "(keyid TEXT PRIMARY KEY)")
    SQLExecute(
        cur,
        "create table host" + "(hostname TEXT PRIMARY KEY, longhostname TEXT, modtime INT, deltime INT)",
    )
    SQLExecute(
        cur,
        "create table directory" + "(hostid INT, name TEXT, modtime INT, deltime INT, PRIMARY KEY (hostid, name))",
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
        + "(datasetid INT, hostid INT, dirid INT, name TEXT, modtime INT, deltime INT, keyid INT, size INT"
        + ", PRIMARY KEY (datasetid, hostid, dirid, name))",
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
        "create table accuracy" + "(replicaid INT, accuracy REAL, norm REAL, relative INT, PRIMARY KEY (replicaid))",
    )
    SQLExecute(cur, "create table resolution" + "(replicaid INT, x INT, y INT, PRIMARY KEY (replicaid))")
    SQLExecute(cur, "create table archive" + "(dirid INT, system TEXT, notes BLOB, PRIMARY KEY (dirid))")
    SQLCommit(con)
    cur.close()
    con.close()
    while not exists(args.CampaignFileName):
        sleep(0.1)


def DeleteDatasetIfEmpty(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection, datasetid: int):
    print(f"Check if dataset {datasetid} still has replicas")
    res = SQLExecute(cur, "select rowid from replica " + f" where datasetid = {datasetid} and deltime = 0")
    replicas = res.fetchall()
    if len(replicas) == 0:
        print("    Dataset without replicas found. Deleting.")
        SQLExecute(cur, f"update dataset set deltime = {CURRENT_TIME} " + f"where rowid = {datasetid}")


def DeleteReplica(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    repid: int,
    delete_empty_dataset: bool,
):
    print(f"Delete replica with id {repid}")
    res = SQLExecute(cur, "select datasetid, hostid, dirid from replica " + f"where rowid = {repid}")
    replicas = res.fetchall()
    datasetid = 0
    for rep in replicas:
        datasetid = rep[0]
        SQLExecute(cur, f"update replica set deltime = {CURRENT_TIME} " + f"where rowid = {repid}")
    if delete_empty_dataset:
        SQLExecute(cur, f"delete from file where replicaid = {repid}")
        DeleteDatasetIfEmpty(args, cur, con, datasetid)


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
            f"update dataset set deltime = {CURRENT_TIME} " f'where name = "{name}" returning rowid',
        )
    elif len(uniqueid) > 0:
        print(f"Delete dataset with uuid = {uniqueid}")
        curDS = SQLExecute(
            cur,
            f"update dataset set deltime = {CURRENT_TIME} " f'where uuid = "{uniqueid}" returning rowid',
        )
    else:
        raise Exception("DeleteDataset() requires name or unique id")

    rowID = curDS.fetchone()[0]
    res = SQLExecute(curDS, "select rowid from replica " + f" where datasetid = {rowID} and deltime = 0")
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
    args: argparse.Namespace, dataset: list, cur: sqlite3.Cursor, delete_condition_and: str, dirs_archived: list[bool]
):
    datasetID = dataset[0]
    t = timestamp_to_str(dataset[3])
    print(f"    {dataset[1]}  {dataset[5]:5}  {t}   {dataset[2]}", end="")
    if dataset[4] > 0:
        print(f"  - deleted {timestamp_to_str(dataset[4])}")
    else:
        print()

    if not args.list_replicas and not args.list_files:
        return

    res2 = SQLExecute(
        cur,
        "select rowid, hostid, dirid, name, modtime, deltime, keyid, size from replica "
        + 'where datasetid = "'
        + str(datasetID)
        + '"'
        + delete_condition_and,
    )
    replicas = res2.fetchall()
    for rep in replicas:
        if rep[5] > 0 and not args.show_deleted:
            return

        flagDel = "-"
        flagRemote = "r"
        flagEncrypted = "-"
        flagAccuracy = "-"
        flagArchive = "-"
        if rep[5] > 0:
            flagDel = "D"

        if rep[6] > 0:
            flagEncrypted = "k"

        if dirs_archived[rep[2]]:
            flagArchive = "A"

        if dataset[5] == "ADIOS" or dataset[5] == "HDF5":
            res3 = SQLExecute(cur, f"select rowid from accuracy where replicaid = {rep[0]}")
            acc = res3.fetchall()
            if len(acc) > 0:
                flagAccuracy = "a"

        if dataset[5] == "IMAGE" or dataset[5] == "TEXT":
            res3 = SQLExecute(cur, f"select rowid from file where replicaid = {rep[0]}")
            res = res3.fetchall()
            if len(res) > 0:
                flagRemote = "e"

        t = timestamp_to_str(rep[4])
        print(
            f"  {rep[0]:>7} {flagRemote}{flagEncrypted}{flagAccuracy}{flagArchive}{flagDel} {rep[1]}",
            end="",
        )

        if dataset[5] == "IMAGE":
            res3 = SQLExecute(
                cur,
                'select rowid, x, y from resolution where replicaid = "' + str(rep[0]) + '"',
            )
            res = res3.fetchall()
            if len(res) > 0:
                print(f" {res[0][1]} x {res[0][2]}".rjust(14), end="")
        else:
            print(" ".rjust(14), end="")

        print(
            f" {sizeof_fmt(rep[7]):>11}  {t}",
            end="",
        )

        print(f"      {rep[3]}", end="")

        if rep[5] > 0:
            print(f"  - deleted {timestamp_to_str(rep[5])}", end="")

        print()
        if not args.list_files:
            continue

        res3 = SQLExecute(
            cur,
            "select name, lenorig, lencompressed, modtime, checksum from file " + f"where replicaid = {rep[0]}",
        )
        files = res3.fetchall()
        for file in files:
            if rep[6] > 0:
                print("".rjust(28), end="")
                print(f"k{rep[6]:<3}", end="")
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
    res = SQLExecute(cur, "select rowid, hostname, longhostname from host" + delete_condition_where)
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
            + delete_condition_and,
        )
        dirs = res2.fetchall()
        for dir in dirs:
            if dir[3] == 0 or args.show_deleted:
                # check if it's archive dir
                archive_system = "  "
                res3 = SQLExecute(cur, f"select rowid, system from archive where dirid = {dir[0]}")
                archs = res3.fetchall()
                if len(archs) > 0:
                    archive_system = f"  - Archive: {archs[0][1]}"
                    dirs_archived.append(True)
                else:
                    dirs_archived.append(False)
                print(f"     {dir[0]}. {dir[1]}{archive_system}")
    print()

    #
    # Keys
    #
    res = SQLExecute(cur, "select rowid, keyid from key")
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
    res = SQLExecute(cur, "select tsid, name from timeseries")
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
        "select rowid, uuid, name, modtime, deltime, fileformat from dataset " "where tsid = 0 " + delete_condition_and,
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


if __name__ == "__main__":
    parser = ArgParser()
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
        print("=============================")
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
        elif parser.args.command == "dataset" or parser.args.command == "text" or parser.args.command == "image":
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
        else:
            print("This should not happen. " f"Unknown command accepted by argparser: {parser.args.command}")

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
