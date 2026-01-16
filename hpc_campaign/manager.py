#!/usr/bin/env python3

# pylint: disable=too-many-lines
# pylint: disable=import-error
# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
# pylint: disable=unused-argument
# pylint: disable=too-many-positional-arguments

import argparse
import csv
import glob
import re
import sqlite3
import sys
import uuid
import zlib
from hashlib import sha1
from os import chdir, getcwd, remove, stat
from os.path import basename, exists, isdir, join
from pathlib import Path
from socket import getfqdn
from time import sleep, time_ns

import nacl.secret
import nacl.utils
from dateutil.parser import parse
from PIL import Image

from .config import ACA_VERSION
from .hdf5_metadata import copy_hdf5_file_without_data, is_hdf5_dataset
from .info import InfoResult, collect_info, print_info
from .key import read_key
from .manager_args import ArgParser
from .taridx import TARTYPES
from .upgrade import upgrade_aca
from .utils import (
    get_folder_size,
    sql_commit,
    sql_error_list,
    sql_execute,
)

CURRENT_TIME = time_ns()


def check_campaign_store(args):
    if args.campaign_store is not None and not isdir(args.campaign_store):
        print(
            "ERROR: Campaign directory " + args.campaign_store + " does not exist",
            flush=True,
        )
        sys.exit(1)


def check_local_campaign_dir(args):
    if not isdir(args.local_campaign_dir):
        print(
            "ERROR: Shot campaign data '"
            + args.local_campaign_dir
            + "' does not exist. Run this command where the code was executed.",
            flush=True,
        )
        sys.exit(1)


def parse_date_to_utc(date, fmt=None):
    if fmt is None:
        fmt = "%Y-%m-%d %H:%M:%S %z"  # Defaults to : 2022-08-31 07:47:30 -0000
    get_date_obj = parse(str(date))
    return get_date_obj.timestamp()


def is_adios_dataset(dataset):
    if not isdir(dataset):
        return False
    if not exists(dataset + "/" + "md.idx"):
        return False
    if not exists(dataset + "/" + "data.0"):
        return False
    return True


def compress_bytes(b: bytes) -> tuple[bytes, int, int, str]:
    comp_obj = zlib.compressobj()
    compressed = bytearray()
    len_orig = len(b)
    len_compressed = 0
    checksum = sha1(b)

    c_block = comp_obj.compress(b)
    compressed += c_block
    len_compressed += len(c_block)

    c_block = comp_obj.flush()
    compressed += c_block
    len_compressed += len(c_block)

    return bytes(memoryview(compressed)), len_orig, len_compressed, checksum.hexdigest()


def compress_file(f) -> tuple[bytes, int, int, str]:
    comp_obj = zlib.compressobj()
    compressed = bytearray()
    blocksize = 1073741824  # 1GB #1024*1048576
    len_orig = 0
    len_compressed = 0
    checksum = sha1()
    block = f.read(blocksize)
    while block:
        len_orig += len(block)
        c_block = comp_obj.compress(block)
        compressed += c_block
        len_compressed += len(c_block)
        checksum.update(block)
        block = f.read(blocksize)
    c_block = comp_obj.flush()
    compressed += c_block
    len_compressed += len(c_block)

    return bytes(memoryview(compressed)), len_orig, len_compressed, checksum.hexdigest()


def decompress_buffer(buf: bytearray):
    data = zlib.decompress(buf)
    return data


def encrypt_buffer(args: argparse.Namespace, buf: bytes):
    if args.encryption_key:
        box = nacl.secret.SecretBox(args.encryption_key)
        nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
        e = box.encrypt(buf, nonce)
        print("Encoded buffer size: ", len(e))
        return e
    return buf


def lastrowid_or_zero(cur_ds: sqlite3.Cursor) -> int:
    row_id = cur_ds.lastrowid
    if not row_id:
        row_id = 0
    return row_id


def add_file_to_archive(
    args: argparse.Namespace,
    filename: str,
    cur: sqlite3.Cursor,
    rep_id: int,
    mt: float = 0.0,
    filename_as_recorded: str = "",
    compress: bool = True,
    content: bytes = bytes(),
    indent: str = "",
):
    if compress:
        compressed = 1
        if content:
            compressed_data, len_orig, len_compressed, checksum = compress_bytes(content)
        else:
            try:
                with open(filename, "rb") as f:
                    compressed_data, len_orig, len_compressed, checksum = compress_file(f)

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

    encrypted_data = encrypt_buffer(args, compressed_data)

    if mt == 0.0:
        statres = stat(filename)
        mt = statres.st_mtime_ns

    if len(filename_as_recorded) == 0:
        filename_as_recorded = filename

    cur_file = sql_execute(
        cur,
        "select file.fileid from file "
        "join repfiles on file.fileid = repfiles.fileid "
        "where repfiles.replicaid = ? and file.name = ?",
        (rep_id, filename_as_recorded),
    )
    row = cur_file.fetchone()
    if row is None:
        cur_file = sql_execute(
            cur,
            "insert into file "
            "(name, compression, lenorig, lencompressed, modtime, checksum, data) "
            "values (?, ?, ?, ?, ?, ?, ?) "
            "returning fileid",
            (
                filename_as_recorded,
                compressed,
                len_orig,
                len_compressed,
                mt,
                checksum,
                encrypted_data,
            ),
        )
        fileid = cur_file.fetchone()[0]
        sql_execute(
            cur,
            "insert into repfiles (replicaid, fileid) values (?, ?)",
            (rep_id, fileid),
        )
    else:
        fileid = row[0]
        sql_execute(
            cur,
            "update file set compression = ?, lenorig = ?, lencompressed = ?, modtime = ?, checksum = ?, data = ? "
            "where fileid = ?",
            (
                compressed,
                len_orig,
                len_compressed,
                mt,
                checksum,
                encrypted_data,
                fileid,
            ),
        )


def add_replica_to_archive(
    args: argparse.Namespace,
    host_id: int,
    dir_id: int,
    archive_id: int,
    key_id: int,
    dataset: str,
    cur: sqlite3.Cursor,
    datasetid: int,
    mt: float,
    size: int,
    indent: str = "",
) -> int:
    print(f"{indent}Add replica {dataset} to archive")
    print(
        f"{indent}add_replica_to_archive(host={host_id}, dir={dir_id}, archive={archive_id}, "
        f"key={key_id}, name={dataset} dsid={datasetid}, time={mt}, size={size})"
    )
    cur_ds = sql_execute(
        cur,
        "insert into replica (datasetid, hostid, dirid, archiveid, name, modtime, deltime, keyid, size) "
        "values  (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "on conflict (datasetid, hostid, dirid, archiveid, name) "
        "do update set modtime = ?, deltime = ?, keyid = ?, size = ? "
        "returning rowid",
        (
            datasetid,
            host_id,
            dir_id,
            archive_id,
            dataset,
            mt,
            0,
            key_id,
            size,
            mt,
            0,
            key_id,
            size,
        ),
    )
    row_id = cur_ds.fetchone()[0]
    print(f"{indent}  Replica rowid = {row_id}")
    return row_id


def add_dataset_to_archive(
    args: argparse.Namespace,
    name: str,
    cur: sqlite3.Cursor,
    unique_id: str,
    fileformat: str,
    mt: float = 0.0,
    indent: str = "",
) -> int:
    print(f"{indent}Add dataset {name} to archive")
    cur_ds = sql_execute(
        cur,
        "insert into dataset (name, uuid, modtime, deltime, fileformat, tsid, tsorder) "
        "values  (?, ?, ?, ?, ?, ?, ?) "
        "on conflict (name) do update set deltime = ? "
        "returning rowid",
        (name, unique_id, mt, 0, fileformat, 0, 0, 0),
    )
    dataset_id = cur_ds.fetchone()[0]
    return dataset_id


def add_resolution_to_archive(
    args: argparse.Namespace,
    rep_id: int,
    x: int,
    y: int,
    cur: sqlite3.Cursor,
    indent: str = "",
) -> int:
    print(f"{indent}Add resolution {x} {y} for replica {rep_id} to archive")
    cur_ds = sql_execute(
        cur,
        "insert into resolution (replicaid, x, y) "
        "values  (?, ?, ?) "
        "on conflict (replicaid) do update set x = ?, y = ? returning rowid",
        (rep_id, x, y, x, y),
    )
    row_id = cur_ds.fetchone()[0]
    return row_id


def process_datasets(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    host_id: int,
    dir_id: int,
    key_id: int,
    dirpath: str,
    location: str,
):
    for entry in args.files:
        dataset = entry
        if args.name is not None:
            dataset = args.name
        unique_id = uuid.uuid3(uuid.NAMESPACE_URL, location + "/" + entry).hex
        ds_id = 0

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
            ds_id = add_dataset_to_archive(args, dataset, cur, unique_id, "ADIOS", mt)
            rep_id = add_replica_to_archive(
                args,
                host_id,
                dir_id,
                0,
                key_id,
                entry,
                cur,
                ds_id,
                mt,
                filesize,
                indent="  ",
            )
        elif is_adios_dataset(entry):
            ds_id = add_dataset_to_archive(args, dataset, cur, unique_id, "ADIOS", mt)
            filesize = get_folder_size(entry)
            rep_id = add_replica_to_archive(
                args,
                host_id,
                dir_id,
                0,
                key_id,
                entry,
                cur,
                ds_id,
                mt,
                filesize,
                indent="  ",
            )
            cwd = getcwd()
            chdir(entry)
            md_file_list = glob.glob("*md.*")
            profile_list = glob.glob("profiling.json")
            files = md_file_list + profile_list
            for f in files:
                add_file_to_archive(args, f, cur, rep_id)
            chdir(cwd)
        elif is_hdf5_dataset(entry):
            mdfilename = "/tmp/md_" + basename(entry)
            copy_hdf5_file_without_data(entry, mdfilename)
            ds_id = add_dataset_to_archive(args, dataset, cur, unique_id, "HDF5", mt)
            rep_id = add_replica_to_archive(
                args,
                host_id,
                dir_id,
                0,
                key_id,
                entry,
                cur,
                ds_id,
                mt,
                filesize,
                indent="  ",
            )
            add_file_to_archive(args, mdfilename, cur, rep_id, mt, basename(entry))
            remove(mdfilename)
        else:
            print(f"WARNING: Dataset {dataset} is neither an ADIOS nor an HDF5 dataset. Skip")


def process_text_files(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    host_id: int,
    dir_id: int,
    key_id: int,
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
        unique_id = uuid.uuid3(uuid.NAMESPACE_URL, location + "/" + entry).hex
        ds_id = add_dataset_to_archive(args, dataset, cur, unique_id, "TEXT", ct)
        rep_id = add_replica_to_archive(args, host_id, dir_id, 0, key_id, entry, cur, ds_id, ct, filesize, indent="  ")
        if args.store:
            add_file_to_archive(args, entry, cur, rep_id, ct, basename(entry))


def process_image(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    host_id: int,
    dir_id: int,
    key_id: int,
    dirpath: str,
    location: str,
):
    dataset = args.file
    if args.name is not None:
        dataset = args.name

    statres = stat(args.file)
    mt = statres.st_mtime_ns
    filesize = statres.st_size
    unique_id = uuid.uuid3(uuid.NAMESPACE_URL, location + "/" + args.file).hex
    print(f"Process image {location}/{args.file}")

    img = Image.open(args.file)
    imgres = img.size

    ds_id = add_dataset_to_archive(args, dataset, cur, unique_id, "IMAGE", mt, indent="  ")
    rep_id = add_replica_to_archive(args, host_id, dir_id, 0, key_id, args.file, cur, ds_id, mt, filesize, indent="  ")
    add_resolution_to_archive(args, rep_id, imgres[0], imgres[1], cur, indent="  ")

    if args.store or args.thumbnail is not None:
        imgsuffix = Path(args.file).suffix
        if args.store:
            print("Storing the image in the archive")
            resname = f"{imgres[0]}x{imgres[1]}{imgsuffix}"
            add_file_to_archive(args, args.file, cur, rep_id, mt, resname, compress=False, indent="  ")

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
            thumb_rep_id = add_replica_to_archive(
                args,
                host_id,
                dir_id,
                0,
                key_id,
                join("thumbnails", args.file),
                cur,
                ds_id,
                now,
                filesize,
                indent="  ",
            )
            add_file_to_archive(
                args,
                thumbfilename,
                cur,
                thumb_rep_id,
                now,
                resname,
                compress=False,
                indent="  ",
            )
            add_resolution_to_archive(args, thumb_rep_id, imgres[0], imgres[1], cur, indent="  ")
            remove(thumbfilename)


# pylint: disable=too-many-statements
def archive_dataset(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
):
    # Find dataset
    res = sql_execute(cur, f'select rowid, fileformat from dataset where name = "{args.name}"')
    rows = res.fetchall()
    if len(rows) == 0:
        raise LookupError(f"Dataset not found: {args.name} ")

    datasetid: int = rows[0][0]
    fileformat: str = rows[0][1]

    # Find archive dir
    res = sql_execute(cur, f"select hostid, name from directory where rowid = {args.dirid}")
    rows = res.fetchall()
    if len(rows) == 0:
        raise LookupError(f"Directory ID not found: {args.dirid} ")

    host_id: int = rows[0][0]
    dir_name: str = rows[0][1]

    if args.archiveid is None:
        res = sql_execute(cur, f"select rowid from archive where dirid = {args.dirid}")
        rows = res.fetchall()
        if len(rows) == 0:
            raise LookupError(f"Directory {dir_name} with ID {args.dirid} is not an archival storage directory")
        archive_id = rows[0][0]
    else:
        res = sql_execute(cur, f"select rowid, dirid from archive where rowid = {args.archiveid}")
        rows = res.fetchall()
        if len(rows) == 0:
            raise LookupError(f"Archive ID {args.archiveid} is not found in the archive list")
        archive_id = args.archiveid
        dir_id = rows[0][1]
        if dir_id != args.dirid:
            raise LookupError(f"Archive ID {args.archiveid} belongs to dir ID {dir_id}, not to {args.dirid}")

    # Check replicas of dataset and see if there is conflict (need --replica option)
    orig_rep_id: int = args.replica
    if args.replica is None:
        res = sql_execute(
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
            raise LookupError(
                f"There are {len(live_nonarch_rows)} non-deleted, not-in-archive, replicas for this dataset. "
                f"Use --replica to identify which is archived now. Replicas: {[r[0] for r in live_nonarch_rows]}"
            )
        if len(live_nonarch_rows) + len(live_arch_rows) == 0:
            if fileformat in ("ADIOS", "HDF5"):
                raise LookupError(
                    f"There are no replicas for a {fileformat} dataset. Cannot archive without "
                    "access to the embedded metadata files of a replica"
                )
            if len(delrows) == 1:
                orig_rep_id = delrows[0][0]
            else:
                raise LookupError(
                    f"There are no replicas but {len(delrows)} deleted replicas for this {fileformat} dataset. "
                    "Use --replica to identify which deleted replica is archived."
                    f"Deleted replicas: {[r[0] for r in delrows]}"
                )
        else:
            if len(live_nonarch_rows) > 0:
                orig_rep_id = live_nonarch_rows[0][0]
            elif len(live_arch_rows) > 1:
                raise LookupError(
                    f"There are {len(live_arch_rows)} archived replicas for this dataset. "
                    f"Use --replica to identify which is archived now. Replicas: {[r[0] for r in live_arch_rows]}"
                )
            else:
                orig_rep_id = live_arch_rows[0][0]

    # get name and KeyID for selected replica
    res = sql_execute(
        cur,
        f"select datasetid, name, modtime, keyid, size from replica where rowid = {orig_rep_id}",
    )
    row = res.fetchone()
    if datasetid != row[0]:
        res = sql_execute(cur, f'select name from dataset where rowid = "{row[0]}"')
        wrong_dsname = res.fetchone()[0]
        raise LookupError(f"Replica belongs to dataset {wrong_dsname}, not this dataset")
    replica_name: str = row[1]
    mt: int = row[2]
    key_id: int = row[3]
    filesize: int = row[4]

    # create new replica for this dataset
    dsname = replica_name
    if args.newpath:
        dsname = args.newpath

    rep_id = add_replica_to_archive(
        args,
        host_id,
        args.dirid,
        archive_id,
        key_id,
        dsname,
        cur,
        datasetid,
        mt,
        filesize,
        indent=indent,
    )

    # if replica has Resolution, copy that to new replica
    res = sql_execute(cur, f"select x, y from resolution where replicaid = {orig_rep_id}")
    rows = res.fetchall()
    if len(rows) > 0:
        x = rows[0][0]
        y = rows[0][1]
        add_resolution_to_archive(args, rep_id, x, y, cur, indent=indent)

    # # if replica has Accuracy, copy that to new replica
    # res = sql_execute(cur, f"select accuracy, norm, relative from accuracy where replicaid = {orig_rep_id}")
    # rows = res.fetchall()
    # if len(rows) > 0:
    #     accuracy = rows[0][0]
    #     norm = rows[0][1]
    #     relative = rows[0][2]
    #     AddAccuracyToArchive(args, rep_id, accuracy, norm, relative, cur)

    # if --move, delete the original replica but assign embedded files to archived replica
    # otherwise, make a copy of all embedded files
    if args.move:
        sql_execute(cur, f"update repfiles set replicaid = {rep_id} where replicaid = {orig_rep_id}")
        delete_replica(args, cur, con, orig_rep_id, False, indent=indent)
    else:
        res = sql_execute(
            cur,
            f"select fileid from repfiles where replicaid = {orig_rep_id}",
        )
        files = res.fetchall()
        print(f"{indent}Copying {len(files)} files from original replica to archived one")
        for f in files:
            sql_execute(
                cur,
                "insert into repfiles (replicaid, fileid) values (?, ?) on conflict (replicaid, fileid) do nothing",
                (
                    rep_id,
                    f[0],
                ),
            )

    sql_commit(con)
    return rep_id


def add_time_series(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    if args.remove:
        res = sql_execute(cur, f'select tsid from timeseries where name = "{args.name}"')
        rows = res.fetchall()
        if len(rows) > 0:
            ts_id = rows[-1][0]
            print(f"Remove {args.name} from time-series but leave datasets alone")
            res = sql_execute(cur, f'delete from timeseries where name = "{args.name}"')
            cur_ds = sql_execute(cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{ts_id}"')
        else:
            print(f"Time series {args.name} was not found")
        sql_commit(con)
        return

    print(f"Add {args.name} to time-series")
    # we need to know if it already exists
    ts_exists = False
    res = sql_execute(cur, f'select tsid from timeseries where name = "{args.name}"')
    rows = res.fetchall()
    if len(rows) > 0:
        ts_exists = True

    # insert/update timeseries
    cur_ts = sql_execute(
        cur,
        "insert into timeseries (name) values  (?) on conflict (name) do update set name = ? returning rowid",
        (args.name, args.name),
    )
    ts_id = cur_ts.fetchone()[0]
    print(f"Time series ID = {ts_id}, already existed = {ts_exists}")

    # if --replace, "delete" the existing dataset connections
    tsorder = 0
    if args.replace:
        cur_ds = sql_execute(cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{ts_id}"')
    else:
        # otherwise we need to know how many datasets we have already
        res = sql_execute(cur, f"select tsorder from dataset where tsid = {ts_id} order by tsorder")
        rows = res.fetchall()
        if len(rows) > 0:
            tsorder = rows[-1][0] + 1

    for dsname in args.dataset:
        cur_ds = sql_execute(
            cur,
            f"update dataset set tsid = {ts_id}, tsorder = {tsorder} "
            + f'where name = "{dsname}" returning rowid, name',
        )
        ret = cur_ds.fetchone()
        if ret is None:
            print(f"    {dsname}  Error: dataset is not in the database, skipping")
        else:
            row_id = ret[0]
            name = ret[1]
            print(f"    {name} (dataset {row_id}) tsorder = {tsorder}")
            tsorder += 1

    sql_commit(con)


def get_host_name(args: argparse.Namespace):
    if args.s3_endpoint:
        longhost = args.s3_endpoint
    else:
        longhost = getfqdn()
        if longhost.startswith("login"):
            longhost = re.sub("^login[0-9]*\\.", "", longhost)
        if longhost.startswith("batch"):
            longhost = re.sub("^batch[0-9]*\\.", "", longhost)

    if args.hostname is None:
        shorthost = longhost.split(".")[0]
    else:
        shorthost = args.hostname
    return longhost, shorthost


def add_host_name(
    long_host_name,
    short_host_name,
    cur: sqlite3.Cursor,
    default_protocol: str = "",
    indent: str = "",
) -> int:
    res = sql_execute(cur, 'select rowid from host where hostname = "' + short_host_name + '"')
    row = res.fetchone()
    if row is not None:
        host_id = row[0]
        print(f"{indent}Found host {short_host_name} in database, rowid = {host_id}")
    else:
        cur_host = sql_execute(
            cur,
            "insert into host values (?, ?, ?, ?, ?)",
            (short_host_name, long_host_name, CURRENT_TIME, 0, default_protocol),
        )
        host_id = lastrowid_or_zero(cur_host)
        print(
            f"{indent}Inserted host {short_host_name} into database, rowid = {host_id}, longhostname = {long_host_name}"
        )
    return host_id


def add_directory(host_id: int, path: str, cur: sqlite3.Cursor, indent: str = "") -> int:
    res = sql_execute(
        cur,
        "select rowid from directory where hostid = " + str(host_id) + ' and name = "' + path + '"',
    )
    row = res.fetchone()
    if row is not None:
        dir_id = row[0]
        print(f"{indent}Found directory {path} with host_id {host_id} in database, rowid = {dir_id}")
    else:
        cur_directory = sql_execute(
            cur,
            "insert into directory values (?, ?, ?, ?)",
            (host_id, path, CURRENT_TIME, 0),
        )
        dir_id = lastrowid_or_zero(cur_directory)
        print(f"{indent}Inserted directory {path} into database, rowid = {dir_id}")
    return dir_id


def add_key_id(key_id: str, cur: sqlite3.Cursor) -> int:
    key_row_id: int = 0  # an invalid row id
    if key_id:
        res = sql_execute(cur, 'select rowid from key where keyid = "' + key_id + '"')
        row = res.fetchone()
        if row is not None:
            key_row_id = int(row[0])
            print(f"Found key {key_id} in database, rowid = {key_row_id}")
        else:
            cmd = f'insert into key values ("{(key_id)}")'
            cur_key = sql_execute(cur, cmd)
            # cur_key = sql_execute(cur,"insert into key values (?)", (key_id))
            key_row_id = lastrowid_or_zero(cur_key)
            print(f"Inserted key {key_id} into database, rowid = {key_row_id}")
    return key_row_id


def archive_idx_replica(
    dsname: str,
    dir_id: int,
    archive_id: int,
    replica_id: int,
    entries: dict[str, list[int]],
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
):
    # Archive replica
    args = argparse.Namespace()
    args.name = dsname
    args.dirid = dir_id
    args.archiveid = archive_id
    args.replica = replica_id
    args.move = False
    args.newpath = ""

    archived_replica_id = archive_dataset(args, cur, con, indent=indent + "  ")
    if archived_replica_id > 0:
        for fname, entry_info in entries.items():
            # add replica and register offsets
            offset = entry_info[0]
            data_offset = entry_info[1]
            size = entry_info[2]
            sql_execute(
                cur,
                "insert into archiveidx (archiveid, replicaid, filename, offset, offset_data, size)"
                " values  (?, ?, ?, ?, ?, ?) "
                "on conflict (archiveid, replicaid, filename) do update set offset = ?, offset_data = ?, size = ?",
                (
                    archive_id,
                    archived_replica_id,
                    fname,
                    offset,
                    data_offset,
                    size,
                    offset,
                    data_offset,
                    size,
                ),
            )
        sql_commit(con)


def archive_idx(
    args: argparse.Namespace,
    archive_id: int,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
):
    try:
        # pylint: disable=consider-using-with
        csvfile = open(args.tarfileidx, newline="", encoding="utf8")
        reader = csv.reader(csvfile)
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{args.tarfileidx}' not found.") from None
    except Exception as e:
        raise EnvironmentError(f"Error occurred when opening '{args.tarfileidx}': {e}") from e

    # Find archive dir
    res = sql_execute(cur, f"select dirid, tarname from archive where rowid = {archive_id}")
    rows = res.fetchall()
    if len(rows) == 0:
        raise LookupError(f"Archive ID not found: {archive_id}")

    dir_id: int = rows[0][0]
    tarname: str = rows[0][1]
    if not tarname:
        raise LookupError(f"Directory.Archive {dir_id}.{archive_id} is not a TAR archive.")

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
        if entrytype not in (0, 5):  # process only Regular and Directory entries
            continue
        offset = int(row[1].strip())
        data_offset = int(row[2].strip())
        size = int(row[3].strip())
        archivename = row[4].strip()

        # find (first non-deleted) replica of dataset that matches the name
        res = sql_execute(
            cur,
            f"select rowid, datasetid, hostid, dirid, size from replica where name = '{archivename}' and deltime = 0",
        )
        replica_row = res.fetchone()
        if replica_row is None:
            if args.verbose:
                print(f"{indent}  No suitable replica of {archivename} found. Skip")
            continue
        replica_id: int = replica_row[0]
        replica_dataset_id: int = replica_row[1]
        replica_host_id: int = replica_row[2]
        replica_dir_id: int = replica_row[3]
        replica_size: int = replica_row[4]
        print(f"{indent}Replica id = {replica_id} on host {replica_host_id}, dir {replica_dir_id}")

        # find dataset of this replica
        res = sql_execute(
            cur,
            f"select name, fileformat from dataset where rowid = '{replica_dataset_id}'",
        )
        dsrow = res.fetchone()
        dsname = dsrow[0]
        fileformat: str = dsrow[1]
        print(f"{indent}  Dataset {replica_dataset_id:<5} {dsname}")

        entries: dict = {"": [offset, data_offset, size]}
        if entrytype == TARTYPES["reg"]:
            if size == replica_size:
                archive_idx_replica(
                    dsname,
                    dir_id,
                    archive_id,
                    replica_id,
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

        elif entrytype == TARTYPES["dir"] and fileformat == "ADIOS":
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
            archive_idx_replica(
                dsname,
                dir_id,
                archive_id,
                replica_id,
                entries,
                cur,
                con,
                indent=indent + "  ",
            )
    csvfile.close()


def add_archival_storage(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    protocol = args.system.lower()
    if protocol not in ("https", "http", "ftp", "s3"):
        protocol = ""

    print(f"Add archival storage host = {args.host}, directory = {args.directory}, archive system {args.system}")
    print(f"                     tarfile = {args.tarfilename} taridx = {args.tarfileidx}")

    host_id = add_host_name(args.longhostname, args.host, cur, protocol, indent="  ")
    dir_id = add_directory(host_id, args.directory, cur, indent="  ")
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

    res = sql_execute(
        cur,
        "select rowid from archive where dirid = " + str(dir_id) + ' and tarname = "' + tarname + '"',
    )
    row = res.fetchone()
    if row is not None:
        archive_id = row[0]
        print(f"  Found archive already in the database, rowid = {archive_id}")
    else:
        cur_archive = sql_execute(
            cur,
            "insert into archive (dirid, tarname, system, notes) values  (?, ?, ?, ?) ",
            (dir_id, tarname, args.system, notes),
        )
        archive_id = lastrowid_or_zero(cur_archive)
        sql_commit(con)

    if archive_id == 0:
        print("  ERROR: Could not insert information into table 'archive' for some reason")
    elif args.tarfileidx:
        archive_idx(args, archive_id, cur, con, indent="  ")


def update(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    long_host_name, short_host_name = get_host_name(args)

    host_id = add_host_name(long_host_name, short_host_name, cur)
    key_id = add_key_id(args.encryption_key_id, cur)

    if args.remote_data and args.s3_bucket is not None:
        rootdir = args.s3_bucket
    else:
        rootdir = getcwd()

    dir_id = add_directory(host_id, rootdir, cur)
    sql_commit(con)

    if args.command == "dataset":
        process_datasets(args, cur, host_id, dir_id, key_id, long_host_name + rootdir, rootdir)
    elif args.command == "text":
        process_text_files(args, cur, host_id, dir_id, key_id, long_host_name + rootdir, rootdir)
    elif args.command == "image":
        process_image(args, cur, host_id, dir_id, key_id, long_host_name + rootdir, rootdir)

    sql_commit(con)


def create(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    print(f"Create new archive {args.campaign_file_name}")
    sql_execute(cur, "create table info(id TEXT, name TEXT, version TEXT, modtime INT)")
    sql_commit(con)
    sql_execute(
        cur,
        "insert into info values (?, ?, ?, ?)",
        ("ACA", "ADIOS Campaign Archive", ACA_VERSION, CURRENT_TIME),
    )

    sql_execute(cur, "create table key" + "(keyid TEXT PRIMARY KEY)")
    sql_execute(
        cur,
        "create table host"
        + "(hostname TEXT PRIMARY KEY, longhostname TEXT, modtime INT, deltime INT, default_protocol TEXT)",
    )
    sql_execute(
        cur,
        "create table directory" + "(hostid INT, name TEXT, modtime INT, deltime INT, PRIMARY KEY (hostid, name))",
    )
    sql_execute(
        cur,
        "create table timeseries" + "(tsid INTEGER PRIMARY KEY, name TEXT UNIQUE)",
    )
    sql_execute(
        cur,
        "create table dataset"
        + "(name TEXT, uuid TEXT, modtime INT, deltime INT, fileformat TEXT, tsid INT, tsorder INT"
        + ", PRIMARY KEY (name))",
    )
    sql_execute(
        cur,
        "create table replica"
        + "(datasetid INT, hostid INT, dirid INT, archiveid INT, name TEXT, modtime INT, deltime INT"
        + ", keyid INT, size INT"
        + ", PRIMARY KEY (datasetid, hostid, dirid, archiveid, name))",
    )
    sql_execute(
        cur,
        "create table file"
        + "(fileid INTEGER PRIMARY KEY, name TEXT, compression INT, lenorig INT"
        + ", lencompressed INT, modtime INT, checksum TEXT, data BLOB)",
    )
    sql_execute(
        cur,
        "create table repfiles" + "(replicaid INT, fileid INT, PRIMARY KEY (replicaid, fileid))",
    )
    sql_execute(
        cur,
        "create table accuracy" + "(replicaid INT, accuracy REAL, norm REAL, relative INT, PRIMARY KEY (replicaid))",
    )
    sql_execute(
        cur,
        "create table resolution" + "(replicaid INT, x INT, y INT, PRIMARY KEY (replicaid))",
    )
    sql_execute(
        cur,
        "create table archive" + "(dirid INT, tarname TEXT,system TEXT, notes BLOB, PRIMARY KEY (dirid, tarname))",
    )
    sql_execute(
        cur,
        "create table archiveidx"
        + "(archiveid INT, replicaid INT, filename TEXT, offset INT, offset_data INT, size INT"
        + ", PRIMARY KEY (archiveid, replicaid, filename))",
    )
    sql_commit(con)
    cur.close()
    con.close()
    while not exists(args.campaign_file_name):
        sleep(0.1)


def delete_dataset_if_empty(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    datasetid: int,
    indent: str,
):
    print(f"{indent}Check if dataset {datasetid} still has replicas")
    res = sql_execute(
        cur,
        "select rowid from replica " + f" where datasetid = {datasetid} and deltime = 0",
    )
    replicas = res.fetchall()
    if len(replicas) == 0:
        print("{indent}  Dataset without replicas found. Deleting.")
        sql_execute(
            cur,
            f"update dataset set deltime = {CURRENT_TIME} " + f"where rowid = {datasetid}",
        )


def delete_replica(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    repid: int,
    delete_empty_dataset: bool,
    indent: str = "",
):
    print(f"{indent}delete replica with id {repid}")
    res = sql_execute(cur, "select datasetid, hostid, dirid from replica " + f"where rowid = {repid}")
    replicas = res.fetchall()
    datasetid = 0
    for rep in replicas:
        datasetid = rep[0]
        sql_execute(
            cur,
            f"update replica set deltime = {CURRENT_TIME} " + f"where rowid = {repid}",
        )
    if delete_empty_dataset:
        sql_execute(cur, f"delete from repfiles where replicaid = {repid}")
        sql_execute(cur, "delete from file where fileid not in (select fileid from repfiles)")
        delete_dataset_if_empty(args, cur, con, datasetid, indent=indent + "  ")


def delete_dataset(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    name: str = "",
    uniqueid: str = "",
):
    if len(name) > 0:
        print(f"Delete dataset with name {name}")
        cur_ds = sql_execute(
            cur,
            f'update dataset set deltime = {CURRENT_TIME} where name = "{name}" returning rowid',
        )
    elif len(uniqueid) > 0:
        print(f"Delete dataset with uuid = {uniqueid}")
        cur_ds = sql_execute(
            cur,
            f'update dataset set deltime = {CURRENT_TIME} where uuid = "{uniqueid}" returning rowid',
        )
    else:
        raise LookupError("delete_dataset() requires name or unique id")

    row_id = cur_ds.fetchone()[0]
    res = sql_execute(
        cur_ds,
        "select rowid from replica " + f" where datasetid = {row_id} and deltime = 0",
    )
    replicas = res.fetchall()
    for rep in replicas:
        delete_replica(args, cur, con, rep[0], False)


def delete(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
    if args.uuid is not None:
        for uid in args.uuid:
            delete_dataset(args, cur, con, uniqueid=uid)
            sql_commit(con)

    if args.name is not None:
        for name in args.name:
            delete_dataset(args, cur, con, name=name)
            sql_commit(con)

    if args.replica is not None:
        for repid in args.replica:
            delete_replica(args, cur, con, repid, True)
            sql_commit(con)


class Manager:  # pylint: disable=too-many-public-methods
    """Manager API for campaign archives."""

    def __init__(
        self,
        archive: str | None = None,
        hostname: str | None = None,
        campaign_store: str | None = None,
        keyfile: str | None = None,
        verbose: int = 0,
        base_args: argparse.Namespace | None = None,
    ):
        if base_args is None:
            if archive is None:
                raise ValueError("Manager requires an archive path")
            base_args = self.build_base_args(
                archive=archive,
                hostname=hostname,
                campaign_store=campaign_store,
                keyfile=keyfile,
                verbose=verbose,
            )
        self.base_args = base_args
        self.apply_encryption_key(self.base_args)
        check_campaign_store(self.base_args)

    def build_base_args(
        self,
        archive: str,
        hostname: str | None,
        campaign_store: str | None,
        keyfile: str | None,
        verbose: int,
    ) -> argparse.Namespace:
        cmdline = []
        if verbose:
            cmdline.extend(["-v"] * verbose)
        if campaign_store is not None:
            cmdline.extend(["--campaign_store", campaign_store])
        if hostname is not None:
            cmdline.extend(["--hostname", hostname])
        if keyfile is not None:
            cmdline.extend(["--keyfile", keyfile])
        cmdline.append(archive)
        parser = ArgParser(args=cmdline, prog=None)
        return parser.args

    def apply_encryption_key(self, args: argparse.Namespace):
        if args.keyfile:
            key = read_key(args.keyfile)
            # ask for password at this point
            args.encryption_key = key.get_decrypted_key()
            args.encryption_key_id = key.id
        else:
            args.encryption_key = None
            args.encryption_key_id = None

    def build_command_args(self, command: str, updates: dict | None = None) -> argparse.Namespace:
        cmd_args = argparse.Namespace(**vars(self.base_args))
        cmd_args.command = command
        if updates:
            for key, value in updates.items():
                setattr(cmd_args, key, value)
        return cmd_args

    def ensure_archive_exists(self, args: argparse.Namespace):
        if not exists(args.campaign_file_name):
            print(f"ERROR: archive {args.campaign_file_name} does not exist")
            sys.exit(1)

    def ensure_archive_missing(self, args: argparse.Namespace):
        if exists(args.campaign_file_name):
            print(f"ERROR: archive {args.campaign_file_name} already exist")
            sys.exit(1)

    def open_connection(self, args: argparse.Namespace) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        con = sqlite3.connect(args.campaign_file_name)
        cur = con.cursor()
        return con, cur

    def create(
        self,
        args: argparse.Namespace | None = None,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        if args is None:
            args = self.build_command_args("create")
        if cur is None or con is None:
            con, cur = self.open_connection(args)
        create(args, cur, con)

    def info(
        self,
        args: argparse.Namespace | None = None,
        cur: sqlite3.Cursor | None = None,
        list_replicas: bool = False,
        list_files: bool = False,
        show_deleted: bool = False,
        show_checksum: bool = False,
    ) -> InfoResult:
        if args is None:
            args = self.build_command_args(
                "info",
                {
                    "list_replicas": list_replicas,
                    "list_files": list_files,
                    "show_deleted": show_deleted,
                    "show_checksum": show_checksum,
                },
            )
            self.ensure_archive_exists(args)
            con, cur = self.open_connection(args)
            info_data = collect_info(args, cur)
            cur.close()
            con.close()
            return info_data
        if cur is None:
            raise ValueError("info requires a cursor when args are provided")
        return collect_info(args, cur)

    def update(
        self,
        args: argparse.Namespace,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        self.ensure_archive_exists(args)
        if cur is None or con is None:
            con, cur = self.open_connection(args)
            update(args, cur, con)
            cur.close()
            con.close()
            return
        update(args, cur, con)

    def dataset(self, files: list[str | Path] | str | Path, name: str | None = None):
        self.add_dataset(files, name=name)

    def add_dataset(self, files: list[str | Path] | str | Path, name: str | None = None):
        file_list = self.normalize_files(files)
        if name is not None and len(file_list) > 1:
            raise ValueError("Invalid arguments for dataset: when using --name <name>, only one dataset is allowed")
        cmd_args = self.build_command_args("dataset", {"files": file_list, "name": name})
        self.update(cmd_args)

    def text(self, files: list[str | Path] | str | Path, name: str | None = None, store: bool = False):
        self.add_text(files, name=name, store=store)

    def add_text(self, files: list[str | Path] | str | Path, name: str | None = None, store: bool = False):
        file_list = self.normalize_files(files)
        if name is not None and len(file_list) > 1:
            raise ValueError("Invalid arguments for text: when using --name <name>, only one text file is allowed")
        cmd_args = self.build_command_args(
            "text",
            {"files": file_list, "name": name, "store": store},
        )
        self.update(cmd_args)

    def image(
        self,
        file_path: str | Path,
        name: str | None = None,
        store: bool = False,
        thumbnail: list[int] | tuple[int, int] | None = None,
    ):
        self.add_image(file_path, name=name, store=store, thumbnail=thumbnail)

    def add_image(
        self,
        file_path: str | Path,
        name: str | None = None,
        store: bool = False,
        thumbnail: list[int] | tuple[int, int] | None = None,
    ):
        file_path = str(file_path)
        thumb_value = None
        if thumbnail is not None:
            thumb_value = [int(thumbnail[0]), int(thumbnail[1])]
        cmd_args = self.build_command_args(
            "image",
            {"file": file_path, "name": name, "store": store, "thumbnail": thumb_value},
        )
        self.update(cmd_args)

    def delete(
        self,
        args: argparse.Namespace,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        self.ensure_archive_exists(args)
        if cur is None or con is None:
            con, cur = self.open_connection(args)
            delete(args, cur, con)
            cur.close()
            con.close()
            return
        delete(args, cur, con)

    def delete_campaign_file(self, args: argparse.Namespace | None = None) -> int:
        if args is None:
            cmd_args = self.build_command_args("delete", {"campaign": True})
            return delete_campaign_file(cmd_args)
        return delete_campaign_file(args)

    def add_archival_storage(
        self,
        args: argparse.Namespace,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        if cur is None or con is None:
            con, cur = self.open_connection(args)
            add_archival_storage(args, cur, con)
            cur.close()
            con.close()
            return
        add_archival_storage(args, cur, con)

    def archive_dataset(
        self,
        args: argparse.Namespace,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        if cur is None or con is None:
            con, cur = self.open_connection(args)
            archive_dataset(args, cur, con)
            cur.close()
            con.close()
            return
        archive_dataset(args, cur, con)

    def add_time_series(
        self,
        args: argparse.Namespace,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        if cur is None or con is None:
            con, cur = self.open_connection(args)
            add_time_series(args, cur, con)
            cur.close()
            con.close()
            return
        add_time_series(args, cur, con)

    def upgrade(
        self,
        args: argparse.Namespace,
        cur: sqlite3.Cursor | None = None,
        con: sqlite3.Connection | None = None,
    ):
        if cur is None or con is None:
            con, cur = self.open_connection(args)
            upgrade_aca(args, cur, con)
            cur.close()
            con.close()
            return
        upgrade_aca(args, cur, con)

    def normalize_files(self, files: list[str | Path] | str | Path) -> list[str]:
        if isinstance(files, (str, Path)):
            return [str(files)]
        return [str(entry) for entry in files]


def delete_campaign_file(args: argparse.Namespace):
    if exists(args.campaign_file_name):
        print(f"Delete campaign archive {args.campaign_file_name}")
        remove(args.campaign_file_name)
        while exists(args.campaign_file_name):
            sleep(0.1)
        return 0
    print(f"ERROR: archive {args.campaign_file_name} does not exist")
    return 1


def main(args=None, prog=None):
    parser = ArgParser(args=args, prog=prog)
    manager = Manager(base_args=parser.args)

    con: sqlite3.Connection
    cur: sqlite3.Cursor
    connected = False

    while parser.parse_next_command():
        print("=" * 70)
        # print(parser.args)
        # print("--------------------------")
        if parser.args.command == "delete" and parser.args.campaign is True:
            manager.delete_campaign_file(parser.args)
            continue

        if parser.args.command == "create":
            manager.ensure_archive_missing(parser.args)
            manager.ensure_archive_missing(parser.args)
        else:
            manager.ensure_archive_exists(parser.args)

        if not connected:
            con = sqlite3.connect(parser.args.campaign_file_name)
            cur = con.cursor()
            connected = True

        # pylint: disable=no-else-continue
        if parser.args.command == "info":
            info_data = manager.info(args=parser.args, cur=cur)
            print_info(info_data, parser.args)
            continue
        elif parser.args.command == "create":
            manager.create(parser.args, cur, con)
            connected = False
            continue
        elif parser.args.command in ("dataset", "text", "image"):
            manager.update(parser.args, cur, con)
            continue
        elif parser.args.command == "delete":
            manager.delete(parser.args, cur, con)
            continue
        elif parser.args.command == "add-archival-storage":
            manager.add_archival_storage(parser.args, cur, con)
        elif parser.args.command == "archived":
            manager.archive_dataset(parser.args, cur, con)
        elif parser.args.command == "time-series":
            manager.add_time_series(parser.args, cur, con)
        elif parser.args.command == "upgrade":
            manager.upgrade(parser.args, cur, con)
        else:
            print(f"This should not happen. Unknown command accepted by argparser: {parser.args.command}")

    if connected:
        cur.close()
        con.close()

    if len(sql_error_list) > 0:
        print()
        print("!!!! SQL Errors encountered")
        for e in sql_error_list:
            print(f"  {e.sqlite_errorcode}  {e.sqlite_errorname}: {e}")
        print("!!!!")
        print()


if __name__ == "__main__":
    main()
