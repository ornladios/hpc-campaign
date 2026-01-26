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

from .config import ACA_VERSION, Config
from .hdf5_metadata import copy_hdf5_file_without_data, is_hdf5_dataset
from .taridx import TARTYPES
from .utils import (
    get_folder_size,
    sql_commit,
    sql_execute,
)

CURRENT_TIME = time_ns()


def parse_date_to_utc(date, fmt=None):
    if fmt is None:
        fmt = "%Y-%m-%d %H:%M:%S %z"  # Defaults to : 2022-08-31 07:47:30 -0000
    get_date_obj = parse(str(date))
    return get_date_obj.timestamp()


def set_default_args(args: argparse.Namespace) -> argparse.Namespace:
    """Set default values after user arguments are already parsed"""
    args.user_options = Config()
    args.host_options = args.user_options.read_host_config()

    if args.verbose == 0:
        args.verbose = args.user_options.verbose

    if not args.campaign_store:
        args.campaign_store = args.user_options.campaign_store_path

    if args.campaign_store:
        while args.campaign_store[-1] == "/":
            args.campaign_store = args.campaign_store[:-1]

    args.remote_data = False
    args.s3_endpoint = None
    if not args.hostname:
        args.hostname = args.user_options.host_name
    elif args.hostname in args.host_options and args.hostname != args.user_options.host_name:
        args.remote_data = True
        hostopt = args.host_options.get(args.hostname)
        if hostopt is not None:
            opt_id = next(iter(hostopt))
            if hostopt[opt_id]["protocol"].casefold() == "s3":
                args.s3_endpoint = hostopt[opt_id]["endpoint"]
                if args.s3_bucket is None:
                    print("ERROR: Remote option for an S3 server requires --s3_bucket")
                    sys.exit(1)
                if args.s3_datetime is None:
                    print("ERROR: Remote option for an S3 server requires --s3_datetime")
                    sys.exit(1)

    args.campaign_file_name = args.archive
    if args.archive is not None:
        if not args.archive.endswith(".aca"):
            args.campaign_file_name += ".aca"
        if not exists(args.campaign_file_name) and not args.campaign_file_name.startswith("/") and args.campaign_store:
            args.campaign_file_name = args.campaign_store + "/" + args.campaign_file_name

    args.local_campaign_dir = ".adios-campaign/"

    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Campaign File Name = {args.campaign_file_name}")
        print(f"# Campaign Store = {args.campaign_store}")
        print(f"# Host name = {args.hostname}")
        print(f"# Key file = {args.keyfile}")

    return args


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


def process_data(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    host_id: int,
    dir_id: int,
    key_id: int,
    dirpath: str,
    location: str,
):
    for entry in args.files:
        dataset_name = entry
        if args.name is not None:
            dataset_name = args.name
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
            ds_id = add_dataset_to_archive(dataset_name, cur, unique_id, "ADIOS", mt)
            rep_id = add_replica_to_archive(
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
            ds_id = add_dataset_to_archive(dataset_name, cur, unique_id, "ADIOS", mt)
            filesize = get_folder_size(entry)
            rep_id = add_replica_to_archive(
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
            ds_id = add_dataset_to_archive(dataset_name, cur, unique_id, "HDF5", mt)
            rep_id = add_replica_to_archive(
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
            print(f"WARNING: Data {entry} is neither an ADIOS nor an HDF5 file. Skip")


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
        ds_id = add_dataset_to_archive(dataset, cur, unique_id, "TEXT", ct)
        rep_id = add_replica_to_archive(host_id, dir_id, 0, key_id, entry, cur, ds_id, ct, filesize, indent="  ")
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

    ds_id = add_dataset_to_archive(dataset, cur, unique_id, "IMAGE", mt, indent="  ")
    rep_id = add_replica_to_archive(host_id, dir_id, 0, key_id, args.file, cur, ds_id, mt, filesize, indent="  ")
    add_resolution_to_archive(rep_id, imgres[0], imgres[1], cur, indent="  ")

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
            add_resolution_to_archive(thumb_rep_id, imgres[0], imgres[1], cur, indent="  ")
            remove(thumbfilename)


# pylint: disable=too-many-statements
def archive_dataset(
    args: argparse.Namespace,
    cur: sqlite3.Cursor,
    con: sqlite3.Connection,
    indent: str = "",
) -> int:
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
    if args.replica == 0:
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
    print(f"----- select datasetid, name, modtime, keyid, size from replica where rowid = {orig_rep_id}")
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
        add_resolution_to_archive(rep_id, x, y, cur, indent=indent)

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


def delete_time_series(name: str, cur: sqlite3.Cursor, con: sqlite3.Connection):
    res = sql_execute(cur, f'select tsid from timeseries where name = "{name}"')
    rows = res.fetchall()
    if len(rows) > 0:
        ts_id = rows[-1][0]
        print(f"Remove {name} from time-series but leave datasets alone")
        res = sql_execute(cur, f'delete from timeseries where name = "{name}"')
        sql_execute(cur, f'update dataset set tsid = 0, tsorder = 0 where tsid = "{ts_id}"')
    else:
        print(f"Time series {name} was not found")
    sql_commit(con)


def add_time_series(args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection):
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


def check_archival_storage_system_name(system: str):
    s = system.lower()
    if s not in ("https", "http", "ftp", "s3", "kronos", "hpss", "fs"):
        raise ValueError("Archival storage system/protocol must be one of:Kronos, HPSS, HTTPS, S3, HTTP, FTP")


def add_archival_storage(
    args: argparse.Namespace, cur: sqlite3.Cursor, con: sqlite3.Connection
) -> tuple[int, int, int]:
    """return tuple [hostid, directoryid, archiveid]"""
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
            print(f"WARNING: Failed to read notes from {args.note}: {e.strerror}.")
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

    return host_id, dir_id, archive_id


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

    if args.command == "data":
        process_data(args, cur, host_id, dir_id, key_id, long_host_name + rootdir, rootdir)
    elif args.command == "text":
        process_text_files(args, cur, host_id, dir_id, key_id, long_host_name + rootdir, rootdir)
    elif args.command == "image":
        process_image(args, cur, host_id, dir_id, key_id, long_host_name + rootdir, rootdir)

    sql_commit(con)


def create_tables(campaign_file_name: str, con: sqlite3.Connection):
    print(f"Create new archive {campaign_file_name}")
    cur = con.cursor()
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
    while not exists(campaign_file_name):
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
