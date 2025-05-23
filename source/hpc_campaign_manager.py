#!/usr/bin/env python3

import argparse
import glob
import sqlite3
import zlib
import yaml
import uuid
import nacl.encoding
import nacl.secret
import nacl.utils
import nacl.pwhash
import sys
from dateutil.parser import parse
from os import chdir, getcwd, remove, stat
from os.path import exists, isdir, dirname, basename, expanduser
from re import sub
from socket import getfqdn
from time import time_ns

from hpc_campaign_key import Key, read_key
from hpc_campaign_config import Config, ADIOS_ACA_VERSION
from hpc_campaign_utils import timestamp_to_datetime
from hpc_campaign_hdf5_metadata import copy_hdf5_file_without_data, IsHDF5Dataset
from hpc_campaign_manager_args import ArgParser


def CheckCampaignStore(args):
    if args.campaign_store is not None and not isdir(args.campaign_store):
        print("ERROR: Campaign directory " + args.campaign_store + " does not exist", flush=True)
        exit(1)


def CheckLocalCampaignDir(args):
    if not isdir(args.LocalCampaignDir):
        print(
            "ERROR: Shot campaign data '" +
            args.LocalCampaignDir +
            "' does not exist. Run this command where the code was executed.",
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

def compressFile(f):
    compObj = zlib.compressobj()
    compressed = bytearray()
    blocksize = 1073741824  # 1GB #1024*1048576
    len_orig = 0
    len_compressed = 0
    block = f.read(blocksize)
    while block:
        len_orig += len(block)
        cBlock = compObj.compress(block)
        compressed += cBlock
        len_compressed += len(cBlock)
        block = f.read(blocksize)
    cBlock = compObj.flush()
    compressed += cBlock
    len_compressed += len(cBlock)

    return compressed, len_orig, len_compressed


def decompressBuffer(buf: bytearray):
    data = zlib.decompress(buf)
    return data

def encryptBuffer(args: argparse.Namespace, buf: bytearray):
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

def AddFileToArchive(args: argparse.Namespace, filename: str, cur: sqlite3.Cursor, dsID: int):
    compressed = 1
    try:
        f = open(filename, "rb")
        compressed_data, len_orig, len_compressed = compressFile(f)

    except IOError:
        print(f"ERROR While reading file {filename}")
        return

    encrypted_data = encryptBuffer(args, compressed_data)

    statres = stat(filename)
    ct = statres.st_ctime_ns

    cur.execute(
        "insert into file "
        "(datasetid, name, compression, lenorig, lencompressed, ctime, data) "
        "values (?, ?, ?, ?, ?, ?, ?) "
        "on conflict (datasetid, name) do update "
        "set compression = ?, lenorig = ?, lencompressed = ?, ctime = ?, data = ?",
        (
            dsID,
            filename,
            compressed,
            len_orig,
            len_compressed,
            ct,
            encrypted_data,
            compressed,
            len_orig,
            len_compressed,
            ct,
            encrypted_data,
        ),
    )


def AddDatasetToArchive(
    args: argparse.Namespace, hostID: int, dirID: int, keyID: int, dataset: str, cur: sqlite3.Cursor, uniqueID: str
, format: str) -> int:

    print(f"Add dataset {dataset} to archive")

    if args.remote_data:
        if args.s3_datetime:
            ct = parse_date_to_utc(args.s3_datetime)
        else:
            ct = 0
    else:
        statres = stat(dataset)
        ct = statres.st_ctime_ns

    curDS = cur.execute(
        "insert into dataset (uuid, hostid, dirid, name, ctime, keyid, fileformat) "
        "values  (?, ?, ?, ?, ?, ?, ?) "
        "on conflict (uuid) do update set ctime = ?, keyid = ?",
        (
            uniqueID,
            hostID,
            dirID,
            dataset,
            ct,
            keyID,
            format,
            ct,
            keyID
        ),
    )

    rowID = lastrowid_or_zero(curDS)
    return rowID


def ProcessDatasets(args: argparse.Namespace, cur: sqlite3.Cursor, hostID: int, dirID: int, keyID: int, 
                 dirpath: str, location: str):
    for entry in args.files:
        print(f"Process entry {entry}:")
        uniqueID = uuid.uuid3(uuid.NAMESPACE_URL, location+"/"+entry).hex
        dsID = 0
        dataset = entry
        if args.remote_data:
            dsID = AddDatasetToArchive(args, hostID, dirID, -1, dataset, cur, uniqueID, "ADIOS")
        elif IsADIOSDataset(dataset):
            dsID = AddDatasetToArchive(args, hostID, dirID, keyID, dataset, cur, uniqueID, "ADIOS")
            cwd = getcwd()
            chdir(dataset)
            mdFileList = glob.glob("*md.*")
            profileList = glob.glob("profiling.json")
            files = mdFileList + profileList
            for f in files:
                AddFileToArchive(args, f, cur, dsID)
            chdir(cwd)
        elif IsHDF5Dataset(dataset):
            mdfilename = dirname(dataset)+"/md_"+basename(dataset)
            copy_hdf5_file_without_data(dataset, mdfilename)
            dsID = AddDatasetToArchive(args, hostID, dirID, keyID, dataset, cur, uniqueID, "HDF5")
            AddFileToArchive(args, mdfilename, cur, dsID)
            remove(mdfilename)
        else:
            print(f"WARNING: Dataset {dataset} is neither an ADIOS nor an HDF5 dataset. Skip")


def ProcessTextFiles(args: argparse.Namespace, cur: sqlite3.Cursor, hostID: int, dirID: int, keyID: int, 
                 dirpath: str, location: str):
    for entry in args.files:
        print(f"Process entry {entry}:")
        uniqueID = uuid.uuid3(uuid.NAMESPACE_URL, location+"/"+entry).hex
        dsID = AddDatasetToArchive(args, hostID, dirID, keyID, entry, cur, uniqueID, "TEXT")
        AddFileToArchive(args, entry, cur, dsID)


def ProcessImage(args: argparse.Namespace, cur: sqlite3.Cursor, hostID: int, dirID: int, keyID: int, 
                 dirpath: str, location: str):
    print("Adding images is not supported yet")


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


def AddHostName(longHostName, shortHostName) -> int:
    res = cur.execute('select rowid from host where hostname = "' + shortHostName + '"')
    row = res.fetchone()
    if row is not None:
        hostID = row[0]
        print(f"Found host {shortHostName} in database, rowid = {hostID}")
    else:
        curHost = cur.execute("insert into host values (?, ?)", (shortHostName, longHostName))
        hostID = lastrowid_or_zero(curHost)
        print(f"Inserted host {shortHostName} into database, rowid = {hostID}")
    return hostID


def AddDirectory(hostID: int, path: str) -> int:
    res = cur.execute(
        "select rowid from directory where hostid = " + str(hostID) + ' and name = "' + path + '"'
    )
    row = res.fetchone()
    if row is not None:
        dirID = row[0]
        print(f"Found directory {path} with hostID {hostID} in database, rowid = {dirID}")
    else:
        curDirectory = cur.execute("insert into directory values (?, ?)", (hostID, path))
        dirID = lastrowid_or_zero(curDirectory)
        print(f"Inserted directory {path} into database, rowid = {dirID}")
    return dirID


def AddKeyID(key_id: str, cur: sqlite3.Cursor) -> int:
    if key_id:
        res = cur.execute('select rowid from key where keyid = "' + key_id + '"')
        row = res.fetchone()
        if row is not None:
            keyID = row[0]
            print(f"Found key {key_id} in database, rowid = {keyID}")
        else:
            cmd = f"insert into key values (\"{(key_id)}\")"
            curKey = cur.execute(cmd)
            # curKey = cur.execute("insert into key values (?)", (key_id))
            keyID = lastrowid_or_zero(curKey)
            print(f"Inserted key {key_id} into database, rowid = {keyID}")
        return keyID
    else:
        return 0  # an invalid row id


def Update(args: argparse.Namespace, cur: sqlite3.Cursor):
    longHostName, shortHostName = GetHostName(args)

    hostID = AddHostName(longHostName, shortHostName)
    keyID = AddKeyID(args.encryption_key_id, cur)

    if args.remote_data and args.s3_bucket is not None:
        rootdir = args.s3_bucket
    else:
        rootdir = getcwd()
    dirID = AddDirectory(hostID, rootdir)
    con.commit()

    if (args.command == "dataset"):
        ProcessDatasets(args, cur, hostID, dirID, keyID, longHostName+rootdir, rootdir)
    elif (args.command == "text"):
        ProcessTextFiles(args, cur, hostID, dirID, keyID, longHostName+rootdir, rootdir)
    elif (args.command == "image"):
        ProcessImage(args, cur, hostID, dirID, keyID, longHostName+rootdir, rootdir)

    con.commit()


def Create(args: argparse.Namespace, cur: sqlite3.Cursor):
    epoch = time_ns()
    cur.execute("create table info(id TEXT, name TEXT, version TEXT, ctime INT)")
    cur.execute(
        "insert into info values (?, ?, ?, ?)",
        ("ACA", "ADIOS Campaign Archive", ADIOS_ACA_VERSION, epoch),
    )
    cur.execute("create table key" + "(keyid TEXT PRIMARY KEY)")
    cur.execute("create table host" + "(hostname TEXT PRIMARY KEY, longhostname TEXT)")
    cur.execute("create table directory" + "(hostid INT, name TEXT, PRIMARY KEY (hostid, name))")
    cur.execute(
        "create table dataset" +
        "(uuid TEXT, hostid INT, dirid INT, name TEXT, ctime INT, keyid INT, fileformat TEXT" +
        ", PRIMARY KEY (uuid))"
    )
    cur.execute(
        "create table file" +
        "(datasetid INT, name TEXT, compression INT, lenorig INT" +
        ", lencompressed INT, ctime INT, data BLOB" +
        ", PRIMARY KEY (datasetid, name))"
    )
    con.commit()


def Info(cur: sqlite3.Cursor):
    res = cur.execute("select id, name, version, ctime from info")
    info = res.fetchone()
    t = timestamp_to_datetime(info[3])
    print(f"{info[1]}, version {info[2]}, created on {t}")
    version = float(info[2])
    res = cur.execute("select rowid, hostname, longhostname from host")
    hosts = res.fetchall()
    for host in hosts:
        print(f"hostname = {host[1]}   longhostname = {host[2]}")
        res2 = cur.execute(
            'select rowid, name from directory where hostid = "' + str(host[0]) + '"'
        )
        dirs = res2.fetchall()
        for dir in dirs:
            print(f"    dir = {dir[1]}")
            if version >= 0.4:
                res3 = cur.execute(
                    'select rowid, uuid, name, ctime, fileformat from dataset where hostid = "' +
                    str(host[0]) +
                    '" and dirid = "' +
                    str(dir[0]) +
                    '"'
                )
                datasets = res3.fetchall()
                for dataset in datasets:
                    t = timestamp_to_datetime(dataset[3])
                    print(f"        dataset = {dataset[1]}  {dataset[4]:5}  {t}   {dataset[2]} ")
            else:
                res3 = cur.execute(
                    'select rowid, uuid, name, ctime from bpdataset where hostid = "' +
                    str(host[0]) +
                    '" and dirid = "' +
                    str(dir[0]) +
                    '"'
                )
                datasets = res3.fetchall()
                for dataset in datasets:
                    t = timestamp_to_datetime(dataset[3])
                    print(f"        dataset = {dataset[1]}  ADIOS  {t}   {dataset[2]} ")


def Delete(args: argparse.Namespace):
    if exists(args.CampaignFileName):
        print(f"Delete archive {args.CampaignFileName}")
        remove(args.CampaignFileName)
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

        if parser.args.command == "delete":
            Delete(parser.args)
            continue

        if parser.args.command == "create":
            print("Create archive")
            if exists(parser.args.CampaignFileName):
                print(f"ERROR: archive {parser.args.CampaignFileName} already exist")
                exit(1)
        else:
            print(f"{parser.args.command} archive")
            if not exists(parser.args.CampaignFileName):
                print(f"ERROR: archive {parser.args.CampaignFileName} does not exist")
                exit(1)

        if not connected:
            con = sqlite3.connect(parser.args.CampaignFileName)
            cur = con.cursor()
            connected = True

        if parser.args.command == "info":
            Info(cur)
            continue
        elif parser.args.command == "create":
            Create(parser.args, cur)
            continue
        elif (parser.args.command == "dataset" or 
              parser.args.command == "text" or 
              parser.args.command == "image"
        ):
            Update(parser.args, cur)
            continue
        else:
            print(f"This should not happen. Unknown command accepted by argparser: {parser.args.command}")

    if connected:
        cur.close()
        con.close()
