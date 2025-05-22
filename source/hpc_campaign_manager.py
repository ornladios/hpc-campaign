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

def SetupArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        help="Command: create/update/delete/info/list",
        choices=["create", "update", "delete", "info", "list"],
    )
    parser.add_argument(
        "campaign", help="Campaign name or path, with .aca or without", default=None, nargs="?"
    )
    parser.add_argument("--verbose", "-v", help="More verbosity", action="count", default=0)
    parser.add_argument(
        "--campaign_store", "-s", help="Path to local campaign store", default=None
    )
    parser.add_argument("--hostname", "-n", help="Host name unique for hosts in a campaign")
    parser.add_argument("--keyfile", "-k", help="Key file to encrypt metadata")
    parser.add_argument("--s3_bucket", "-b", help="Bucket on S3 server", default=None)
    parser.add_argument(
        "--s3_datetime",
        "-t",
        help="Datetime of data on S3 server in " "'2024-04-19 10:20:15 -0400' format",
        default=None,
    )
    parser.add_argument("--files", "-f", nargs="+", help="Add ADIOS/HDF5 files manually")
    parser.add_argument("--textfiles", "-x", nargs="+", help="Add text files manually")
    args = parser.parse_args()

    # default values
    args.user_options = Config()
    args.host_options = args.user_options.read_host_config()

    if args.verbose == 0:
        args.verbose = args.user_options.verbose

    if args.campaign_store is None:
        args.campaign_store = args.user_options.campaign_store_path

    if args.campaign_store is not None:
        while args.campaign_store[-1] == "/":
            args.campaign_store = args.campaign_store[:-1]

    args.remote_data = False
    args.s3_endpoint = None
    if args.hostname is None:
        args.hostname = args.user_options.host_name
    elif args.hostname in args.host_options and args.hostname != args.user_options.host_name:
        args.remote_data = True
        hostopt = args.host_options.get(args.hostname)
        if hostopt is not None:
            optID = next(iter(hostopt))
            if hostopt[optID]["protocol"].casefold() == "s3":
                args.s3_endpoint = hostopt[optID]["endpoint"]
                if args.s3_bucket is None:
                    print("ERROR: Remote option for an S3 server requires --s3_bucket")
                    exit(1)
                if args.s3_datetime is None:
                    print("ERROR: Remote option for an S3 server requires --s3_datetime")
                    exit(1)

    args.CampaignFileName = args.campaign
    if args.campaign is not None:
        if not args.campaign.endswith(".aca"):
            args.CampaignFileName += ".aca"
        if (
            not exists(args.CampaignFileName) and
            not args.CampaignFileName.startswith("/") and
            args.campaign_store is not None
        ):
            args.CampaignFileName = args.campaign_store + "/" + args.CampaignFileName

    if args.files is None: args.files = []
    if args.textfiles is None: args.textfiles = []
    args.LocalCampaignDir = ".adios-campaign/"

    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Command = {args.command}")
        print(f"# Campaign File Name = {args.CampaignFileName}")
        print(f"# Campaign Store = {args.campaign_store}")
        print(f"# Host name = {args.hostname}")
        print(f"# Key file = {args.keyfile}")
    return args


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


def ProcessFiles(args: argparse.Namespace, cur: sqlite3.Cursor, hostID: int, dirID: int, keyID: int, 
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

    for entry in args.textfiles:
        print(f"Process entry {entry}:")
        uniqueID = uuid.uuid3(uuid.NAMESPACE_URL, location+"/"+entry).hex
        dsID = AddDatasetToArchive(args, hostID, dirID, keyID, entry, cur, uniqueID, "TEXT")
        AddFileToArchive(args, entry, cur, dsID)

def GetHostName():
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


def MergeDBFiles(dbfiles: list):
    # read db files here
    result = list()
    for f1 in dbfiles:
        try:
            con = sqlite3.connect(f1)
        except sqlite3.Error as e:
            print(e)

        cur = con.cursor()
        try:
            cur.execute("select  * from bpfiles")
        except sqlite3.Error as e:
            print(e)
        record = cur.fetchall()
        for item in record:
            result.append(item[0])
        cur.close()
    return result


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
    longHostName, shortHostName = GetHostName()

    hostID = AddHostName(longHostName, shortHostName)
    keyID = AddKeyID(args.encryption_key_id, cur)

    if args.remote_data and args.s3_bucket is not None:
        rootdir = args.s3_bucket
    else:
        rootdir = getcwd()
    dirID = AddDirectory(hostID, rootdir)
    con.commit()

    ProcessFiles(args, cur, hostID, dirID, keyID, longHostName+rootdir, rootdir)

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
    Update(args, cur)


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


def List(args: argparse.Namespace):
    path = args.campaign
    if path is None:
        if args.campaign_store is None:
            print("ERROR: Set --campaign_store for this command")
            return 1
        path = args.campaign_store
    else:
        while path[-1] == "/":
            path = path[:-1]

    # List the local campaign store
    acaList = glob.glob(path + "/**/*.aca", recursive=True)
    if len(acaList) == 0:
        print("There are no campaign archives in  " + path)
        return 2
    else:
        startCharPos = len(path) + 1
        for f in acaList:
            print(f[startCharPos:])
    return 0


def Delete(args: argparse.Namespace):
    if exists(args.CampaignFileName):
        print(f"Delete archive {args.CampaignFileName}")
        remove(args.CampaignFileName)
        return 0
    else:
        print(f"ERROR: archive {args.CampaignFileName} does not exist")
        return 1


if __name__ == "__main__":
    args = SetupArgs()
    CheckCampaignStore(args)

    if args.command == "list":
        exit(List(args))

    if args.command == "delete":
        exit(Delete(args))

    if args.keyfile:
        key = read_key(args.keyfile)
        # ask for password at this point
        args.encryption_key = key.get_decrypted_key()
        args.encryption_key_id = key.id
    else:
        args.encryption_key = None
        args.encryption_key_id = None

    if args.command == "create":
        print("Create archive")
        if exists(args.CampaignFileName):
            print(f"ERROR: archive {args.CampaignFileName} already exist")
            exit(1)
    elif args.command == "update" or args.command == "info":
        print(f"{args.command} archive")
        if not exists(args.CampaignFileName):
            print(f"ERROR: archive {args.CampaignFileName} does not exist")
            exit(1)

    con = sqlite3.connect(args.CampaignFileName)
    cur = con.cursor()

    if args.command == "info":
        Info(cur)
    else:
        if not args.files and not args.textfiles:
            CheckLocalCampaignDir(args)
            # List the local campaign directory
            dbFileList = glob.glob(args.LocalCampaignDir + "/*.db")
            if len(dbFileList) == 0:
                print("There are no campaign data files in  " + args.LocalCampaignDir)
                exit(2)
            args.files = MergeDBFiles(dbFileList)

        if args.command == "create":
            Create(args, cur)
        elif args.command == "update":
            Update(args, cur)

    cur.close()
    con.close()
