#!/usr/bin/env python3
import argparse
import sqlite3
import redis
from os import walk, listdir
from shutil import rmtree
from re import match
from glob import glob
from os.path import exists, join, getsize
from sys import exit
from time import sleep

import redis.exceptions

from hpc_campaign_config import Config, REDIS_PORT
from hpc_campaign_utils import timestamp_to_datetime, input_yes_or_no


def setup_args(cfg: Config):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        help="Command: list/clear",
        choices=["list", "clear"],
    )
    parser.add_argument(
        "campaign", help="Campaign name or path, with .aca or without", default=None, nargs="?"
    )
    parser.add_argument("--redis-port", "-p", help="Key-value database port", default=REDIS_PORT)
    parser.add_argument("--verbose", "-v", help="More verbosity", action="count", default=0)
    parser.add_argument(
        "--yes-to-all", "-y", help="Answer yes automatically", action="store_true", default=False
    )
    args = parser.parse_args()

    args.CampaignFileName = args.campaign
    if args.campaign is not None:
        if not args.campaign.endswith(".aca"):
            args.CampaignFileName += ".aca"
        if (
            not exists(args.CampaignFileName)
            and not args.CampaignFileName.startswith("/")
            and cfg.campaign_store_path is not None
        ):
            args.CampaignFileName = cfg.campaign_store_path + "/" + args.CampaignFileName

    if args.verbose > 1:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Command = {args.command}")
        print(f"# REDIS port = {args.redis_port}")
        print(f"# Archive = {args.CampaignFileName}")
        print(f"# Auto yes = {args.yes_to_all}")
    return args


def folder_size(folder_path: str) -> int:
    folder_size = 0
    for path, dirs, files in walk(folder_path):
        for f in files:
            fp = join(path, f)
            folder_size += getsize(fp)
    return folder_size


def list_cache(args: argparse.Namespace, kvdb: redis.Redis):
    archives = {}  # organize datasets to archives
    cache_folders = glob("[0-9a-f][0-9a-f][0-9a-f]", root_dir=cfg.cache_path)
    if args.verbose > 1:
        print(f"# Found {len(cache_folders)} cache folders in cache directory")
    for folder in cache_folders:
        folder_path = join(cfg.cache_path, folder)
        dataset_ids = glob("[0-9a-f]*", root_dir=folder_path)
        if args.verbose > 1:
            print(f"# Found {len(dataset_ids)} datasets in cache folder {folder}")
        for id in dataset_ids:
            archive_name = "unknown"
            infoname = join(folder_path, id, "info.txt")
            if exists(infoname):
                infofile = open(infoname, "r")
                for line in infofile:
                    if match("Campaign = ", line):
                        archive_name = line[11:-1]
            dirsize = folder_size(join(folder_path, id))

            kvkeys = kvdb.keys(id + "*")
            nkv = len(kvkeys)
            kvsize = 0
            for key in kvkeys:
                kvsize += kvdb.memory_usage(key)

            if args.verbose > 1:
                print(
                    f"# {id} from archive {archive_name}, cache size = {dirsize}, # of keys = {nkv}"
                )
            entry = {id: {"dirsize": dirsize, "nkv": nkv, "kvsize": kvsize}}
            if archive_name not in archives:
                archives[archive_name] = {}
            archives[archive_name].update(entry)
    if args.verbose > 1:
        print("")

    print("folder-size     db-entries db-size     campaign name")
    print("--------------------------------------------------------------------------")
    size_all = 0
    nkv_all = 0
    kvsize_all = 0
    for arch in archives:
        size_arch = 0
        nkv_arch = 0
        kvsize_arch = 0
        for id, idvalues in archives[arch].items():
            size_arch += idvalues["dirsize"]
            nkv_arch += idvalues["nkv"]
            kvsize_arch += idvalues["kvsize"]
        print(f"{size_arch:<15} {nkv_arch:<10} {kvsize_arch:<11} {arch}")
        size_all += size_arch
        nkv_all += nkv_arch
        kvsize_all += kvsize_arch
        if args.verbose > 0:
            for id, idvalues in archives[arch].items():
                print(
                    f"{idvalues['dirsize']:>14}   {idvalues['nkv']:>8}  "
                    f"{idvalues['kvsize']:>10}     {id}"
                )
    print(f"{size_all:<15} {nkv_all:<10} {kvsize_all}")


def delete_cache_items(args: argparse.Namespace, cfg: Config, kvdb: redis.Redis, id: str):
    kvkeys = kvdb.keys(id + "*")
    nkeys = len(kvkeys)
    parent_path = join(cfg.cache_path, id[0:3])
    path = join(parent_path, id)

    if nkeys > 0 or exists(path):
        if args.yes_to_all or input_yes_or_no("Do you want to clear cache for " + id + " (y/n)? "):
            # delete KV entries
            if nkeys > 0:
                kvdb.delete(*kvkeys)
            if args.verbose > 0:
                print(f"  deleted {nkeys} keys from cache db")

            # delete files
            rmtree(path, ignore_errors=True)
            if args.verbose > 0:
                print(f"  deleted folder {path}")
    else:
        if args.verbose > 0:
            print("  nothing in cache")

    # delete cache_path/XXX is empty
    if exists(parent_path):
        dir = listdir(parent_path)
        if len(dir) == 0:
            rmtree(parent_path, ignore_errors=True)
            if args.verbose > 0:
                print(f"  deleted folder {parent_path}")


def clear_cache(args: argparse.Namespace, cfg: Config, kvdb: redis.Redis):
    con = sqlite3.connect(args.CampaignFileName)
    cur = con.cursor()

    res = cur.execute("select id, name, version, ctime from info")
    info = res.fetchone()
    t = timestamp_to_datetime(info[3])
    print(f"{info[1]}, version {info[2]}, created on {t}")

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
            res3 = cur.execute(
                'select rowid, uuid, name, ctime from bpdataset where hostid = "'
                + str(host[0])
                + '" and dirid = "'
                + str(dir[0])
                + '"'
            )
            bpdatasets = res3.fetchall()
            for bpdataset in bpdatasets:
                id = bpdataset[1]
                t = timestamp_to_datetime(bpdataset[3])
                print(f"        dataset = {id}    {t}    {bpdataset[2]} ")
                delete_cache_items(args, cfg, kvdb, id)

    cur.close()
    con.close()


def connect_to_redis(host: str, port: int, db: int) -> redis.Redis:
    r = redis.Redis(host=host, port=port, db=db)
    try:
        r.ping()
    except (redis.exceptions.ConnectionError, ConnectionRefusedError):
        print(f"Could not connect to Redis at {host}:{port}, db={db}. Check if Redis is running.")
        return None
    return r


if __name__ == "__main__":
    # default values
    cfg = Config()
    args = setup_args(cfg)
    if not cfg.cache_path:
        print("No cachepath specified in user config")
        exit(1)

    if not exists(cfg.cache_path):
        print(f"Could not find {cfg.cache_path}")
        exit(1)

    kvdb = connect_to_redis(host="localhost", port=args.redis_port, db=0)
    if not kvdb:
        exit(1)

    if args.command == "list":
        if args.CampaignFileName is not None:
            print("Ignoring campaign archive argument")
        list_cache(args, kvdb)

    elif args.command == "clear":
        if args.CampaignFileName is None:
            print("Missing campaign archive argument for clearing cache")
            exit(1)
        clear_cache(args, cfg, kvdb)
