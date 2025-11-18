#!/usr/bin/env python3
"""
Functions for handling the caching of datasets within campaign archives
"""
import argparse
import sqlite3
from shutil import rmtree
from re import match
from glob import glob
from os import walk, listdir
from os.path import exists, join, getsize
import sys

import redis
import redis.exceptions

from .config import Config, REDIS_PORT
from .utils import timestamp_to_datetime, input_yes_or_no


def setup_args(cfg: Config, args=None, prog=None):
    """Function for setting up the configuration arguments for caching
    """
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument(
        "command",
        help="Command: list/clear",
        choices=["list", "clear"],
    )
    parser.add_argument(
        "campaign",
        help="Campaign name or path, with .aca or without",
        default=None,
        nargs="?",
    )
    parser.add_argument(
        "--redis-port", "-p", help="Key-value database port", default=REDIS_PORT
    )
    parser.add_argument(
        "--verbose", "-v", help="More verbosity", action="count", default=0
    )
    parser.add_argument(
        "--yes-to-all",
        "-y",
        help="Answer yes automatically",
        action="store_true",
        default=False,
    )
    args = parser.parse_args(args=args)

    args.CampaignFileName = args.campaign
    if args.campaign is not None:
        if not args.campaign.endswith(".aca"):
            args.CampaignFileName += ".aca"
        if (
            not exists(args.CampaignFileName)
            and not args.CampaignFileName.startswith("/")
            and cfg.campaign_store_path is not None
        ):
            args.CampaignFileName = (
                cfg.campaign_store_path + "/" + args.CampaignFileName
            )

    if args.verbose > 1:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Command = {args.command}")
        print(f"# REDIS port = {args.redis_port}")
        print(f"# Archive = {args.CampaignFileName}")
        print(f"# Auto yes = {args.yes_to_all}")
    return args


def folder_size(folder_path: str) -> int:
    """Function returning the folder size for a given path
    """
    fsize = 0
    for path, _, files in walk(folder_path):
        for f in files:
            fp = join(path, f)
            fsize += getsize(fp)
    return fsize


async def list_cache(args: argparse.Namespace, cfg: Config, kvdb: redis.Redis):
    """Function to list all the cached datasets
    """
    archives: dict = {}  # organize datasets to archives
    cache_folders = glob("[0-9a-f][0-9a-f][0-9a-f]", root_dir=cfg.cache_path)
    if args.verbose > 1:
        print(f"# Found {len(cache_folders)} cache folders in cache directory")
    for folder in cache_folders:
        folder_path = join(cfg.cache_path, folder)
        dataset_ids = glob("[0-9a-f]*", root_dir=folder_path)
        if args.verbose > 1:
            print(f"# Found {len(dataset_ids)} datasets in cache folder {folder}")
        for did in dataset_ids:
            archive_name = "unknown"
            infoname = join(folder_path, did, "info.txt")
            if exists(infoname):
                with open(infoname, "r", encoding='utf-8') as infofile:
                    for line in infofile:
                        if match("Campaign = ", line):
                            archive_name = line[11:-1]
            dirsize = folder_size(join(folder_path, did))

            kvkeys = await kvdb.keys(did + "*")
            nkv = len(kvkeys)
            kvsize = 0
            for key in kvkeys:
                size = await kvdb.memory_usage(key)
                kvsize += size

            if args.verbose > 1:
                print(
                    f"# {did} from archive {archive_name}, cache size = {dirsize}, # of keys = {nkv}"
                )
            entry = {did: {"dirsize": dirsize, "nkv": nkv, "kvsize": kvsize}}
            if archive_name not in archives:
                archives[archive_name] = {}
            archives[archive_name].update(entry)
    if args.verbose > 1:
        print("")
    print_archives(archives, args.verbose)

def print_archives(archives, verbose):
    """Function to print the content of cached archives
    """
    print("folder-size     db-entries db-size     campaign name")
    print("--------------------------------------------------------------------------")
    size_all = 0
    nkv_all = 0
    kvsize_all = 0
    for arch in archives:
        size_arch = 0
        nkv_arch = 0
        kvsize_arch = 0
        for did, idvalues in archives[arch].items():
            size_arch += idvalues["dirsize"]
            nkv_arch += idvalues["nkv"]
            kvsize_arch += idvalues["kvsize"]
        print(f"{size_arch:<15} {nkv_arch:<10} {kvsize_arch:<11} {arch}")
        size_all += size_arch
        nkv_all += nkv_arch
        kvsize_all += kvsize_arch
        if verbose > 0:
            for did, idvalues in archives[arch].items():
                print(
                    f"{idvalues['dirsize']:>14}   {idvalues['nkv']:>8}  "
                    f"{idvalues['kvsize']:>10}     {did}"
                )
    print(f"{size_all:<15} {nkv_all:<10} {kvsize_all}")


async def delete_cache_items(
    args: argparse.Namespace, cfg: Config, kvdb: redis.Redis, did: str
):
    """Function to delete the cache items
    """
    kvkeys = await kvdb.keys(did + "*")
    nkeys = len(kvkeys)
    parent_path = join(cfg.cache_path, did[0:3])
    path = join(parent_path, did)

    if nkeys > 0 or exists(path):
        if args.yes_to_all or input_yes_or_no(
            "Do you want to clear cache for " + did + " (y/n)? "
        ):
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
        pdir = listdir(parent_path)
        if len(pdir) == 0:
            rmtree(parent_path, ignore_errors=True)
            if args.verbose > 0:
                print(f"  deleted folder {parent_path}")


async def clear_cache(args: argparse.Namespace, cfg: Config, kvdb: redis.Redis):
    """Function for clearing the cache
    """
    con = sqlite3.connect(args.CampaignFileName)
    cur = con.cursor()

    res = cur.execute("select id, name, version, modtime from info")
    info = res.fetchone()
    t = timestamp_to_datetime(info[3])
    print(f"{info[1]}, version {info[2]}, created on {t}")

    res = cur.execute("select rowid, uuid, name, modtime from dataset")
    datasets = res.fetchall()
    for dataset in datasets:
        did = dataset[1]
        t = timestamp_to_datetime(dataset[3])
        print(f"        dataset = {did}    {t}    {dataset[2]} ")
        await delete_cache_items(args, cfg, kvdb, did)

    cur.close()
    con.close()


def connect_to_redis(host: str, port: int, db: int) -> redis.Redis | None:
    """Function to connect to Redis
    """
    r = redis.Redis(host=host, port=port, db=db)
    try:
        r.ping()
    except (redis.exceptions.ConnectionError, ConnectionRefusedError):
        print(
            f"Could not connect to Redis at {host}:{port}, db={db}. Check if Redis is running."
        )
        return None
    return r


def main(args=None, prog=None):
    """Function to test the functionality of the caching system
    """
    # default values
    cfg = Config()
    args = setup_args(cfg, args=args, prog=prog)
    if not cfg.cache_path:
        print("No cachepath specified in user config")
        sys.exit(1)

    if not exists(cfg.cache_path):
        print(f"Could not find {cfg.cache_path}")
        sys.exit(1)

    kvdb = connect_to_redis(host="localhost", port=args.redis_port, db=0)
    if not kvdb:
        sys.exit(1)

    if args.command == "list":
        if args.CampaignFileName is not None:
            print("Ignoring campaign archive argument")
        list_cache(args, cfg, kvdb)

    elif args.command == "clear":
        if args.CampaignFileName is None:
            print("Missing campaign archive argument for clearing cache")
            sys.exit(1)
        clear_cache(args, cfg, kvdb)


if __name__ == "__main__":
    main()
