#!/usr/bin/env python3

import argparse
import glob
import re
import fnmatch
from os.path import exists, isdir, dirname, basename, expanduser

from hpc_campaign_config import Config, ADIOS_ACA_VERSION

def List(*patterns, wildcard: bool=False):
    args = _SetupArgs()
    _CheckCampaignStore(args)
    if wildcard:
        args.wildcard = True
    for p in patterns:
        args.pattern.append(p)
    return _List(args, collect=True)

def _SetupArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", help="filter pattern(s) as regular expressions", default=None, nargs="*")
    parser.add_argument(
        "-w",
        "--wildcard",
        help="Use patterns as path wildcard patterns",
        action="store_true",
        default=False,
    )
    parser.add_argument("-s", "--campaign_store", help="Path to local campaign store", default=None)
    parser.add_argument("-v", "--verbose", help="More verbosity", action="count", default=0)
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

    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Campaign Store = {args.campaign_store}")
        print(f"# pattern(s) = {args.pattern}")
    return args


def _CheckCampaignStore(args):
    if args.campaign_store is not None and not isdir(args.campaign_store):
        print("ERROR: Campaign directory " + args.campaign_store + " does not exist", flush=True)
        exit(1)


def _List(args: argparse.Namespace, collect: bool=True) -> list[str]:
    result = []
    if args.campaign_store is None:
        print("ERROR: Set --campaign_store for this command")
        return result
    path = args.campaign_store

    # List the local campaign store
    acaList = glob.glob(path + "/**/*.aca", recursive=True)
    if len(acaList) == 0:
        print("There are no campaign archives in  " + path)
        return result
    else:
        startCharPos = len(path) + 1
        for f in acaList:
            name = f[startCharPos:]
            matches = False
            if len(args.pattern) == 0:
                matches = True
            else:
                for p in args.pattern:
                    if args.wildcard:
                        if fnmatch.fnmatch(name, p):
                            matches = True
                            break
                    else:
                        if re.search(p, name):
                            matches = True
                            break

            if matches:
                if collect:
                    result.append(f[startCharPos:])
                else:
                    print(f[startCharPos:])
    return result


if __name__ == "__main__":
    args = _SetupArgs()
    _CheckCampaignStore(args)
    _List(args, collect=False)
