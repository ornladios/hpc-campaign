#!/usr/bin/env python3

import argparse
import glob
import re
import fnmatch
from os.path import isdir

from .config import Config


def List(*patterns, wildcard: bool = False, campaign_store=None):
    args = argparse.Namespace()
    if wildcard:
        args.wildcard = True
    else:
        args.wildcard = False
    args.pattern = []
    for p in patterns:
        args.pattern.append(p)
    args.verbose = 0
    args.campaign_store = None
    args = _SetDefaults(args)
    _CheckCampaignStore(args)
    return _List(args, collect=True, campaign_store=campaign_store)


def _SetupArgs(args=None, prog=None):
    parser = argparse.ArgumentParser(prog=prog)
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
    args = parser.parse_args(args=args)
    return _SetDefaults(args)


def _SetDefaults(args: argparse.Namespace):
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


def _List(args: argparse.Namespace, collect: bool = True, campaign_store=None) -> list[str]:
    result = []
    path = campaign_store
    if path is None:
        path = args.campaign_store
    if path is None:
        print("ERROR: Set --campaign_store for this command")
        return result

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


def main(args=None, prog=None):
    args = _SetupArgs(args=args, prog=prog)
    _CheckCampaignStore(args)
    _List(args, collect=False)


if __name__ == "__main__":
    main()
