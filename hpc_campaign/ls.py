#!/usr/bin/env python3

import argparse
import glob
import re

from .utils import check_campaign_store, matches_pattern, set_default_args_from_config


def ls(*patterns, wildcard: bool = False, index: bool = False, campaign_store: str = "") -> list[str]:
    args = argparse.Namespace()
    args.wildcard = wildcard
    args.index = index
    args.pattern = []
    for p in patterns:
        args.pattern.append(p)
    args.verbose = 0
    args.campaign_store = campaign_store
    args = _set_defaults_ls(args)
    check_campaign_store(args.campaign_store, True)
    return _list(args, collect=True)


def _setup_args_ls(args=None, prog=None):
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument(
        "pattern",
        help="filter pattern(s) as regular expressions",
        default=None,
        nargs="*",
    )
    parser.add_argument(
        "-w",
        "--wildcard",
        help="Use patterns as path wildcard patterns",
        action="store_true",
        default=False,
    )
    parser.add_argument("-x", "--index", help="look for indexes not archives", action="store_true")
    parser.add_argument("-s", "--campaign_store", help="Path to local campaign store", default="")
    parser.add_argument("-v", "--verbose", help="More verbosity", action="count", default=0)
    args = parser.parse_args(args=args)
    return _set_defaults_ls(args)


def _set_defaults_ls(args: argparse.Namespace):
    set_default_args_from_config(args, False)
    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Campaign Store = {args.campaign_store}")
        print(f"# pattern(s) = {args.pattern}")
    return args


def _list(args: argparse.Namespace, collect: bool = True) -> list[str]:
    """List the local campaign store"""
    result: list[str] = []
    ext = ".aca"
    if args.index:
        ext = ".acx"
    aca_list = glob.glob(args.campaign_store + "/**/*" + ext, recursive=True)
    if len(aca_list) == 0:
        print("There are no campaign archives in  " + args.campaign_store)
        return result

    start_char_pos = len(args.campaign_store) + 1
    for f in aca_list:
        name = f[start_char_pos:]
        if matches_pattern(name, args.pattern, args.wildcard):
            if collect:
                result.append(f[start_char_pos:])
            else:
                print(f[start_char_pos:])
    return result


def main(args=None, prog=None):
    args = _setup_args_ls(args=args, prog=prog)
    try:
        check_campaign_store(args.campaign_store, True)
    except (FileNotFoundError, ValueError) as e:
        print(e)
    else:
        try:
            _list(args, collect=False)
        except re.error as e:
            print(f"Error using regular expression '{str(e.pattern)}': {e}")


if __name__ == "__main__":
    main()
