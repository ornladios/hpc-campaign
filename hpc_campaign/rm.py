#!/usr/bin/env python3

import argparse
import glob
import re
from os import remove

from .utils import check_campaign_store, input_yes_or_no, matches_pattern, set_default_args_from_config


def rm(
    *patterns, wildcard: bool = False, campaign_store: str = "", interactive: bool = False, force: bool = False
) -> list[str]:
    args = argparse.Namespace()
    args.wildcard = wildcard
    args.interactive = interactive
    args.force = force
    args.pattern = []
    for p in patterns:
        args.pattern.append(p)
    args.verbose = 0
    args.campaign_store = campaign_store
    args = _set_defaults_rm(args)
    check_campaign_store(args.campaign_store, True)
    return _remove(args, collect=True)


def _setup_args_rm(args=None, prog=None):
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument(
        "pattern",
        help="filter pattern(s) as regular expressions",
        default=None,
        nargs="+",
    )
    parser.add_argument(
        "-w",
        "--wildcard",
        help="Use patterns as path wildcard patterns",
        action="store_true",
        default=False,
    )
    parser.add_argument("-i", "--interactive", help="prompt before every removal", action="store_true")
    parser.add_argument("-f", "--force", help="ignore errors, never prompt", action="store_true")
    parser.add_argument("-s", "--campaign_store", help="Path to local campaign store", default=None)
    parser.add_argument("-v", "--verbose", help="More verbosity", action="count", default=0)
    args = parser.parse_args(args=args)
    return _set_defaults_rm(args)


def _set_defaults_rm(args: argparse.Namespace):
    set_default_args_from_config(args, False)

    if args.force:
        args.interactive = False

    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Campaign Store = {args.campaign_store}")
        print(f"# pattern(s) = {args.pattern}")
        print(f"# force = {args.force}")
        print(f"# interactive = {args.interactive}")
    return args


# pylint: disable=too-many-nested-blocks
def _remove(args: argparse.Namespace, collect: bool = True) -> list[str]:
    # List the local campaign store
    result: list[str] = []
    aca_list = glob.glob(args.campaign_store + "/**/*.aca", recursive=True)
    if len(aca_list) == 0 and not args.force:
        print("There are no campaign archives in  " + args.campaign_store)
        return result

    start_char_pos = len(args.campaign_store) + 1
    for f in aca_list:
        name = f[start_char_pos:]
        if matches_pattern(name, args.pattern, args.wildcard, not args.force):
            do_remove = True
            if args.interactive:
                do_remove = input_yes_or_no(f"Remove {f[start_char_pos:]} (y/n)? ")
            if do_remove:
                remove(f)
                if collect:
                    result.append(f[start_char_pos:])
                else:
                    print(f[start_char_pos:])
    return result


def main(args=None, prog=None):
    args = _setup_args_rm(args=args, prog=prog)
    try:
        check_campaign_store(args.campaign_store, True)
    except (FileNotFoundError, ValueError) as e:
        print(e)
    else:
        try:
            _remove(args, collect=False)
        except re.error as e:
            print(f"Error using regular expression '{str(e.pattern)}': {e}")


if __name__ == "__main__":
    main()
