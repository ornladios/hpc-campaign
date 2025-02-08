#!/usr/bin/env python3
import argparse
from os import remove
from os.path import exists
from hpc_campaign_key import Key

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        help="Command: generate/verify/info",
        choices=["generate", "verify", "info"],
    )
    parser.add_argument("path", help="path of keyfile")
    parser.add_argument("--verbose", "-v", help="More verbosity", action="count", default=0)
    parser.add_argument("--password", "-p", help="Protect with password", action="store_true", default=False)
    args = parser.parse_args()

    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Command = {args.command}")
        print(f"# Path = {args.path}")
        print(f"# Password required = {args.password}")
    return args

def check_path_for_creation(path: str):
    if exists(path):
        print(f"The file {path} already exist. "
            "Do not destroy a key that is in use of existing campaign archives. ")
        while (True):
            print("Do you want to overwrite Y/N? ", end='')
            answer = input()
            if answer == 'N' or answer == 'n':
                exit(1)
            if answer == 'Y' or answer == 'y':
                break
    else:
        try:
            with open(path, 'wb') as f:
                f.write(b'test')
            remove(path)
        except:
            print(f"Could not create/write to {path}")
            exit(1)

def check_path_for_reading(path: str) -> bool:
    if not exists(path):
        print(f"Could not find {path}")
        exit(1)


if __name__ == "__main__":
    args = setup_args()
    key = Key()
    if args.command == "generate":
        check_path_for_creation(args.path)
        key.generate_interactive(args.password)
        key.write(args.path)
    
    elif args.command == "verify":
        check_path_for_reading(args.path)
        key.read(args.path)
        key.info(True)

    elif args.command == "info":
        check_path_for_reading(args.path)
        key.read(args.path)
        key.info(False)
