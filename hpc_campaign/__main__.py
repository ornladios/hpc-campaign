import sys
import argparse
from .list import main as List


def ArgParse():
    if ('--help' in sys.argv) and (sys.argv[1] == '--help'):
        add_help = True
    else:
        add_help=False
    parser = argparse.ArgumentParser(add_help=add_help, prog="hpc_campaign")

    parser.add_argument("subcmd", help='Sub command', choices=["list"])
    known, unknown = parser.parse_known_args()

    return known.subcmd, unknown


if __name__ == "__main__":

    subcmd, args = ArgParse()
    prog = "hpc_campaign {0}".format(subcmd)

    if subcmd == "list":
        List(args=args, prog=prog)
