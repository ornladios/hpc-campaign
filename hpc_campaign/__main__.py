import sys
import argparse



def ArgParse():
    if ('--help' in sys.argv) and (sys.argv[1] == '--help'):
        add_help = True
    else:
        add_help=False
    parser = argparse.ArgumentParser(add_help=add_help, prog="hpc_campaign")

    parser.add_argument(
        "subcmd",
        help='Sub command',
        choices=[
            "cache",
            "connector",
            "genkey",
            "hdf5_metadata",
            "list",
            "manager",
        ]
    )

    known, unknown = parser.parse_known_args()

    return known.subcmd, unknown


def main():

    subcmd, args = ArgParse()
    prog = "hpc_campaign {0}".format(subcmd)

    exec(
        'from .{0} import main as cmd'.format(subcmd),
        globals()
    )

    cmd(args=args, prog=prog)


if __name__ == "__main__":
    
    main()
