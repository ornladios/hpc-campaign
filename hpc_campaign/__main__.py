import argparse
import importlib
import sys


def arg_parse():
    add_help = bool("--help" in sys.argv) and (sys.argv[1] == "--help")
    parser = argparse.ArgumentParser(add_help=add_help, prog="hpc_campaign")

    parser.add_argument(
        "subcmd",
        help="Sub command",
        choices=[
            "cache",
            "connector",
            "genkey",
            "hdf5_metadata",
            "ls",
            "manager",
            "rm",
            "taridx",
        ],
    )

    known, unknown = parser.parse_known_args()

    return known.subcmd, unknown


def main():
    subcmd, args = arg_parse()
    prog = f"hpc_campaign {subcmd}"

    # pylint: disable=pointless-string-statement
    """
    exec(
        'from .{0} import main as cmd'.format(subcmd),
        globals()
    )

    cmd(args=args, prog=prog)
    """

    # Dynamically import the module using importlib.import_module()
    aliased_module = importlib.import_module(f".{subcmd}", package="hpc_campaign")

    # Run main()
    aliased_module.main(args=args, prog=prog)


if __name__ == "__main__":
    main()
