import argparse
import tarfile

TARTYPES = {
    "reg": 0,
    "lnk": 1,
    "sym": 2,
    "chr": 3,
    "blk": 4,
    "dir": 5,
    "fifo": 6,
    "cont": 7,
    "longname": 8,
    "longlink": 9,
    "sparse": 10,
}


def CreateTarIndex(tarfilename: str, indexfile: str | None):
    tf = tarfile.open(tarfilename)
    if indexfile is None:
        indexfile = tarfilename + ".idx"
    with open(indexfile, "w") as idxf:
        for ti in tf:
            idxf.write(f'{int(ti.type)},{ti.offset},{ti.offset_data},{ti.size},"{ti.name}"\n')


def _SetupArgs(args=None, prog=None):
    parser = argparse.ArgumentParser(
        prog=prog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
Create an index file from a TAR file. It can be used in the
'manager <archive> add-archival-storage' command to make automatic
replicas of all datasets in the archive and register their offsets
and sizes in the TAR file.
""",
    )
    parser.add_argument("tarfile", help="Name of the TAR file", type=str)
    parser.add_argument("idxfile", nargs="?", help="Optional name of the index file", type=str)
    args = parser.parse_args(args=args)
    #    if args.idxfile is None:
    #        args.idxfile = args.tarfile + ".idx"
    return args


def main(args=None, prog=None):
    args = _SetupArgs(args=args, prog=prog)
    CreateTarIndex(args.tarfile, args.idxfile)
