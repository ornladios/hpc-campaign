#!/usr/bin/env python3

import argparse
import sys
from os.path import exists, basename

from hpc_campaign_config import Config

__accepted_commands__ = [
    "create",
    "delete",
    "info",
    "dataset",
    "text",
    "image",
    "add-archival-storage",
    "archived",
    "time-series",
]
__accepted_commands_str__ = " | ".join(__accepted_commands__)

prog = basename(sys.argv[0])


class ArgParser:
    """
    Process command-line arguments for the campaign manager

    Usage:
        from hpc_campaign_manager_args import Args
        parser = ArgParser()
        while parser.parse_next_command():
            ... use x.args for argparse namespace

    """

    def __init__(self):
        self.parsers = self.setup_args()
        self.commandlines = self.divide_cmdline(__accepted_commands__)
        self.args = self.parse_args_main(self.parsers["main"], self.commandlines[0])
        self.cmdidx = 1
        self.prev_command = None

    def parse_next_command(self) -> bool:
        if self.cmdidx < len(self.commandlines):
            if self.prev_command is not None:
                self.remove_prev_args(self.prev_command, self.args)
            cmdline = self.commandlines[self.cmdidx]
            self.args = self.parse_args_command(self.args, self.parsers[cmdline[0]], cmdline)
            self.cmdidx += 1
            if self.args.command == "dataset":
                self.check_args_dataset(self.args)
            elif self.args.command == "text":
                self.check_args_text(self.args)
            self.prev_command = cmdline[0]
            return True
        else:
            return False

    def divide_cmdline(self, commands: list):
        # Divide argv by commands
        split_argv = [[]]
        for c in sys.argv[1:]:
            if c in commands:
                split_argv.append([c])
            else:
                split_argv[-1].append(c)
        return split_argv

    def parse_args_main(self, parser, argv) -> argparse.Namespace:
        args = parser.parse_args(argv)  # Without command

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

        args.remote_data = False
        args.s3_endpoint = None
        if args.hostname is None:
            args.hostname = args.user_options.host_name
        elif args.hostname in args.host_options and args.hostname != args.user_options.host_name:
            args.remote_data = True
            hostopt = args.host_options.get(args.hostname)
            if hostopt is not None:
                optID = next(iter(hostopt))
                if hostopt[optID]["protocol"].casefold() == "s3":
                    args.s3_endpoint = hostopt[optID]["endpoint"]
                    if args.s3_bucket is None:
                        print("ERROR: Remote option for an S3 server requires --s3_bucket")
                        exit(1)
                    if args.s3_datetime is None:
                        print("ERROR: Remote option for an S3 server requires --s3_datetime")
                        exit(1)

        args.CampaignFileName = args.archive
        if args.archive is not None:
            if not args.archive.endswith(".aca"):
                args.CampaignFileName += ".aca"
            if (
                not exists(args.CampaignFileName)
                and not args.CampaignFileName.startswith("/")
                and args.campaign_store is not None
            ):
                args.CampaignFileName = args.campaign_store + "/" + args.CampaignFileName

        args.LocalCampaignDir = ".adios-campaign/"

        if args.verbose > 0:
            print(f"# Verbosity = {args.verbose}")
            print(f"# Campaign File Name = {args.CampaignFileName}")
            print(f"# Campaign Store = {args.campaign_store}")
            print(f"# Host name = {args.hostname}")
            print(f"# Key file = {args.keyfile}")

        return args

    def parse_args_command(self, args: argparse.Namespace, parser, argv) -> argparse.Namespace:
        # Parse one command
        # n = argparse.Namespace()
        # setattr(args, argv[0], n)
        args.command = argv[0]
        parser.parse_args(argv[1:], namespace=args)
        return args

    def check_args_dataset(self, args):
        if args.name is not None:
            if len(args.files) > 1:
                raise Exception(
                    "Invalid arguments for dataset: when using --name <name>, " "only one dataset is allowed"
                )

    def check_args_text(self, args):
        if args.name is not None:
            if len(args.files) > 1:
                raise Exception(
                    "Invalid arguments for text: when using --name <name>, " "only one text file is allowed"
                )

    def setup_args(self) -> dict:
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
A campaign archive name without '.aca' extension will be forced to have '.aca'.
If it exists, 'campaignstorepath' in ~/.config/adios2/adios2.yaml will be used for
relative paths for <archive> names.
Multiple commands can be used in one run.
Type '%(prog)s x <command> -h' for help on commands.
""",
        )
        parsers = {}
        parsers["main"] = parser
        parser.add_argument("--verbose", "-v", help="More verbosity", action="count", default=0)
        parser.add_argument("--campaign_store", "-s", help="Path to local campaign store", default=None)
        parser.add_argument("--hostname", "-n", help="Host name unique for hosts in a campaign")
        parser.add_argument("--keyfile", "-k", help="Key file to encrypt metadata")
        parser.add_argument("--s3_bucket", help="Bucket on S3 server", default=None)
        parser.add_argument(
            "--s3_datetime",
            help="Datetime of data on S3 server in " "'2024-04-19 10:20:15 -0400' format",
            default=None,
        )
        parser.add_argument("archive", help="Campaign archive name or path, with .aca or without", default=None)
        parser.add_argument("command", nargs="?", help=__accepted_commands_str__, default=None)

        # parser for the "create" command
        parser_create = argparse.ArgumentParser(
            prog=f"{prog} <archive> create",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Create a new campaign archive file.""",
        )
        parsers["create"] = parser_create

        # parser for the "delete" command
        parser_delete = argparse.ArgumentParser(
            prog=f"{prog} <archive> delete",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Delete items from a campaign archive file.""",
        )
        parser_delete.add_argument("--uuid", nargs="+", metavar="id", help="Remove datasets by UUID")
        parser_delete.add_argument("--name", nargs="+", metavar="str", help="Remove datasets by name")
        parser_delete.add_argument("--replica", nargs="+", metavar="id", help="Remove replicas by ID number")
        parser_delete.add_argument("--campaign", help="Delete entire campaign file", action="store_true")

        parsers["delete"] = parser_delete

        # parser for the "info" command
        parser_info = argparse.ArgumentParser(
            prog=f"{prog} <archive> info",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Print content of a campaign archive file.""",
            epilog="""
Listing format:
dataset:  uuid  type  date  representation-name
replica:  replica-id  modes  directory-id  [resolution]  size  date  name
file:     [encryption key]  size  date  [cheksum]  filename
  modes:  e - embedded in archive
          r - remote dataset
          k - encrypted with key (only inside the archive)
          a - dataset with accuracy notes
          A - archival location (no direct access to remote dataset)
          D - deleted from campaign archive (with --show_deleted option)
""",
        )
        parser_info.add_argument("-r", "--list-replicas", help="List replicas for each dataset", action="store_true")
        parser_info.add_argument(
            "-f", "--list-files", help="List files embedded in campaign archive", action="store_true"
        )
        parser_info.add_argument("-d", "--show-deleted", help="Show deleted entries", action="store_true")
        parser_info.add_argument("-c", "--show-checksum", help="Show checksums of files", action="store_true")
        parsers["info"] = parser_info

        # parser for the "dataset" command
        parser_dataset = argparse.ArgumentParser(
            prog=f"{prog} <archive> dataset",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Add one or more datasets to the archive. Datasets can be valid HDF5 or ADIOS2-BP files.
A temporary file is created from HDF5 files so one must have write access to /tmp.
""",
        )
        parsers["dataset"] = parser_dataset
        parser_dataset.add_argument("files", nargs="+", help="add ADIOS/HDF5 files manually")
        parser_dataset.add_argument("--name", "-n", default=None, help="Representation name in the campaign hierarchy")

        # parser for the "text" command
        parser_text = argparse.ArgumentParser(
            prog=f"{prog} <archive> text",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Add one or more text files to the archive. They are always stored in the archive,
so be mindful about the size of the resulting archive. Text is stored compressed.
""",
        )
        parsers["text"] = parser_text
        parser_text.add_argument("files", nargs="+", help="add text files manually")
        parser_text.add_argument("--name", "-n", default=None, help="Representation name in the campaign hierarchy")
        parser_text.add_argument("--store", "-s", help="Store image in campaign", action="store_true")

        # parser for the "image" command
        parser_image = argparse.ArgumentParser(
            prog=f"{prog} <archive> image",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Add an image file to the archive (remote reference by default).
Multiple files with different resolutions can represent an image --name in the archive.
The archive can '--store' directly the image file, or store a --thumbnail with X,Y size.
""",
        )
        parsers["image"] = parser_image
        parser_image.add_argument("file", help="image file")
        parser_image.add_argument("--name", "-n", default=None, help="Representation name in the campaign hierarchy")
        parser_image.add_argument("--store", "-s", help="Store image in campaign", action="store_true")
        parser_image.add_argument(
            "--thumbnail",
            nargs=2,
            default=None,
            type=int,
            metavar=("x", "y"),
            help="Store a resized image with resolution of x-by-y and refer to original",
        )

        # parser for the "add-archival-storage" command
        parser_addarchive = argparse.ArgumentParser(
            prog=f"{prog} <archive>  add-archival-storage",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Record an archival storage of <system> type to the list of <host>/<directory>.
Attach a --note that contains specific information on how to access the archival storage.
The <host> is a short name used to identify hosts in the campaign. Use --longhostname
to record the full hostname.
""",
        )
        parsers["add-archival-storage"] = parser_addarchive
        parser_addarchive.add_argument(
            "system",
            choices=["Kronos", "HPSS", "fs"],
            help="Name of archival system of this location",
        )
        parser_addarchive.add_argument("host", help="Archival host's name", type=str)
        parser_addarchive.add_argument("directory", help="Archival host's directory", type=str)
        parser_addarchive.add_argument("--longhostname", metavar="str", help="Optional long host name")
        parser_addarchive.add_argument("--note", metavar="fname", help="Optional notes file")

        # parser for the "archived" command
        parser_archive = argparse.ArgumentParser(
            prog=f"{prog} <archive>  archived",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Indicate that a dataset/replica is copied/moved to an archival storage.
A new replica is created, pointing to the archival host/directory.
If the replica's relative path under the archival directory is different
from the original replica's path, specify it with <newpath>.
--move will delete the original replica.
Use info -r to list replicas in the campaign archive (and also to find the
archival directory's id).
If there are more than one, conflicting, replicas then --repid must be used
to indicate which version was archived.
""",
        )
        parsers["archived"] = parser_archive
        parser_archive.add_argument("name", help="Name of dataset", type=str)
        parser_archive.add_argument("dirid", help="Archival host's directory ID", type=int)
        parser_archive.add_argument(
            "--newpath", help="Replica's new relative path under archival directory", type=str, metavar="str"
        )
        parser_archive.add_argument("--replica", help="Replica ID", metavar="id", type=int)
        parser_archive.add_argument("--move", help="Delete original replica", action="store_true")

        # parser for the "time-series" command
        parser_timeseries = argparse.ArgumentParser(
            prog=f"{prog} <archive>  time-series",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Organize datasets into a time-series. Enumerate the datasets in the order
they should be presented. Subsequent calls with the same <name> will add
datasets to the list of existing datasets, unless --replace is given.
""",
        )
        parsers["time-series"] = parser_timeseries
        parser_timeseries.add_argument("name", help="Name of time series", type=str)
        parser_timeseries.add_argument("dataset", nargs="*", help="Datasets in order", type=str)
        parser_timeseries.add_argument("--replace", help="Overwrite existing time-series", action="store_true")
        parser_timeseries.add_argument(
            "--remove", help="Remove the series definition (not the datasets)", action="store_true"
        )

        return parsers

    def remove_prev_args(self, command: str, args: argparse.Namespace):
        if command == "dataset":
            del args.name
            del args.files
        elif command == "text":
            del args.name
            del args.files
            del args.store
        elif command == "image":
            del args.name
            del args.file
            del args.store
            del args.thumbnail
        elif command == "info":
            del args.show_deleted
            del args.show_checksum
            del args.list_replicas
            del args.list_files
        elif command == "delete":
            del args.name
            del args.uuid
            del args.replica
            del args.campaign
        elif command == "add-archival-storage":
            del args.host
            del args.system
            del args.directory
        elif command == "archived":
            del args.name
            del args.dirid
            del args.newpath
            del args.replica
            del args.move
        elif command == "time-series":
            del args.name
            del args.dataset
            del args.replace
            del args.remove
