#!/usr/bin/env python3

import argparse
import sys
from os.path import exists, basename

from hpc_campaign_config import Config

__accepted_commands__ = ["create", "delete", "info", "dataset", "text", "image"]
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

    def parse_next_command(self) -> bool:
        if self.cmdidx < len(self.commandlines):
            cmdline = self.commandlines[self.cmdidx]
            self.args = self.parse_args_command(self.args, self.parsers[cmdline[0]], cmdline)
            self.cmdidx += 1
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

    def parse_args_main(self, parser, argv):
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

    def parse_args_command(self, args, parser, argv):
        # Parse one command
        # n = argparse.Namespace()
        # setattr(args, argv[0], n)
        args.command = argv[0]
        parser.parse_args(argv[1:], namespace=args)
        return args

    def setup_args(self) -> dict:
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
An campaign archive name without '.aca' extension will be forced to have '.aca'.
If it exists, 'campaignstorepath' in ~/.config/adios2/adios2.yaml will be used for
relative paths for <archive> names.
Multiple commands can be used in one run.
Type '%(prog)s x <command> -h' for help on commands.
""",
        )
        parsers = {}
        parsers["main"] = parser
        parser.add_argument("--verbose", "-v", help="More verbosity", action="count", default=0)
        parser.add_argument(
            "--campaign_store", "-s", help="Path to local campaign store", default=None
        )
        parser.add_argument("--hostname", "-n", help="Host name unique for hosts in a campaign")
        parser.add_argument("--keyfile", "-k", help="Key file to encrypt metadata")
        parser.add_argument("--s3_bucket", "-b", help="Bucket on S3 server", default=None)
        parser.add_argument(
            "--s3_datetime",
            "-t",
            help="Datetime of data on S3 server in " "'2024-04-19 10:20:15 -0400' format",
            default=None,
        )
        parser.add_argument(
            "archive", help="Campaign archive name or path, with .aca or without", default=None
        )

        parser.add_argument("command", nargs="?", help=__accepted_commands_str__, default=None)

        # create the parser for the "create" command
        parser_create = argparse.ArgumentParser(
            prog=f"{prog} <archive> create",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Create a new campaign archive file.""",
        )
        parsers["create"] = parser_create

        # create the parser for the "delete" command
        parser_delete = argparse.ArgumentParser(
            prog=f"{prog} <archive> delete",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Delete a campaign archive file.""",
        )
        parsers["delete"] = parser_delete

        # create the parser for the "info" command
        parser_info = argparse.ArgumentParser(
            prog=f"{prog} <archive> info",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Print content of a campaign archive file.""",
        )
        parsers["info"] = parser_info

        # create the parser for the "dataset" command
        parser_dataset = argparse.ArgumentParser(
            prog=f"{prog} <archive> dataset",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Add one or more datasets to the archive. Datasets can be valid HDF5 or ADIOS2-BP files.
A temporary file is created from HDF5 files so one must have write access to the folder
where the HDF5 file resides.
""",
        )
        parsers["dataset"] = parser_dataset
        parser_dataset.add_argument("files", nargs="+", help="add ADIOS/HDF5 files manually")

        # create the parser for the "text" command
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

        # create the parser for the "image" command
        parser_image = argparse.ArgumentParser(
            prog=f"{prog} <archive> image",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""
Add an image file to the archive.
Multiple files with different resolutions can represent an image in the archive.
The archive can 'store' the image file, or contain only the reference to the 'remote' file.
""",
        )
        parsers["image"] = parser_image
        parser_image.add_argument("name", nargs=1, help="image name")
        parser_image.add_argument("file", nargs=1, help="image file")
        parser_image.add_argument(
            "store",
            choices=["store", "remote"],
            help="store in archive or add reference only",
        )

        return parsers

    def none(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "command",
            help="Command: create/update/delete/info/list",
            choices=["create", "update", "delete", "info", "list"],
        )
        parser.add_argument(
            "campaign",
            help="Campaign name or path, with .aca or without",
            default=None,
            nargs="?",
        )
        parser.add_argument("--verbose", "-v", help="More verbosity", action="count", default=0)
        parser.add_argument(
            "--campaign_store", "-s", help="Path to local campaign store", default=None
        )
        parser.add_argument("--hostname", "-n", help="Host name unique for hosts in a campaign")
        parser.add_argument("--keyfile", "-k", help="Key file to encrypt metadata")
        parser.add_argument("--s3_bucket", "-b", help="Bucket on S3 server", default=None)
        parser.add_argument(
            "--s3_datetime",
            "-t",
            help="Datetime of data on S3 server in " "'2024-04-19 10:20:15 -0400' format",
            default=None,
        )
        parser.add_argument("--files", "-f", nargs="+", help="Add ADIOS/HDF5 files manually")
        parser.add_argument("--textfiles", "-x", nargs="+", help="Add text files manually")
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

        args.CampaignFileName = args.campaign
        if args.campaign is not None:
            if not args.campaign.endswith(".aca"):
                args.CampaignFileName += ".aca"
            if (
                not exists(args.CampaignFileName)
                and not args.CampaignFileName.startswith("/")
                and args.campaign_store is not None
            ):
                args.CampaignFileName = args.campaign_store + "/" + args.CampaignFileName

        if args.files is None:
            args.files = []
        if args.textfiles is None:
            args.textfiles = []
        args.LocalCampaignDir = ".adios-campaign/"

        if args.verbose > 0:
            print(f"# Verbosity = {args.verbose}")
            print(f"# Command = {args.command}")
            print(f"# Campaign File Name = {args.CampaignFileName}")
            print(f"# Campaign Store = {args.campaign_store}")
            print(f"# Host name = {args.hostname}")
            print(f"# Key file = {args.keyfile}")
        return args
