#!/usr/bin/env python3

import argparse
import sqlite3
import sys
from os.path import exists
from pathlib import Path
from time import sleep

import adios2

from .config import ACA_VERSION, Config
from .ls import ls
from .manager import Manager
from .utils import (
    CURRENT_TIME,
    check_campaign_store,
    get_path,
    matches_pattern,
    set_default_args_from_config,
    sql_commit,
    sql_execute,
    timestamp_to_str,
)


class Index:
    """Index API for campaign store indexing."""

    def __init__(self, index_file: str, campaign_store: str = "", verbose: int = 0):
        """
        Create Index object for a campaign index file
        :param index_file: The name of the campaign index file (relative path under campaign_store)
        :param campaign_store: Optional base path for all campaign archives, default is from
            ~/.config/hpc-campaign/config.yaml.
        :param verbose: Optional verbose for printing debug information if verbose > 0
        """

        self.args: argparse.Namespace = argparse.Namespace(
            index_file=index_file, campaign_store=campaign_store, verbose=verbose
        )
        self.args = _set_defaults_index(self.args)
        check_campaign_store(self.args.campaign_store, False)
        self.con: sqlite3.Connection
        self.connected = False

    def _build_command_args(self, command: str, updates: dict | None = None) -> argparse.Namespace:
        cmd_args = argparse.Namespace(**vars(self.args))
        cmd_args.command = command
        if updates:
            for key, value in updates.items():
                setattr(cmd_args, key, value)
        return cmd_args

    def open(self, create: bool = False, truncate: bool = False) -> None:
        """
        Open/create an ACX campaign index
        :param create: if True create new index file if it does not exists. Default is to throw an error.
        :param truncate: if True and archive already exists, remove all content of the index first.
        """
        fileexists = exists(self.args.index_file)
        if not create and not fileexists:
            raise FileNotFoundError(f"index {self.args.index_file} does not exist")

        self.con = sqlite3.connect(self.args.index_file)
        self.connected = True

        if truncate:
            self._wipe_acx()

        if not fileexists or truncate:
            self._create_tables(self.args.index_file)

    def close(self):
        """
        Close the ACX campaign index.
        All operations have committed their changes, so close is only for freeing up database resources.
        """
        if self.connected:
            self.con.close()
            self.connected = False

    def add(self, patterns: list[str], wildcard: bool = False) -> None:
        if not self.connected:
            self.open(create=True, truncate=False)
        archives = []
        for pattern in patterns:
            archives += ls(pattern, wildcard=wildcard, index=False, campaign_store=self.args.campaign_store)
        for archive in archives:
            if self.args.verbose > 0:
                print(f"index archive={archive}")
            self._index_archive(archive)

    def _add_archive(self, archive: str, indent: str) -> int:
        cur = self.con.cursor()
        cur_ds = sql_execute(
            cur,
            "insert into archives (name, date_indexed) "
            "values  (?, ?) "
            "on conflict (name) "
            "do update set date_indexed = ? "
            "returning rowid",
            (
                archive,
                CURRENT_TIME,
                CURRENT_TIME,
            ),
        )
        row_id = cur_ds.fetchone()[0]
        if self.args.verbose > 0:
            print(f"{indent}  Archive rowid = {row_id}")
        return row_id

    def _index_archive(self, archive: str):
        manager = Manager(archive, campaign_store=self.args.campaign_store, verbose=0)
        info_res = manager.info()
        if len(info_res.datasets) < 0:
            return

        self._add_archive(archive, "    ")
        for ds in info_res.datasets:
            if self.args.verbose > 0:
                print(f"    index dataset {ds.name}")

        sql_commit(self.con)

    def remove(self, patterns: list[str], wildcard: bool = False) -> None:
        cmd_args = self._build_command_args("remove", {"patterns": patterns, "wildcard": wildcard})
        print(f"index remove patterns={cmd_args.patterns} wildcard={cmd_args.wildcard}")

    def ls(self, patterns: list[str], wildcard: bool = False, collect: bool = True) -> list[str]:
        result: list[str] = []
        cur = self.con.cursor()
        cursor = sql_execute(cur, "SELECT name, date_indexed FROM archives")
        names = [description[0] for description in cursor.description]
        if not collect:
            print(f"{names[0]:<60}    {names[1]}")
            print("-" * 76)
        for row in cursor:
            name = row[0]
            ts = row[1]
            if matches_pattern(name, patterns, wildcard):
                if collect:
                    result.append(name)
                else:
                    print(f"{name:<60}    {timestamp_to_str(ts)}")
        return result

    def _wipe_acx(self):
        cur = self.con.cursor()
        cur.execute("PRAGMA foreign_keys = OFF;")
        objects = cur.execute("""
            SELECT type, name
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%';
        """).fetchall()

        for obj_type, name in objects:
            cur.execute(f'DROP {obj_type.upper()} IF EXISTS "{name}";')

        self.con.commit()
        self.con.execute("VACUUM;")

    def _create_tables(self, index_file: str):
        print(f"Create new index {index_file}")
        cur = self.con.cursor()
        sql_execute(cur, "create table info(id TEXT, name TEXT, version TEXT, modtime INT)")
        sql_commit(self.con)
        sql_execute(
            cur,
            "insert into info values (?, ?, ?, ?)",
            ("ACX", "ADIOS Campaign Index", ACA_VERSION, CURRENT_TIME),
        )

        sql_execute(cur, "CREATE TABLE archives (name TEXT, date_indexed INT, PRIMARY KEY (name))")
        sql_execute(
            cur,
            """
            CREATE TABLE datasets (archiveid INT, datasetid INT, name TEXT, PRIMARY KEY (archiveid, datasetid))
            """,
        )
        sql_execute(
            cur,
            """
            CREATE TABLE variables (
                archiveid INT,
                datasetid INT,
                variable TEXT,
                property TEXT
            )""",
        )
        sql_execute(
            cur,
            """
            CREATE TABLE attributes (
                archiveid INT,
                datasetid INT,
                attribute TEXT
            )
        """,
        )

        sql_commit(self.con)
        cur.close()
        while not exists(index_file):
            sleep(0.1)

    def print_tables(self):
        cur = self.con.cursor()
        print("ATTRIBUTES")
        cursor = sql_execute(cur, "SELECT * FROM attributes")
        names = [description[0] for description in cursor.description]
        print(names)
        for row in cursor:
            print(row)

        print("VARIABLES")
        cursor = sql_execute(cur, "SELECT * FROM variables")
        names = [description[0] for description in cursor.description]
        print(names)
        for row in cursor:
            print(row)


def _set_defaults_index(args: argparse.Namespace) -> argparse.Namespace:
    set_default_args_from_config(args, False)
    args.index_file = get_path(args.index_file, args.campaign_store)
    if not args.index_file.endswith(".acx"):
        args.index_file += ".acx"

    if args.verbose > 0:
        print(f"# Verbosity = {args.verbose}")
        print(f"# Campaign Store = {args.campaign_store}")
    return args


"""
class CampaignSqlite:
    separator = "/"
    prefix_elements = 2
    dataset_list = []

    def set_dataset_list(self, campaign, path):
        manager = Manager(archive=str(campaign))
        if path != None:
            manager = Manager(archive=str(campaign), campaign_store=str(path))
        info_data = manager.info(
            False,  # list_replicas
            False,  # list_files
            False,  # show_deleted
            False,  # show_checksum
        )
        # info data is a InfoResult, which has datasets: list[DatasetInfo]
        # class DatasetInfo:
        # id: int
        # uuid: str
        # name: str
        # mod_time: int
        # del_time: int
        # file_format: str
        # replicas: list[ReplicaInfo] = field(default_factory=list)
        self.dataset_list = [d.name for d in info_data.datasets]

    def _extract_info(self, entry):
        # print(entry, self.dataset_list)
        for dataset in self.dataset_list:
            if entry[: len(dataset)] == dataset:
                # + 1 so we skip the separator
                # print(entry[len(dataset) + 1:], dataset)
                return dataset, entry[len(dataset) + 1 :]

        # identify the file that the variable/attribute belongs to
        temp = entry.split(self.separator)
        prefix = self.prefix_elements
        if len(temp) <= prefix:  # couting the entry name as well
            prefix = len(temp) - 1
        file = self.separator.join(temp[:prefix])
        value = self.separator.join(temp[prefix:])
        return file, value

    def _get_variable_data(self, variables, f):
        variable_data = {}
        attribute_data = {}
        for var in variables:
            file, var_name = self._extract_info(var)
            # json files will be open and the information inside become attributes
            if var_name.endswith(".json"):
                content = f.read(var)
                json_attributes = json.loads("".join(chr(code) for code in content))
                json_file = file + self.separator + var_name
                if json_file not in attribute_data:
                    attribute_data[json_file] = {}
                for atr in json_attributes:
                    attribute_data[json_file][atr] = json_attributes[atr]
            else:  # otherwise we add variable data
                if file not in variable_data:
                    variable_data[file] = {}
                variable_data[file][var_name] = variables[var]
        return variable_data, attribute_data

    def _get_attribute_data(self, attributes, f, attribute_data={}):
        for atr in attributes:
            file, atr_name = self._extract_info(atr)
            read_value = f.read_attribute(atr)
            if file not in attribute_data:
                attribute_data[file] = {}
            if atr_name.endswith(".json"):
                # transform from np.array to json content
                variable_data[file][atr_name] = json.loads("".join(chr(code) for code in read_value))
            else:
                attribute_data[file][atr_name] = read_value
        return attribute_data

    def add_campaign_to_collection(self, campaign, path=None):
        print("Adding", campaign, "to the collection")
        self.set_dataset_list(campaign, path)

        file = campaign
        if path is not None:
            file = path + "/" + file

        with adios2.FileReader(file) as f:
            # add variables to the collection
            variables = f.available_variables()
            variable_data, attribute_data = self._get_variable_data(variables, f)
            for dataset in variable_data:
                for var in variable_data[dataset]:
                    value = variable_data[dataset][var]
                    self.create_variable_entry(campaign, dataset, var, value)

            # add attributes to the collection
            attributes = f.available_attributes()
            attribute_data = self._get_attribute_data(attributes, f, attribute_data)
            for dataset in attribute_data:
                for atr in attribute_data[dataset]:
                    value = attribute_data[dataset][atr]
                    # take care of the cases not supported by MongoDB
                    if isinstance(value, np.ndarray):
                        value = value.tolist()
                    if isinstance(value, np.float32):
                        value = float(value)
                    if isinstance(value, (np.uint32, np.uint64, np.uint8)):
                        value = int(value)
                    self.create_attribute_entry(campaign, dataset, {atr: value})

    def create_variable_entry(self, campaign, dataset, var, value):
        self.cursor.execute("INSERT INTO variables VALUES (?, ?, ?, ?)", (campaign, dataset, var, json.dumps(value)))
        self.db.commit()

    def create_attribute_entry(self, campaign, dataset, attribute):
        self.cursor.execute("INSERT INTO attributes VALUES (?, ?, ?)", (campaign, dataset, json.dumps(attribute)))
        self.db.commit()
"""

def _setup_args_index(args=None, prog=None):
    parser = argparse.ArgumentParser(
        prog=prog,
        epilog="""
Build an index <indexfile>.acx from information gathered from the .aca files that match the pattern.
Type '%(prog)s <indexfile> <command> -h' for help on commands.
""",
    )
    parser.add_argument(
        "indexfile",
        help="Campaign index name or path, with .acx or without",
        default=None,
    )
    parser.add_argument(
        "command",
        choices=["add", "remove", "ls"],
        help="Command to run",
    )
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

    parser.add_argument("-s", "--campaign_store", help="Path to local campaign store", default="")
    parser.add_argument("-v", "--verbose", help="More verbosity", action="count", default=0)
    return parser.parse_args(args=args)


def main(args=None, prog=None):
    args = _setup_args_index(args=args, prog=prog)
    index = Index(index_file=args.indexfile, campaign_store=args.campaign_store, verbose=args.verbose)

    create_allowed = True
    if args.command in ("ls"):
        create_allowed = False
    try:
        index.open(create=create_allowed, truncate=False)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    patterns = args.pattern or []

    if args.command == "add":
        index.add(patterns, wildcard=args.wildcard)
    elif args.command == "remove":
        index.remove(patterns, wildcard=args.wildcard)
    elif args.command == "ls":
        index.ls(patterns, wildcard=args.wildcard, collect=False)
    else:
        print(f"Unknown command for index: {args.command}")

    index.close()


if __name__ == "__main__":
    main()
