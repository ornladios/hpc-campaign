#!/usr/bin/env python3

# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
# pylint: disable=too-many-statements
# pylint: disable=too-many-positional-arguments


import argparse
import json
import sqlite3
import sys
from os.path import exists
from time import sleep
from typing import Any

import adios2  # type: ignore[import-untyped]
import numpy as np

from .config import ACA_VERSION
from .info import InfoResult
from .ls import ls
from .manager import Manager
from .types import ADIOS_AvailableVariables, DatasetsVariables, DatasetType, VariableType, python_type_to_variable_type
from .utils import (
    CURRENT_TIME,
    check_campaign_store,
    get_path,
    matches_pattern,
    set_default_args_from_config,
    sql_commit,
    sql_execute,
    sql_executemany,
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
        self._set_defaults_index()
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
        sql_execute(self.con.cursor(), "PRAGMA foreign_keys = ON")
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

    def remove(self, patterns: list[str], wildcard: bool = False) -> None:
        archives = self.ls(patterns, wildcard, collect=True)
        for archive in archives:
            if self.args.verbose > 0:
                print(f"remove archive={archive}")
            cur = self.con.cursor()
            cur_arch = sql_execute(cur, f'DELETE FROM archives WHERE name = "{archive}" RETURNING archiveid')
            if self.args.verbose > 0:
                for row_arch in cur_arch:
                    print(f"    removed archive id = {row_arch[0]}")
            sql_commit(self.con)

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
        sql_execute(cur, "CREATE TABLE info(id TEXT, name TEXT, version TEXT, modtime INT)")
        sql_commit(self.con)
        sql_execute(
            cur,
            "INSERT INTO info VALUES (?, ?, ?, ?)",
            ("ACX", "ADIOS Campaign Index", ACA_VERSION, CURRENT_TIME),
        )

        sql_execute(cur, "CREATE TABLE variabletypes (typeid INT, name TEXT, PRIMARY KEY (typeid))")
        vt = [(t.value, t.name) for t in VariableType]
        sql_executemany(cur, "INSERT INTO variabletypes VALUES (?, ?)", vt)

        sql_execute(cur, "CREATE TABLE datasettypes (typeid INT, name TEXT, PRIMARY KEY (typeid))")
        dt = [(t.value, t.name) for t in DatasetType]
        sql_executemany(cur, "INSERT INTO datasettypes VALUES (?, ?)", dt)

        sql_execute(
            cur,
            """
            CREATE TABLE archives (
                archiveid    INTEGER PRIMARY KEY,
                name         TEXT    NOT NULL UNIQUE,
                date_indexed INT
            )""",
        )
        sql_execute(
            cur,
            """
            CREATE TABLE datasets (
                datasetid INTEGER PRIMARY KEY,
                archiveid INTEGER NOT NULL,
                dsid      INT     NOT NULL,
                name TEXT,
                type INT,

                UNIQUE (archiveid, dsid),

                FOREIGN KEY (archiveid)
                    REFERENCES archives(archiveid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            )""",
        )
        sql_execute(
            cur,
            """
            CREATE TABLE variables (
                archiveid INTEGER NOT NULL,
                datasetid INTEGER NOT NULL,
                name      TEXT    NOT NULL,
                type INT,
                steps INT,
                min TEXT,
                max TEXT,
                ndim INT,
                shape TEXT,
                value TEXT,

                PRIMARY KEY (archiveid, datasetid, name),

                FOREIGN KEY (datasetid)
                    REFERENCES datasets(datasetid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            )""",
        )
        sql_execute(
            cur,
            """
            CREATE TABLE attributes (
                archiveid INTEGER NOT NULL,
                datasetid INTEGER NOT NULL,
                name      TEXT    NOT NULL,
                type      INT,
                value     TEXT,

                PRIMARY KEY (archiveid, datasetid, name),

                FOREIGN KEY (datasetid)
                    REFERENCES datasets(datasetid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
            )""",
        )

        sql_commit(self.con)
        cur.close()
        while not exists(index_file):
            sleep(0.1)

    def print_table(self, tablename: str):
        cur = self.con.cursor()
        print(f"________  {tablename.upper()}  ________")
        cursor = sql_execute(cur, f"SELECT * FROM {tablename}")
        names = [description[0] for description in cursor.description]
        print(names)
        for row in cursor:
            print(row)
        print()

    def print_tables(self):
        self.print_table("datasettypes")
        self.print_table("variabletypes")
        self.print_table("archives")
        self.print_table("datasets")
        self.print_table("variables")
        self.print_table("attributes")

    def _add_archive(self, archive: str, indent: str) -> int:
        cur = self.con.cursor()
        cur_ds = sql_execute(
            cur,
            "insert into archives (name, date_indexed) values  (?, ?) "
            "on conflict (name) do update set date_indexed = excluded.date_indexed "
            "returning archiveid",
            (archive, CURRENT_TIME),
        )
        archiveid = cur_ds.fetchone()[0]
        if self.args.verbose > 0:
            print(f"{indent}Archive {archive} rowid = {archiveid}")
        return archiveid

    def _add_dataset(self, archiveid: int, dsid: int, dataset: str, dataset_type: DatasetType, indent: str) -> int:
        cur = self.con.cursor()
        cur_ds = sql_execute(
            cur,
            "insert into datasets (archiveid, dsid, name, type) values  (?, ?, ?, ?) "
            "on conflict (archiveid, dsid) do update set name = excluded.name, type = excluded.type "
            "returning datasetid",
            (archiveid, dsid, dataset, int(dataset_type)),
        )
        datasetid = cur_ds.fetchone()[0]
        if self.args.verbose > 0:
            print(f"{indent}Dataset {dataset}  rowid = {datasetid}")
        return datasetid

    def _add_variable(
        self,
        archiveid: int,
        datasetid: int,
        vname: str,
        vtype: VariableType,
        vsteps: int,
        vmin: str,
        vmax: str,
        ndim: int,
        vshape: str,
        value: Any,
    ):
        cur = self.con.cursor()
        if isinstance(value, np.ndarray):
            value = value.tolist()
        if isinstance(value, np.floating):
            value = float(value)
        if isinstance(value, (np.integer)):
            value = int(value)
        sql_execute(
            cur,
            "INSERT INTO variables VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "on conflict (archiveid, datasetid, name) do update "
            "set type = excluded.type, steps = excluded.steps, min = excluded.min, max = excluded.max, "
            "ndim = excluded.ndim, shape = excluded.shape",
            (archiveid, datasetid, vname, vtype, vsteps, vmin, vmax, ndim, vshape, json.dumps(value)),
        )

    def _add_attribute(self, archiveid: int, datasetid: int, attribute: Any, value: Any, atype: str):
        cur = self.con.cursor()
        if isinstance(value, np.ndarray):
            value = value.tolist()
        if isinstance(value, np.floating):
            value = float(value)
        if isinstance(value, (np.integer)):
            value = int(value)
        if self.args.verbose > 1:
            print(f"insert attr name {attribute}, type {atype}, value {value}")
        sql_execute(
            cur,
            "INSERT INTO attributes VALUES (?, ?, ?, ?, ?) "
            "on conflict (archiveid, datasetid, name) do update "
            "set type = excluded.type, value = excluded.value",
            (archiveid, datasetid, attribute, VariableType[atype], json.dumps(value)),
        )

    def _organize_by_dataset(self, entries: ADIOS_AvailableVariables, info: InfoResult) -> DatasetsVariables:
        d: DatasetsVariables = {}
        dsid = -1
        last_used_dsname = "!@#*.|/ This /is surely a/nonexistent path"
        for ename, edict in entries.items():
            if ename.startswith(last_used_dsname):
                # print(f"        Use last dsname {last_used_dsname} for {ename}")
                entryname = ename[len(last_used_dsname) + 1 :]
            else:
                for ds in info.datasets:
                    if ename.startswith(ds.name):
                        dsid = ds.id
                        last_used_dsname = ds.name
                        entryname = ename[len(ds.name) + 1 :]
                        # print(f"        Use new dsname {last_used_dsname} for {ename}")
                        break
            if dsid == -1:
                raise RuntimeError(
                    f"Variable/attribute path {ename} has no matching dataset name. This should not happen"
                )
            if self.args.verbose > 1:
                print(f"        add {entryname} : to d{dsid}")
            d.setdefault(dsid, {}).update({entryname: edict})
        return d

    def _index_archive(self, archive: str):
        manager = Manager(archive, campaign_store=self.args.campaign_store, verbose=0)
        info_res = manager.info()
        if len(info_res.datasets) < 0:
            return

        archiveid = self._add_archive(archive, "    ")

        dsdict: dict[int, tuple[int, str]] = {}
        for ds in info_res.datasets:
            datasetid = self._add_dataset(archiveid, ds.id, ds.name, DatasetType[ds.file_format], "    ")
            dsdict[ds.id] = (datasetid, ds.name)

        # ds.id, dsid: the dataset integer ID in the archive file itself
        # datasetid: rowid of the datasets table in this database

        with adios2.FileReader(archive) as f:
            variables = f.available_variables()
            ds_var_entries: DatasetsVariables = self._organize_by_dataset(variables, info_res)
            attributes = f.available_attributes()
            ds_attr_entries: DatasetsVariables = self._organize_by_dataset(attributes, info_res)

            # add variables to the collection
            for dsid, variables in ds_var_entries.items():
                dataset_id: int = dsdict[dsid][0]
                dataset_name: str = dsdict[dsid][1]
                for var_name, var_info in variables.items():
                    if self.args.verbose > 1:
                        print(f"      Process variable {dataset_name}  /  {var_name}")
                    if dataset_name.endswith(".json") and not var_name:
                        content = f.read(f"{dataset_name}")
                        json_attributes = json.loads("".join(chr(code) for code in content))
                        for atr in json_attributes:
                            alen, atype = python_type_to_variable_type(atr)
                            avalue = json_attributes[atr]
                            aname = atr.title()
                            asteps = 1
                            amin = amax = avalue
                            if alen == 1:
                                ndim = 0
                                shape = ""
                            else:
                                ndim = 1
                                shape = f"{alen}"
                            self._add_variable(
                                archiveid, dataset_id, aname, atype, asteps, amin, amax, ndim, shape, avalue
                            )
                    else:  # otherwise we add variable data
                        var = f.inquire_variable(f"{dataset_name}/{var_name}")
                        if var is None:
                            continue
                        vtype = VariableType[var.type()]
                        vsteps = var.steps()
                        ndim = len(var.shape())
                        value = ""
                        if var.single_value():
                            value = f.read(var, step_selection=[0, vsteps])
                        self._add_variable(
                            archiveid=archiveid,
                            datasetid=dataset_id,
                            vname=var_name,
                            vtype=vtype,
                            vsteps=vsteps,
                            vmin=var_info["Min"],
                            vmax=var_info["Max"],
                            ndim=ndim,
                            vshape=var_info["Shape"],
                            value=value,
                        )

            # add attributes to the collection
            for dsid, attrs in ds_attr_entries.items():
                for attr_name, attr_info in attrs.items():
                    if self.args.verbose > 1:
                        print(f"      Process attribute {dsdict[dsid][1]}  /  {attr_name}")
                    content = f.read_attribute(f"{dsdict[dsid][1]}/{attr_name}")
                    if attr_name.endswith(".json"):
                        json_attributes = json.loads("".join(chr(code) for code in content))
                        for atr in json_attributes:
                            alen, atype = python_type_to_variable_type(atr)
                            avalue = json_attributes[atr]
                            self._add_attribute(archiveid, dsdict[dsid][0], attr_name, avalue, "string")
                    else:  # otherwise we add variable data
                        self._add_attribute(archiveid, dsdict[dsid][0], attr_name, content, attr_info["Type"])

        sql_commit(self.con)

    def _set_defaults_index(self):
        set_default_args_from_config(self.args, False)
        self.args.index_file = get_path(self.args.index_file, self.args.campaign_store)
        if not self.args.index_file.endswith(".acx"):
            self.args.index_file += ".acx"
        if self.args.verbose > 0:
            print(f"# Verbosity = {self.args.verbose}")
            print(f"# Campaign Store = {self.args.campaign_store}")


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
        choices=["add", "remove", "ls", "dump"],
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
    elif args.command == "dump":
        index.print_tables()
    else:
        print(f"Unknown command for index: {args.command}")

    index.close()


if __name__ == "__main__":
    main()
