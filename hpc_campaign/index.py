#!/usr/bin/env python3

# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
# pylint: disable=too-many-statements
# pylint: disable=too-many-positional-arguments


import argparse
import json
import re
import sqlite3
import sys
from contextlib import ExitStack
from decimal import Decimal
from os.path import exists
from pathlib import Path
from tempfile import TemporaryDirectory
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


def _json_value_bounds(value: Any) -> tuple[str | None, str | None]:
    """Return text bounds for comparable JSON values, or null bounds otherwise."""
    if isinstance(value, list):
        if not value or not (
            all(isinstance(item, (int, float)) for item in value) or all(isinstance(item, str) for item in value)
        ):
            return None, None
        lower, upper = min(value), max(value)
    elif value is None or isinstance(value, dict):
        return None, None
    else:
        lower = upper = value
    return str(int(lower) if isinstance(lower, bool) else lower), str(int(upper) if isinstance(upper, bool) else upper)


def _logical_datasets(info: InfoResult) -> dict[int, tuple[str, DatasetType]]:
    datasets = {dsid: (ds.name, DatasetType[ds.file_format]) for dsid, ds in info.datasets.items()}
    for tsid, series in info.time_series.items():
        # Negative ACA IDs identify logical series without colliding with members.
        member = next(iter(series.datasets.values()), None)
        datasets[-tsid] = (series.name, DatasetType[member.file_format] if member else DatasetType.Unknown)
    return datasets


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
        self._ensure_time_series_tables()

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
            path = Path(pattern).expanduser()
            explicit_relative = pattern.startswith(("./", "../"))
            if (path.is_absolute() or explicit_relative) and path.is_file():
                archives.append(str(path.resolve()))
            elif explicit_relative:
                # Search explicit relative patterns from the working directory,
                # not from the configured campaign store. Keep the remaining
                # pattern intact for regex or wildcard matching.
                search_root = Path.cwd()
                while pattern.startswith(("./", "../")):
                    if pattern.startswith("../"):
                        search_root = search_root.parent
                        pattern = pattern[3:]
                    else:
                        pattern = pattern[2:]
                archives += [
                    str(search_root / archive)
                    for archive in ls(pattern, wildcard=wildcard, index=False, campaign_store=str(search_root))
                ]
            else:
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
            cur_arch = sql_execute(cur, "DELETE FROM archives WHERE name = ? RETURNING archiveid", (archive,))
            removed = cur_arch.fetchall()
            cur.close()
            if self.args.verbose > 0:
                for row_arch in removed:
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
        self.con.execute("PRAGMA foreign_keys = ON")

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
        self.print_table("timeseries")
        self.print_table("timeseries_datasets")
        self.print_table("timeseries_variable_steps")

    def _ensure_time_series_tables(self):
        """Add time-series metadata to new and existing ACX files."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS timeseries (
                tsid INTEGER PRIMARY KEY,
                archiveid INTEGER NOT NULL REFERENCES archives(archiveid) ON DELETE CASCADE,
                aca_tsid INTEGER NOT NULL,
                datasetid INTEGER NOT NULL UNIQUE REFERENCES datasets(datasetid) ON DELETE CASCADE,
                name TEXT NOT NULL,
                UNIQUE (archiveid, aca_tsid)
            )
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS timeseries_datasets (
                tsid INTEGER NOT NULL REFERENCES timeseries(tsid) ON DELETE CASCADE,
                datasetid INTEGER NOT NULL REFERENCES datasets(datasetid) ON DELETE CASCADE,
                tsorder INTEGER NOT NULL,
                PRIMARY KEY (tsid, datasetid),
                UNIQUE (tsid, tsorder)
            )
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS timeseries_variable_steps (
                tsid INTEGER NOT NULL,
                datasetid INTEGER NOT NULL,
                name TEXT NOT NULL,
                step_start INTEGER NOT NULL CHECK (step_start >= 0),
                steps INTEGER NOT NULL CHECK (steps > 0),
                PRIMARY KEY (tsid, datasetid, name),
                FOREIGN KEY (tsid, datasetid) REFERENCES timeseries_datasets(tsid, datasetid) ON DELETE CASCADE
            )
        """)
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS timeseries_variable_step_lookup "
            "ON timeseries_variable_steps (tsid, name, step_start)"
        )
        self.con.commit()

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
        vmin: str | None,
        vmax: str | None,
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
            "ndim = excluded.ndim, shape = excluded.shape, value = excluded.value",
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
        # Prefer the most specific dataset and match a complete path component.
        # For example, dataset "input" must not capture "input_dir/file".
        datasets = sorted(_logical_datasets(info).items(), key=lambda item: len(item[1][0]), reverse=True)
        for ename, edict in entries.items():
            for ds_id, (name, _) in datasets:
                if ename == name or ename.startswith(name + "/"):
                    dsid = ds_id
                    entryname = ename[len(name) + 1 :]
                    break
            else:
                raise RuntimeError(
                    f"Variable/attribute path {ename} has no matching dataset name. This should not happen"
                )
            if self.args.verbose > 1:
                print(f"        add {entryname} : to d{dsid}")
            d.setdefault(dsid, {}).update({entryname: edict})
        return d

    def _index_archive(self, archive: str):
        # Publish membership and summaries together, or retain the previous snapshot.
        with self.con:
            self._index_archive_contents(archive)

    def _index_archive_contents(self, archive: str):
        manager = Manager(archive, campaign_store=self.args.campaign_store, verbose=0)
        try:
            info_res = manager.info()
        finally:
            manager.close()

        archiveid = self._add_archive(archive, "    ")
        self.con.execute("DELETE FROM timeseries WHERE archiveid = ?", (archiveid,))
        self.con.execute("DELETE FROM variables WHERE archiveid = ?", (archiveid,))
        self.con.execute("DELETE FROM attributes WHERE archiveid = ?", (archiveid,))

        dsdict: dict[int, tuple[int, str]] = {}
        for ds_id, (name, dataset_type) in _logical_datasets(info_res).items():
            datasetid = self._add_dataset(archiveid, ds_id, name, dataset_type, "    ")
            dsdict[ds_id] = (datasetid, name)

        # dsid: ACA dataset ID, or negated ACA time-series ID
        # datasetid: rowid of the datasets table in this database

        archive_path = get_path(archive, self.args.campaign_store)
        with ExitStack() as stack:
            if info_res.time_series:
                cache = stack.enter_context(TemporaryDirectory(prefix="hpc-index-"))
                adios = adios2.Adios()
                io = adios.declare_io("index-archive")
                io.set_engine("Campaign")
                io.set_parameters({"CachePath": cache})
                f = stack.enter_context(adios2.FileReader(io, archive_path))
            else:
                f = stack.enter_context(adios2.FileReader(archive_path))
            variables = f.available_variables()
            ds_var_entries: DatasetsVariables = self._organize_by_dataset(variables, info_res)
            attributes = f.available_attributes()
            ds_attr_entries: DatasetsVariables = self._organize_by_dataset(attributes, info_res)

            # add variables to the collection
            for dsid, variables in ds_var_entries.items():
                if dsid < 0:
                    # Build a single summary from the individual members below.
                    continue
                dataset_id: int = dsdict[dsid][0]
                dataset_name: str = dsdict[dsid][1]
                for var_name, var_info in variables.items():
                    if self.args.verbose > 1:
                        print(f"      Process variable {dataset_name}  /  {var_name}")
                    if dataset_name.endswith(".json") and not var_name:
                        content = f.read(f"{dataset_name}")
                        json_attributes = json.loads("".join(chr(code) for code in content))
                        for atr in json_attributes:
                            avalue = json_attributes[atr]
                            alen, atype = python_type_to_variable_type(avalue)
                            aname = atr.title()
                            asteps = 1
                            vmin, vmax = _json_value_bounds(avalue)
                            if alen == 1:
                                ndim = 0
                                shape = ""
                            else:
                                ndim = 1
                                shape = f"{alen}"
                            self._add_variable(
                                archiveid, dataset_id, aname, atype, asteps, vmin, vmax, ndim, shape, avalue
                            )
                    else:  # otherwise we add variable data
                        variable_path = f"{dataset_name}/{var_name}" if var_name else dataset_name
                        var = f.inquire_variable(variable_path)
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
                            vmin=var_info.get("Min"),
                            vmax=var_info.get("Max"),
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
                            avalue = json_attributes[atr]
                            alen, atype = python_type_to_variable_type(avalue)
                            self._add_attribute(archiveid, dsdict[dsid][0], atr, avalue, atype.name)
                    else:  # otherwise we add variable data
                        self._add_attribute(archiveid, dsdict[dsid][0], attr_name, content, attr_info["Type"])

        self._index_time_series(archive_path, archiveid, info_res, dsdict)
        active_ids = list(dsdict)
        for series in info_res.time_series.values():
            active_ids.extend(series.datasets)
        placeholders = ",".join("?" for _ in active_ids)
        self.con.execute(
            f"DELETE FROM datasets WHERE archiveid = ? AND dsid NOT IN ({placeholders})", (archiveid, *active_ids)
        )

    def _index_time_series(self, archive: str, archiveid: int, info: InfoResult, dsdict: dict[int, tuple[int, str]]):
        with TemporaryDirectory(prefix="hpc-index-") as cache:
            for aca_tsid, series in info.time_series.items():
                logical_id = dsdict[-aca_tsid][0]
                tsid = self.con.execute(
                    "INSERT INTO timeseries (archiveid, aca_tsid, datasetid, name) VALUES (?, ?, ?, ?) RETURNING tsid",
                    (archiveid, aca_tsid, logical_id, series.name),
                ).fetchone()[0]
                summaries: dict[str, dict] = {}
                for dsid, member in sorted(series.datasets.items(), key=lambda item: item[1].ts_order):
                    datasetid = self._add_dataset(archiveid, dsid, member.name, DatasetType[member.file_format], "    ")
                    self.con.execute(
                        "INSERT INTO timeseries_datasets VALUES (?, ?, ?)", (tsid, datasetid, member.ts_order)
                    )
                    adios = adios2.Adios()
                    io = adios.declare_io("index-member")
                    io.set_engine("Campaign")
                    io.set_parameters(
                        {
                            "include-dataset": "^" + re.escape(member.name).replace(";", r"\x3b") + "$",
                            # ATS cache entries depend on the include filter. Isolate each member.
                            "CachePath": str(Path(cache) / str(dsid)),
                        }
                    )
                    with adios2.FileReader(io, archive) as reader:
                        entries = self._organize_by_dataset(reader.available_variables(), info).get(-aca_tsid, {})
                        for name, entry in entries.items():
                            steps = int(entry["AvailableStepsCount"])
                            variable = reader.inquire_variable(f"{series.name}/{name}")
                            summary = summaries.setdefault(
                                name,
                                {
                                    "steps": 0,
                                    "min": None,
                                    "max": None,
                                    "type": VariableType[entry["Type"]],
                                    "shape": entry["Shape"],
                                    "ndim": len(variable.shape()),
                                    "value": [] if variable.single_value() else "",
                                },
                            )
                            if summary["type"] != VariableType[entry["Type"]]:
                                raise ValueError(f"Variable {series.name}/{name} changes type between members")
                            self.con.execute(
                                "INSERT INTO timeseries_variable_steps VALUES (?, ?, ?, ?, ?)",
                                (tsid, datasetid, name, summary["steps"], steps),
                            )
                            summary["steps"] += steps
                            if variable.single_value():
                                values = reader.read(variable, step_selection=[0, steps])
                                summary["value"].extend(np.asarray(values).reshape(-1).tolist())
                            blocks = reader.all_blocks_info(f"{series.name}/{name}") if "Min" in entry else []
                            for bound, compare in (("min", min), ("max", max)):
                                # Campaign's AvailableVariables may describe only the
                                # first step. Use metadata bounds from every block.
                                values = [
                                    block[bound.title()] for step in blocks for block in step if bound.title() in block
                                ]
                                for value in values:
                                    candidate = Decimal(value)
                                    if candidate.is_nan():
                                        continue
                                    previous = summary[bound]
                                    # Decimal preserves uint64 and long-double bounds exactly.
                                    if previous is None or compare(Decimal(previous), candidate) == candidate:
                                        summary[bound] = value
                            self._add_variable(
                                archiveid,
                                logical_id,
                                name,
                                summary["type"],
                                summary["steps"],
                                summary["min"],
                                summary["max"],
                                summary["ndim"],
                                summary["shape"],
                                summary["value"],
                            )

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
    if args.command == "ls":
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
