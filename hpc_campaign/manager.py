#!/usr/bin/env python3

# pylint: disable=too-many-lines
# pylint: disable=import-error
# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
# pylint: disable=unused-argument
# pylint: disable=too-many-positional-arguments

import argparse
import sqlite3
import sys
from os.path import exists
from pathlib import Path
from time import time_ns

from .info import InfoResult, collect_info, print_info
from .key import read_key
from .manager_args import ArgParser
from .manager_funcs import (
    add_archival_storage,
    add_time_series,
    archive_dataset,
    check_archival_storage_system_name,
    create_tables,
    delete_dataset,
    delete_replica,
    delete_time_series,
    set_default_args,
    update,
)
from .upgrade import upgrade_aca
from .utils import (
    check_campaign_store,
    sql_commit,
    sql_error_list,
)

CURRENT_TIME = time_ns()


class Manager:  # pylint: disable=too-many-public-methods
    """Manager API for campaign archives."""

    def __init__(
        self,
        archive: str,
        hostname: str = "",
        campaign_store: str = "",
        keyfile: str = "",
        verbose: int = 0,
    ):
        """
        Create Manager object for a campaign archive
        :param archive: The name of the campaign archive (relative path under campaign_store)
        :param hostname: Optional hostname, default is from ~/.config/hpc-campaign/config.yaml, or
           the return value of gethostname.
        :param campaign_store: Optional base path for all campaign archives, default is from
            ~/.config/hpc-campaign/config.yaml.
        :param keyfile: Optional encryption key to encrypt all metadata inside the campaign archive.
            Only applied to the operations in this session, existing information is not encrypted.
        :param verbose: Optional verbose for printing debug information if verbose > 0
        """

        if not archive:
            raise ValueError("Manager requires an archive path")

        self.args: argparse.Namespace = argparse.Namespace(archive=archive)
        self.args.verbose = verbose
        self.args.campaign_store = campaign_store
        self.args.hostname = hostname
        self.args.keyfile = keyfile
        self.args = set_default_args(self.args)
        self._apply_encryption_key()
        check_campaign_store(self.args.campaign_store, False)
        self.con: sqlite3.Connection
        self.cur: sqlite3.Cursor
        self.connected = False

    def _apply_encryption_key(self):
        if self.args.keyfile:
            key = read_key(self.args.keyfile)
            # ask for password at this point
            self.args.encryption_key = key.get_decrypted_key()
            self.args.encryption_key_id = key.id
        else:
            self.args.encryption_key = None
            self.args.encryption_key_id = None

    def _build_command_args(self, command: str, updates: dict | None = None) -> argparse.Namespace:
        cmd_args = argparse.Namespace(**vars(self.args))
        cmd_args.command = command
        if updates:
            for key, value in updates.items():
                setattr(cmd_args, key, value)
        return cmd_args

    def _wipe_aca(self):
        self.cur.execute("PRAGMA foreign_keys = OFF;")
        objects = self.cur.execute("""
            SELECT type, name
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%';
        """).fetchall()

        for obj_type, name in objects:
            self.cur.execute(f'DROP {obj_type.upper()} IF EXISTS "{name}";')

        self.con.commit()
        self.con.execute("VACUUM;")

    def open(self, create=False, truncate=False):
        """
        Open/create an ACA campaign archive
        :param create: if True create new archive if it does not exists. Default is to throw an error.
        :param truncate: if True and archive already exists, remove all content of the archive first.
        """
        fileexists = exists(self.args.campaign_file_name)
        if not create and not fileexists:
            raise FileNotFoundError(f"archive {self.args.campaign_file_name} does not exist")

        self.con = sqlite3.connect(self.args.campaign_file_name)
        self.cur = self.con.cursor()
        self.connected = True

        if truncate:
            self._wipe_aca()

        if not fileexists or truncate:
            create_tables(self.args.campaign_file_name, self.con)

    def close(self):
        """
        Close the ACA campaign archive.
        All operations have committed their changes, so close is only for freeing up database resources.
        """
        if self.connected:
            self.cur.close()
            self.con.close()
            self.connected = False

    def info(
        self,
        list_replicas: bool = False,
        list_files: bool = False,
        show_deleted: bool = False,
        show_checksum: bool = False,
    ) -> InfoResult:
        args = self._build_command_args(
            "info",
            {
                "list_replicas": list_replicas,
                "list_files": list_files,
                "show_deleted": show_deleted,
                "show_checksum": show_checksum,
            },
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        info_data = collect_info(args, self.cur)
        return info_data

    def data(self, files: list[str | Path] | str | Path, name: str | None = None):
        file_list = self.normalize_files(files)
        if name is not None and len(file_list) > 1:
            raise ValueError("Invalid arguments for data: when using --name <name>, only one data file is allowed")
        cmd_args = self._build_command_args("data", {"files": file_list, "name": name})
        if not self.connected:
            self.open(create=True, truncate=False)
        update(cmd_args, self.cur, self.con)

    def text(self, files: list[str | Path] | str | Path, name: str | None = None, store: bool = False):
        file_list = self.normalize_files(files)
        if name is not None and len(file_list) > 1:
            raise ValueError("Invalid arguments for text: when using --name <name>, only one text file is allowed")
        cmd_args = self._build_command_args(
            "text",
            {"files": file_list, "name": name, "store": store},
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        update(cmd_args, self.cur, self.con)

    def image(
        self,
        file_path: str | Path,
        name: str | None = None,
        store: bool = False,
        thumbnail: list[int] | tuple[int, int] | None = None,
    ):
        file_path = str(file_path)
        thumb_value = None
        if thumbnail is not None:
            thumb_value = [int(thumbnail[0]), int(thumbnail[1])]
        cmd_args = self._build_command_args(
            "image",
            {"file": file_path, "name": name, "store": store, "thumbnail": thumb_value},
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        update(cmd_args, self.cur, self.con)

    def delete_uuid(self, uuid: str):
        if not self.connected:
            self.open(create=True, truncate=False)
        delete_dataset(self.args, self.cur, self.con, uniqueid=uuid)
        sql_commit(self.con)

    def delete_name(self, name: str):
        if not self.connected:
            self.open(create=True, truncate=False)
        delete_dataset(self.args, self.cur, self.con, name=name)
        sql_commit(self.con)

    def delete_replica(self, replicaid: int):
        if not self.connected:
            self.open(create=True, truncate=False)
        delete_replica(self.args, self.cur, self.con, replicaid, True)
        sql_commit(self.con)

    def delete_time_series(self, name: str):
        if not self.connected:
            self.open(create=True, truncate=False)
        delete_time_series(name, self.cur, self.con)

    def add_archival_storage(
        self,
        system: str,
        host: str,
        directory: str,
        tarfilename: str = "",
        tarfileidx: str = "",
        longhostname: str = "",
        note: str = "",
    ) -> tuple[int, int, int]:
        check_archival_storage_system_name(system)
        cmd_args = self._build_command_args(
            "archival_storage",
            {
                "system": system,
                "host": host,
                "directory": directory,
                "tarfilename": tarfilename,
                "tarfileidx": tarfileidx,
                "longhostname": longhostname,
                "note": note,
            },
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        host_id, dir_id, archive_id = add_archival_storage(cmd_args, self.cur, self.con)
        return host_id, dir_id, archive_id

    def archived_replica(
        self, name: str, dirid: int, archiveid: int = 0, newpath: str = "", replica: int = 0, move: bool = False
    ):
        cmd_args = self._build_command_args(
            "archived_replica",
            {
                "name": name,
                "dirid": dirid,
                "archiveid": archiveid,
                "newpath": newpath,
                "replica": replica,
                "move": move,
            },
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        archive_dataset(cmd_args, self.cur, self.con)

    def add_time_series(self, name: str, datasets: str | list[str], replace: bool = False):
        dslist = datasets
        if isinstance(datasets, str):
            dslist = [datasets]
        cmd_args = self._build_command_args(
            "add_time_series",
            {"name": name, "datasets": dslist, "replace": replace},
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        add_time_series(cmd_args, self.cur, self.con)

    def upgrade(self) -> str:
        if not self.connected:
            self.open(create=True, truncate=False)
        new_version = upgrade_aca(self.args, self.cur, self.con)
        return new_version

    def normalize_files(self, files: list[str | Path] | str | Path) -> list[str]:
        if isinstance(files, (str, Path)):
            return [str(files)]
        return [str(entry) for entry in files]


# pylint:disable = too-many-statements
def main(args=None, prog=None):
    parser = ArgParser(args=args, prog=prog)
    manager = Manager(
        archive=parser.args.archive,
        hostname=parser.args.hostname,
        campaign_store=parser.args.campaign_store,
        keyfile=parser.args.keyfile,
        verbose=parser.args.verbose,
    )

    n_cmd = 0
    while parser.parse_next_command():
        print("=" * 10, f"  {parser.args.command}  ", "=" * 50)
        # print(parser.args)
        # print("--------------------------")
        n_cmd += 1
        create_allowed = True
        if parser.args.command in ("info", "add-archival-storage", "archived-replica", "time-series", "upgrade"):
            create_allowed = False
        if n_cmd == 1:
            try:
                manager.open(create=create_allowed, truncate=parser.args.truncate)
            except FileNotFoundError as e:
                print(f"ERROR: {e}")
                sys.exit(1)

        if parser.args.command == "info":
            info_data = manager.info(
                parser.args.list_replicas, parser.args.list_files, parser.args.show_deleted, parser.args.show_checksum
            )
            print_info(info_data)
        elif parser.args.command == "data":
            manager.data(parser.args.files, parser.args.name)
        elif parser.args.command == "text":
            manager.text(parser.args.files, parser.args.name, parser.args.store)
        elif parser.args.command == "image":
            manager.image(parser.args.file, parser.args.name, parser.args.store, parser.args.thumbnail)
        elif parser.args.command == "delete":
            if parser.args.uuid is not None:
                for uid in parser.args.uuid:
                    manager.delete_uuid(uid)
            if parser.args.name is not None:
                for name in parser.args.name:
                    manager.delete_name(name)
            if parser.args.replica is not None:
                for rep in parser.args.replica:
                    manager.delete_replica(int(rep))
        elif parser.args.command == "add-archival-storage":
            host_id, dir_id, archive_id = manager.add_archival_storage(
                parser.args.system,
                parser.args.host,
                parser.args.directory,
                parser.args.tarfilename,
                parser.args.tarfileidx,
                parser.args.longhostname,
                parser.args.note,
            )
            if archive_id > 0:
                print(f"Archive storage added: host id = {host_id}, directory id = {dir_id} archive id = {archive_id}")
            else:
                print("Adding archive storage FAILED")
        elif parser.args.command == "archived-replica":
            manager.archived_replica(
                parser.args.name, parser.args.dirid, parser.args.archiveid, parser.args.newpath, parser.args.replica
            )
        elif parser.args.command == "time-series":
            if parser.args.remove:
                manager.delete_time_series(parser.args.name)
            manager.add_time_series(parser.args.name, parser.args.dataset, parser.args.replace)
        elif parser.args.command == "upgrade":
            manager.upgrade()
        else:
            print(f"This should not happen. Unknown command accepted by argparser: {parser.args.command}")

    if len(sql_error_list) > 0:
        print()
        print("!!!! SQL Errors encountered")
        for serr in sql_error_list:
            print(f"  {serr.sqlite_errorcode}  {serr.sqlite_errorname}: {serr}")
        print("!!!!")
        print()


if __name__ == "__main__":
    main()
