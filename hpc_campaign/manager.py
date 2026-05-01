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
from io import BytesIO
from os.path import exists
from pathlib import Path
from time import time_ns

from PIL import Image as PILImage

from .info import InfoResult, collect_info, print_info
from .key import read_key
from .manager_args import ArgParser
from .manager_funcs import (
    add_archival_storage,
    add_image_data,
    add_time_series,
    add_visualization_sequence,
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
        self.con.row_factory = sqlite3.Row
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
        info_data = collect_info(args, self.con)
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

    def image_data(
        self,
        data: bytes,
        image_format: str,
        name: str | None = None,
        thumbnail: list[int] | tuple[int, int] | None = None,
        replica_name: str | None = None,
    ):
        thumb_value = None
        if thumbnail is not None:
            thumb_value = [int(thumbnail[0]), int(thumbnail[1])]
        cmd_args = self._build_command_args(
            "image_data",
            {
                "image_data": bytes(data),
                "image_format": image_format,
                "name": name,
                "thumbnail": thumb_value,
                "replica_name": replica_name,
            },
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        add_image_data(cmd_args, self.cur, self.con)

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

    def visualization_sequence(
        self,
        name: str,
        vis_type: str,
        variables,
        items,
        source_dataset: str | None = None,
        thumbnail_name: str | None = None,
        thumbnail_uuid: str | None = None,
        metadata=None,
        replace: bool = False,
    ) -> int:
        cmd_args = self._build_command_args(
            "visualization_sequence",
            {
                "name": name,
                "vis_type": vis_type,
                "variables": variables,
                "items": items,
                "source_dataset": source_dataset,
                "thumbnail_name": thumbnail_name,
                "thumbnail_uuid": thumbnail_uuid,
                "metadata": metadata,
                "replace": replace,
            },
        )
        if not self.connected:
            self.open(create=True, truncate=False)
        return add_visualization_sequence(cmd_args, self.cur, self.con)

    def visualization(
        self,
        images,
        vis_type: str | None = None,
        variables=None,
        source_dataset: str | None = None,
        name: str | None = None,
        sequence_name: str | None = None,
        image_names: str | list[str] | None = None,
        steps: list[int] | tuple[int, ...] | None = None,
        image_format: str | None = None,
        thumbnail: list[int] | tuple[int, int] | None = None,
        thumbnail_image: int = 0,
        store: bool = False,
        metadata=None,
        replace: bool = False,
        kind: str | None = None,
        variable: str | None = None,
        color_by: str | None = None,
        contour_by: str | None = None,
        streamline_by=None,
        x_axis: str | None = None,
        y_axis=None,
    ) -> int:
        image_inputs = self._normalize_visualization_images(images)
        if not image_inputs:
            raise ValueError("visualization requires at least one image")

        resolved_vis_type = self._resolve_visualization_kind(kind, vis_type)
        variable_specs = self._build_visualization_variable_specs(
            variables=variables,
            variable=variable,
            color_by=color_by,
            contour_by=contour_by,
            streamline_by=streamline_by,
            x_axis=x_axis,
            y_axis=y_axis,
            source_dataset=source_dataset,
        )
        sequence_name = self._resolve_visualization_sequence_name(
            source_dataset=source_dataset,
            name=name,
            sequence_name=sequence_name,
            variables=variable_specs,
        )
        logical_image_names = self._resolve_visualization_image_names(
            image_inputs=image_inputs,
            sequence_name=sequence_name,
            image_names=image_names,
            steps=steps,
            image_format=image_format,
        )

        if not 0 <= int(thumbnail_image) < len(image_inputs):
            raise ValueError("thumbnail_image index is out of range")

        for idx, image_input in enumerate(image_inputs):
            logical_name = logical_image_names[idx]
            if self._is_path_like_image(image_input):
                self.image(
                    str(image_input),
                    name=logical_name,
                    store=store,
                    thumbnail=thumbnail,
                )
            else:
                image_bytes, resolved_format = self._coerce_image_input(image_input, image_format)
                self.image_data(
                    image_bytes,
                    resolved_format,
                    name=logical_name,
                    thumbnail=thumbnail,
                    replica_name=f"generated/{Path(logical_name).name}",
                )

        return self.visualization_sequence(
            name=sequence_name,
            vis_type=resolved_vis_type,
            variables=variable_specs,
            items=[{"type": "IMAGE", "name": logical_name} for logical_name in logical_image_names],
            source_dataset=source_dataset,
            thumbnail_name=logical_image_names[int(thumbnail_image)],
            metadata=metadata,
            replace=replace,
        )

    def upgrade(self) -> str:
        if not self.connected:
            self.open(create=True, truncate=False)
        new_version = upgrade_aca(self.args, self.cur, self.con)
        return new_version

    def normalize_files(self, files: list[str | Path] | str | Path) -> list[str]:
        if isinstance(files, (str, Path)):
            return [str(files)]
        return [str(entry) for entry in files]

    def _normalize_visualization_images(self, images):
        if isinstance(images, (str, Path, bytes, bytearray, memoryview, PILImage.Image)):
            return [images]
        if self._is_matplotlib_figure(images):
            return [images]
        return list(images)

    def _is_path_like_image(self, image) -> bool:
        return isinstance(image, (str, Path))

    def _is_matplotlib_figure(self, image) -> bool:
        image_type = type(image)
        return image_type.__module__.startswith("matplotlib.") and hasattr(image, "savefig")

    def _infer_image_format(self, data: bytes) -> str | None:
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return "PNG"
        if data.startswith(b"\xff\xd8\xff"):
            return "JPEG"
        if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
            return "GIF"
        return None

    def _coerce_image_input(self, image, image_format: str | None) -> tuple[bytes, str]:
        if isinstance(image, memoryview):
            image = image.tobytes()
        if isinstance(image, bytearray):
            image = bytes(image)
        if isinstance(image, bytes):
            resolved_format = image_format or self._infer_image_format(image)
            if not resolved_format:
                raise ValueError("image_format is required for unrecognized in-memory image bytes")
            return image, resolved_format
        if isinstance(image, PILImage.Image):
            if not image_format:
                image_format = "PNG"
            buf = BytesIO()
            image.save(buf, format=image_format.upper())
            return buf.getvalue(), image_format
        if self._is_matplotlib_figure(image):
            resolved_format = image_format or "PNG"
            buf = BytesIO()
            image.savefig(buf, format=resolved_format.lower())
            return buf.getvalue(), resolved_format
        raise TypeError(f"Unsupported visualization image input type: {type(image)!r}")

    def _normalize_visualization_variable_specs(self, variables, source_dataset: str | None):
        if isinstance(variables, (str, dict, tuple)):
            variable_list = [variables]
        else:
            variable_list = list(variables)
        normalized = []
        default_source_dataset = source_dataset or ""
        for entry in variable_list:
            if isinstance(entry, str):
                normalized.append({"name": entry, "role": "primary", "source_dataset": default_source_dataset})
                continue
            if isinstance(entry, dict):
                item = dict(entry)
                if "source_dataset" not in item or not item.get("source_dataset"):
                    item["source_dataset"] = default_source_dataset
                if ("role" not in item or not item.get("role")) and item.get("use"):
                    item["role"] = item["use"]
                if "role" not in item or not item.get("role"):
                    item["role"] = "primary"
                normalized.append(item)
                continue
            if isinstance(entry, tuple):
                if len(entry) == 0:
                    continue
                item = {"name": entry[0], "role": "primary", "source_dataset": default_source_dataset}
                if len(entry) >= 2 and entry[1]:
                    item["role"] = entry[1]
                if len(entry) >= 3 and entry[2]:
                    item["source_dataset"] = entry[2]
                normalized.append(item)
                continue
            raise TypeError(f"Unsupported variable specification: {entry!r}")
        if not normalized:
            raise ValueError("visualization requires at least one variable specification")
        for item in normalized:
            if not item.get("source_dataset"):
                raise ValueError(f"Variable {item.get('name')!r} requires source_dataset")
        return normalized

    def _resolve_visualization_kind(self, kind: str | None, vis_type: str | None) -> str:
        if kind and vis_type and kind != vis_type:
            raise ValueError("visualization received both kind and vis_type with different values")
        return str(kind or vis_type or "visualization")

    def _semantic_variable_specs(
        self,
        variable: str | None,
        color_by: str | None,
        contour_by: str | None,
        streamline_by,
        x_axis: str | None,
        y_axis,
    ) -> list[dict[str, str]]:
        specs: list[dict[str, str]] = []
        if variable:
            specs.append({"name": str(variable), "role": "primary"})
        if color_by:
            specs.append({"name": str(color_by), "role": "color-by"})
        if contour_by:
            specs.append({"name": str(contour_by), "role": "contour-by"})
        if streamline_by:
            if isinstance(streamline_by, (str, Path)):
                specs.append({"name": str(streamline_by), "role": "streamline-by"})
            else:
                names = [str(entry) for entry in streamline_by]
                if len(names) == 2:
                    specs.append({"name": names[0], "role": "streamline-x"})
                    specs.append({"name": names[1], "role": "streamline-y"})
                else:
                    for name in names:
                        specs.append({"name": name, "role": "streamline-by"})
        if x_axis:
            specs.append({"name": str(x_axis), "role": "x-axis"})
        if y_axis:
            if isinstance(y_axis, (str, Path)):
                y_names = [str(y_axis)]
            else:
                y_names = [str(entry) for entry in y_axis]
            for name in y_names:
                specs.append({"name": name, "role": "y-axis"})
        return specs

    def _build_visualization_variable_specs(
        self,
        variables,
        variable: str | None,
        color_by: str | None,
        contour_by: str | None,
        streamline_by,
        x_axis: str | None,
        y_axis,
        source_dataset: str | None,
    ):
        semantic_specs = self._semantic_variable_specs(variable, color_by, contour_by, streamline_by, x_axis, y_axis)
        if variables is not None and semantic_specs:
            raise ValueError("Use either variables=... or semantic arguments, not both")
        variable_inputs = variables if variables is not None else semantic_specs
        return self._normalize_visualization_variable_specs(variable_inputs, source_dataset)

    def _default_visualization_token(self, variables) -> str:
        if len(variables) == 1 and variables[0]["role"] == "primary":
            return str(variables[0]["name"])
        parts = [f"{entry['role']}-{entry['name']}" for entry in variables]
        return "__".join(parts)

    def _default_visualization_name(self, source_dataset: str | None, variables) -> str:
        root = source_dataset
        if not root:
            root = variables[0]["source_dataset"]
        if not root:
            root = "visualization"
        return f"{root}/visualizations/{self._default_visualization_token(variables)}"

    def _resolve_visualization_sequence_name(
        self,
        source_dataset: str | None,
        name: str | None,
        sequence_name: str | None,
        variables,
    ) -> str:
        if name and sequence_name:
            raise ValueError("Use either name or sequence_name, not both")
        if sequence_name:
            return str(sequence_name)
        if not name:
            return self._default_visualization_name(source_dataset, variables)
        if "/" in str(name):
            return str(name)
        root = source_dataset or variables[0]["source_dataset"] or "visualization"
        return f"{root}/visualizations/{name}"

    def _resolve_visualization_image_names(
        self,
        image_inputs,
        sequence_name: str,
        image_names,
        steps,
        image_format: str | None,
    ) -> list[str]:
        if image_names is not None:
            if isinstance(image_names, (str, Path)):
                names = [str(image_names)]
            else:
                names = [str(entry) for entry in image_names]
            if len(names) != len(image_inputs):
                raise ValueError("image_names length must match number of images")
            return names

        if steps is not None:
            step_values = [int(step) for step in steps]
            if len(step_values) != len(image_inputs):
                raise ValueError("steps length must match number of images")
        else:
            step_values = list(range(len(image_inputs)))

        generated: list[str] = []
        for step, image_input in zip(step_values, image_inputs, strict=True):
            suffix = self._guess_image_suffix(image_input, image_format)
            generated.append(f"{sequence_name}/image.{step:06d}{suffix}")
        return generated

    def _guess_image_suffix(self, image_input, image_format: str | None) -> str:
        if isinstance(image_input, Path):
            suffix = image_input.suffix
            if suffix:
                return suffix
        if isinstance(image_input, str):
            suffix = Path(image_input).suffix
            if suffix:
                return suffix
        if image_format:
            return "." + image_format.lower().lstrip(".")
        if isinstance(image_input, (bytes, bytearray, memoryview)):
            data = bytes(image_input)
            inferred = self._infer_image_format(data)
            if inferred == "JPEG":
                return ".jpg"
            if inferred:
                return "." + inferred.lower()
        return ".png"


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
