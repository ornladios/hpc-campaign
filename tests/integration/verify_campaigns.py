#!/usr/bin/env python3

import argparse
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import adios2

from tests.integration.create_campaigns import CAMPAIGN_NAMES, DATASET_PATHS

EXPECTED_REMOTE_HOSTS = {
    "https": ("docker-https", "https-service.docker.hpc-campaign"),
    "s3": ("docker-rustfs", "s3-service.docker.hpc-campaign"),
    "ssh": ("docker-ssh", "ssh-service.docker.hpc-campaign"),
    "xrootd": ("docker-xrootd", "xrootd-service.docker.hpc-campaign"),
}


def summarize_failure(output: str, returncode: int) -> str:
    """Return a one-line exception summary from captured child output."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for index in range(len(lines) - 1, -1, -1):
        if "[ADIOS2 EXCEPTION]" in lines[index]:
            return " ".join(lines[index:])
    if lines:
        return lines[-1]
    return f"child process exited with status {returncode}"


def campaign_dataset_name(selection: str) -> str:
    """Return the catalog dataset containing a dataset or variable selection."""
    for dataset_path in sorted(DATASET_PATHS, key=len, reverse=True):
        if selection == dataset_path or selection.startswith(f"{dataset_path}/"):
            return dataset_path
    raise ValueError(f"Unknown integration dataset or variable: {selection}")


def verify_metadata(campaign_path: Path) -> None:
    with sqlite3.connect(campaign_path) as connection:
        datasets = connection.execute("select name, fileformat from dataset where deltime = 0 order by name").fetchall()
        replicas = connection.execute("select count(*) from replica where deltime = 0").fetchone()
        live_hosts = set(
            connection.execute(
                "select distinct h.hostname, h.longhostname "
                "from replica r join host h on h.rowid = r.hostid "
                "where r.deltime = 0"
            ).fetchall()
        )
    assert {name for name, _format in datasets} == set(DATASET_PATHS)
    assert {file_format for _name, file_format in datasets} == {
        "ADIOS",
        "HDF5",
        "IMAGE",
        "TEXT",
    }
    assert replicas is not None and replicas[0] == len(DATASET_PATHS)

    protocol = campaign_path.name.split("-", 2)[1]
    expected_host = EXPECTED_REMOTE_HOSTS.get(protocol)
    if expected_host is not None:
        assert live_hosts == {expected_host}


def verify_campaign(
    campaign_path: Path,
    dataset_paths: tuple[str, ...] = DATASET_PATHS,
    include_pattern: str | None = None,
) -> None:
    verify_metadata(campaign_path)
    adios = adios2.Adios()
    io = adios.declare_io(f"verify-{campaign_path.stem}")
    if include_pattern is not None:
        io.set_parameter("include-dataset", include_pattern)

    with adios2.FileReader(io, str(campaign_path)) as reader:
        variables = reader.available_variables()
        if not variables:
            raise AssertionError(f"Campaign has no readable variables: {campaign_path.name}")
        variables_read = []
        for dataset_path in dataset_paths:
            if dataset_path in variables:
                dataset_variables = [dataset_path]
            else:
                dataset_variables = [name for name in variables if name.startswith(f"{dataset_path}/")]
            if not dataset_variables:
                raise AssertionError(f"Campaign has no variable for {dataset_path}: {campaign_path.name}")
            reader.read(dataset_variables[0])
            variables_read.append(dataset_variables[0])
    print(
        f"PASS {campaign_path.name}: {', '.join(dataset_paths)}; "
        f"{len(variables)} variables; read {len(variables_read)} datasets"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read every Docker integration campaign")
    parser.add_argument("--campaign-store", type=Path, default=Path("/campaigns"))
    parser.add_argument(
        "--single-campaign",
        metavar="CAMPAIGN",
        help="verify only CAMPAIGN instead of every campaign",
    )
    parser.add_argument(
        "--single-dataset",
        metavar="DATASET",
        help="read only DATASET or variable from each selected campaign",
    )
    parser.add_argument("--read-timeout", type=float, default=15)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.single_campaign and args.single_dataset:
        include_dataset = campaign_dataset_name(args.single_dataset)
        verify_campaign(
            args.campaign_store / args.single_campaign,
            (args.single_dataset,),
            rf"^{re.escape(include_dataset)}$",
        )
        return

    campaign_names = (args.single_campaign,) if args.single_campaign else CAMPAIGN_NAMES
    dataset_paths = (args.single_dataset,) if args.single_dataset else DATASET_PATHS

    failures: list[tuple[str, str, str]] = []
    for name in campaign_names:
        campaign_path = args.campaign_store / name
        verify_metadata(campaign_path)
        for dataset_path in dataset_paths:
            try:
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "tests.integration.verify_campaigns",
                        "--campaign-store",
                        str(args.campaign_store),
                        "--single-campaign",
                        name,
                        "--single-dataset",
                        dataset_path,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=args.read_timeout,
                )
            except subprocess.TimeoutExpired:
                message = f"timed out after {args.read_timeout:g} seconds"
                failures.append((name, dataset_path, message))
                print(f"FAIL {name}: {dataset_path}: {message}")
                continue

            if result.returncode == 0:
                print(result.stdout.strip())
                continue

            output = result.stderr or result.stdout
            message = summarize_failure(output, result.returncode)
            failures.append((name, dataset_path, message))
            print(f"FAIL {name}: {dataset_path}: {message}")

    total = len(campaign_names) * len(dataset_paths)
    if failures:
        print(f"\n{total - len(failures)} reads passed; {len(failures)} of {total} reads failed:")
        for campaign_name, dataset_path, message in failures:
            print(f"  {campaign_name}: {dataset_path}: {message}")
        raise SystemExit(1)
    print(f"\nAll {total} reads passed")


if __name__ == "__main__":
    main()
