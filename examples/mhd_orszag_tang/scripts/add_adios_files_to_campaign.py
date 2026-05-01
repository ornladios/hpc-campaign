#!/usr/bin/env python3
"""
Create or update a campaign with only ADIOS/BP datasets.

This is the first step in the intended notebook/script workflow:

  1. Add simulation output BP files to the campaign.
  2. Run analysis/rendering separately and add visualization image bytes with
     the hpc-campaign visualization API.

No rendered images or visualization metadata are added by this script.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


def _import_hpc_campaign():
    """Prefer the local hpc-campaign checkout so new API branches are tested."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[2]
    candidate_checkouts = [
        repo_root,
        Path("~/proj/hpc_campaign/hpc-campaign").expanduser(),
    ]

    local_checkout = next((path for path in candidate_checkouts if path.exists()), None)
    if local_checkout is not None:
        local_checkout_str = str(local_checkout)
        if local_checkout_str in sys.path:
            sys.path.remove(local_checkout_str)
        sys.path.insert(0, local_checkout_str)

        existing = sys.modules.get("hpc_campaign")
        existing_file = getattr(existing, "__file__", "") if existing is not None else ""
        existing_path = getattr(existing, "__path__", []) if existing is not None else []
        existing_locations = [str(entry) for entry in existing_path]
        expected_package_dir = str(local_checkout / "hpc_campaign")
        if (
            existing is not None
            and expected_package_dir not in existing_locations
            and not str(existing_file).startswith(expected_package_dir)
        ):
            sys.modules.pop("hpc_campaign", None)
            stale_submodules = [name for name in sys.modules if name.startswith("hpc_campaign.")]
            for name in stale_submodules:
                sys.modules.pop(name, None)

    importlib.invalidate_caches()

    try:
        from hpc_campaign.info import format_info
        from hpc_campaign.manager import Manager

        return Manager, format_info
    except ModuleNotFoundError as exc:
        if exc.name == "adios2":
            raise SystemExit(
                "The hpc-campaign Python API requires the 'adios2' Python package in this environment."
            ) from exc
        if exc.name in {"hpc_campaign", "hpc_campaign.info", "hpc_campaign.manager"}:
            raise SystemExit(
                f"Could not import hpc-campaign from local checkout candidates: "
                f"{', '.join(str(path) for path in candidate_checkouts)}."
            ) from exc
        raise


@contextmanager
def pushd(path: Path) -> Iterator[None]:
    """Temporarily make dataset replica paths relative to the scan root."""
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def rel_posix(path: Path, root: Path) -> str:
    """Return a stable campaign dataset name relative to the scan root."""
    return path.relative_to(root).as_posix()


def split_csv(values: list[str]) -> list[str]:
    """Parse repeated comma-separated CLI values while preserving order."""
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item and item not in seen:
                output.append(item)
                seen.add(item)
    return output


def find_adios_datasets(out_root: Path, bp_names: list[str]) -> list[Path]:
    """Find all requested BP dataset names under out_root."""
    hits: set[Path] = set()
    for bp_name in bp_names:
        for path in out_root.rglob(bp_name):
            if path.exists():
                hits.add(path)
    return sorted(hits)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add only ADIOS/BP files to a campaign.")
    parser.add_argument("outRoot", type=Path, help="Output root to scan.")
    parser.add_argument("--archive", required=True, help="Campaign archive name/path.")
    parser.add_argument("--campaign_store", default="", help="Campaign store path.")
    parser.add_argument(
        "--bpName",
        action="append",
        default=["output.bp"],
        help=("BP dataset name to add. Can be repeated or comma-separated. Default: output.bp"),
    )
    parser.add_argument("--recreate", action="store_true", help="Truncate the existing campaign first.")
    parser.add_argument("--dryRun", action="store_true", help="Print datasets that would be added.")
    parser.add_argument("--showInfo", action="store_true", help="Print hpc-campaign info after writing.")
    return parser.parse_args()


def print_plan(archive: str, out_root: Path, bp_names: list[str], datasets: list[Path]) -> None:
    print(f"[info] outRoot  : {out_root}")
    print(f"[info] archive  : {archive}")
    print(f"[info] bpName   : {bp_names}")
    print(f"[info] datasets : {len(datasets)}")


def add_datasets(manager: Any, datasets: list[Path], out_root: Path, dry_run: bool) -> int:
    """Add ADIOS datasets with logical names matching their relative paths."""
    added = 0
    seen: set[str] = set()

    with pushd(out_root):
        for dataset_path in datasets:
            rel_dataset = rel_posix(dataset_path, out_root)
            if rel_dataset in seen:
                continue
            seen.add(rel_dataset)

            print(f"[info] + dataset: {rel_dataset}")
            if not dry_run:
                manager.data(rel_dataset, name=rel_dataset)
            added += 1

    return added


def main() -> int:
    args = parse_args()

    out_root = args.outRoot.expanduser().resolve()
    if not out_root.exists():
        raise SystemExit(f"Output root does not exist: {out_root}")

    bp_names = split_csv(args.bpName)
    if not bp_names:
        raise SystemExit("At least one --bpName is required.")

    datasets = find_adios_datasets(out_root, bp_names)
    if not datasets:
        raise SystemExit(f"No BP datasets named {bp_names} found under {out_root}")

    print_plan(args.archive, out_root, bp_names, datasets)

    manager = None
    format_info_fn = None
    if not args.dryRun:
        manager_class, format_info_fn = _import_hpc_campaign()
        manager = manager_class(archive=args.archive, campaign_store=args.campaign_store)
        manager.open(create=True, truncate=args.recreate)

    try:
        count = add_datasets(manager, datasets, out_root, args.dryRun)
    finally:
        if manager is not None:
            manager.close()

    print(f"[ok] done. datasets={count}")

    if args.showInfo and not args.dryRun:
        manager_class, format_info_fn = _import_hpc_campaign()
        info_manager = manager_class(archive=args.archive, campaign_store=args.campaign_store)
        print(format_info_fn(info_manager.info(list_replicas=False, list_files=False)))
        info_manager.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
