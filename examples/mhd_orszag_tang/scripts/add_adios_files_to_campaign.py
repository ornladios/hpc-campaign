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


def is_stats_dataset(dataset_path: Path) -> bool:
    """Return True for derived stats datasets named *_stats.<suffix>."""
    return dataset_path.stem.endswith("_stats")


def stats_output_path(input_path: Path) -> Path:
    """Return the sibling stats path used by scripts/adios_stats.py."""
    if input_path.suffix:
        return input_path.with_name(f"{input_path.stem}_stats{input_path.suffix}")
    return input_path.with_name(f"{input_path.name}_stats")


def derived_output_path(input_path: Path, output_name: str) -> Path:
    """Return the sibling derived-analysis BP path."""
    return input_path.with_name(output_name)


def _import_adios_stats():
    """Import the local stats processor only when stats generation is requested."""
    script_dir = Path(__file__).resolve().parent
    script_dir_str = str(script_dir)
    if script_dir_str not in sys.path:
        sys.path.insert(0, script_dir_str)

    from adios_stats import processFile  # pylint: disable=import-outside-toplevel

    return processFile


def _import_adios_derived_variables():
    """Import the local derived-variable processor only when requested."""
    script_dir = Path(__file__).resolve().parent
    script_dir_str = str(script_dir)
    if script_dir_str not in sys.path:
        sys.path.insert(0, script_dir_str)

    from adios_derived_variables import processFile  # pylint: disable=import-outside-toplevel

    return processFile


def add_derived_datasets(
    datasets: list[Path],
    derived_bp_name: str,
    force_derived: bool,
    dry_run: bool,
) -> list[Path]:
    """Generate/reuse sibling analysis BP files and return primary plus analysis datasets."""
    expanded: list[Path] = []
    process_file = None

    for dataset_path in datasets:
        expanded.append(dataset_path)

        if is_stats_dataset(dataset_path):
            print(f"[info] derived: skipping stats input {dataset_path}")
            continue
        if dataset_path.name == derived_bp_name:
            print(f"[info] derived: skipping derived input {dataset_path}")
            continue

        output_path = derived_output_path(dataset_path, derived_bp_name)
        should_generate = force_derived or not output_path.exists()

        if should_generate:
            if dry_run:
                action = "regenerate" if output_path.exists() and force_derived else "generate"
                print(f"[dry-run] derived: would {action} {output_path}")
                expanded.append(output_path)
            else:
                if process_file is None:
                    process_file = _import_adios_derived_variables()
                action = "regenerating" if output_path.exists() and force_derived else "generating"
                print(f"[info] derived: {action} {output_path}")
                written_steps, written_vars = process_file(dataset_path, output_path)
                if output_path.exists():
                    expanded.append(output_path)
                    print(f"[info] derived: wrote steps={written_steps} variables={len(written_vars)}")
                else:
                    print(f"[info] derived: no output written for {dataset_path}")
        else:
            print(f"[info] derived: reusing {output_path}")
            expanded.append(output_path)

    return sorted(dict.fromkeys(expanded))


def add_stats_datasets(datasets: list[Path], force_stats: bool, dry_run: bool) -> list[Path]:
    """Generate/reuse sibling stats BP files and return primary plus stats datasets."""
    expanded: list[Path] = []
    process_file = None

    for dataset_path in datasets:
        expanded.append(dataset_path)

        if is_stats_dataset(dataset_path):
            print(f"[info] stats: skipping stats input {dataset_path}")
            continue

        output_path = stats_output_path(dataset_path)
        should_generate = force_stats or not output_path.exists()

        if should_generate:
            if dry_run:
                action = "regenerate" if output_path.exists() and force_stats else "generate"
                print(f"[dry-run] stats: would {action} {output_path}")
            else:
                if process_file is None:
                    process_file = _import_adios_stats()
                action = "regenerating" if output_path.exists() and force_stats else "generating"
                print(f"[info] stats: {action} {output_path}")
                process_file(dataset_path, output_path)
        else:
            print(f"[info] stats: reusing {output_path}")

        expanded.append(output_path)

    return sorted(dict.fromkeys(expanded))


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
    parser.add_argument(
        "--withStats",
        action="store_true",
        help="Generate or reuse sibling *_stats BP files and add them to the campaign.",
    )
    parser.add_argument(
        "--forceStats",
        action="store_true",
        help="Regenerate stats BP files when --withStats is set, replacing existing outputs.",
    )
    parser.add_argument(
        "--withDerived",
        action="store_true",
        help="Generate or reuse sibling derived-analysis BP files and add them to the campaign.",
    )
    parser.add_argument(
        "--forceDerived",
        action="store_true",
        help="Regenerate derived-analysis BP files when --withDerived is set, replacing existing outputs.",
    )
    parser.add_argument(
        "--derivedBpName",
        default="analysis.bp",
        help="Sibling BP filename for derived variables. Default: analysis.bp",
    )
    return parser.parse_args()


def print_plan(
    archive: str,
    out_root: Path,
    bp_names: list[str],
    datasets: list[Path],
    with_stats: bool,
    force_stats: bool,
    with_derived: bool,
    force_derived: bool,
    derived_bp_name: str,
) -> None:
    print(f"[info] outRoot  : {out_root}")
    print(f"[info] archive  : {archive}")
    print(f"[info] bpName   : {bp_names}")
    print(f"[info] datasets : {len(datasets)}")
    print(f"[info] derived : {'enabled' if with_derived else 'disabled'}")
    if with_derived:
        print(f"[info] derived name : {derived_bp_name}")
        print(f"[info] derived force: {force_derived}")
    print(f"[info] stats    : {'enabled' if with_stats else 'disabled'}")
    if with_stats:
        print(f"[info] force    : {force_stats}")


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

    print_plan(
        args.archive,
        out_root,
        bp_names,
        datasets,
        args.withStats,
        args.forceStats,
        args.withDerived,
        args.forceDerived,
        args.derivedBpName,
    )

    if args.forceStats and not args.withStats:
        raise SystemExit("--forceStats requires --withStats.")
    if args.forceDerived and not args.withDerived:
        raise SystemExit("--forceDerived requires --withDerived.")

    if args.withDerived:
        datasets = add_derived_datasets(datasets, args.derivedBpName, args.forceDerived, args.dryRun)
        print(f"[info] datasets with derived: {len(datasets)}")

    if args.withStats:
        datasets = add_stats_datasets(datasets, args.forceStats, args.dryRun)
        print(f"[info] datasets with stats: {len(datasets)}")

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
