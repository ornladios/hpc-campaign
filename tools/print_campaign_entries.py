#!/usr/bin/env python3
"""Print campaign entries and image association metadata."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print campaign entries. Images include visualization association data."
    )
    parser.add_argument(
        "campaign",
        help="Campaign archive name or path to a .aca file.",
    )
    parser.add_argument(
        "--campaign-store",
        default="",
        help="Campaign store directory when CAMPAIGN is not a full path.",
    )
    parser.add_argument(
        "--show-deleted",
        action="store_true",
        help="Include deleted entries.",
    )
    return parser.parse_args()


def resolve_archive(campaign: str, campaign_store: str) -> tuple[str, str]:
    candidate = Path(campaign).expanduser()
    if candidate.exists():
        archive_path = candidate.resolve()
        if archive_path.suffix != ".aca":
            raise SystemExit(f"Expected a .aca file path, got: {archive_path}")
        return archive_path.stem, str(archive_path.parent)
    return campaign, campaign_store


def collect_datasets(info_data) -> list:
    datasets: list = []
    for ts_info in info_data.time_series.values():
        datasets.extend(ts_info.datasets.values())
    datasets.extend(info_data.datasets.values())
    datasets.sort(key=lambda dataset: (dataset.name, dataset.file_format, dataset.uuid))
    return datasets


def image_associations(
    image_name: str,
    sequences: list,
) -> list[tuple[object, bool]]:
    matches: list[tuple[object, bool]] = []
    for sequence in sequences:
        is_thumbnail = sequence.thumbnail_dataset_name == image_name
        in_sequence = any(item.dataset_name == image_name for item in sequence.items if item.dataset_name is not None)
        if is_thumbnail or in_sequence:
            matches.append((sequence, is_thumbnail))
    matches.sort(key=lambda item: item[0].name)
    return matches


def format_metadata(metadata: str | None) -> str | None:
    if not metadata:
        return None
    try:
        return json.dumps(json.loads(metadata), sort_keys=True)
    except json.JSONDecodeError:
        return metadata


def main() -> None:
    args = parse_args()
    from hpc_campaign import Manager

    archive, campaign_store = resolve_archive(args.campaign, args.campaign_store)

    manager = Manager(archive=archive, campaign_store=campaign_store)
    manager.open(create=False, truncate=False)
    try:
        info_data = manager.info(show_deleted=args.show_deleted)
    finally:
        manager.close()

    sequences = list(info_data.visualization_sequences.values())
    datasets = collect_datasets(info_data)

    for dataset in datasets:
        print(f"{dataset.name}: {dataset.file_format}")
        if dataset.file_format != "IMAGE":
            continue

        associations = image_associations(dataset.name, sequences)
        if not associations:
            print("  associations: none")
            continue

        for sequence, is_thumbnail in associations:
            print(f"  sequence: {sequence.name}")
            print(f"    vis_type: {sequence.vis_type}")
            if is_thumbnail:
                print("    thumbnail: true")
            if sequence.variables:
                source_names: list[str] = []
                for variable in sequence.variables:
                    if variable.source_dataset_name not in source_names:
                        source_names.append(variable.source_dataset_name)
                sources = ", ".join(source_names)
                print(f"    sources: {sources}")
                variables = ", ".join(
                    f"{variable.role}={variable.name}@{variable.source_dataset_name}" for variable in sequence.variables
                )
                print(f"    variables: {variables}")
            item_uuids = ", ".join(item.item_uuid for item in sequence.items if item.dataset_name == dataset.name)
            if item_uuids:
                print(f"    item_uuid: {item_uuids}")
            metadata = format_metadata(sequence.metadata)
            if metadata:
                print(f"    metadata: {metadata}")


if __name__ == "__main__":
    main()
