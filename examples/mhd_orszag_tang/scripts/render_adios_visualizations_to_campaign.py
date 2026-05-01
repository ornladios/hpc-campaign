#!/usr/bin/env python3
"""
Render single-variable ADIOS visualizations directly into a campaign.

The script opens ADIOS/BP datasets already registered in a campaign, reads one
or more variables, renders simple PNG images in memory, and stores those image
bytes with Manager.visualization(). It does not write intermediate PNG files to
disk.
"""

from __future__ import annotations

import argparse
import fnmatch
import importlib
import io
import os
import sys
import tempfile
from collections import deque
from pathlib import Path
from typing import Any

import numpy as np


class MissingVariableError(RuntimeError):
    """Raised when a requested ADIOS variable is not present in a dataset."""


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
        if existing is not None and expected_package_dir not in existing_locations and not str(existing_file).startswith(
            expected_package_dir
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


def import_runtime_modules():
    """Import ADIOS2 and matplotlib using a non-GUI backend."""
    try:
        import adios2
    except ImportError as exc:
        raise SystemExit("This script requires the Python package 'adios2'.") from exc

    try:
        cache_root = Path(tempfile.gettempdir()) / "hpc_campaign_visualization_cache"
        cache_root.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("MPLCONFIGDIR", str(cache_root / "matplotlib"))
        os.environ.setdefault("XDG_CACHE_HOME", str(cache_root / "xdg"))

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit("This script requires matplotlib to render PNG images.") from exc

    return adios2, plt


def split_csv(values: list[str] | None) -> list[str]:
    """Parse repeated comma-separated CLI values while preserving order."""
    if not values:
        return []

    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item and item not in seen:
                output.append(item)
                seen.add(item)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render single-variable ADIOS visualizations directly into an existing campaign."
    )
    parser.add_argument("--archive", required=True, help="Campaign archive name/path.")
    parser.add_argument("--campaign_store", default="", help="Campaign store path.")
    parser.add_argument(
        "--dataset",
        action="append",
        default=None,
        help="Exact campaign ADIOS dataset name. Can be repeated or comma-separated.",
    )
    parser.add_argument(
        "--datasetPattern",
        action="append",
        default=None,
        help="fnmatch pattern for campaign ADIOS dataset names, e.g. '*/output.bp'.",
    )
    parser.add_argument("--allDatasets", action="store_true", help="Render all ADIOS datasets in the campaign.")
    variable_group = parser.add_mutually_exclusive_group()
    variable_group.add_argument(
        "--variable",
        action="append",
        help="ADIOS variable to render. Can be repeated or comma-separated.",
    )
    variable_group.add_argument(
        "--allVariables",
        action="store_true",
        help="Render every ADIOS variable present in each selected dataset.",
    )
    parser.add_argument(
        "--data_root",
        type=Path,
        default=None,
        help="Root used to resolve relative BP replica paths stored in the campaign.",
    )
    parser.add_argument("--step", type=int, default=-1, help="Step to render. Negative means from the end.")
    parser.add_argument("--allSteps", action="store_true", help="Render every ADIOS step instead of --step.")
    parser.add_argument("--visType", default="heatmap", help="Visualization kind recorded in the campaign.")
    parser.add_argument("--cmap", default="viridis", help="matplotlib colormap for 2D fields.")
    parser.add_argument("--dpi", type=int, default=150, help="Output image DPI.")
    parser.add_argument("--figureWidth", type=float, default=6.0, help="Figure width in inches.")
    parser.add_argument("--figureHeight", type=float, default=5.0, help="Figure height in inches.")
    parser.add_argument(
        "--name",
        default=None,
        help=(
            "Short visualization name for one variable, expanded to "
            "<dataset>/visualizations/<name>. If omitted, defaults to "
            "<variable>_<visType>."
        ),
    )
    parser.add_argument("--replace", action="store_true", help="Replace existing visualization sequences.")
    parser.add_argument(
        "--thumbnailStep",
        type=int,
        default=0,
        help="Index within each rendered image list to use as the visualization thumbnail.",
    )
    parser.add_argument("--skipMissing", action="store_true", help="Skip datasets missing a requested variable.")
    parser.add_argument("--dryRun", action="store_true", help="Print selected work without reading or writing images.")
    parser.add_argument("--listDatasets", action="store_true", help="Print matching campaign datasets and exit.")
    parser.add_argument("--showInfo", action="store_true", help="Print hpc-campaign info after writing.")
    return parser.parse_args()


def build_directory_map(info_data) -> dict[int, str]:
    """Build a lookup from hpc-campaign directory id to directory path."""
    directories: dict[int, str] = {}
    for host in info_data.hosts:
        for directory in host.directories:
            directories[directory.id] = directory.name
    return directories


def live_adios_datasets(info_data) -> list[Any]:
    """Return live ADIOS datasets sorted by campaign dataset name."""
    datasets = []
    for dataset in info_data.datasets.values():
        if dataset.del_time == 0 and dataset.file_format == "ADIOS":
            datasets.append(dataset)
    return sorted(datasets, key=lambda item: item.name)


def select_datasets(
    info_data,
    exact_names: list[str],
    patterns: list[str],
    all_datasets: bool,
) -> list[Any]:
    """Select campaign ADIOS datasets by exact name, pattern, or all-datasets mode."""
    available = live_adios_datasets(info_data)
    if all_datasets:
        return available

    selected: list[Any] = []
    seen: set[str] = set()
    by_name = {dataset.name: dataset for dataset in available}

    for name in exact_names:
        dataset = by_name.get(name)
        if dataset is None:
            raise SystemExit(f"ADIOS dataset not found in campaign: {name}")
        if dataset.name not in seen:
            selected.append(dataset)
            seen.add(dataset.name)

    for pattern in patterns:
        for dataset in available:
            if fnmatch.fnmatch(dataset.name, pattern) and dataset.name not in seen:
                selected.append(dataset)
                seen.add(dataset.name)

    if not selected:
        raise SystemExit("Select at least one dataset with --dataset, --datasetPattern, or --allDatasets.")

    return selected


def list_dataset_names(datasets: list[Any]) -> None:
    for dataset in datasets:
        print(dataset.name)


def candidate_replica_paths(dataset, data_root: Path | None, directory_map: dict[int, str]) -> list[Path]:
    """
    Generate possible local filesystem paths for a dataset replica.

    For campaigns created from a scan root, data_root / replica.name is usually
    the path that resolves.
    """
    candidates: list[Path] = []
    for replica in dataset.replicas.values():
        if replica.del_time != 0 or replica.flags.deleted or replica.flags.embedded:
            continue

        replica_path = Path(replica.name)
        if replica_path.is_absolute():
            candidates.append(replica_path)
        if data_root is not None:
            candidates.append(data_root / replica.name)

        directory_name = directory_map.get(replica.dir_id)
        if directory_name:
            directory_path = Path(directory_name)
            if directory_path.is_absolute():
                candidates.append(directory_path / replica.name)

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            deduped.append(candidate)
            seen.add(key)
    return deduped


def resolve_dataset_path(dataset, data_root: Path | None, directory_map: dict[int, str]) -> Path:
    """Return the first existing local path for a campaign ADIOS dataset."""
    candidates = candidate_replica_paths(dataset, data_root, directory_map)
    for candidate in candidates:
        if candidate.exists():
            return candidate

    candidate_text = "\n".join(f"  {path}" for path in candidates) or "  <none>"
    raise SystemExit(
        f"Could not resolve a local path for dataset {dataset.name!r}. Tried:\n"
        f"{candidate_text}\n"
        "Pass --data_root pointing at the root used when the campaign was created."
    )


def normalize_step_selection(step: int, nsteps: int) -> int:
    """Translate a possibly negative step index into a valid ADIOS step."""
    resolved = step if step >= 0 else nsteps + step
    if resolved < 0 or resolved >= nsteps:
        raise SystemExit(f"Invalid step {step}; available range is [0, {nsteps - 1}]")
    return resolved


def read_stream_arrays(adios2, bp_path: Path, variable: str, step: int, all_steps: bool) -> tuple[list[int], list[np.ndarray]]:
    """
    Read ADIOS arrays using adios2.Stream, matching the existing MHD renderer.

    For a single non-negative step, the stream stops after that frame. For a
    negative step, only the needed trailing frames are kept in memory.
    """
    steps: list[int] = []
    arrays: list[np.ndarray] = []
    trailing: deque[tuple[int, np.ndarray]] = deque(maxlen=max(1, abs(step)))
    saw_any_step = False

    with adios2.Stream(str(bp_path), "r") as stream:
        for frame_index, _ in enumerate(stream.steps()):
            saw_any_step = True
            available = set((stream.available_variables() or {}).keys())
            if variable not in available:
                preview = ", ".join(sorted(available)[:20])
                raise MissingVariableError(
                    f"Variable {variable!r} not found in {bp_path} at step {frame_index}. "
                    f"Available variables include: {preview}"
                )

            if all_steps:
                steps.append(frame_index)
                arrays.append(np.asarray(stream.read(variable)).squeeze())
                continue

            if step >= 0:
                if frame_index == step:
                    steps.append(frame_index)
                    arrays.append(np.asarray(stream.read(variable)).squeeze())
                    break
                continue

            trailing.append((frame_index, np.asarray(stream.read(variable)).squeeze()))

    if not saw_any_step:
        raise SystemExit(f"No steps found in ADIOS file: {bp_path}")

    if not all_steps and step < 0:
        if len(trailing) < abs(step):
            raise SystemExit(f"Invalid step {step}; available steps={len(trailing)} or fewer were found")
        selected_step, selected_array = trailing[0]
        steps = [selected_step]
        arrays = [selected_array]

    if not arrays:
        raise SystemExit(f"Step {step} was not found in ADIOS file: {bp_path}")

    return steps, arrays


def reader_num_steps(reader) -> int:
    """Return the number of ADIOS steps for FileReader-like APIs."""
    try:
        return int(reader.steps())
    except AttributeError:
        return int(reader.num_steps())


def read_variable_step(reader, variable: str, step: int) -> np.ndarray:
    """Read one variable at one step with ADIOS2 signature compatibility."""
    try:
        data = reader.read(variable, step_selection=[step, 1])
    except TypeError:
        data = reader.read(variable, start=[], count=[], step_selection=[step, 1])
    return np.asarray(data).squeeze()


def read_filereader_arrays(
    adios2,
    bp_path: Path,
    variable: str,
    step: int,
    all_steps: bool,
) -> tuple[list[int], list[np.ndarray]]:
    """Fallback reader for ADIOS2 builds without Stream."""
    with adios2.FileReader(str(bp_path)) as reader:
        available = set((reader.available_variables() or {}).keys())
        if variable not in available:
            preview = ", ".join(sorted(available)[:20])
            raise MissingVariableError(
                f"Variable {variable!r} not found in {bp_path}. Available variables include: {preview}"
            )

        nsteps = reader_num_steps(reader)
        if nsteps <= 0:
            raise SystemExit(f"No steps found in ADIOS file: {bp_path}")

        steps = list(range(nsteps)) if all_steps else [normalize_step_selection(step, nsteps)]
        arrays = [read_variable_step(reader, variable, step_index) for step_index in steps]
    return steps, arrays


def read_variable_arrays(bp_path: Path, variable: str, step: int, all_steps: bool) -> tuple[list[int], list[np.ndarray]]:
    """Read one ADIOS variable from one dataset."""
    adios2, _ = import_runtime_modules()
    if hasattr(adios2, "Stream"):
        return read_stream_arrays(adios2, bp_path, variable, step, all_steps)
    if hasattr(adios2, "FileReader"):
        return read_filereader_arrays(adios2, bp_path, variable, step, all_steps)
    raise SystemExit("This ADIOS2 Python module exposes neither Stream nor FileReader.")


def discover_stream_variables(adios2, bp_path: Path) -> list[str]:
    """Return variable names from the first ADIOS step using Stream."""
    with adios2.Stream(str(bp_path), "r") as stream:
        for _, _step in enumerate(stream.steps()):
            return sorted((stream.available_variables() or {}).keys())
    raise SystemExit(f"No steps found in ADIOS file: {bp_path}")


def discover_filereader_variables(adios2, bp_path: Path) -> list[str]:
    """Return variable names using the FileReader metadata path."""
    with adios2.FileReader(str(bp_path)) as reader:
        if reader_num_steps(reader) <= 0:
            raise SystemExit(f"No steps found in ADIOS file: {bp_path}")
        return sorted((reader.available_variables() or {}).keys())


def discover_variables(bp_path: Path) -> list[str]:
    """Discover available ADIOS variables for one dataset."""
    adios2, _ = import_runtime_modules()
    if hasattr(adios2, "Stream"):
        return discover_stream_variables(adios2, bp_path)
    if hasattr(adios2, "FileReader"):
        return discover_filereader_variables(adios2, bp_path)
    raise SystemExit("This ADIOS2 Python module exposes neither Stream nor FileReader.")


def render_array_to_png_bytes(
    array: np.ndarray,
    title: str,
    cmap: str,
    dpi: int,
    figure_size: tuple[float, float],
    plt,
) -> bytes:
    """
    Render scalar, 1D, 2D, or higher-dimensional data to PNG bytes.

    Higher-dimensional arrays use the first leading slice until a 2D image
    remains. This is intentionally simple for the first single-variable pass.
    """
    arr = np.asarray(array).squeeze()
    fig, ax = plt.subplots(figsize=figure_size, constrained_layout=True)

    if arr.ndim == 0:
        ax.text(0.5, 0.5, f"{float(arr):.6g}", ha="center", va="center", fontsize=18)
        ax.set_axis_off()
    elif arr.ndim == 1:
        ax.plot(np.arange(arr.shape[0]), arr)
        ax.set_xlabel("Index")
        ax.set_ylabel("Value")
    else:
        while arr.ndim > 2:
            arr = arr[0]
        image = ax.imshow(arr, origin="lower", cmap=cmap, aspect="auto")
        fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
        ax.set_xticks([])
        ax.set_yticks([])

    ax.set_title(title)
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=dpi)
    plt.close(fig)
    return buffer.getvalue()


def render_scalar_timeseries_to_png_bytes(
    steps: list[int],
    arrays: list[np.ndarray],
    title: str,
    dpi: int,
    figure_size: tuple[float, float],
    plt,
) -> bytes:
    """Render one scalar variable across all steps as a time-series PNG."""
    values = [float(np.asarray(array).squeeze()) for array in arrays]
    fig, ax = plt.subplots(figsize=figure_size, constrained_layout=True)
    ax.plot(steps, values, marker="o")
    ax.set_xlabel("Step")
    ax.set_ylabel("Value")
    ax.set_title(title)
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=dpi)
    plt.close(fig)
    return buffer.getvalue()


def render_images(
    dataset_name: str,
    variable: str,
    bp_path: Path,
    step: int,
    all_steps: bool,
    vis_type: str,
    cmap: str,
    dpi: int,
    figure_size: tuple[float, float],
) -> tuple[list[int], list[bytes], str]:
    """Read one variable and render each selected step to PNG bytes."""
    _, plt = import_runtime_modules()
    print(f"[info] reading: {bp_path}")
    steps, arrays = read_variable_arrays(bp_path, variable, step, all_steps)

    first_array = np.asarray(arrays[0]).squeeze()
    if first_array.ndim == 0:
        if not all_steps or len(steps) == 1:
            steps, arrays = read_variable_arrays(bp_path, variable, step=0, all_steps=True)
        title = f"{dataset_name}:{variable} over steps"
        image = render_scalar_timeseries_to_png_bytes(steps, arrays, title, dpi, figure_size, plt)
        return steps, [image], "timeseries"

    images: list[bytes] = []
    for step_index, array in zip(steps, arrays):
        title = f"{dataset_name}:{variable} step={step_index}"
        images.append(render_array_to_png_bytes(array, title, cmap, dpi, figure_size, plt))
    return steps, images, vis_type


def visualization_semantic_kwargs(variable: str, vis_type: str) -> dict[str, Any]:
    """Map one variable to the semantic arguments used by Manager.visualization()."""
    if vis_type == "contour":
        return {"contour_by": variable}
    if vis_type in {"line-plot", "timeseries"}:
        return {"x_axis": "step", "y_axis": variable}
    return {"color_by": variable}


def visualization_name(variable: str, vis_type: str, explicit_name: str | None, variable_count: int) -> str:
    """Return a stable visualization name for replace=True reruns."""
    if explicit_name is not None:
        if variable_count > 1:
            raise SystemExit("--name can only be used with one --variable.")
        return explicit_name
    return f"{variable}_{vis_type}".replace("/", "_").replace(" ", "_")


def add_visualization(
    manager,
    dataset_name: str,
    variable: str,
    vis_type: str,
    name: str,
    steps: list[int],
    images: list[bytes],
    replace: bool,
    thumbnail_step: int,
) -> int:
    """Store PNG bytes directly in the campaign with new visualization metadata."""
    if not images:
        raise SystemExit("No images were rendered.")
    if thumbnail_step < 0 or thumbnail_step >= len(images):
        raise SystemExit(f"--thumbnailStep must be in [0, {len(images) - 1}]")

    image_steps = steps if len(steps) == len(images) else None

    return manager.visualization(
        images=images,
        kind=vis_type,
        source_dataset=dataset_name,
        name=name,
        steps=image_steps,
        thumbnail_image=thumbnail_step,
        metadata={"generated_by": Path(__file__).name, "steps": steps},
        replace=replace,
        **visualization_semantic_kwargs(variable, vis_type),
    )


def main() -> int:
    args = parse_args()

    exact_names = split_csv(args.dataset)
    patterns = split_csv(args.datasetPattern)
    variables = split_csv(args.variable)

    data_root = args.data_root.expanduser().resolve() if args.data_root is not None else None
    figure_size = (float(args.figureWidth), float(args.figureHeight))

    manager_class, format_info_fn = _import_hpc_campaign()
    manager = manager_class(archive=args.archive, campaign_store=args.campaign_store)
    manager.open(create=False)

    added = 0
    skipped = 0
    try:
        info_data = manager.info(list_replicas=True, list_files=False)
        if args.listDatasets:
            if exact_names or patterns or args.allDatasets:
                datasets = select_datasets(info_data, exact_names, patterns, args.allDatasets)
            else:
                datasets = live_adios_datasets(info_data)
            list_dataset_names(datasets)
            return 0

        if args.allVariables and args.name is not None:
            raise SystemExit("--name cannot be used with --allVariables.")
        if not variables and not args.allVariables:
            raise SystemExit("Select at least one variable with --variable or use --allVariables.")

        datasets = select_datasets(info_data, exact_names, patterns, args.allDatasets)
        print(f"[info] datasets : {len(datasets)}")
        print(f"[info] variables: {'<all>' if args.allVariables else variables}")
        print(f"[info] visType  : {args.visType}")

        directory_map = build_directory_map(info_data)
        for dataset in datasets:
            bp_path = resolve_dataset_path(dataset, data_root, directory_map)
            print(f"[info] dataset path: {bp_path}")
            dataset_variables = discover_variables(bp_path) if args.allVariables else variables

            if args.dryRun:
                for variable in dataset_variables:
                    name = visualization_name(variable, args.visType, args.name, len(dataset_variables))
                    print(f"[dry-run] {dataset.name} variable={variable} visualization={name}")
                continue

            for variable in dataset_variables:
                try:
                    steps, images, resolved_vis_type = render_images(
                        dataset_name=dataset.name,
                        variable=variable,
                        bp_path=bp_path,
                        step=args.step,
                        all_steps=args.allSteps,
                        vis_type=args.visType,
                        cmap=args.cmap,
                        dpi=args.dpi,
                        figure_size=figure_size,
                    )
                except MissingVariableError as exc:
                    if args.skipMissing:
                        print(f"[warn] skipped: {exc}")
                        skipped += 1
                        continue
                    raise SystemExit(str(exc)) from exc

                name = visualization_name(variable, resolved_vis_type, args.name, len(dataset_variables))

                visid = add_visualization(
                    manager=manager,
                    dataset_name=dataset.name,
                    variable=variable,
                    vis_type=resolved_vis_type,
                    name=name,
                    steps=steps,
                    images=images,
                    replace=args.replace,
                    thumbnail_step=args.thumbnailStep,
                )
                sequence_name = name if "/" in name else f"{dataset.name}/visualizations/{name}"
                print(f"[ok] added visualization visid={visid} name={sequence_name} images={len(images)}")
                added += 1

        if args.showInfo:
            print(format_info_fn(manager.info(list_replicas=False, list_files=False)))
    finally:
        manager.close()

    print(f"[ok] done. visualizations={added} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
