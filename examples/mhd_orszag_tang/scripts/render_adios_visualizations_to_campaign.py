#!/usr/bin/env python3
"""
Render single-variable ADIOS visualizations directly into a campaign.

The script opens ADIOS/BP datasets already registered in a campaign, reads one
or more variables, renders simple PNG images in memory, and stores those image
bytes with Manager.visualization(). It can also write PNG files to disk for
inspection, and optionally register those files as external image replicas.
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

RANGE_ANALYSIS_PERCENTILE_LOW = 2.0
RANGE_ANALYSIS_PERCENTILE_HIGH = 98.0
RANGE_ANALYSIS_SIGNED_ABS_PERCENTILE = 98.0
STREAMLINE_DENSITY = 1.2
STREAMLINE_LINEWIDTH = 0.45
STREAMLINE_ARROWSIZE = 0.6
SIGNED_VARIABLE_NAMES = {
    "mx",
    "my",
    "mz",
    "vx",
    "vy",
    "vz",
    "bx",
    "by",
    "bz",
    "psi",
    "current_z",
    "jx",
    "jy",
    "jz",
    "omega",
    "u",
    "v",
    "w",
    "div_b",
}
STREAMLINE_TARGETS = {
    "velocity_streamlines": {
        "components": ("vx", "vy"),
        "background": "speed",
        "fallback_background": "velocity_mag",
    },
    "magnetic_streamlines": {
        "components": ("bx", "by"),
        "background": "pressure",
        "fallback_background": "bmag",
    },
}
STREAMLINE_ALIASES = {
    "velocity": "velocity_streamlines",
    "velocity_streamline": "velocity_streamlines",
    "velocity_streamlines": "velocity_streamlines",
    "magnetic": "magnetic_streamlines",
    "magnetic_streamline": "magnetic_streamlines",
    "magnetic_streamlines": "magnetic_streamlines",
}


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
        from matplotlib import colors as mcolors
    except ImportError as exc:
        raise SystemExit("This script requires matplotlib to render PNG images.") from exc

    return adios2, plt, mcolors


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
    parser.add_argument("--divergingCmap", default="RdBu_r", help="matplotlib colormap for signed 2D fields.")
    parser.add_argument("--contourLevels", type=int, default=3, help="Number of contour levels for contour views.")
    parser.add_argument(
        "--contourLineWidth",
        type=float,
        default=0.6,
        help="Contour line width for contour and heatmap_contour views.",
    )
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
    parser.add_argument(
        "--imageOutputDir",
        type=Path,
        default=None,
        help=(
            "Optional directory where rendered PNG files are written as "
            "<dataset>/visualizations/<name>/image.<step>.png."
        ),
    )
    parser.add_argument(
        "--externalImages",
        action="store_true",
        help=(
            "Register PNG files from --imageOutputDir as external image replicas "
            "instead of storing in-memory PNG bytes in the campaign."
        ),
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


def read_stream_arrays(
    adios2, bp_path: Path, variable: str, step: int, all_steps: bool
) -> tuple[list[int], list[np.ndarray]]:
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


def read_variable_arrays(
    bp_path: Path, variable: str, step: int, all_steps: bool
) -> tuple[list[int], list[np.ndarray]]:
    """Read one ADIOS variable from one dataset."""
    adios2, _, _ = import_runtime_modules()
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
    adios2, _, _ = import_runtime_modules()
    if hasattr(adios2, "Stream"):
        return discover_stream_variables(adios2, bp_path)
    if hasattr(adios2, "FileReader"):
        return discover_filereader_variables(adios2, bp_path)
    raise SystemExit("This ADIOS2 Python module exposes neither Stream nor FileReader.")


def normalize_vis_type(vis_type: str) -> str:
    normalized = str(vis_type or "heatmap").strip().lower().replace("-", "_")
    if normalized == "streamline":
        return "streamlines"
    return normalized or "heatmap"


def normalize_streamline_target(variable: str) -> str | None:
    """Map user-facing streamline variable names to internal target tokens."""
    token = str(variable or "").strip().lower().replace("-", "_")
    return STREAMLINE_ALIASES.get(token)


def canonical_var_name(name: str) -> str:
    for logical_name in sorted(SIGNED_VARIABLE_NAMES, key=len, reverse=True):
        if name == logical_name or name.endswith(logical_name):
            return logical_name
    return name


def is_signed_variable(name: str) -> bool:
    return canonical_var_name(name) in SIGNED_VARIABLE_NAMES


def to_2d(field: np.ndarray) -> np.ndarray:
    data = np.asarray(field).squeeze()
    while data.ndim > 2:
        data = data[0]
    if data.ndim != 2:
        raise ValueError(f"Expected 2D data after squeeze, got ndim={data.ndim}")
    return data


def resolve_var_name(available: set[str], logical_name: str) -> str | None:
    """Resolve exact or prefix-style ADIOS names such as hll_vx -> vx."""
    if logical_name in available:
        return logical_name

    candidates = [name for name in available if name.endswith(logical_name)]
    if not candidates:
        return None

    sep_candidates = [
        name for name in candidates if len(name) > len(logical_name) and name[-len(logical_name) - 1] == "_"
    ]
    if sep_candidates:
        candidates = sep_candidates

    return min(candidates, key=len)


def available_streamline_targets(variable_names: list[str]) -> list[str]:
    """Return streamline targets whose vector components are present."""
    available = set(variable_names)
    targets: list[str] = []
    for target_name, spec in STREAMLINE_TARGETS.items():
        u_name, v_name = spec["components"]
        if resolve_var_name(available, u_name) is not None and resolve_var_name(available, v_name) is not None:
            targets.append(target_name)
    return targets


def sanitize_limits(vmin: float, vmax: float) -> tuple[float, float]:
    if not np.isfinite(vmin) or not np.isfinite(vmax):
        return (0.0, 1.0)
    if vmin == vmax:
        delta = 1.0 if vmin == 0.0 else abs(vmin) * 1.0e-6
        return (vmin - delta, vmax + delta)
    return (vmin, vmax)


def downsample_finite_values(values: np.ndarray, max_points: int = 200000) -> np.ndarray:
    flat = np.asarray(values, dtype=float).reshape(-1)
    finite = flat[np.isfinite(flat)]
    if finite.size <= max_points:
        return finite
    step = max(1, int(np.ceil(finite.size / float(max_points))))
    return finite[::step][:max_points]


def display_limits_for_arrays(variable_name: str, arrays: list[np.ndarray]) -> tuple[float, float]:
    samples: list[np.ndarray] = []
    fallback_min = np.inf
    fallback_max = -np.inf

    for array in arrays:
        try:
            frame = to_2d(array)
        except ValueError:
            continue
        finite = downsample_finite_values(frame)
        if finite.size == 0:
            continue
        samples.append(finite)
        fallback_min = min(fallback_min, float(np.nanmin(finite)))
        fallback_max = max(fallback_max, float(np.nanmax(finite)))

    fallback_min, fallback_max = sanitize_limits(float(fallback_min), float(fallback_max))
    if not samples:
        return (fallback_min, fallback_max)

    merged = downsample_finite_values(np.concatenate(samples))
    if merged.size < 2:
        return (fallback_min, fallback_max)

    if is_signed_variable(variable_name):
        sample_min = float(np.nanmin(merged))
        sample_max = float(np.nanmax(merged))
        if sample_min < 0.0 < sample_max:
            abs_cap = float(np.nanpercentile(np.abs(merged), RANGE_ANALYSIS_SIGNED_ABS_PERCENTILE))
            if np.isfinite(abs_cap) and abs_cap > 0.0:
                return sanitize_limits(-abs_cap, abs_cap)

    low = float(np.nanpercentile(merged, RANGE_ANALYSIS_PERCENTILE_LOW))
    high = float(np.nanpercentile(merged, RANGE_ANALYSIS_PERCENTILE_HIGH))
    return sanitize_limits(low, high)


def contour_levels_from_limits(vmin: float, vmax: float, level_count: int) -> np.ndarray | None:
    if level_count < 2 or not np.isfinite(vmin) or not np.isfinite(vmax) or vmin >= vmax:
        return None

    levels = np.linspace(float(vmin), float(vmax), int(level_count), dtype=float)
    levels = np.unique(levels)
    if levels.size < 2:
        return None
    return levels


def choose_cmap(variable_name: str, sequential_cmap: str, diverging_cmap: str) -> str:
    return diverging_cmap if is_signed_variable(variable_name) else sequential_cmap


def choose_norm(variable_name: str, vmin: float, vmax: float, mcolors):
    if is_signed_variable(variable_name) and vmin < 0.0 < vmax:
        return mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0.0, vmax=vmax)
    return mcolors.Normalize(vmin=vmin, vmax=vmax)


def render_array_to_png_bytes(
    array: np.ndarray,
    variable_name: str,
    title: str,
    vis_type: str,
    sequential_cmap: str,
    diverging_cmap: str,
    contour_level_count: int,
    contour_line_width: float,
    display_limits: tuple[float, float] | None,
    dpi: int,
    figure_size: tuple[float, float],
    plt,
    mcolors,
) -> bytes:
    """Render scalar, 1D, heatmap, contour, or heatmap_contour PNG bytes."""
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
        frame = to_2d(arr)
        if display_limits is None:
            vmin, vmax = sanitize_limits(float(np.nanmin(frame)), float(np.nanmax(frame)))
        else:
            vmin, vmax = display_limits

        cmap = choose_cmap(variable_name, sequential_cmap, diverging_cmap)
        norm = choose_norm(variable_name, vmin, vmax, mcolors)
        levels = contour_levels_from_limits(vmin, vmax, contour_level_count)
        render_kind = normalize_vis_type(vis_type)
        uses_diverging = is_signed_variable(variable_name) and vmin < 0.0 < vmax

        if render_kind == "contour":
            if levels is not None:
                ax.contour(frame, levels=levels, origin="lower", linewidths=contour_line_width, colors="black")
            else:
                ax.text(0.5, 0.5, "No contour levels", ha="center", va="center", transform=ax.transAxes)
        else:
            image = ax.imshow(frame, origin="lower", cmap=cmap, norm=norm, aspect="auto")
            if render_kind == "heatmap_contour" and levels is not None:
                overlay_color = "black" if uses_diverging else "white"
                ax.contour(
                    frame,
                    levels=levels,
                    origin="lower",
                    linewidths=contour_line_width,
                    colors=overlay_color,
                )
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


def read_streamline_arrays(
    bp_path: Path,
    target: str,
    step: int,
    all_steps: bool,
) -> tuple[list[int], list[np.ndarray], list[np.ndarray], list[np.ndarray], str]:
    """Read vector components plus background arrays for one streamline target."""
    target_name = normalize_streamline_target(target)
    if target_name is None:
        choices = ", ".join(sorted(STREAMLINE_TARGETS))
        raise MissingVariableError(f"Unknown streamline target {target!r}. Use one of: {choices}")

    available = set(discover_variables(bp_path))
    spec = STREAMLINE_TARGETS[target_name]
    u_logical, v_logical = spec["components"]
    u_var = resolve_var_name(available, str(u_logical))
    v_var = resolve_var_name(available, str(v_logical))
    if u_var is None or v_var is None:
        preview = ", ".join(sorted(available)[:20])
        raise MissingVariableError(
            f"Streamline target {target_name!r} requires variables {u_logical!r} and {v_logical!r} "
            f"in {bp_path}. Available variables include: {preview}"
        )

    steps, u_arrays = read_variable_arrays(bp_path, u_var, step, all_steps)
    v_steps, v_arrays = read_variable_arrays(bp_path, v_var, step, all_steps)
    if v_steps != steps:
        raise SystemExit(f"Step mismatch while reading {u_var!r} and {v_var!r} from {bp_path}")

    background_name = str(spec["background"])
    background_var = resolve_var_name(available, background_name)
    if background_var is not None:
        background_steps, background_arrays = read_variable_arrays(bp_path, background_var, step, all_steps)
        if background_steps != steps:
            raise SystemExit(f"Step mismatch while reading streamline background {background_var!r} from {bp_path}")
    else:
        background_name = str(spec["fallback_background"])
        background_arrays = []
        for u_array, v_array in zip(u_arrays, v_arrays, strict=True):
            u = to_2d(u_array)
            v = to_2d(v_array)
            if u.shape != v.shape:
                raise SystemExit(f"Shape mismatch while building {background_name}: {u.shape} vs {v.shape}")
            background_arrays.append(np.sqrt(u * u + v * v))

    return steps, u_arrays, v_arrays, background_arrays, background_name


def render_streamline_to_png_bytes(
    background: np.ndarray,
    background_name: str,
    u: np.ndarray,
    v: np.ndarray,
    title: str,
    sequential_cmap: str,
    diverging_cmap: str,
    display_limits: tuple[float, float],
    dpi: int,
    figure_size: tuple[float, float],
    plt,
    mcolors,
) -> bytes:
    """Render one streamline frame over a scalar background."""
    background2d = to_2d(background)
    u2d = to_2d(u)
    v2d = to_2d(v)
    if u2d.shape != v2d.shape or u2d.shape != background2d.shape:
        raise SystemExit(f"Streamline shape mismatch: background={background2d.shape}, u={u2d.shape}, v={v2d.shape}")

    vmin, vmax = display_limits
    cmap = choose_cmap(background_name, sequential_cmap, diverging_cmap)
    norm = choose_norm(background_name, vmin, vmax, mcolors)

    u_plot = np.nan_to_num(u2d, nan=0.0, posinf=0.0, neginf=0.0)
    v_plot = np.nan_to_num(v2d, nan=0.0, posinf=0.0, neginf=0.0)
    mag = np.sqrt(u_plot * u_plot + v_plot * v_plot)
    finite_mag = mag[np.isfinite(mag)]
    width: float | np.ndarray = STREAMLINE_LINEWIDTH
    if finite_mag.size >= 2:
        ref = float(np.nanpercentile(finite_mag, 95.0))
        if np.isfinite(ref) and ref > 0.0:
            width = STREAMLINE_LINEWIDTH * (0.5 + 0.5 * np.clip(mag / ref, 0.0, 1.0))

    ny, nx = u_plot.shape
    x = np.arange(nx, dtype=float)
    y = np.arange(ny, dtype=float)

    fig, ax = plt.subplots(figsize=figure_size, constrained_layout=True)
    image = ax.imshow(background2d, origin="lower", cmap=cmap, norm=norm, aspect="auto")
    try:
        ax.streamplot(
            x,
            y,
            u_plot,
            v_plot,
            density=STREAMLINE_DENSITY,
            linewidth=width,
            color="white",
            arrowsize=STREAMLINE_ARROWSIZE,
            minlength=0.08,
            maxlength=4.0,
            integration_direction="both",
            broken_streamlines=True,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] streamplot failed for {title}: {type(exc).__name__}: {exc}")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(title)
    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=dpi)
    plt.close(fig)
    return buffer.getvalue()


def render_streamline_images(
    dataset_name: str,
    variable: str,
    bp_path: Path,
    step: int,
    all_steps: bool,
    cmap: str,
    diverging_cmap: str,
    dpi: int,
    figure_size: tuple[float, float],
) -> tuple[list[int], list[bytes], str]:
    """Read vector components and render streamline PNG bytes."""
    _, plt, mcolors = import_runtime_modules()
    print(f"[info] reading streamline target: {bp_path}")
    steps, u_arrays, v_arrays, background_arrays, background_name = read_streamline_arrays(
        bp_path, variable, step, all_steps
    )
    display_limits = display_limits_for_arrays(background_name, background_arrays)

    images: list[bytes] = []
    for step_index, u_array, v_array, background in zip(steps, u_arrays, v_arrays, background_arrays, strict=True):
        title = f"{dataset_name}:{variable} step={step_index}"
        images.append(
            render_streamline_to_png_bytes(
                background=background,
                background_name=background_name,
                u=u_array,
                v=v_array,
                title=title,
                sequential_cmap=cmap,
                diverging_cmap=diverging_cmap,
                display_limits=display_limits,
                dpi=dpi,
                figure_size=figure_size,
                plt=plt,
                mcolors=mcolors,
            )
        )
    return steps, images, "streamlines"


def render_images(
    dataset_name: str,
    variable: str,
    bp_path: Path,
    step: int,
    all_steps: bool,
    vis_type: str,
    cmap: str,
    diverging_cmap: str,
    contour_level_count: int,
    contour_line_width: float,
    dpi: int,
    figure_size: tuple[float, float],
) -> tuple[list[int], list[bytes], str]:
    """Read one variable and render each selected step to PNG bytes."""
    resolved_vis_type = normalize_vis_type(vis_type)
    if resolved_vis_type == "streamlines":
        return render_streamline_images(
            dataset_name=dataset_name,
            variable=variable,
            bp_path=bp_path,
            step=step,
            all_steps=all_steps,
            cmap=cmap,
            diverging_cmap=diverging_cmap,
            dpi=dpi,
            figure_size=figure_size,
        )

    _, plt, mcolors = import_runtime_modules()
    print(f"[info] reading: {bp_path}")
    steps, arrays = read_variable_arrays(bp_path, variable, step, all_steps)

    first_array = np.asarray(arrays[0]).squeeze()
    if first_array.ndim == 0:
        if not all_steps or len(steps) == 1:
            steps, arrays = read_variable_arrays(bp_path, variable, step=0, all_steps=True)
        title = f"{dataset_name}:{variable} over steps"
        image = render_scalar_timeseries_to_png_bytes(steps, arrays, title, dpi, figure_size, plt)
        return steps, [image], "timeseries"

    display_limits = display_limits_for_arrays(variable, arrays) if first_array.ndim >= 2 else None

    images: list[bytes] = []
    for step_index, array in zip(steps, arrays, strict=True):
        title = f"{dataset_name}:{variable} step={step_index}"
        images.append(
            render_array_to_png_bytes(
                array=array,
                variable_name=variable,
                title=title,
                vis_type=resolved_vis_type,
                sequential_cmap=cmap,
                diverging_cmap=diverging_cmap,
                contour_level_count=contour_level_count,
                contour_line_width=contour_line_width,
                display_limits=display_limits,
                dpi=dpi,
                figure_size=figure_size,
                plt=plt,
                mcolors=mcolors,
            )
        )
    return steps, images, resolved_vis_type


def visualization_semantic_kwargs(variable: str, vis_type: str) -> dict[str, Any]:
    """Map one variable to the semantic arguments used by Manager.visualization()."""
    normalized_vis_type = normalize_vis_type(vis_type)
    if normalized_vis_type == "streamlines":
        return {"streamline_by": variable}
    if normalized_vis_type == "contour":
        return {"contour_by": variable}
    if normalized_vis_type == "heatmap_contour":
        return {"color_by": variable, "contour_by": variable}
    if normalized_vis_type in {"line_plot", "line-plot", "timeseries"}:
        return {"x_axis": "step", "y_axis": variable}
    return {"color_by": variable}


def visualization_name(variable: str, vis_type: str, explicit_name: str | None, variable_count: int) -> str:
    """Return a stable visualization name for replace=True reruns."""
    if explicit_name is not None:
        if variable_count > 1:
            raise SystemExit("--name can only be used with one --variable.")
        return explicit_name
    if normalize_vis_type(vis_type) == "streamlines":
        target_name = normalize_streamline_target(variable)
        if target_name is not None:
            return target_name
    return f"{variable}_{vis_type}".replace("/", "_").replace(" ", "_")


def visualization_sequence_name(dataset_name: str, name: str) -> str:
    """Return the full visualization sequence name used by Manager.visualization()."""
    return name if "/" in name else f"{dataset_name}/visualizations/{name}"


def _safe_relative_parts(path_text: str) -> list[str]:
    """Convert a logical campaign path into safe relative filesystem parts."""
    parts: list[str] = []
    for raw in str(path_text or "").replace("\\", "/").split("/"):
        part = raw.strip()
        if not part or part in {".", ".."}:
            continue
        parts.append(part)
    if not parts:
        raise SystemExit(f"Cannot build output path from empty logical path: {path_text!r}")
    return parts


def write_png_images(
    image_output_dir: Path,
    sequence_name: str,
    steps: list[int],
    images: list[bytes],
) -> list[Path]:
    """Write rendered PNG bytes to deterministic files and return absolute paths."""
    relative_dir = Path(*_safe_relative_parts(sequence_name))
    output_dir = image_output_dir / relative_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    step_values = steps if len(steps) == len(images) else list(range(len(images)))
    paths: list[Path] = []
    for step_value, image in zip(step_values, images, strict=True):
        path = output_dir / f"image.{int(step_value):06d}.png"
        path.write_bytes(image)
        paths.append(path)
    return paths


def add_legacy_image_sequence(
    manager,
    dataset_name: str,
    name: str,
    steps: list[int],
    images: list[bytes | Path],
    image_workdir: Path | None = None,
) -> None:
    """
    Register rendered images with Manager.image() when Manager.visualization() is unavailable.

    This preserves the old campaign behavior: images are named as children of
    <dataset>/visualizations/<name>, but no explicit visualization metadata
    tables are populated because this Manager implementation does not expose
    that API.
    """
    sequence_name = visualization_sequence_name(dataset_name, name)
    image_steps = steps if len(steps) == len(images) else list(range(len(images)))

    if image_workdir is not None:
        cwd = Path.cwd()
        os.chdir(image_workdir)
        try:
            for step_value, image in zip(image_steps, images, strict=True):
                logical_name = f"{sequence_name}/image.{int(step_value):06d}.png"
                manager.image(image, name=logical_name, store=False)
        finally:
            os.chdir(cwd)
        return

    with tempfile.TemporaryDirectory(prefix="hpc_campaign_visualization_") as tmpdir:
        tmp_path = Path(tmpdir)
        for step_value, image in zip(image_steps, images, strict=True):
            logical_name = f"{sequence_name}/image.{int(step_value):06d}.png"
            if isinstance(image, (bytes, bytearray)):
                image_path = tmp_path / f"image.{int(step_value):06d}.png"
                image_path.write_bytes(image)
            else:
                image_path = Path(image)
            manager.image(image_path, name=logical_name, store=True)


def add_visualization(
    manager,
    dataset_name: str,
    variable: str,
    vis_type: str,
    name: str,
    steps: list[int],
    images: list[bytes | Path],
    replace: bool,
    thumbnail_step: int,
    image_workdir: Path | None = None,
) -> int | None:
    """Store or register rendered images with new visualization metadata."""
    if not images:
        raise SystemExit("No images were rendered.")
    if thumbnail_step < 0 or thumbnail_step >= len(images):
        raise SystemExit(f"--thumbnailStep must be in [0, {len(images) - 1}]")

    image_steps = steps if len(steps) == len(images) else None

    if not hasattr(manager, "visualization"):
        add_legacy_image_sequence(
            manager=manager,
            dataset_name=dataset_name,
            name=name,
            steps=steps,
            images=images,
            image_workdir=image_workdir,
        )
        return None

    cwd = Path.cwd()
    if image_workdir is not None:
        os.chdir(image_workdir)
    try:
        return manager.visualization(
            images=images,
            kind=vis_type,
            source_dataset=dataset_name,
            name=name,
            steps=image_steps,
            thumbnail_image=thumbnail_step,
            store=False,
            metadata={
                "generated_by": Path(__file__).name,
                "steps": steps,
                "external_images": image_workdir is not None,
            },
            replace=replace,
            **visualization_semantic_kwargs(variable, vis_type),
        )
    finally:
        if image_workdir is not None:
            os.chdir(cwd)


def prepare_visualization_images(
    rendered_images: list[bytes],
    steps: list[int],
    dataset_name: str,
    name: str,
    image_output_dir: Path | None,
    external_images: bool,
) -> tuple[list[bytes | Path], Path | None]:
    """Optionally write PNGs and return the inputs for Manager.visualization()."""
    if image_output_dir is None:
        return (rendered_images, None)

    sequence_name = visualization_sequence_name(dataset_name, name)
    written_paths = write_png_images(image_output_dir, sequence_name, steps, rendered_images)
    print(f"[ok] wrote PNG files: {written_paths[0].parent}")

    if not external_images:
        return (rendered_images, None)

    relative_paths = [path.relative_to(image_output_dir) for path in written_paths]
    return (relative_paths, image_output_dir)


def selected_dataset_variables(
    bp_path: Path,
    requested_variables: list[str],
    all_variables: bool,
    vis_type: str,
) -> list[str]:
    """Return variables or streamline targets to render for one dataset."""
    if not all_variables:
        return requested_variables

    discovered = discover_variables(bp_path)
    if normalize_vis_type(vis_type) != "streamlines":
        return discovered

    targets = available_streamline_targets(discovered)
    if not targets:
        raise MissingVariableError(f"No streamline targets found in {bp_path}")
    return targets


def main() -> int:
    args = parse_args()

    exact_names = split_csv(args.dataset)
    patterns = split_csv(args.datasetPattern)
    variables = split_csv(args.variable)

    data_root = args.data_root.expanduser().resolve() if args.data_root is not None else None
    image_output_dir = args.imageOutputDir.expanduser().resolve() if args.imageOutputDir is not None else None
    if args.externalImages and image_output_dir is None:
        raise SystemExit("--externalImages requires --imageOutputDir.")
    if image_output_dir is not None and not args.dryRun:
        image_output_dir.mkdir(parents=True, exist_ok=True)

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
        print(f"[info] visType  : {normalize_vis_type(args.visType)}")

        directory_map = build_directory_map(info_data)
        for dataset in datasets:
            bp_path = resolve_dataset_path(dataset, data_root, directory_map)
            print(f"[info] dataset path: {bp_path}")
            try:
                dataset_variables = selected_dataset_variables(bp_path, variables, args.allVariables, args.visType)
            except MissingVariableError as exc:
                if args.skipMissing:
                    print(f"[warn] skipped dataset: {exc}")
                    skipped += 1
                    continue
                raise SystemExit(str(exc)) from exc

            if args.dryRun:
                for variable in dataset_variables:
                    name = visualization_name(
                        variable,
                        normalize_vis_type(args.visType),
                        args.name,
                        len(dataset_variables),
                    )
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
                        diverging_cmap=args.divergingCmap,
                        contour_level_count=args.contourLevels,
                        contour_line_width=args.contourLineWidth,
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
                visualization_images, image_workdir = prepare_visualization_images(
                    rendered_images=images,
                    steps=steps,
                    dataset_name=dataset.name,
                    name=name,
                    image_output_dir=image_output_dir,
                    external_images=bool(args.externalImages),
                )

                visid = add_visualization(
                    manager=manager,
                    dataset_name=dataset.name,
                    variable=variable,
                    vis_type=resolved_vis_type,
                    name=name,
                    steps=steps,
                    images=visualization_images,
                    replace=args.replace,
                    thumbnail_step=args.thumbnailStep,
                    image_workdir=image_workdir,
                )
                sequence_name = visualization_sequence_name(dataset.name, name)
                if visid is None:
                    print(f"[ok] added image sequence name={sequence_name} images={len(images)}")
                else:
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
