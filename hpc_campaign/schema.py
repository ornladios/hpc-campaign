import fnmatch
import re
from typing import Any, Mapping, Sequence


class SchemaInterpretationError(ValueError):
    """Raised when a schema cannot be interpreted against available datasets."""


def interpret_schema_layout(
    schema: Mapping[str, Any],
    datasets: Sequence[str],
    timeseries: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """
    Interpret a minimal ingestion schema against known campaign dataset names.

    This function resolves file groups, append vs file-per-timestep mode,
    timestep extraction, group-level associations, and time references
    normalized onto time-series groups. It intentionally does not open
    ADIOS/HDF5 data.
    """
    schema_version = _schema_version(schema)
    if schema_version != 1:
        raise SchemaInterpretationError(f"Unsupported schema_version={schema_version}; expected 1")

    files = _mapping(schema.get("files"), "files")
    dataset_names = [str(name) for name in datasets]
    timeseries_map = {str(name): [str(dataset) for dataset in values] for name, values in (timeseries or {}).items()}

    file_groups: dict[str, dict[str, Any]] = {}
    for group_name, raw_group in files.items():
        group_key = str(group_name)
        group = _mapping(raw_group, f"files.{group_key}")
        file_groups[group_key] = _interpret_file_group(group_key, group, files, dataset_names, timeseries_map)

    _apply_root_time(schema.get("time"), file_groups)

    return {
        "schema_version": schema_version,
        "schema_name": str(schema.get("name", "") or ""),
        "file_groups": file_groups,
    }


def interpret_campaign_schema_layout(
    schema: Mapping[str, Any],
    datasets: Sequence[str],
    timeseries: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """
    Interpret a schema against campaign dataset names.

    First try the schema at campaign root. If it does not match root datasets,
    apply the same schema independently to each immediate child directory by
    resolving schema paths relative to that directory.
    """
    dataset_names = [str(name) for name in datasets]
    timeseries_map = {str(name): [str(dataset) for dataset in values] for name, values in (timeseries or {}).items()}

    try:
        layout = interpret_schema_layout(schema, dataset_names, timeseries_map)
        layout["scope"] = ""
        return layout
    except SchemaInterpretationError as root_error:
        prefixes = _immediate_child_prefixes(dataset_names)
        if not prefixes:
            raise root_error

    instances = {}
    for prefix in prefixes:
        scoped_datasets = _strip_scope_prefix(prefix, dataset_names)
        scoped_timeseries = _strip_timeseries_scope_prefix(prefix, timeseries_map)
        try:
            scoped_layout = interpret_schema_layout(schema, scoped_datasets, scoped_timeseries)
        except SchemaInterpretationError as exc:
            raise SchemaInterpretationError(f"{prefix}: {exc}") from exc
        instances[prefix] = _prefix_layout_datasets(prefix, scoped_layout)

    return {
        "schema_version": _schema_version(schema),
        "schema_name": str(schema.get("name", "") or ""),
        "instances": instances,
    }


def _immediate_child_prefixes(dataset_names: Sequence[str]) -> list[str]:
    prefixes = {name.split("/", 1)[0] for name in dataset_names if "/" in name and name.split("/", 1)[0]}
    return sorted(prefixes)


def _strip_scope_prefix(prefix: str, dataset_names: Sequence[str]) -> list[str]:
    prefix_slash = f"{prefix}/"
    return [name[len(prefix_slash) :] for name in dataset_names if name.startswith(prefix_slash)]


def _strip_timeseries_scope_prefix(
    prefix: str,
    timeseries: Mapping[str, Sequence[str]],
) -> dict[str, list[str]]:
    prefix_slash = f"{prefix}/"
    scoped = {}
    for name, datasets in timeseries.items():
        scoped_datasets = [dataset[len(prefix_slash) :] for dataset in datasets if dataset.startswith(prefix_slash)]
        if not scoped_datasets:
            continue
        scoped_name = name[len(prefix_slash) :] if name.startswith(prefix_slash) else name
        scoped[scoped_name] = scoped_datasets
    return scoped


def _prefix_layout_datasets(prefix: str, layout: Mapping[str, Any]) -> dict[str, Any]:
    prefixed_layout = dict(layout)
    prefixed_layout["scope"] = prefix
    file_groups = {}
    for group_name, group in layout.get("file_groups", {}).items():
        prefixed_group = dict(group)
        prefixed_group["datasets"] = [f"{prefix}/{dataset}" for dataset in group.get("datasets", [])]
        file_groups[group_name] = prefixed_group
    prefixed_layout["file_groups"] = file_groups
    return prefixed_layout


def _schema_version(schema: Mapping[str, Any]) -> int:
    try:
        return int(schema.get("schema_version", 0))
    except Exception as exc:
        raise SchemaInterpretationError("schema_version must be an integer") from exc


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SchemaInterpretationError(f"{field_name} must be a mapping")
    return value


def _nonempty_string(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SchemaInterpretationError(f"{field_name} is required")
    return text


def _interpret_file_group(
    group_name: str,
    group: Mapping[str, Any],
    all_groups: Mapping[str, Any],
    dataset_names: Sequence[str],
    timeseries: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    role = _nonempty_string(group.get("role"), f"files.{group_name}.role")
    if role not in {"static", "time_series"}:
        raise SchemaInterpretationError(f"Unsupported files.{group_name}.role={role!r}")

    associations = _interpret_associations(group_name, group.get("associations", {}), all_groups)

    result: dict[str, Any]
    if role == "static":
        if "time" in group:
            raise SchemaInterpretationError(f"files.{group_name}.time is only valid for time_series groups")
        result = {
            "role": role,
            "mode": "none",
            "datasets": _resolve_static_datasets(group_name, group, dataset_names),
        }
    else:
        mode = _nonempty_string(group.get("mode"), f"files.{group_name}.mode")
        if mode == "append":
            result = {
                "role": role,
                "mode": mode,
                "datasets": [_resolve_path_dataset(group_name, group, dataset_names)],
            }
        elif mode == "file_per_timestep":
            datasets = _resolve_file_per_timestep_datasets(group_name, group, dataset_names, timeseries)
            result = {
                "role": role,
                "mode": mode,
                "datasets": datasets,
                "step_indices": _extract_step_indices(group_name, group, datasets),
            }
        else:
            raise SchemaInterpretationError(f"Unsupported files.{group_name}.mode={mode!r}")

    if associations:
        result["associations"] = associations
    if "time" in group:
        result["time"] = _interpret_group_time(group.get("time"), f"files.{group_name}.time")
    return result


def _resolve_static_datasets(group_name: str, group: Mapping[str, Any], dataset_names: Sequence[str]) -> list[str]:
    if group.get("path"):
        return [_resolve_path_dataset(group_name, group, dataset_names)]
    pattern = _nonempty_string(group.get("pattern"), f"files.{group_name}.path or files.{group_name}.pattern")
    matches = sorted(name for name in dataset_names if fnmatch.fnmatch(name, pattern))
    if not matches:
        raise SchemaInterpretationError(f"files.{group_name}.pattern matched no datasets: {pattern}")
    return matches


def _resolve_path_dataset(group_name: str, group: Mapping[str, Any], dataset_names: Sequence[str]) -> str:
    path = _nonempty_string(group.get("path"), f"files.{group_name}.path")
    if path not in dataset_names:
        raise SchemaInterpretationError(f"files.{group_name}.path does not match a dataset: {path}")
    return path


def _resolve_file_per_timestep_datasets(
    group_name: str,
    group: Mapping[str, Any],
    dataset_names: Sequence[str],
    timeseries: Mapping[str, Sequence[str]],
) -> list[str]:
    pattern = _nonempty_string(group.get("pattern"), f"files.{group_name}.pattern")
    matches = {name for name in dataset_names if fnmatch.fnmatch(name, pattern)}
    if not matches:
        raise SchemaInterpretationError(f"files.{group_name}.pattern matched no datasets: {pattern}")

    if group_name not in timeseries:
        return sorted(matches)

    ordered = []
    for dataset in timeseries[group_name]:
        if dataset not in dataset_names:
            raise SchemaInterpretationError(f"timeseries.{group_name} references missing dataset: {dataset}")
        if dataset not in matches:
            raise SchemaInterpretationError(
                f"timeseries.{group_name} dataset does not match files.{group_name}.pattern: {dataset}"
            )
        ordered.append(dataset)
    if not ordered:
        raise SchemaInterpretationError(f"timeseries.{group_name} is empty")
    return ordered


def _extract_step_indices(group_name: str, group: Mapping[str, Any], datasets: Sequence[str]) -> list[int]:
    pattern = _nonempty_string(group.get("step_from_filename"), f"files.{group_name}.step_from_filename")
    try:
        regex = re.compile(pattern)
    except re.error as exc:
        raise SchemaInterpretationError(f"Invalid files.{group_name}.step_from_filename regex: {exc}") from exc

    steps = []
    for dataset in datasets:
        match = regex.search(dataset)
        if match is None:
            raise SchemaInterpretationError(f"files.{group_name}.step_from_filename did not match dataset: {dataset}")
        if not match.groups():
            raise SchemaInterpretationError(f"files.{group_name}.step_from_filename must capture a step number")
        try:
            steps.append(int(match.group(1)))
        except Exception as exc:
            raise SchemaInterpretationError(
                f"files.{group_name}.step_from_filename captured a non-integer step for {dataset}: {match.group(1)}"
            ) from exc
    return steps


def _interpret_associations(
    group_name: str,
    associations: Any,
    all_groups: Mapping[str, Any],
) -> dict[str, str]:
    if associations in (None, {}):
        return {}
    assoc_map = _mapping(associations, f"files.{group_name}.associations")
    result = {}
    for role, target in assoc_map.items():
        target_group = _nonempty_string(target, f"files.{group_name}.associations.{role}")
        if target_group not in all_groups:
            raise SchemaInterpretationError(
                f"files.{group_name}.associations.{role} references unknown group: {target_group}"
            )
        result[str(role)] = target_group
    return result


def _interpret_time_fields(time_spec: Any, field_name: str) -> dict[str, str]:
    time_map = _mapping(time_spec, field_name)
    variable = str(time_map.get("variable", "") or "").strip()
    index = str(time_map.get("index", "") or "").strip()
    has_variable = bool(variable)
    has_index = bool(index)
    if has_variable == has_index:
        raise SchemaInterpretationError(f"{field_name} requires exactly one of variable or index")

    if has_variable:
        return {"variable": variable}
    return {"index": index}


def _interpret_group_time(time_spec: Any, field_name: str) -> dict[str, str]:
    time_map = _mapping(time_spec, field_name)
    if "file" in time_map:
        raise SchemaInterpretationError(f"{field_name}.file is not supported; file group is implicit")
    return _interpret_time_fields(time_map, field_name)


def _interpret_root_time(time_spec: Any, file_groups: Mapping[str, dict[str, Any]]) -> dict[str, str]:
    time_map = _mapping(time_spec, "time")
    result = _interpret_time_fields(time_map, "time")
    if "file" in time_map:
        file_group = _nonempty_string(time_map.get("file"), "time.file")
        group = file_groups.get(file_group)
        if group is None:
            raise SchemaInterpretationError(f"time.file references unknown group: {file_group}")
        if group.get("role") != "time_series":
            raise SchemaInterpretationError(f"time.file references non-time_series group: {file_group}")
        result["file"] = file_group
    return result


def _apply_root_time(time_spec: Any, file_groups: dict[str, dict[str, Any]]) -> None:
    if time_spec in (None, {}):
        return

    root_time = _interpret_root_time(time_spec, file_groups)
    root_group = root_time.get("file", "")
    group_time = {key: value for key, value in root_time.items() if key != "file"}

    if root_group:
        file_groups[root_group].setdefault("time", dict(group_time))
        return

    for group in file_groups.values():
        if group.get("role") == "time_series":
            group.setdefault("time", dict(group_time))
